"""
Subscription manager for coordinating catch-up and live event subscriptions.

The SubscriptionManager provides a unified API for:
- Registering subscribers (projections)
- Coordinating catch-up from event store
- Transitioning to live events from event bus
- Managing subscription lifecycle
- Graceful shutdown with signal handling
- Comprehensive error handling and health monitoring

Example:
    >>> from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig
    >>>
    >>> manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
    >>> await manager.subscribe(my_projection)
    >>> await manager.start()
    >>> # ... projection now receives events ...
    >>> await manager.stop()

Daemon-style operation with signal handling:
    >>> manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
    >>> await manager.subscribe(my_projection)
    >>> await manager.run_until_shutdown()
    >>> # Runs until SIGTERM/SIGINT, then gracefully shuts down

Error Handling:
    >>> # Register error callbacks
    >>> async def alert_on_error(error_info):
    ...     await send_alert(error_info)
    >>> manager.on_error(alert_on_error)
    >>> manager.on_critical_error(send_pager_alert)
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_SUBSCRIPTION_NAME
from eventsource.subscriptions.config import SubscriptionConfig
from eventsource.subscriptions.error_handling import (
    ErrorCallback,
    ErrorCategory,
    ErrorHandlingConfig,
    ErrorSeverity,
    SubscriptionErrorHandler,
)
from eventsource.subscriptions.health import (
    HealthCheckConfig,
    HealthCheckResult,
    LivenessStatus,
    ManagerHealth,
    ReadinessStatus,
    SubscriptionHealth,
)
from eventsource.subscriptions.health_provider import HealthCheckProvider
from eventsource.subscriptions.lifecycle import SubscriptionLifecycleManager
from eventsource.subscriptions.pause_resume import PauseResumeController
from eventsource.subscriptions.registry import SubscriptionRegistry
from eventsource.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownResult,
)
from eventsource.subscriptions.subscription import (
    PauseReason,
    Subscription,
    SubscriptionStatus,
)

if TYPE_CHECKING:
    from eventsource.bus.interface import EventBus
    from eventsource.protocols import EventSubscriber
    from eventsource.repositories.checkpoint import CheckpointRepository
    from eventsource.repositories.dlq import DLQRepository
    from eventsource.stores.interface import EventStore

logger = logging.getLogger(__name__)


class SubscriptionManager:
    """
    Manages catch-up and live event subscriptions.

    The SubscriptionManager coordinates between the event store (for historical
    events) and the event bus (for live events), providing a seamless subscription
    experience for projections.

    This class follows the Composition pattern, delegating to specialized
    components for specific responsibilities:
    - SubscriptionRegistry: Subscription CRUD operations
    - SubscriptionLifecycleManager: Start/stop operations
    - HealthCheckProvider: Health monitoring and probes
    - PauseResumeController: Pause/resume operations

    Example:
        >>> from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig
        >>>
        >>> manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
        >>> await manager.subscribe(my_projection)
        >>> await manager.start()
        >>> # ... projection now receives events ...
        >>> await manager.stop()

    Attributes:
        event_store: Event store for historical event retrieval
        event_bus: Event bus for live event subscription
        checkpoint_repo: Repository for checkpoint persistence
    """

    def __init__(
        self,
        event_store: "EventStore",
        event_bus: "EventBus",
        checkpoint_repo: "CheckpointRepository",
        shutdown_timeout: float = 30.0,
        drain_timeout: float = 10.0,
        dlq_repo: "DLQRepository | None" = None,
        error_handling_config: ErrorHandlingConfig | None = None,
        health_check_config: HealthCheckConfig | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the subscription manager.

        Args:
            event_store: Event store for historical events
            event_bus: Event bus for live events
            checkpoint_repo: Checkpoint repository for position tracking
            shutdown_timeout: Total shutdown timeout in seconds
            drain_timeout: Time to wait for in-flight events during shutdown
            dlq_repo: Optional DLQ repository for dead letter handling
            error_handling_config: Configuration for error handling behavior
            health_check_config: Configuration for health check thresholds
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing (default True).
                          Ignored if tracer is explicitly provided.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self.event_store = event_store
        self.event_bus = event_bus
        self.checkpoint_repo = checkpoint_repo
        self._dlq_repo = dlq_repo
        self._running = False

        # Configuration
        self._error_handling_config = error_handling_config or ErrorHandlingConfig()
        self._health_check_config = health_check_config or HealthCheckConfig()

        # Error handlers (managed by manager, shared with health provider)
        self._error_handlers: dict[str, SubscriptionErrorHandler] = {}
        self._global_error_callbacks: list[ErrorCallback] = []

        # Composed components
        self._registry = SubscriptionRegistry()
        self._lifecycle = SubscriptionLifecycleManager(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=enable_tracing,
        )
        self._health_provider = HealthCheckProvider(
            registry=self._registry,
            error_handlers=self._error_handlers,
            config=self._health_check_config,
            lock=self._registry.lock,
        )
        self._pause_resume = PauseResumeController(
            registry=self._registry,
            lifecycle=self._lifecycle,
            enable_tracing=enable_tracing,
        )

        # Shutdown coordination
        self._shutdown_coordinator = ShutdownCoordinator(
            timeout=shutdown_timeout,
            drain_timeout=drain_timeout,
        )
        self._last_shutdown_result: ShutdownResult | None = None

        # Manager tracking
        self._created_at = datetime.now(UTC)
        self._started_at: datetime | None = None

    # =========================================================================
    # Subscription Management (delegates to SubscriptionRegistry)
    # =========================================================================

    async def subscribe(
        self,
        subscriber: "EventSubscriber",
        config: SubscriptionConfig | None = None,
        name: str | None = None,
    ) -> Subscription:
        """
        Register a subscriber for event delivery.

        Creates a subscription that will receive events from both the
        event store (historical) and event bus (live).

        Args:
            subscriber: The event subscriber (e.g., projection) to register
            config: Optional configuration for this subscription
            name: Optional custom name for the subscription. If not provided,
                  uses the subscriber's class name.

        Returns:
            The created Subscription object for monitoring

        Raises:
            SubscriptionAlreadyExistsError: If a subscription with this name exists

        Example:
            >>> subscription = await manager.subscribe(
            ...     my_projection,
            ...     SubscriptionConfig(start_from="beginning", batch_size=500)
            ... )
        """
        subscription_name = name or subscriber.__class__.__name__

        with self._tracer.span(
            "eventsource.subscription_manager.subscribe",
            {ATTR_SUBSCRIPTION_NAME: subscription_name},
        ):
            subscription = await self._registry.register(subscriber, config, name)

            # Create error handler for this subscription
            error_handler = SubscriptionErrorHandler(
                subscription_name=subscription_name,
                config=self._error_handling_config,
                dlq_repo=self._dlq_repo,
            )

            # Register global callbacks on the subscription handler
            for callback in self._global_error_callbacks:
                error_handler.on_error(callback)

            self._error_handlers[subscription_name] = error_handler

            # Register with health provider
            self._health_provider.register_subscription(subscription, error_handler)

            return subscription

    async def unsubscribe(self, name: str) -> bool:
        """
        Unregister a subscription by name.

        If the subscription is running, it will be stopped first.

        Args:
            name: The subscription name to remove

        Returns:
            True if the subscription was found and removed, False otherwise
        """
        with self._tracer.span(
            "eventsource.subscription_manager.unsubscribe",
            {ATTR_SUBSCRIPTION_NAME: name},
        ):
            # Stop coordinator if running
            coordinator = self._lifecycle.remove_coordinator(name)
            if coordinator:
                await coordinator.stop()

            # Unregister from registry
            subscription = await self._registry.unregister(name)
            if subscription is None:
                return False

            # Clean up error handler and health checker
            self._error_handlers.pop(name, None)
            self._health_provider.unregister_subscription(name)

            return True

    @property
    def subscriptions(self) -> list[Subscription]:
        """Get all registered subscriptions."""
        return self._registry.get_all()

    @property
    def subscription_names(self) -> list[str]:
        """Get all registered subscription names."""
        return self._registry.get_names()

    @property
    def subscription_count(self) -> int:
        """Get the number of registered subscriptions."""
        return len(self._registry)

    def get_subscription(self, name: str) -> Subscription | None:
        """
        Get a subscription by name.

        Args:
            name: The subscription name

        Returns:
            The Subscription, or None if not found
        """
        return self._registry.get(name)

    def get_all_statuses(self) -> dict[str, SubscriptionStatus]:
        """
        Get status snapshots of all subscriptions.

        Returns a dictionary mapping subscription names to their
        SubscriptionStatus objects.

        Returns:
            Dictionary of subscription name to SubscriptionStatus
        """
        return self._registry.get_statuses()

    # =========================================================================
    # Lifecycle Management (delegates to SubscriptionLifecycleManager)
    # =========================================================================

    async def start(
        self,
        subscription_names: list[str] | None = None,
        concurrent: bool = True,
    ) -> dict[str, Exception | None]:
        """
        Start registered subscriptions concurrently.

        For each subscription:
        1. Resolves the starting position
        2. Performs catch-up from event store
        3. Transitions to live events from event bus

        Subscriptions are started in parallel by default for optimal startup time.
        Each subscription is isolated - a failure in one subscription does not
        prevent others from starting successfully.

        Args:
            subscription_names: Optional list of subscription names to start.
                              If None, starts all registered subscriptions.
            concurrent: If True (default), start subscriptions concurrently.
                       If False, start sequentially (legacy behavior).

        Returns:
            Dictionary mapping subscription names to None (success) or Exception
            (failure). Returns empty dict if manager was already running.

        Example:
            >>> results = await manager.start()
            >>> failures = {k: v for k, v in results.items() if v is not None}
            >>> if failures:
            ...     logger.warning(f"Some subscriptions failed: {failures}")
        """
        if self._running:
            logger.warning("Subscription manager already running")
            return {}

        self._running = True
        self._started_at = datetime.now(UTC)
        self._health_provider.set_started(self._started_at)

        # Determine which subscriptions to start
        if subscription_names is None:
            subscriptions_to_start = self._registry.get_all()
        else:
            subscriptions_to_start = [
                sub for sub in self._registry.get_all() if sub.name in subscription_names
            ]

        logger.info(
            "Starting subscription manager",
            extra={
                "subscription_count": len(subscriptions_to_start),
                "concurrent": concurrent,
            },
        )

        return await self._lifecycle.start_all(subscriptions_to_start, concurrent)

    async def stop(
        self,
        timeout: float = 30.0,
        subscription_names: list[str] | None = None,
    ) -> None:
        """
        Stop subscriptions gracefully.

        Waits for in-flight events to complete and saves checkpoints.

        Args:
            timeout: Maximum seconds to wait for graceful shutdown
            subscription_names: Optional list of subscription names to stop.
                              If None, stops all subscriptions.
        """
        with self._tracer.span(
            "eventsource.subscription_manager.stop",
        ):
            if not self._running and subscription_names is None:
                return

            # Determine which subscriptions to stop
            if subscription_names is None:
                subscriptions_to_stop = self._registry.get_all()
                self._running = False
                self._health_provider.clear_started()
            else:
                subscriptions_to_stop = [
                    sub for sub in self._registry.get_all() if sub.name in subscription_names
                ]

            logger.info(
                "Stopping subscription manager",
                extra={
                    "subscription_count": len(subscriptions_to_stop),
                    "timeout": timeout,
                },
            )

            await self._lifecycle.stop_all(subscriptions_to_stop, timeout)

            logger.info("Subscription manager stopped")

    @property
    def is_running(self) -> bool:
        """Check if the manager is running."""
        return self._running

    # =========================================================================
    # Legacy Health Methods (delegates to HealthCheckProvider)
    # =========================================================================

    def get_health(self) -> dict[str, Any]:
        """
        Get health status of all subscriptions.

        Returns a dictionary with overall health and individual
        subscription statuses.

        Returns:
            Dictionary with health information
        """
        subscription_statuses = []
        has_errors = False
        all_live = True

        for subscription in self._registry.get_all():
            status = subscription.get_status()
            subscription_statuses.append(status.to_dict())

            if status.state == "error":
                has_errors = True
                all_live = False
            elif status.state != "live":
                all_live = False

        # Determine overall health
        if has_errors:
            overall_status = "unhealthy"
        elif all_live and self._running:
            overall_status = "healthy"
        elif self._running:
            overall_status = "starting"
        else:
            overall_status = "stopped"

        return {
            "status": overall_status,
            "running": self._running,
            "subscription_count": len(self._registry),
            "subscriptions": subscription_statuses,
        }

    # =========================================================================
    # Signal Handling and Shutdown
    # =========================================================================

    def register_signals(self) -> None:
        """
        Register signal handlers for graceful shutdown.

        Registers handlers for SIGTERM and SIGINT. When a signal is
        received, the manager will initiate graceful shutdown.

        Note:
            Call this before run_until_shutdown() or when running
            the manager manually and wanting signal-based shutdown.

        Example:
            >>> manager.register_signals()
            >>> await manager.start()
            >>> # Now SIGTERM/SIGINT will trigger graceful shutdown
        """
        self._shutdown_coordinator.register_signals()
        self._shutdown_coordinator.on_shutdown(self._on_shutdown_signal)

    def unregister_signals(self) -> None:
        """
        Unregister signal handlers.

        Removes the signal handlers previously registered with
        register_signals().
        """
        self._shutdown_coordinator.unregister_signals()

    async def _on_shutdown_signal(self) -> None:
        """Handle shutdown signal by stopping the manager."""
        logger.info(
            "Shutdown signal received, stopping manager",
            extra={"subscription_count": len(self._registry)},
        )

    async def run_until_shutdown(
        self,
        shutdown_timeout: float | None = None,
    ) -> ShutdownResult:
        """
        Run the manager until a shutdown signal is received.

        This is the main entry point for daemon-style operation. It:
        1. Registers signal handlers (SIGTERM, SIGINT)
        2. Starts all registered subscriptions
        3. Blocks until a shutdown signal is received
        4. Executes graceful shutdown with the configured timeout

        Args:
            shutdown_timeout: Override the default shutdown timeout.
                            If None, uses the manager's configured timeout.

        Returns:
            ShutdownResult with shutdown details

        Raises:
            SubscriptionError: If starting fails

        Example:
            >>> manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
            >>> await manager.subscribe(my_projection)
            >>> result = await manager.run_until_shutdown()
            >>> if result.forced:
            ...     logger.warning("Shutdown was forced")

        Note:
            This method does not return until shutdown is complete.
            It handles SIGTERM and SIGINT for graceful shutdown.
        """
        # Update shutdown timeout if provided
        if shutdown_timeout is not None:
            self._shutdown_coordinator.timeout = shutdown_timeout

        logger.info(
            "Starting manager in daemon mode",
            extra={
                "subscription_count": len(self._registry),
                "shutdown_timeout": self._shutdown_coordinator.timeout,
            },
        )

        # Register signal handlers
        self.register_signals()

        try:
            # Start subscriptions
            await self.start()

            logger.info(
                "Manager running, waiting for shutdown signal",
                extra={"subscriptions": self._registry.get_names()},
            )

            # Wait for shutdown signal
            await self._shutdown_coordinator.wait_for_shutdown()

            # Execute graceful shutdown
            result = await self._execute_graceful_shutdown()
            self._last_shutdown_result = result
            return result

        except Exception as e:
            logger.error(
                "Error during run_until_shutdown",
                extra={"error": str(e)},
                exc_info=True,
            )
            # Attempt cleanup
            await self.stop()
            raise
        finally:
            # Unregister signal handlers
            self.unregister_signals()

    async def _execute_graceful_shutdown(self) -> ShutdownResult:
        """
        Execute graceful shutdown sequence.

        Stops accepting events, drains in-flight events, saves checkpoints,
        and closes connections.

        Returns:
            ShutdownResult with shutdown details
        """
        return await self._shutdown_coordinator.shutdown(
            stop_func=self._stop_accepting_events,
            drain_func=self._drain_in_flight_events,
            checkpoint_func=self._save_final_checkpoints,
        )

    async def _stop_accepting_events(self) -> None:
        """Stop accepting new events by stopping all coordinators."""
        logger.info(
            "Stopping event acceptance",
            extra={"subscription_count": len(self._lifecycle.coordinators)},
        )
        await self.stop()

    async def _drain_in_flight_events(self) -> int:
        """
        Drain in-flight events from all subscriptions.

        Waits for all active event handlers to complete, up to drain_timeout.
        Each subscription's FlowController is queried for in-flight events,
        and we wait for them to complete or timeout.

        Returns:
            Number of events that were in-flight at drain start.
            This helps track how many events were being processed when
            shutdown was initiated.
        """
        total_in_flight = 0
        drain_tasks: list[tuple[str, asyncio.Task[int]]] = []

        # Collect all FlowControllers and their current in-flight counts
        for name, coordinator in self._lifecycle.coordinators.items():
            flow_controller = coordinator.flow_controller
            if flow_controller is None:
                logger.debug(
                    "No flow controller for subscription",
                    extra={"subscription": name},
                )
                continue

            in_flight = flow_controller.in_flight
            total_in_flight += in_flight

            if in_flight > 0:
                logger.info(
                    "Draining in-flight events for subscription",
                    extra={
                        "subscription": name,
                        "in_flight_count": in_flight,
                    },
                )

                # Create drain task with timeout from shutdown coordinator
                drain_task = asyncio.create_task(
                    flow_controller.wait_for_drain(self._shutdown_coordinator.drain_timeout),
                    name=f"drain-{name}",
                )
                drain_tasks.append((name, drain_task))

        logger.info(
            "Draining in-flight events",
            extra={
                "total_in_flight": total_in_flight,
                "subscriptions_with_events": len(drain_tasks),
            },
        )

        if not drain_tasks:
            return total_in_flight

        # Wait for all drain tasks to complete
        try:
            results = await asyncio.gather(
                *(task for _, task in drain_tasks),
                return_exceptions=True,
            )

            # Log results per subscription
            total_remaining = 0
            for (name, _), result in zip(drain_tasks, results, strict=True):
                if isinstance(result, BaseException):
                    logger.error(
                        "Drain task failed",
                        extra={
                            "subscription": name,
                            "error": str(result),
                        },
                    )
                elif result > 0:
                    total_remaining += result
                    logger.warning(
                        "Subscription did not fully drain",
                        extra={
                            "subscription": name,
                            "remaining_events": result,
                        },
                    )
                else:
                    logger.debug(
                        "Subscription drained successfully",
                        extra={"subscription": name},
                    )

            if total_remaining > 0:
                logger.warning(
                    "Some events did not drain before timeout",
                    extra={
                        "total_remaining": total_remaining,
                        "drain_timeout": self._shutdown_coordinator.drain_timeout,
                    },
                )
            else:
                logger.info(
                    "All events drained successfully",
                    extra={"total_drained": total_in_flight},
                )

        except Exception as e:
            logger.error(
                "Error during drain phase",
                extra={"error": str(e)},
                exc_info=True,
            )

        return total_in_flight

    async def _save_final_checkpoints(self) -> int:
        """
        Save final checkpoints for all subscriptions.

        Returns:
            Number of checkpoints saved
        """
        saved_count = 0
        for name, subscription in self._registry.items():
            if subscription.last_event_id is None or subscription.last_event_type is None:
                logger.debug(
                    "Skipping checkpoint save - no event info",
                    extra={"subscription": name},
                )
                continue

            try:
                await self.checkpoint_repo.save_position(
                    subscription_id=name,
                    position=subscription.last_processed_position,
                    event_id=subscription.last_event_id,
                    event_type=subscription.last_event_type,
                )
                saved_count += 1
                logger.debug(
                    "Checkpoint saved",
                    extra={
                        "subscription": name,
                        "position": subscription.last_processed_position,
                    },
                )
            except Exception as e:
                logger.error(
                    "Failed to save checkpoint",
                    extra={
                        "subscription": name,
                        "error": str(e),
                    },
                )
        return saved_count

    def request_shutdown(self) -> None:
        """
        Programmatically request shutdown.

        Triggers the same shutdown sequence as receiving a SIGTERM/SIGINT
        signal. Useful for programmatic shutdown from health checks or
        other application logic.

        Example:
            >>> # In health check handler
            >>> if critical_error:
            ...     manager.request_shutdown()
        """
        self._shutdown_coordinator.request_shutdown()

    @property
    def is_shutting_down(self) -> bool:
        """
        Check if shutdown has been requested.

        Returns:
            True if shutdown signal received or requested
        """
        return self._shutdown_coordinator.is_shutting_down

    @property
    def shutdown_phase(self) -> ShutdownPhase:
        """
        Get current shutdown phase.

        Returns:
            Current ShutdownPhase enum value
        """
        return self._shutdown_coordinator.phase

    @property
    def last_shutdown_result(self) -> ShutdownResult | None:
        """
        Get the result of the last shutdown operation.

        Returns:
            ShutdownResult from last shutdown, or None if not shut down yet
        """
        return self._last_shutdown_result

    @property
    def shutdown_coordinator(self) -> ShutdownCoordinator:
        """
        Get the shutdown coordinator.

        Provides direct access to the shutdown coordinator for advanced
        use cases like registering custom shutdown callbacks.

        Returns:
            The ShutdownCoordinator instance
        """
        return self._shutdown_coordinator

    async def __aenter__(self) -> "SubscriptionManager":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit."""
        await self.stop()

    # =========================================================================
    # Error Handling Methods
    # =========================================================================

    def on_error(self, callback: ErrorCallback) -> None:
        """
        Register a callback for all error notifications.

        The callback will be invoked for errors from any subscription.

        Args:
            callback: Async function to call on error

        Example:
            >>> async def log_error(error_info: ErrorInfo):
            ...     logger.error(f"Error: {error_info.error_message}")
            >>> manager.on_error(log_error)
        """
        self._global_error_callbacks.append(callback)

        # Register on existing handlers
        for handler in self._error_handlers.values():
            handler.on_error(callback)

    def on_error_category(
        self,
        category: ErrorCategory,
        callback: ErrorCallback,
    ) -> None:
        """
        Register a callback for errors of a specific category.

        Args:
            category: The error category to filter on
            callback: Async function to call for matching errors

        Example:
            >>> async def handle_transient(error_info: ErrorInfo):
            ...     # Handle transient errors
            ...     pass
            >>> manager.on_error_category(ErrorCategory.TRANSIENT, handle_transient)
        """
        for handler in self._error_handlers.values():
            handler.on_category(category, callback)

    def on_error_severity(
        self,
        severity: ErrorSeverity,
        callback: ErrorCallback,
    ) -> None:
        """
        Register a callback for errors of a specific severity.

        Args:
            severity: The error severity to filter on
            callback: Async function to call for matching errors

        Example:
            >>> async def alert_critical(error_info: ErrorInfo):
            ...     await send_pager_alert(error_info)
            >>> manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical)
        """
        for handler in self._error_handlers.values():
            handler.on_severity(severity, callback)

    def get_error_handler(self, subscription_name: str) -> SubscriptionErrorHandler | None:
        """
        Get the error handler for a specific subscription.

        Args:
            subscription_name: Name of the subscription

        Returns:
            SubscriptionErrorHandler or None if not found
        """
        return self._error_handlers.get(subscription_name)

    def get_error_stats(self) -> dict[str, Any]:
        """
        Get error statistics for all subscriptions.

        Returns:
            Dictionary with error stats per subscription
        """
        stats: dict[str, Any] = {
            "total_errors": 0,
            "total_dlq_count": 0,
            "subscriptions": {},
        }

        for name, handler in self._error_handlers.items():
            sub_stats = handler.stats.to_dict()
            stats["subscriptions"][name] = sub_stats
            stats["total_errors"] += handler.total_errors
            stats["total_dlq_count"] += handler.dlq_count

        return stats

    # =========================================================================
    # Health Check Methods (delegates to HealthCheckProvider)
    # =========================================================================

    def get_health_checker(
        self,
        subscription_name: str,
    ) -> Any:
        """
        Get the health checker for a specific subscription.

        Args:
            subscription_name: Name of the subscription

        Returns:
            SubscriptionHealthChecker or None if not found
        """
        return self._health_provider.get_health_checker(subscription_name)

    def check_health(self, subscription_name: str) -> HealthCheckResult:
        """
        Check health of a specific subscription.

        Args:
            subscription_name: Name of the subscription

        Returns:
            HealthCheckResult with status and indicators

        Raises:
            KeyError: If subscription not found
        """
        return self._health_provider.check_health(subscription_name)

    def check_all_health(self) -> dict[str, Any]:
        """
        Check health of all subscriptions.

        Returns comprehensive health status including:
        - Overall manager status
        - Per-subscription health status
        - Error statistics
        - Aggregated indicators

        Returns:
            Dictionary with overall and per-subscription health
        """
        return self._health_provider.check_all_health(self._running)

    def get_comprehensive_health(self) -> dict[str, Any]:
        """
        Get comprehensive health report including all resilience metrics.

        Combines:
        - Subscription states
        - Error statistics
        - Health check results
        - Recent errors
        - DLQ status

        Returns:
            Complete health report dictionary
        """
        return self._health_provider.get_comprehensive_health(self._running)

    @property
    def total_errors(self) -> int:
        """Get total error count across all subscriptions."""
        return self._health_provider.total_errors

    @property
    def total_dlq_count(self) -> int:
        """Get total DLQ count across all subscriptions."""
        return self._health_provider.total_dlq_count

    @property
    def is_healthy(self) -> bool:
        """
        Quick health check.

        Returns:
            True if all subscriptions are healthy
        """
        return self._health_provider.is_healthy(self._running)

    @property
    def uptime_seconds(self) -> float:
        """
        Get manager uptime in seconds.

        Returns:
            Seconds since manager was started, or 0 if not started.
        """
        return self._health_provider.uptime_seconds

    # =========================================================================
    # Health Check API (PHASE3-003) - delegates to HealthCheckProvider
    # =========================================================================

    async def health_check(self) -> ManagerHealth:
        """
        Get comprehensive health status of the subscription manager.

        This is the primary health check API that provides:
        - Overall manager health status (healthy/degraded/unhealthy/critical)
        - Running state
        - Subscription counts by health category
        - Aggregate metrics (events processed, failed, lag)
        - Per-subscription health details

        The health status is determined by evaluating all subscriptions:
        - healthy: LIVE state with lag < warning threshold
        - degraded: CATCHING_UP, PAUSED, or high lag
        - unhealthy: ERROR state or critical lag

        Returns:
            ManagerHealth with complete health information

        Example:
            >>> health = await manager.health_check()
            >>> if health.status == "unhealthy":
            ...     await alert_ops_team(health.to_dict())
            >>> print(f"Healthy: {health.healthy_count}/{health.subscription_count}")
        """
        return await self._health_provider.health_check(self._running)

    async def readiness_check(self) -> ReadinessStatus:
        """
        Check if the manager is ready to accept work (Kubernetes-style probe).

        A manager is ready when:
        - It is running
        - It has at least one subscription (optional, configurable)
        - No subscriptions are in ERROR state
        - Not currently shutting down

        This is designed for Kubernetes readiness probes to determine
        if the service should receive traffic.

        Returns:
            ReadinessStatus indicating readiness state

        Example:
            >>> readiness = await manager.readiness_check()
            >>> if not readiness.ready:
            ...     logger.warning(f"Not ready: {readiness.reason}")
        """
        return await self._health_provider.readiness_check(
            self._running,
            self.is_shutting_down,
        )

    async def liveness_check(self) -> LivenessStatus:
        """
        Check if the manager is alive and responsive (Kubernetes-style probe).

        A manager is live when:
        - It has not crashed
        - Its internal async lock is not deadlocked
        - It can respond to health checks

        This is designed for Kubernetes liveness probes to determine
        if the service should be restarted.

        Returns:
            LivenessStatus indicating liveness state

        Example:
            >>> liveness = await manager.liveness_check()
            >>> if not liveness.alive:
            ...     logger.critical(f"Manager not alive: {liveness.reason}")
        """
        return await self._health_provider.liveness_check(self._running)

    def get_subscription_health(self, subscription_name: str) -> SubscriptionHealth | None:
        """
        Get health status for a specific subscription.

        Args:
            subscription_name: Name of the subscription

        Returns:
            SubscriptionHealth for the subscription, or None if not found

        Example:
            >>> health = manager.get_subscription_health("OrderProjection")
            >>> if health and health.status == "unhealthy":
            ...     await restart_subscription(health.name)
        """
        return self._health_provider.get_subscription_health(subscription_name)

    # =========================================================================
    # Pause/Resume Methods (delegates to PauseResumeController)
    # =========================================================================

    async def pause_subscription(
        self,
        name: str,
        reason: PauseReason | None = None,
    ) -> bool:
        """
        Pause a specific subscription by name.

        The subscription will stop processing events but maintain its position.
        Events received during pause will be buffered for processing on resume.

        Args:
            name: The subscription name to pause
            reason: Optional reason for pausing (defaults to PauseReason.MANUAL)

        Returns:
            True if subscription was paused, False if not found or not pausable

        Example:
            >>> await manager.pause_subscription("OrderProjection")
            >>> # ... do maintenance ...
            >>> await manager.resume_subscription("OrderProjection")
        """
        return await self._pause_resume.pause(name, reason)

    async def resume_subscription(self, name: str) -> bool:
        """
        Resume a paused subscription by name.

        Continues processing from the last position. Events that were
        buffered during pause will be processed first.

        Args:
            name: The subscription name to resume

        Returns:
            True if subscription was resumed, False if not found or not paused

        Example:
            >>> await manager.pause_subscription("OrderProjection")
            >>> # ... do maintenance ...
            >>> await manager.resume_subscription("OrderProjection")
        """
        return await self._pause_resume.resume(name)

    async def pause_all(
        self,
        reason: PauseReason | None = None,
    ) -> dict[str, bool]:
        """
        Pause all running subscriptions.

        Pauses all subscriptions that are currently in a running state
        (CATCHING_UP or LIVE). Subscriptions in other states are skipped.

        Args:
            reason: Optional reason for pausing (defaults to PauseReason.MANUAL)

        Returns:
            Dictionary mapping subscription names to pause result (True/False)

        Example:
            >>> results = await manager.pause_all()
            >>> paused_count = sum(1 for r in results.values() if r)
            >>> print(f"Paused {paused_count} subscriptions")
        """
        return await self._pause_resume.pause_all(reason)

    async def resume_all(self) -> dict[str, bool]:
        """
        Resume all paused subscriptions.

        Resumes all subscriptions that are currently in PAUSED state.
        Subscriptions in other states are skipped.

        Returns:
            Dictionary mapping subscription names to resume result (True/False)

        Example:
            >>> await manager.pause_all()
            >>> # ... do maintenance ...
            >>> results = await manager.resume_all()
            >>> resumed_count = sum(1 for r in results.values() if r)
            >>> print(f"Resumed {resumed_count} subscriptions")
        """
        return await self._pause_resume.resume_all()

    @property
    def paused_subscriptions(self) -> list[Subscription]:
        """
        Get all paused subscriptions.

        Returns:
            List of subscriptions currently in PAUSED state
        """
        return self._pause_resume.get_paused()

    @property
    def paused_subscription_names(self) -> list[str]:
        """
        Get names of all paused subscriptions.

        Returns:
            List of subscription names currently in PAUSED state
        """
        return self._pause_resume.get_paused_names()


__all__ = [
    "SubscriptionManager",
]
