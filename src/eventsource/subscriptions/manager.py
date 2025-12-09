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

from eventsource.subscriptions.config import SubscriptionConfig
from eventsource.subscriptions.error_handling import (
    ErrorCallback,
    ErrorCategory,
    ErrorHandlingConfig,
    ErrorSeverity,
    SubscriptionErrorHandler,
)
from eventsource.subscriptions.exceptions import (
    SubscriptionAlreadyExistsError,
    SubscriptionError,
)
from eventsource.subscriptions.health import (
    HealthCheckConfig,
    HealthCheckResult,
    HealthStatus,
    LivenessStatus,
    ManagerHealth,
    ManagerHealthChecker,
    ReadinessStatus,
    SubscriptionHealth,
    SubscriptionHealthChecker,
)
from eventsource.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownResult,
)
from eventsource.subscriptions.subscription import (
    PauseReason,
    Subscription,
    SubscriptionState,
    SubscriptionStatus,
)
from eventsource.subscriptions.transition import (
    StartFromResolver,
    TransitionCoordinator,
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
        """
        self.event_store = event_store
        self.event_bus = event_bus
        self.checkpoint_repo = checkpoint_repo
        self._dlq_repo = dlq_repo

        self._subscriptions: dict[str, Subscription] = {}
        self._coordinators: dict[str, TransitionCoordinator] = {}
        self._running = False
        self._start_resolver = StartFromResolver(event_store, checkpoint_repo)
        self._lock = asyncio.Lock()
        self._shutdown_coordinator = ShutdownCoordinator(
            timeout=shutdown_timeout,
            drain_timeout=drain_timeout,
        )
        self._last_shutdown_result: ShutdownResult | None = None

        # Error handling
        self._error_handling_config = error_handling_config or ErrorHandlingConfig()
        self._error_handlers: dict[str, SubscriptionErrorHandler] = {}
        self._global_error_callbacks: list[ErrorCallback] = []

        # Health checks
        self._health_check_config = health_check_config or HealthCheckConfig()
        self._health_checkers: dict[str, SubscriptionHealthChecker] = {}

        # Manager tracking
        self._created_at = datetime.now(UTC)
        self._started_at: datetime | None = None

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
        # Use provided name or subscriber class name as subscription name
        subscription_name = name or subscriber.__class__.__name__
        config = config or SubscriptionConfig()

        async with self._lock:
            if subscription_name in self._subscriptions:
                raise SubscriptionAlreadyExistsError(subscription_name)

            subscription = Subscription(
                name=subscription_name,
                config=config,
                subscriber=subscriber,
            )

            self._subscriptions[subscription_name] = subscription

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

            # Create health checker for this subscription
            health_checker = SubscriptionHealthChecker(
                subscription=subscription,
                config=self._health_check_config,
                error_handler=error_handler,
            )
            self._health_checkers[subscription_name] = health_checker

            logger.info(
                "Subscription registered",
                extra={
                    "subscription": subscription_name,
                    "start_from": str(config.start_from),
                    "batch_size": config.batch_size,
                },
            )

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
        async with self._lock:
            if name not in self._subscriptions:
                return False

            # Stop coordinator if running
            if name in self._coordinators:
                coordinator = self._coordinators[name]
                await coordinator.stop()
                del self._coordinators[name]

            subscription = self._subscriptions[name]
            if not subscription.is_terminal:
                await subscription.transition_to(SubscriptionState.STOPPED)

            del self._subscriptions[name]

            # Clean up error handler and health checker
            if name in self._error_handlers:
                del self._error_handlers[name]
            if name in self._health_checkers:
                del self._health_checkers[name]

            logger.info(
                "Subscription unregistered",
                extra={"subscription": name},
            )

            return True

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

        # Determine which subscriptions to start
        if subscription_names is None:
            subscriptions_to_start = list(self._subscriptions.items())
        else:
            subscriptions_to_start = [
                (name, sub)
                for name, sub in self._subscriptions.items()
                if name in subscription_names
            ]

        logger.info(
            "Starting subscription manager",
            extra={
                "subscription_count": len(subscriptions_to_start),
                "concurrent": concurrent,
            },
        )

        results: dict[str, Exception | None] = {}

        if concurrent and len(subscriptions_to_start) > 1:
            # Start all subscriptions concurrently
            tasks = []
            for name, subscription in subscriptions_to_start:
                task = asyncio.create_task(
                    self._start_subscription_isolated(subscription),
                    name=f"start_{name}",
                )
                tasks.append((name, task))

            # Wait for all to complete (exceptions are returned, not raised)
            for name, task in tasks:
                try:
                    result = await task
                    results[name] = result
                except Exception as e:
                    # Should not happen with _start_subscription_isolated,
                    # but handle it for safety
                    results[name] = e
                    logger.error(
                        "Unexpected error starting subscription",
                        extra={"subscription": name, "error": str(e)},
                        exc_info=True,
                    )
        else:
            # Start sequentially (single subscription or legacy mode)
            for name, subscription in subscriptions_to_start:
                result = await self._start_subscription_isolated(subscription)
                results[name] = result

        # Log summary
        succeeded = sum(1 for r in results.values() if r is None)
        failed = sum(1 for r in results.values() if r is not None)

        if failed > 0:
            logger.warning(
                "Some subscriptions failed to start",
                extra={
                    "succeeded": succeeded,
                    "failed": failed,
                    "failures": {k: str(v) for k, v in results.items() if v is not None},
                },
            )
        else:
            logger.info(
                "All subscriptions started successfully",
                extra={"count": succeeded},
            )

        return results

    async def _start_subscription_isolated(self, subscription: Subscription) -> Exception | None:
        """
        Start a single subscription with isolation.

        This method catches and returns exceptions instead of raising them,
        ensuring that one subscription failure does not affect others.

        Args:
            subscription: The subscription to start

        Returns:
            None if successful, Exception if failed
        """
        try:
            await self._start_subscription(subscription)
            return None
        except Exception as e:
            # Error is already logged and subscription state is set in _start_subscription
            return e

    async def _start_subscription(self, subscription: Subscription) -> None:
        """
        Start a single subscription.

        Args:
            subscription: The subscription to start
        """
        name = subscription.name

        try:
            # Resolve starting position
            start_position = await self._start_resolver.resolve(subscription)
            subscription.last_processed_position = start_position

            logger.info(
                "Starting subscription",
                extra={
                    "subscription": name,
                    "start_position": start_position,
                },
            )

            # Create and execute transition coordinator
            coordinator = TransitionCoordinator(
                event_store=self.event_store,
                event_bus=self.event_bus,
                checkpoint_repo=self.checkpoint_repo,
                subscription=subscription,
            )
            self._coordinators[name] = coordinator

            # Execute transition (catch-up to live)
            result = await coordinator.execute()

            if not result.success:
                await subscription.set_error(result.error or SubscriptionError("Transition failed"))
                raise SubscriptionError(f"Subscription {name} failed to transition: {result.error}")

            logger.info(
                "Subscription started successfully",
                extra={
                    "subscription": name,
                    "catchup_events": result.catchup_events_processed,
                    "buffer_events": result.buffer_events_processed,
                    "final_position": result.final_position,
                },
            )

        except Exception as e:
            logger.error(
                "Failed to start subscription",
                extra={
                    "subscription": name,
                    "error": str(e),
                },
                exc_info=True,
            )
            await subscription.set_error(e)
            raise

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
        if not self._running and subscription_names is None:
            return

        # Determine which subscriptions to stop
        if subscription_names is None:
            names_to_stop = list(self._coordinators.keys())
            subscriptions_to_stop = list(self._subscriptions.values())
            self._running = False
        else:
            names_to_stop = [name for name in subscription_names if name in self._coordinators]
            subscriptions_to_stop = [
                sub for name, sub in self._subscriptions.items() if name in subscription_names
            ]

        logger.info(
            "Stopping subscription manager",
            extra={
                "subscription_count": len(names_to_stop),
                "timeout": timeout,
            },
        )

        # Stop all coordinators
        stop_tasks = []
        for name in names_to_stop:
            if name in self._coordinators:
                coordinator = self._coordinators[name]
                stop_tasks.append(self._stop_subscription(name, coordinator, timeout))

        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        # Update subscription states
        for subscription in subscriptions_to_stop:
            if not subscription.is_terminal:
                await subscription.transition_to(SubscriptionState.STOPPED)

        logger.info("Subscription manager stopped")

    async def _stop_subscription(
        self,
        name: str,
        coordinator: TransitionCoordinator,
        timeout: float,
    ) -> None:
        """
        Stop a single subscription.

        Args:
            name: Subscription name
            coordinator: The subscription's coordinator
            timeout: Shutdown timeout
        """
        try:
            await asyncio.wait_for(coordinator.stop(), timeout=timeout)
            logger.info(
                "Subscription stopped",
                extra={"subscription": name},
            )
        except TimeoutError:
            logger.warning(
                "Subscription stop timed out",
                extra={
                    "subscription": name,
                    "timeout": timeout,
                },
            )

    @property
    def subscriptions(self) -> list[Subscription]:
        """Get all registered subscriptions."""
        return list(self._subscriptions.values())

    @property
    def subscription_names(self) -> list[str]:
        """Get all registered subscription names."""
        return list(self._subscriptions.keys())

    @property
    def subscription_count(self) -> int:
        """Get the number of registered subscriptions."""
        return len(self._subscriptions)

    @property
    def is_running(self) -> bool:
        """Check if the manager is running."""
        return self._running

    def get_subscription(self, name: str) -> Subscription | None:
        """
        Get a subscription by name.

        Args:
            name: The subscription name

        Returns:
            The Subscription, or None if not found
        """
        return self._subscriptions.get(name)

    def get_all_statuses(self) -> dict[str, SubscriptionStatus]:
        """
        Get status snapshots of all subscriptions.

        Returns a dictionary mapping subscription names to their
        SubscriptionStatus objects. Useful for querying subscription
        states and statistics.

        Returns:
            Dictionary of subscription name to SubscriptionStatus

        Example:
            >>> statuses = manager.get_all_statuses()
            >>> for name, status in statuses.items():
            ...     print(f"{name}: {status.state}, processed={status.events_processed}")
        """
        return {
            name: subscription.get_status() for name, subscription in self._subscriptions.items()
        }

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

        for subscription in self._subscriptions.values():
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
            "subscription_count": len(self._subscriptions),
            "subscriptions": subscription_statuses,
        }

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
            extra={"subscription_count": len(self._subscriptions)},
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
                "subscription_count": len(self._subscriptions),
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
                extra={"subscriptions": list(self._subscriptions.keys())},
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
            extra={"subscription_count": len(self._coordinators)},
        )
        await self.stop()

    async def _drain_in_flight_events(self) -> int:
        """
        Drain in-flight events from all subscriptions.

        Returns:
            Number of events drained
        """
        # Currently, the stop() method waits for in-flight events
        # This is a hook for future implementation of explicit draining
        logger.info("Draining in-flight events")
        return 0

    async def _save_final_checkpoints(self) -> int:
        """
        Save final checkpoints for all subscriptions.

        Returns:
            Number of checkpoints saved
        """
        saved_count = 0
        for name, subscription in self._subscriptions.items():
            # Skip if we don't have event info to save
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
    # Health Check Methods
    # =========================================================================

    def get_health_checker(
        self,
        subscription_name: str,
    ) -> SubscriptionHealthChecker | None:
        """
        Get the health checker for a specific subscription.

        Args:
            subscription_name: Name of the subscription

        Returns:
            SubscriptionHealthChecker or None if not found
        """
        return self._health_checkers.get(subscription_name)

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
        if subscription_name not in self._health_checkers:
            raise KeyError(f"Subscription '{subscription_name}' not found")

        return self._health_checkers[subscription_name].check()

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
        checker = ManagerHealthChecker(
            subscriptions=list(self._subscriptions.values()),
            config=self._health_check_config,
            subscription_checkers=self._health_checkers,
        )

        health = checker.check()

        # Add error stats
        health["error_stats"] = self.get_error_stats()

        return health

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
        health = self.check_all_health()

        # Add recent errors per subscription
        recent_errors: dict[str, list[dict[str, Any]]] = {}
        for name, handler in self._error_handlers.items():
            recent_errors[name] = [e.to_dict() for e in handler.recent_errors[-10:]]

        health["recent_errors"] = recent_errors

        # Add subscription-level DLQ counts
        dlq_status: dict[str, int] = {}
        for name, subscription in self._subscriptions.items():
            dlq_status[name] = subscription.dlq_count

        health["dlq_status"] = dlq_status

        return health

    @property
    def total_errors(self) -> int:
        """Get total error count across all subscriptions."""
        return sum(handler.total_errors for handler in self._error_handlers.values())

    @property
    def total_dlq_count(self) -> int:
        """Get total DLQ count across all subscriptions."""
        return sum(handler.dlq_count for handler in self._error_handlers.values())

    @property
    def is_healthy(self) -> bool:
        """
        Quick health check.

        Returns:
            True if all subscriptions are healthy
        """
        health = self.check_all_health()
        return health.get("status") in (
            HealthStatus.HEALTHY.value,
            "healthy",
        )

    @property
    def uptime_seconds(self) -> float:
        """
        Get manager uptime in seconds.

        Returns:
            Seconds since manager was started, or 0 if not started.
        """
        if self._started_at is None:
            return 0.0
        return (datetime.now(UTC) - self._started_at).total_seconds()

    # =========================================================================
    # Health Check API (PHASE3-003)
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
        # Collect health info for each subscription
        subscription_health_list: list[SubscriptionHealth] = []
        healthy = degraded = unhealthy = 0
        total_lag = 0
        total_processed = 0
        total_failed = 0

        for name, subscription in self._subscriptions.items():
            # Get health check result from checker
            checker = self._health_checkers.get(name)
            if checker:
                result = checker.check()
                status = result.overall_status.value
            else:
                # Basic status determination without checker
                from eventsource.subscriptions.subscription import SubscriptionState

                if subscription.state == SubscriptionState.LIVE:
                    if subscription.lag < self._health_check_config.max_lag_events_warning:
                        status = "healthy"
                    else:
                        status = "degraded"
                elif subscription.state in (
                    SubscriptionState.CATCHING_UP,
                    SubscriptionState.PAUSED,
                ):
                    status = "degraded"
                elif subscription.state == SubscriptionState.ERROR:
                    status = "unhealthy"
                else:
                    status = "unknown"

            # Count by status
            if status == "healthy":
                healthy += 1
            elif status == "degraded":
                degraded += 1
            else:  # unhealthy, critical, unknown
                unhealthy += 1

            # Get error rate from handler if available
            error_rate = 0.0
            handler = self._error_handlers.get(name)
            if handler:
                error_rate = handler.stats.error_rate_per_minute

            # Build subscription health
            sub_health = SubscriptionHealth(
                name=name,
                status=status,
                state=subscription.state.value,
                events_processed=subscription.events_processed,
                events_failed=subscription.events_failed,
                lag_events=subscription.lag,
                error_rate=error_rate,
                last_error=str(subscription.last_error) if subscription.last_error else None,
                uptime_seconds=subscription.uptime_seconds,
            )
            subscription_health_list.append(sub_health)

            # Aggregate metrics
            total_lag += subscription.lag
            total_processed += subscription.events_processed
            total_failed += subscription.events_failed

        # Determine overall status
        if unhealthy > 0:
            overall = "unhealthy"
        elif degraded > 0:
            overall = "degraded"
        elif healthy > 0:
            overall = "healthy"
        else:
            overall = "unknown"

        return ManagerHealth(
            status=overall,
            running=self._running,
            subscription_count=len(self._subscriptions),
            healthy_count=healthy,
            degraded_count=degraded,
            unhealthy_count=unhealthy,
            total_events_processed=total_processed,
            total_events_failed=total_failed,
            total_lag_events=total_lag,
            uptime_seconds=self.uptime_seconds,
            subscriptions=subscription_health_list,
        )

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
        from eventsource.subscriptions.subscription import SubscriptionState

        details: dict[str, Any] = {
            "running": self._running,
            "subscription_count": len(self._subscriptions),
            "shutting_down": self.is_shutting_down,
        }

        # Check if shutting down
        if self.is_shutting_down:
            return ReadinessStatus(
                ready=False,
                reason="Manager is shutting down",
                details=details,
            )

        # Check if running
        if not self._running:
            return ReadinessStatus(
                ready=False,
                reason="Manager is not running",
                details=details,
            )

        # Check for error states
        error_subscriptions = []
        for name, subscription in self._subscriptions.items():
            if subscription.state == SubscriptionState.ERROR:
                error_subscriptions.append(name)

        if error_subscriptions:
            details["error_subscriptions"] = error_subscriptions
            return ReadinessStatus(
                ready=False,
                reason=f"Subscriptions in error state: {', '.join(error_subscriptions)}",
                details=details,
            )

        # Check for subscriptions still starting
        starting_subscriptions = []
        for name, subscription in self._subscriptions.items():
            if subscription.state == SubscriptionState.STARTING:
                starting_subscriptions.append(name)

        if starting_subscriptions:
            details["starting_subscriptions"] = starting_subscriptions
            return ReadinessStatus(
                ready=False,
                reason=f"Subscriptions still starting: {', '.join(starting_subscriptions)}",
                details=details,
            )

        # All checks passed
        return ReadinessStatus(
            ready=True,
            reason="Manager is ready",
            details=details,
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
        details: dict[str, Any] = {
            "running": self._running,
            "uptime_seconds": self.uptime_seconds,
        }

        # Try to acquire lock with timeout to detect deadlocks
        try:
            lock_acquired = await asyncio.wait_for(
                self._try_acquire_lock(),
                timeout=5.0,
            )
            details["lock_responsive"] = lock_acquired
        except TimeoutError:
            return LivenessStatus(
                alive=False,
                reason="Internal lock not responsive (possible deadlock)",
                details=details,
            )

        # Check if we can enumerate subscriptions
        try:
            details["subscription_count"] = len(self._subscriptions)
        except Exception as e:
            return LivenessStatus(
                alive=False,
                reason=f"Error accessing subscriptions: {e}",
                details=details,
            )

        # All checks passed
        return LivenessStatus(
            alive=True,
            reason="Manager is alive and responsive",
            details=details,
        )

    async def _try_acquire_lock(self) -> bool:
        """
        Try to acquire the internal lock.

        Used by liveness check to detect deadlocks.

        Returns:
            True if lock was acquired (and released), False otherwise
        """
        try:
            async with asyncio.timeout(1.0):
                async with self._lock:
                    return True
        except TimeoutError:
            return False

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
        subscription = self._subscriptions.get(subscription_name)
        if subscription is None:
            return None

        # Get health check result from checker
        checker = self._health_checkers.get(subscription_name)
        if checker:
            result = checker.check()
            status = result.overall_status.value
        else:
            # Basic status determination
            from eventsource.subscriptions.subscription import SubscriptionState

            if subscription.state == SubscriptionState.LIVE:
                if subscription.lag < self._health_check_config.max_lag_events_warning:
                    status = "healthy"
                else:
                    status = "degraded"
            elif subscription.state in (
                SubscriptionState.CATCHING_UP,
                SubscriptionState.PAUSED,
            ):
                status = "degraded"
            elif subscription.state == SubscriptionState.ERROR:
                status = "unhealthy"
            else:
                status = "unknown"

        # Get error rate from handler if available
        error_rate = 0.0
        handler = self._error_handlers.get(subscription_name)
        if handler:
            error_rate = handler.stats.error_rate_per_minute

        return SubscriptionHealth(
            name=subscription_name,
            status=status,
            state=subscription.state.value,
            events_processed=subscription.events_processed,
            events_failed=subscription.events_failed,
            lag_events=subscription.lag,
            error_rate=error_rate,
            last_error=str(subscription.last_error) if subscription.last_error else None,
            uptime_seconds=subscription.uptime_seconds,
        )

    # =========================================================================
    # Pause/Resume Methods (PHASE3-005)
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
        from eventsource.subscriptions.subscription import PauseReason

        subscription = self._subscriptions.get(name)
        if subscription is None:
            logger.warning(
                "Cannot pause subscription: not found",
                extra={"subscription": name},
            )
            return False

        if not subscription.is_running:
            logger.warning(
                "Cannot pause subscription: not running",
                extra={"subscription": name, "state": subscription.state.value},
            )
            return False

        try:
            pause_reason = reason or PauseReason.MANUAL
            await subscription.pause(reason=pause_reason)
            logger.info(
                "Subscription paused via manager",
                extra={
                    "subscription": name,
                    "reason": pause_reason.value,
                    "position": subscription.last_processed_position,
                },
            )
            return True
        except Exception as e:
            logger.error(
                "Failed to pause subscription",
                extra={"subscription": name, "error": str(e)},
                exc_info=True,
            )
            return False

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
        subscription = self._subscriptions.get(name)
        if subscription is None:
            logger.warning(
                "Cannot resume subscription: not found",
                extra={"subscription": name},
            )
            return False

        if not subscription.is_paused:
            logger.warning(
                "Cannot resume subscription: not paused",
                extra={"subscription": name, "state": subscription.state.value},
            )
            return False

        try:
            await subscription.resume()

            # Process any buffered events from the live runner
            coordinator = self._coordinators.get(name)
            if coordinator and coordinator.live_runner:
                pause_buffer_size = coordinator.live_runner.pause_buffer_size
                if pause_buffer_size > 0:
                    processed = await coordinator.live_runner.process_pause_buffer()
                    logger.info(
                        "Processed pause buffer after resume",
                        extra={
                            "subscription": name,
                            "events_processed": processed,
                        },
                    )

            logger.info(
                "Subscription resumed via manager",
                extra={
                    "subscription": name,
                    "position": subscription.last_processed_position,
                    "state": subscription.state.value,
                },
            )
            return True
        except Exception as e:
            logger.error(
                "Failed to resume subscription",
                extra={"subscription": name, "error": str(e)},
                exc_info=True,
            )
            return False

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
        from eventsource.subscriptions.subscription import PauseReason

        pause_reason = reason or PauseReason.MANUAL
        results: dict[str, bool] = {}

        for name, subscription in self._subscriptions.items():
            if subscription.is_running:
                results[name] = await self.pause_subscription(name, reason=pause_reason)
            else:
                results[name] = False

        paused_count = sum(1 for r in results.values() if r)
        logger.info(
            "Paused all subscriptions",
            extra={
                "reason": pause_reason.value,
                "paused_count": paused_count,
                "total_count": len(self._subscriptions),
            },
        )

        return results

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
        results: dict[str, bool] = {}

        for name, subscription in self._subscriptions.items():
            if subscription.is_paused:
                results[name] = await self.resume_subscription(name)
            else:
                results[name] = False

        resumed_count = sum(1 for r in results.values() if r)
        logger.info(
            "Resumed all subscriptions",
            extra={
                "resumed_count": resumed_count,
                "total_count": len(self._subscriptions),
            },
        )

        return results

    @property
    def paused_subscriptions(self) -> list[Subscription]:
        """
        Get all paused subscriptions.

        Returns:
            List of subscriptions currently in PAUSED state
        """
        return [sub for sub in self._subscriptions.values() if sub.is_paused]

    @property
    def paused_subscription_names(self) -> list[str]:
        """
        Get names of all paused subscriptions.

        Returns:
            List of subscription names currently in PAUSED state
        """
        return [name for name, sub in self._subscriptions.items() if sub.is_paused]


__all__ = [
    "SubscriptionManager",
]
