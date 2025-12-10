"""
Graceful shutdown coordination for subscriptions.

Handles signal registration, shutdown sequencing, and timeout enforcement.

This module provides:
- ShutdownPhase: Enum of shutdown phases
- ShutdownResult: Result of shutdown operation
- ShutdownMetrics: Dataclass for shutdown metrics snapshot
- ShutdownCoordinator: Coordinates graceful shutdown

Example:
    >>> coordinator = ShutdownCoordinator(timeout=30.0)
    >>> coordinator.register_signals()
    >>> # When signal received:
    >>> result = await coordinator.shutdown(subscriptions)
"""

import asyncio
import logging
import signal
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

# Type alias for pre-shutdown hooks: async callable with individual timeout
PreShutdownHook = tuple[Callable[[], Awaitable[None]], float]

# Type alias for post-shutdown hooks: async callable receiving ShutdownResult
PostShutdownHook = Callable[["ShutdownResult"], Awaitable[None]]

logger = logging.getLogger(__name__)

# Optional OpenTelemetry import - graceful degradation when not available
try:
    from opentelemetry import metrics as otel_metrics

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    OTEL_METRICS_AVAILABLE = False
    otel_metrics = None  # type: ignore[assignment]


# Module-level meter and instruments - lazy initialization
_meter: Any = None
_shutdown_initiated_counter: Any = None
_shutdown_completed_counter: Any = None
_shutdown_duration_histogram: Any = None
_drain_duration_histogram: Any = None
_events_drained_counter: Any = None
_in_flight_gauge_value: int = 0


def _get_meter() -> Any:
    """
    Get or create the meter instance for shutdown metrics.

    Returns the OpenTelemetry meter for the subscriptions.shutdown namespace,
    or None if OpenTelemetry is not available.

    Returns:
        OpenTelemetry Meter or None
    """
    global _meter
    if _meter is None and OTEL_METRICS_AVAILABLE and otel_metrics is not None:
        _meter = otel_metrics.get_meter("eventsource.subscriptions.shutdown", version="1.0.0")
    return _meter


def _init_shutdown_metrics() -> None:
    """
    Initialize shutdown metrics instruments if not already done.

    This creates all the OpenTelemetry metric instruments for shutdown tracking.
    Safe to call multiple times - instruments are only created once.
    """
    global _shutdown_initiated_counter, _shutdown_completed_counter
    global _shutdown_duration_histogram, _drain_duration_histogram
    global _events_drained_counter

    meter = _get_meter()
    if meter is None:
        return

    if _shutdown_initiated_counter is None:
        _shutdown_initiated_counter = meter.create_counter(
            name="eventsource.shutdown.initiated_total",
            unit="1",
            description="Total number of shutdown operations initiated",
        )

    if _shutdown_completed_counter is None:
        _shutdown_completed_counter = meter.create_counter(
            name="eventsource.shutdown.completed_total",
            unit="1",
            description="Total number of shutdown operations completed",
        )

    if _shutdown_duration_histogram is None:
        _shutdown_duration_histogram = meter.create_histogram(
            name="eventsource.shutdown.duration_seconds",
            unit="s",
            description="Duration of shutdown operations in seconds",
        )

    if _drain_duration_histogram is None:
        _drain_duration_histogram = meter.create_histogram(
            name="eventsource.shutdown.drain_duration_seconds",
            unit="s",
            description="Duration of drain phase in seconds",
        )

    if _events_drained_counter is None:
        _events_drained_counter = meter.create_counter(
            name="eventsource.shutdown.events_drained_total",
            unit="events",
            description="Total number of events drained during shutdown",
        )


def record_shutdown_initiated() -> None:
    """
    Record that a shutdown operation has been initiated.

    Safe to call even when OpenTelemetry is not configured.
    """
    _init_shutdown_metrics()
    if _shutdown_initiated_counter is not None:
        _shutdown_initiated_counter.add(1)


def record_shutdown_completed(outcome: str, duration_seconds: float) -> None:
    """
    Record a completed shutdown operation.

    Args:
        outcome: The outcome of shutdown - "clean", "forced", or "timeout"
        duration_seconds: Total duration of the shutdown in seconds

    Safe to call even when OpenTelemetry is not configured.
    """
    _init_shutdown_metrics()

    if _shutdown_completed_counter is not None:
        _shutdown_completed_counter.add(1, {"outcome": outcome})

    if _shutdown_duration_histogram is not None:
        _shutdown_duration_histogram.record(duration_seconds, {"outcome": outcome})


def record_drain_duration(duration_seconds: float) -> None:
    """
    Record the duration of the drain phase.

    Args:
        duration_seconds: Duration of drain phase in seconds

    Safe to call even when OpenTelemetry is not configured.
    """
    _init_shutdown_metrics()
    if _drain_duration_histogram is not None:
        _drain_duration_histogram.record(duration_seconds)


def record_events_drained(count: int) -> None:
    """
    Record the number of events drained during shutdown.

    Args:
        count: Number of events that were drained

    Safe to call even when OpenTelemetry is not configured.
    """
    _init_shutdown_metrics()
    if _events_drained_counter is not None and count > 0:
        _events_drained_counter.add(count)


def record_in_flight_at_shutdown(count: int) -> None:
    """
    Record the number of in-flight events when shutdown started.

    This updates the gauge value that can be observed.

    Args:
        count: Number of events in flight at shutdown start

    Safe to call even when OpenTelemetry is not configured.
    """
    global _in_flight_gauge_value
    _in_flight_gauge_value = count


def get_in_flight_at_shutdown() -> int:
    """
    Get the recorded number of in-flight events at shutdown.

    Returns:
        Number of in-flight events recorded at shutdown start
    """
    return _in_flight_gauge_value


def reset_shutdown_metrics() -> None:
    """
    Reset the shutdown metrics state.

    Useful for testing to ensure clean state between tests.
    Resets the meter and all instrument references.
    """
    global _meter, _shutdown_initiated_counter, _shutdown_completed_counter
    global _shutdown_duration_histogram, _drain_duration_histogram
    global _events_drained_counter, _in_flight_gauge_value

    _meter = None
    _shutdown_initiated_counter = None
    _shutdown_completed_counter = None
    _shutdown_duration_histogram = None
    _drain_duration_histogram = None
    _events_drained_counter = None
    _in_flight_gauge_value = 0


@dataclass(frozen=True)
class ShutdownMetricsSnapshot:
    """
    Snapshot of shutdown metrics for a single shutdown operation.

    Captures key metrics about the shutdown process that can be
    inspected after shutdown completes.

    Attributes:
        shutdown_duration_seconds: Total duration of shutdown
        drain_duration_seconds: Duration of drain phase (0 if no drain)
        events_drained: Number of events drained during shutdown
        checkpoints_saved: Number of checkpoints saved during shutdown
        in_flight_at_start: Number of in-flight events at shutdown start
        outcome: Shutdown outcome - "clean", "forced", or "timeout"
    """

    shutdown_duration_seconds: float
    drain_duration_seconds: float
    events_drained: int
    checkpoints_saved: int
    in_flight_at_start: int
    outcome: str

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation of metrics snapshot
        """
        return {
            "shutdown_duration_seconds": self.shutdown_duration_seconds,
            "drain_duration_seconds": self.drain_duration_seconds,
            "events_drained": self.events_drained,
            "checkpoints_saved": self.checkpoints_saved,
            "in_flight_at_start": self.in_flight_at_start,
            "outcome": self.outcome,
        }


class ShutdownPhase(Enum):
    """
    Phases of graceful shutdown.

    The shutdown sequence follows these phases:
    1. RUNNING: Normal operation, no shutdown requested
    2. STOPPING: Stop accepting new events
    3. DRAINING: Drain in-flight events
    4. CHECKPOINTING: Save final checkpoints
    5. STOPPED: Graceful shutdown complete
    6. FORCED: Forced shutdown (timeout or double signal)
    """

    RUNNING = "running"
    """Normal operation, no shutdown requested."""

    STOPPING = "stopping"
    """Stop accepting new events."""

    DRAINING = "draining"
    """Draining in-flight events."""

    CHECKPOINTING = "checkpointing"
    """Saving final checkpoints."""

    STOPPED = "stopped"
    """Graceful shutdown complete."""

    FORCED = "forced"
    """Forced shutdown due to timeout or double signal."""


class ShutdownReason(Enum):
    """
    Reason for shutdown initiation.

    Tracks what triggered the shutdown sequence, useful for
    debugging, metrics, and operational visibility.
    """

    SIGNAL_SIGTERM = "signal_sigterm"
    """Shutdown triggered by SIGTERM signal (Kubernetes, container orchestrators)."""

    SIGNAL_SIGINT = "signal_sigint"
    """Shutdown triggered by SIGINT signal (Ctrl+C, interactive termination)."""

    PROGRAMMATIC = "programmatic"
    """Shutdown triggered by application code via request_shutdown()."""

    HEALTH_CHECK = "health_check"
    """Shutdown triggered by health check failure (future)."""

    TIMEOUT = "timeout"
    """Shutdown forced due to overall timeout expiration."""

    DOUBLE_SIGNAL = "double_signal"
    """Shutdown forced by receiving second termination signal."""


@dataclass(frozen=True)
class ShutdownResult:
    """
    Result of shutdown operation.

    Provides comprehensive details about the shutdown process including
    timing, counts, reason, and whether the shutdown was forced.

    Attributes:
        phase: Final shutdown phase reached
        duration_seconds: Total shutdown duration in seconds
        subscriptions_stopped: Number of subscriptions stopped
        events_drained: Number of events drained during shutdown
        checkpoints_saved: Number of checkpoints saved
        forced: True if shutdown was forced (timeout or double signal)
        error: Error message if shutdown failed, None otherwise
        reason: The reason that triggered the shutdown
        in_flight_at_start: Number of in-flight events when shutdown started
        events_not_drained: Number of events that could not be drained
    """

    phase: ShutdownPhase
    duration_seconds: float
    subscriptions_stopped: int
    events_drained: int
    checkpoints_saved: int = 0
    forced: bool = False
    error: str | None = None
    reason: ShutdownReason | None = None
    in_flight_at_start: int = 0
    events_not_drained: int = 0

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation of result
        """
        return {
            "phase": self.phase.value,
            "duration_seconds": self.duration_seconds,
            "subscriptions_stopped": self.subscriptions_stopped,
            "events_drained": self.events_drained,
            "checkpoints_saved": self.checkpoints_saved,
            "forced": self.forced,
            "error": self.error,
            "reason": self.reason.value if self.reason else None,
            "in_flight_at_start": self.in_flight_at_start,
            "events_not_drained": self.events_not_drained,
        }


@dataclass
class ShutdownCoordinator:
    """
    Coordinates graceful shutdown of subscriptions.

    Handles:
    - Signal registration (SIGTERM, SIGINT)
    - Phased shutdown with timeout
    - In-flight event draining
    - Periodic checkpoint persistence during drain
    - Final checkpoint persistence

    The shutdown sequence:
    1. Stop accepting new events
    2. Drain in-flight events (with timeout)
       - During drain, periodically save checkpoints at checkpoint_interval
    3. Save final checkpoints
    4. Close connections

    If a second signal is received or timeout is exceeded, forced
    shutdown is triggered.

    Example:
        >>> coordinator = ShutdownCoordinator(timeout=30.0)
        >>> coordinator.register_signals()
        >>> await coordinator.wait_for_shutdown()
        >>> # Shutdown was requested
        >>> result = await coordinator.shutdown(stop_func, drain_func, checkpoint_func)

    Attributes:
        timeout: Total shutdown timeout in seconds
        drain_timeout: Time to wait for in-flight events to drain
        checkpoint_timeout: Time to wait for checkpoint saves
        checkpoint_interval: Interval for periodic checkpoint saves during drain
    """

    timeout: float = 30.0
    """Total shutdown timeout in seconds."""

    drain_timeout: float = 10.0
    """Time to wait for in-flight events to drain."""

    checkpoint_timeout: float = 5.0
    """Time to wait for checkpoint saves."""

    checkpoint_interval: float = 5.0
    """Interval in seconds for periodic checkpoint saves during drain phase."""

    # Internal state
    _phase: ShutdownPhase = field(default=ShutdownPhase.RUNNING, repr=False)
    _shutdown_requested: bool = field(default=False, repr=False)
    _shutdown_reason: ShutdownReason | None = field(default=None, repr=False)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    _on_shutdown_callbacks: list[Callable[[], Awaitable[None]]] = field(
        default_factory=list, repr=False
    )
    _signal_handlers_registered: bool = field(default=False, repr=False)
    _original_handlers: dict[signal.Signals, signal.Handlers | None] = field(
        default_factory=dict, repr=False
    )
    _periodic_checkpoint_task: asyncio.Task[None] | None = field(default=None, repr=False)
    _periodic_checkpoints_saved: int = field(default=0, repr=False)

    # Pre-shutdown hooks storage
    _pre_shutdown_hooks: list[PreShutdownHook] = field(default_factory=list, repr=False)

    # Post-shutdown hooks storage
    _post_shutdown_hooks: list[PostShutdownHook] = field(default_factory=list, repr=False)

    # Metrics tracking state
    _last_metrics_snapshot: ShutdownMetricsSnapshot | None = field(default=None, repr=False)
    _in_flight_at_start: int = field(default=0, repr=False)
    _drain_start_time: float = field(default=0.0, repr=False)
    _drain_duration_seconds: float = field(default=0.0, repr=False)

    # Shutdown deadline support
    _shutdown_deadline: datetime | None = field(default=None, repr=False)

    def register_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """
        Register signal handlers for graceful shutdown.

        Registers handlers for SIGTERM and SIGINT signals. On receiving
        a signal, the shutdown event is set and callbacks are invoked.

        A second signal will trigger forced shutdown.

        Args:
            loop: Event loop to register handlers on. Defaults to the
                  running event loop.

        Note:
            On Windows, signal handling has limitations. The handlers
            will still work but may not catch all signals.
        """
        if self._signal_handlers_registered:
            logger.warning("Signal handlers already registered")
            return

        loop = loop or asyncio.get_running_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(  # type: ignore[misc]
                        self._handle_signal(s)
                    ),
                )
                logger.debug(
                    "Registered signal handler",
                    extra={"signal": sig.name},
                )
            except NotImplementedError:
                # Windows doesn't fully support add_signal_handler
                logger.warning(
                    "Signal handling not fully supported on this platform",
                    extra={"signal": sig.name},
                )

        self._signal_handlers_registered = True
        logger.info("Shutdown signal handlers registered")

    def unregister_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """
        Unregister signal handlers.

        Removes the registered signal handlers for SIGTERM and SIGINT.

        Args:
            loop: Event loop to unregister handlers from. Defaults to
                  the running event loop.
        """
        if not self._signal_handlers_registered:
            return

        loop = loop or asyncio.get_running_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.remove_signal_handler(sig)
                logger.debug(
                    "Removed signal handler",
                    extra={"signal": sig.name},
                )
            except (NotImplementedError, ValueError):
                pass

        self._signal_handlers_registered = False
        logger.info("Shutdown signal handlers unregistered")

    async def _handle_signal(self, sig: signal.Signals) -> None:
        """
        Handle a shutdown signal.

        On first signal, initiates graceful shutdown. On second signal,
        forces immediate shutdown.

        Args:
            sig: The signal received
        """
        if self._shutdown_requested:
            logger.warning(
                "Received second shutdown signal, forcing shutdown",
                extra={
                    "signal": sig.name,
                    "current_phase": self._phase.value,
                },
            )
            self._phase = ShutdownPhase.FORCED
            self._shutdown_reason = ShutdownReason.DOUBLE_SIGNAL
            self._shutdown_event.set()
            return

        # Record reason based on signal type
        if sig == signal.SIGTERM:
            self._shutdown_reason = ShutdownReason.SIGNAL_SIGTERM
        elif sig == signal.SIGINT:
            self._shutdown_reason = ShutdownReason.SIGNAL_SIGINT

        logger.info(
            "Received shutdown signal, initiating graceful shutdown",
            extra={
                "signal": sig.name,
                "timeout": self.timeout,
                "reason": self._shutdown_reason.value if self._shutdown_reason else None,
            },
        )
        self._shutdown_requested = True
        self._shutdown_event.set()

        # Invoke shutdown callbacks
        for callback in self._on_shutdown_callbacks:
            try:
                await callback()
            except Exception as e:
                logger.error(
                    "Shutdown callback error",
                    extra={"error": str(e)},
                    exc_info=True,
                )

    def on_shutdown(self, callback: Callable[[], Awaitable[None]]) -> None:
        """
        Register a callback to be invoked on shutdown signal.

        The callback is an async function that will be called when
        a shutdown signal is received. Callbacks are invoked in
        registration order.

        Args:
            callback: Async function to call on shutdown

        Example:
            >>> async def cleanup():
            ...     await connection.close()
            >>> coordinator.on_shutdown(cleanup)
        """
        self._on_shutdown_callbacks.append(callback)

    def remove_callback(self, callback: Callable[[], Awaitable[None]]) -> bool:
        """
        Remove a registered shutdown callback.

        Args:
            callback: The callback to remove

        Returns:
            True if callback was found and removed, False otherwise
        """
        try:
            self._on_shutdown_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    def on_pre_shutdown(
        self,
        callback: Callable[[], Awaitable[None]],
        timeout: float = 5.0,
    ) -> None:
        """
        Register callback to execute before shutdown begins.

        Pre-shutdown hooks run before the STOPPING phase, allowing
        applications to perform preparatory work such as:
        - Notifying external services of impending shutdown
        - Flushing buffers or caches
        - Releasing locks or resources
        - Sending shutdown notifications to peers

        Hooks execute in registration order. Each hook has an individual
        timeout; if a hook exceeds its timeout, execution continues with
        the next hook. Hook failures are logged but do not prevent shutdown.

        Args:
            callback: Async function to call before shutdown.
                      Should be idempotent and handle its own errors.
            timeout: Maximum seconds to wait for this hook (default 5.0).
                     Set higher for hooks that need more time.

        Raises:
            TypeError: If callback is not an async function.
            ValueError: If timeout is not positive.

        Example:
            >>> async def notify_load_balancer():
            ...     await lb_client.deregister(instance_id)
            ...
            >>> coordinator.on_pre_shutdown(notify_load_balancer, timeout=10.0)

            >>> async def flush_metrics():
            ...     await metrics_exporter.flush()
            ...
            >>> coordinator.on_pre_shutdown(flush_metrics, timeout=2.0)

        Note:
            Pre-shutdown hooks should complete quickly to minimize
            shutdown delay. Consider using fire-and-forget patterns
            for non-critical notifications.
        """
        if not asyncio.iscoroutinefunction(callback):
            raise TypeError(f"Pre-shutdown callback must be async function, got {type(callback)}")

        if timeout <= 0:
            raise ValueError(f"Timeout must be positive, got {timeout}")

        self._pre_shutdown_hooks.append((callback, timeout))

        logger.debug(
            "Registered pre-shutdown hook",
            extra={
                "callback": callback.__name__,
                "timeout": timeout,
                "total_hooks": len(self._pre_shutdown_hooks),
            },
        )

    async def _execute_pre_shutdown_hooks(self) -> None:
        """
        Execute all pre-shutdown hooks in registration order.

        Each hook has its own timeout. Failures are logged but
        do not prevent subsequent hooks or shutdown from proceeding.
        """
        if not self._pre_shutdown_hooks:
            return

        logger.info(
            "Executing pre-shutdown hooks",
            extra={"hook_count": len(self._pre_shutdown_hooks)},
        )

        for callback, timeout in self._pre_shutdown_hooks:
            hook_name = getattr(callback, "__name__", str(callback))

            try:
                logger.debug(
                    "Executing pre-shutdown hook",
                    extra={"hook": hook_name, "timeout": timeout},
                )

                await asyncio.wait_for(callback(), timeout=timeout)

                logger.debug(
                    "Pre-shutdown hook completed",
                    extra={"hook": hook_name},
                )

            except TimeoutError:
                logger.warning(
                    "Pre-shutdown hook timed out",
                    extra={"hook": hook_name, "timeout": timeout},
                )

            except Exception as e:
                logger.error(
                    "Pre-shutdown hook failed",
                    extra={"hook": hook_name, "error": str(e)},
                    exc_info=True,
                )

        logger.info("Pre-shutdown hooks completed")

    def on_post_shutdown(
        self,
        callback: Callable[[ShutdownResult], Awaitable[None]],
    ) -> None:
        """
        Register callback to execute after shutdown completes.

        Post-shutdown hooks run after the shutdown sequence completes,
        including after forced shutdowns. They receive the ShutdownResult
        which contains details about the shutdown process.

        Use cases include:
        - Reporting shutdown metrics to external systems
        - Cleanup of external resources
        - Sending shutdown notifications with result details
        - Logging final shutdown state

        Hooks execute in registration order. Hook failures are logged
        but do not affect the shutdown result.

        Args:
            callback: Async function accepting ShutdownResult.
                      Should handle its own errors gracefully.

        Raises:
            TypeError: If callback is not an async function.

        Example:
            >>> async def report_shutdown(result: ShutdownResult):
            ...     await metrics_service.report_shutdown(
            ...         duration=result.duration_seconds,
            ...         forced=result.forced,
            ...         reason=result.reason.value if result.reason else None,
            ...     )
            ...
            >>> coordinator.on_post_shutdown(report_shutdown)

            >>> async def cleanup_external(result: ShutdownResult):
            ...     if result.forced:
            ...         await emergency_cleanup()
            ...     else:
            ...         await graceful_cleanup()
            ...
            >>> coordinator.on_post_shutdown(cleanup_external)

        Note:
            Post-shutdown hooks have a fixed timeout of 5 seconds each.
            They should complete quickly as the process may terminate soon.
            Hooks run even after forced shutdown to allow critical cleanup.
        """
        if not asyncio.iscoroutinefunction(callback):
            raise TypeError(f"Post-shutdown callback must be async function, got {type(callback)}")

        self._post_shutdown_hooks.append(callback)

        logger.debug(
            "Registered post-shutdown hook",
            extra={
                "callback": callback.__name__,
                "total_hooks": len(self._post_shutdown_hooks),
            },
        )

    async def _execute_post_shutdown_hooks(self, result: ShutdownResult) -> None:
        """
        Execute all post-shutdown hooks with the shutdown result.

        Each hook receives the ShutdownResult. Hooks run even after
        forced shutdown to allow critical cleanup operations.

        Args:
            result: The shutdown result to pass to each hook
        """
        if not self._post_shutdown_hooks:
            return

        logger.info(
            "Executing post-shutdown hooks",
            extra={
                "hook_count": len(self._post_shutdown_hooks),
                "shutdown_forced": result.forced,
            },
        )

        for callback in self._post_shutdown_hooks:
            hook_name = getattr(callback, "__name__", str(callback))

            try:
                logger.debug(
                    "Executing post-shutdown hook",
                    extra={"hook": hook_name},
                )

                # Fixed timeout for post-shutdown hooks
                await asyncio.wait_for(callback(result), timeout=5.0)

                logger.debug(
                    "Post-shutdown hook completed",
                    extra={"hook": hook_name},
                )

            except TimeoutError:
                logger.warning(
                    "Post-shutdown hook timed out",
                    extra={"hook": hook_name, "timeout": 5.0},
                )

            except Exception as e:
                logger.error(
                    "Post-shutdown hook failed",
                    extra={"hook": hook_name, "error": str(e)},
                    exc_info=True,
                )

        logger.info("Post-shutdown hooks completed")

    async def wait_for_shutdown(self) -> None:
        """
        Wait for shutdown signal.

        Blocks until a shutdown signal is received (SIGTERM or SIGINT)
        or request_shutdown() is called programmatically.

        Example:
            >>> coordinator.register_signals()
            >>> await manager.start()
            >>> await coordinator.wait_for_shutdown()
            >>> # Signal received, proceed with shutdown
        """
        await self._shutdown_event.wait()

    def request_shutdown(self, reason: ShutdownReason = ShutdownReason.PROGRAMMATIC) -> None:
        """
        Programmatically request shutdown.

        Triggers the same shutdown sequence as receiving a signal.
        Useful for programmatic shutdown without signals.

        Args:
            reason: The reason for shutdown. Defaults to PROGRAMMATIC.

        Example:
            >>> # In some error handler or health check
            >>> coordinator.request_shutdown()
            >>> # Or with specific reason
            >>> coordinator.request_shutdown(ShutdownReason.HEALTH_CHECK)
        """
        if not self._shutdown_requested:
            self._shutdown_reason = reason
            logger.info(
                "Programmatic shutdown requested",
                extra={"reason": reason.value},
            )
            self._shutdown_requested = True
            self._shutdown_event.set()

    async def _periodic_checkpoint_loop(
        self,
        checkpoint_func: Callable[[], Awaitable[int]],
    ) -> None:
        """
        Background task for periodic checkpoint saves during drain phase.

        Runs continuously during the drain phase, saving checkpoints at
        regular intervals. This ensures checkpoint freshness even during
        a long drain with many in-flight events.

        Args:
            checkpoint_func: Async function to save checkpoints.
                            Should return the number of checkpoints saved.
        """
        logger.info(
            "Starting periodic checkpoint loop during drain",
            extra={"interval_seconds": self.checkpoint_interval},
        )

        while self._phase == ShutdownPhase.DRAINING:
            try:
                # Wait for interval
                await asyncio.sleep(self.checkpoint_interval)

                # Check if still draining after sleep
                if self._phase != ShutdownPhase.DRAINING:
                    break

                # Save checkpoints
                saved = await checkpoint_func()
                self._periodic_checkpoints_saved += saved

                if saved > 0:
                    logger.debug(
                        "Periodic checkpoint save completed during drain",
                        extra={
                            "checkpoints_saved": saved,
                            "total_periodic_checkpoints": self._periodic_checkpoints_saved,
                        },
                    )
            except asyncio.CancelledError:
                logger.debug("Periodic checkpoint loop cancelled")
                break
            except Exception as e:
                logger.warning(
                    "Error in periodic checkpoint loop",
                    extra={"error": str(e)},
                )
                # Continue running - don't crash the loop on errors
                # Brief pause before retry to avoid tight error loops
                await asyncio.sleep(0.5)

        logger.debug(
            "Periodic checkpoint loop ended",
            extra={"total_periodic_checkpoints": self._periodic_checkpoints_saved},
        )

    def _start_periodic_checkpoints(
        self,
        checkpoint_func: Callable[[], Awaitable[int]],
    ) -> None:
        """
        Start the periodic checkpoint background task.

        Only starts if checkpoint_interval > 0 and checkpoint_func is provided.

        Args:
            checkpoint_func: Async function to save checkpoints.
        """
        if self.checkpoint_interval <= 0:
            logger.debug("Periodic checkpoints disabled (interval <= 0)")
            return

        self._periodic_checkpoints_saved = 0
        self._periodic_checkpoint_task = asyncio.create_task(
            self._periodic_checkpoint_loop(checkpoint_func),
            name="periodic-checkpoint-loop",
        )

    async def _stop_periodic_checkpoints(self) -> None:
        """
        Stop the periodic checkpoint background task.

        Cancels the task if running and waits for it to complete.
        """
        if self._periodic_checkpoint_task is None:
            return

        self._periodic_checkpoint_task.cancel()
        try:
            await self._periodic_checkpoint_task
        except asyncio.CancelledError:
            pass
        finally:
            self._periodic_checkpoint_task = None

    async def shutdown(
        self,
        stop_func: Callable[[], Awaitable[None]],
        drain_func: Callable[[], Awaitable[int]] | None = None,
        checkpoint_func: Callable[[], Awaitable[int]] | None = None,
        close_func: Callable[[], Awaitable[None]] | None = None,
    ) -> ShutdownResult:
        """
        Execute graceful shutdown sequence.

        Performs shutdown in phases:
        1. STOPPING: Call stop_func to stop accepting new events
        2. DRAINING: Call drain_func to drain in-flight events
        3. CHECKPOINTING: Call checkpoint_func to save checkpoints
        4. Close connections via close_func

        Each phase has a timeout. If the total shutdown exceeds the
        configured timeout, forced shutdown occurs.

        Args:
            stop_func: Async function to stop accepting new events
            drain_func: Optional async function to drain in-flight events.
                       Should return the number of events drained.
            checkpoint_func: Optional async function to save checkpoints.
                            Should return the number of checkpoints saved.
            close_func: Optional async function to close connections

        Returns:
            ShutdownResult with shutdown details

        Example:
            >>> result = await coordinator.shutdown(
            ...     stop_func=manager.stop,
            ...     drain_func=manager.drain_events,
            ...     checkpoint_func=manager.save_checkpoints,
            ... )
            >>> if result.forced:
            ...     logger.warning("Shutdown was forced")
        """
        start_time = datetime.now(UTC)
        events_drained = 0
        checkpoints_saved = 0
        subscriptions_stopped = 0
        forced = False
        error_msg: str | None = None
        self._drain_duration_seconds = 0.0
        self._drain_start_time = 0.0
        # Note: _in_flight_at_start is NOT reset here - it should be set before shutdown via set_in_flight_count()

        # Get adjusted timeouts based on deadline (if set)
        stop_timeout, drain_timeout, checkpoint_timeout = self._get_adjusted_timeouts()

        # Log if using deadline-adjusted timeouts
        if self._shutdown_deadline is not None:
            logger.info(
                "Using deadline-adjusted timeouts",
                extra={
                    "stop_timeout": stop_timeout,
                    "drain_timeout": drain_timeout,
                    "checkpoint_timeout": checkpoint_timeout,
                    "deadline": self._shutdown_deadline.isoformat(),
                },
            )

        # Record shutdown initiated metric
        try:
            record_shutdown_initiated()
        except Exception as e:
            logger.debug("Failed to record shutdown initiated metric", extra={"error": str(e)})

        try:
            # Execute pre-shutdown hooks before STOPPING phase
            await self._execute_pre_shutdown_hooks()

            # Phase 1: Stop accepting new events
            self._phase = ShutdownPhase.STOPPING
            logger.info(
                "Shutdown phase: stopping",
                extra={"phase": self._phase.value, "timeout": stop_timeout},
            )

            try:
                await asyncio.wait_for(stop_func(), timeout=stop_timeout)
                subscriptions_stopped = 1  # Will be updated by caller if needed
            except TimeoutError:
                logger.warning(
                    "Stop phase timed out",
                    extra={"timeout": stop_timeout},
                )
                forced = True

            # Check if forced shutdown was requested
            if self._phase == ShutdownPhase.FORCED:
                forced = True
                self._phase = ShutdownPhase.FORCED
                return self._create_result(
                    start_time,
                    subscriptions_stopped,
                    events_drained,
                    checkpoints_saved,
                    forced,
                    "Forced by double signal",
                )

            # Phase 2: Drain in-flight events
            # Skip drain if timeout is 0 (critical time constraint)
            if drain_func and not forced and drain_timeout > 0:
                self._phase = ShutdownPhase.DRAINING
                self._drain_start_time = time.perf_counter()
                logger.info(
                    "Shutdown phase: draining in-flight events",
                    extra={
                        "phase": self._phase.value,
                        "timeout": drain_timeout,
                        "checkpoint_interval": self.checkpoint_interval,
                    },
                )

                # Start periodic checkpoints during drain if checkpoint_func provided
                if checkpoint_func and self.checkpoint_interval > 0:
                    self._start_periodic_checkpoints(checkpoint_func)

                try:
                    events_drained = await asyncio.wait_for(
                        drain_func(),
                        timeout=drain_timeout,
                    )
                    logger.info(
                        "Events drained successfully",
                        extra={
                            "events_drained": events_drained,
                            "periodic_checkpoints_saved": self._periodic_checkpoints_saved,
                        },
                    )
                except TimeoutError:
                    logger.warning(
                        "Drain phase timed out",
                        extra={"timeout": drain_timeout},
                    )
                    forced = True
                finally:
                    # Always stop periodic checkpoints when drain completes
                    await self._stop_periodic_checkpoints()
                    # Record drain duration
                    self._drain_duration_seconds = time.perf_counter() - self._drain_start_time
                    try:
                        record_drain_duration(self._drain_duration_seconds)
                        record_events_drained(events_drained)
                    except Exception as e:
                        logger.debug("Failed to record drain metrics", extra={"error": str(e)})
            elif drain_func and drain_timeout == 0:
                logger.info("Skipping drain phase due to time constraint")

            # Check if forced shutdown was requested
            if self._phase == ShutdownPhase.FORCED:
                forced = True
                return self._create_result(
                    start_time,
                    subscriptions_stopped,
                    events_drained,
                    checkpoints_saved,
                    forced,
                    "Forced by double signal",
                )

            # Phase 3: Save final checkpoints
            # Always try checkpoint if time remains (checkpoint_timeout > 0), even after forced drain
            if checkpoint_func and checkpoint_timeout > 0:
                self._phase = ShutdownPhase.CHECKPOINTING
                logger.info(
                    "Shutdown phase: saving checkpoints",
                    extra={
                        "phase": self._phase.value,
                        "timeout": checkpoint_timeout,
                    },
                )

                try:
                    checkpoints_saved = await asyncio.wait_for(
                        checkpoint_func(),
                        timeout=checkpoint_timeout,
                    )
                    logger.info(
                        "Checkpoints saved successfully",
                        extra={"checkpoints_saved": checkpoints_saved},
                    )
                except TimeoutError:
                    logger.warning(
                        "Checkpoint save timed out",
                        extra={"timeout": checkpoint_timeout},
                    )
                    forced = True

            # Phase 4: Close connections
            if close_func:
                try:
                    await asyncio.wait_for(close_func(), timeout=5.0)
                    logger.debug("Connections closed")
                except TimeoutError:
                    logger.warning("Connection close timed out")
                except Exception as e:
                    logger.error(
                        "Error closing connections",
                        extra={"error": str(e)},
                    )

            # Mark as stopped or forced
            if forced:
                self._phase = ShutdownPhase.FORCED
            else:
                self._phase = ShutdownPhase.STOPPED

        except Exception as e:
            logger.error(
                "Shutdown error",
                extra={"error": str(e)},
                exc_info=True,
            )
            self._phase = ShutdownPhase.FORCED
            forced = True
            error_msg = str(e)

        result = self._create_result(
            start_time, subscriptions_stopped, events_drained, checkpoints_saved, forced, error_msg
        )

        # Execute post-shutdown hooks (even if forced)
        try:
            await self._execute_post_shutdown_hooks(result)
        except Exception as e:
            logger.error(
                "Error executing post-shutdown hooks",
                extra={"error": str(e)},
                exc_info=True,
            )

        return result

    def _create_result(
        self,
        start_time: datetime,
        subscriptions_stopped: int,
        events_drained: int,
        checkpoints_saved: int,
        forced: bool,
        error_msg: str | None,
    ) -> ShutdownResult:
        """Create a ShutdownResult from current state."""
        duration = (datetime.now(UTC) - start_time).total_seconds()

        # Determine outcome for metrics
        if forced and error_msg and "double signal" in error_msg.lower():
            outcome = "forced"
        elif forced:
            outcome = "timeout"
        else:
            outcome = "clean"

        # Determine final reason - timeout overrides original reason if forced
        final_reason = self._shutdown_reason
        if forced and self._shutdown_reason != ShutdownReason.DOUBLE_SIGNAL:
            final_reason = ShutdownReason.TIMEOUT

        # Calculate events not drained
        events_not_drained = max(0, self._in_flight_at_start - events_drained)

        # Record shutdown completion metrics
        try:
            record_shutdown_completed(outcome, duration)
        except Exception as e:
            logger.debug("Failed to record shutdown completed metric", extra={"error": str(e)})

        # Create metrics snapshot for inspection
        self._last_metrics_snapshot = ShutdownMetricsSnapshot(
            shutdown_duration_seconds=duration,
            drain_duration_seconds=self._drain_duration_seconds,
            events_drained=events_drained,
            checkpoints_saved=checkpoints_saved,
            in_flight_at_start=self._in_flight_at_start,
            outcome=outcome,
        )

        logger.info(
            "Shutdown complete",
            extra={
                "phase": self._phase.value,
                "duration_seconds": duration,
                "subscriptions_stopped": subscriptions_stopped,
                "events_drained": events_drained,
                "checkpoints_saved": checkpoints_saved,
                "forced": forced,
                "error": error_msg,
                "outcome": outcome,
                "reason": final_reason.value if final_reason else None,
                "in_flight_at_start": self._in_flight_at_start,
                "events_not_drained": events_not_drained,
            },
        )

        return ShutdownResult(
            phase=self._phase,
            duration_seconds=duration,
            subscriptions_stopped=subscriptions_stopped,
            events_drained=events_drained,
            checkpoints_saved=checkpoints_saved,
            forced=forced,
            error=error_msg,
            reason=final_reason,
            in_flight_at_start=self._in_flight_at_start,
            events_not_drained=events_not_drained,
        )

    @property
    def is_shutting_down(self) -> bool:
        """
        Check if shutdown has been requested.

        Returns:
            True if shutdown signal received or requested
        """
        return self._shutdown_requested

    @property
    def phase(self) -> ShutdownPhase:
        """
        Get current shutdown phase.

        Returns:
            Current ShutdownPhase enum value
        """
        return self._phase

    @property
    def is_forced(self) -> bool:
        """
        Check if shutdown was forced.

        Returns:
            True if shutdown is in FORCED phase
        """
        return self._phase == ShutdownPhase.FORCED

    @property
    def last_metrics_snapshot(self) -> ShutdownMetricsSnapshot | None:
        """
        Get the metrics snapshot from the last shutdown operation.

        Returns:
            ShutdownMetricsSnapshot if shutdown has completed, None otherwise
        """
        return self._last_metrics_snapshot

    def set_in_flight_count(self, count: int) -> None:
        """
        Set the in-flight event count before shutdown drain.

        This should be called before or at the start of drain phase
        to record how many events are in-flight when shutdown begins.

        Args:
            count: Number of events currently in flight
        """
        self._in_flight_at_start = count
        try:
            record_in_flight_at_shutdown(count)
        except Exception as e:
            logger.debug("Failed to record in-flight count metric", extra={"error": str(e)})

    def set_shutdown_deadline(self, deadline: datetime) -> None:
        """
        Set absolute deadline for shutdown completion.

        When a deadline is set, the shutdown process will adapt its
        behavior to complete within the available time:
        - Phase timeouts are adjusted proportionally
        - If time is critically short, some phases may be skipped
        - Checkpointing is prioritized over draining

        This is useful when:
        - Receiving cloud termination notices with specific end times
        - Working with Kubernetes terminationGracePeriodSeconds
        - Coordinating with external shutdown timers

        Args:
            deadline: Absolute datetime by which shutdown must complete.
                      Should be timezone-aware (UTC recommended).

        Example:
            >>> # AWS spot termination gives 2 minutes
            >>> deadline = datetime.now(UTC) + timedelta(seconds=120)
            >>> coordinator.set_shutdown_deadline(deadline)

            >>> # Kubernetes terminationGracePeriodSeconds
            >>> deadline = datetime.now(UTC) + timedelta(seconds=30)
            >>> coordinator.set_shutdown_deadline(deadline)

        Note:
            The deadline should be set BEFORE calling shutdown().
            Setting a deadline during shutdown has undefined behavior.
        """
        if deadline.tzinfo is None:
            logger.warning(
                "Shutdown deadline is not timezone-aware, assuming UTC",
                extra={"deadline": deadline.isoformat()},
            )
            deadline = deadline.replace(tzinfo=UTC)

        self._shutdown_deadline = deadline

        remaining = self.get_remaining_shutdown_time()
        logger.info(
            "Shutdown deadline set",
            extra={
                "deadline": deadline.isoformat(),
                "remaining_seconds": remaining,
            },
        )

    def get_remaining_shutdown_time(self) -> float:
        """
        Get seconds remaining until shutdown deadline.

        If no deadline is set, returns the configured timeout.
        Returns 0 if deadline has passed.

        Returns:
            Seconds remaining until deadline, or configured timeout if no deadline.

        Example:
            >>> remaining = coordinator.get_remaining_shutdown_time()
            >>> if remaining < 10:
            ...     logger.warning("Very little time remaining for shutdown")
        """
        if self._shutdown_deadline is None:
            return self.timeout

        now = datetime.now(UTC)
        remaining = (self._shutdown_deadline - now).total_seconds()
        return max(0.0, remaining)

    @property
    def deadline(self) -> datetime | None:
        """
        Get the current shutdown deadline.

        Returns:
            The configured deadline, or None if not set.
        """
        return self._shutdown_deadline

    def _get_adjusted_timeouts(self) -> tuple[float, float, float]:
        """
        Calculate adjusted phase timeouts based on remaining time.

        Returns proportionally adjusted timeouts for:
        - stop phase
        - drain phase
        - checkpoint phase

        If time is critically short, may return 0 for some phases
        to prioritize checkpointing.

        Returns:
            Tuple of (stop_timeout, drain_timeout, checkpoint_timeout)
        """
        remaining = self.get_remaining_shutdown_time()

        if remaining <= 0:
            logger.warning("No time remaining for shutdown, returning zero timeouts")
            return (0.0, 0.0, 0.0)

        # Calculate total configured timeout for the three main phases
        # Stop phase has a hardcoded 5.0s timeout in the current implementation
        default_stop_timeout = 5.0
        total_configured = default_stop_timeout + self.drain_timeout + self.checkpoint_timeout

        if remaining >= total_configured:
            # Plenty of time, use configured values
            return (default_stop_timeout, self.drain_timeout, self.checkpoint_timeout)

        # Need to adjust proportionally
        # Minimum times for each phase
        min_stop = 1.0
        min_checkpoint = 2.0

        if remaining < (min_stop + min_checkpoint):
            # Critical time constraint - only checkpoint
            logger.warning(
                "Critical time constraint, skipping drain phase",
                extra={"remaining_seconds": remaining},
            )
            return (min(remaining * 0.2, 1.0), 0.0, remaining * 0.8)

        # Allocate time: 10% stop, 30% checkpoint, remaining for drain
        adjusted_stop = max(min_stop, remaining * 0.1)
        adjusted_checkpoint = max(min_checkpoint, remaining * 0.3)
        adjusted_drain = remaining - adjusted_stop - adjusted_checkpoint

        logger.info(
            "Adjusted shutdown timeouts for deadline",
            extra={
                "remaining_seconds": remaining,
                "stop_timeout": adjusted_stop,
                "drain_timeout": adjusted_drain,
                "checkpoint_timeout": adjusted_checkpoint,
            },
        )

        return (adjusted_stop, adjusted_drain, adjusted_checkpoint)

    def reset(self) -> None:
        """
        Reset the coordinator for reuse.

        Resets all internal state to initial values. Useful for
        testing or restarting the shutdown coordinator.

        Note:
            Signal handlers are NOT unregistered. Call unregister_signals()
            first if needed.
        """
        self._phase = ShutdownPhase.RUNNING
        self._shutdown_requested = False
        self._shutdown_reason = None
        self._shutdown_event = asyncio.Event()
        self._on_shutdown_callbacks.clear()
        self._pre_shutdown_hooks.clear()
        self._post_shutdown_hooks.clear()
        self._periodic_checkpoint_task = None
        self._periodic_checkpoints_saved = 0
        # Reset metrics state
        self._last_metrics_snapshot = None
        self._in_flight_at_start = 0
        self._drain_start_time = 0.0
        self._drain_duration_seconds = 0.0
        # Reset deadline
        self._shutdown_deadline = None
        logger.debug("Shutdown coordinator reset")


__all__ = [
    "ShutdownPhase",
    "ShutdownReason",
    "ShutdownResult",
    "ShutdownMetricsSnapshot",
    "ShutdownCoordinator",
    # Type aliases
    "PreShutdownHook",
    "PostShutdownHook",
    # Metrics functions
    "OTEL_METRICS_AVAILABLE",
    "record_shutdown_initiated",
    "record_shutdown_completed",
    "record_drain_duration",
    "record_events_drained",
    "record_in_flight_at_shutdown",
    "get_in_flight_at_shutdown",
    "reset_shutdown_metrics",
]
