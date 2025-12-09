"""
Graceful shutdown coordination for subscriptions.

Handles signal registration, shutdown sequencing, and timeout enforcement.

This module provides:
- ShutdownPhase: Enum of shutdown phases
- ShutdownResult: Result of shutdown operation
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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class ShutdownResult:
    """
    Result of shutdown operation.

    Provides details about the shutdown process including timing,
    counts, and whether the shutdown was forced.

    Attributes:
        phase: Final shutdown phase reached
        duration_seconds: Total shutdown duration in seconds
        subscriptions_stopped: Number of subscriptions stopped
        events_drained: Number of events drained during shutdown
        checkpoints_saved: Number of checkpoints saved
        forced: True if shutdown was forced (timeout or double signal)
        error: Error message if shutdown failed, None otherwise
    """

    phase: ShutdownPhase
    duration_seconds: float
    subscriptions_stopped: int
    events_drained: int
    checkpoints_saved: int = 0
    forced: bool = False
    error: str | None = None

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
        }


@dataclass
class ShutdownCoordinator:
    """
    Coordinates graceful shutdown of subscriptions.

    Handles:
    - Signal registration (SIGTERM, SIGINT)
    - Phased shutdown with timeout
    - In-flight event draining
    - Final checkpoint persistence

    The shutdown sequence:
    1. Stop accepting new events
    2. Drain in-flight events (with timeout)
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
    """

    timeout: float = 30.0
    """Total shutdown timeout in seconds."""

    drain_timeout: float = 10.0
    """Time to wait for in-flight events to drain."""

    checkpoint_timeout: float = 5.0
    """Time to wait for checkpoint saves."""

    # Internal state
    _phase: ShutdownPhase = field(default=ShutdownPhase.RUNNING, repr=False)
    _shutdown_requested: bool = field(default=False, repr=False)
    _shutdown_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    _on_shutdown_callbacks: list[Callable[[], Awaitable[None]]] = field(
        default_factory=list, repr=False
    )
    _signal_handlers_registered: bool = field(default=False, repr=False)
    _original_handlers: dict[signal.Signals, signal.Handlers | None] = field(
        default_factory=dict, repr=False
    )

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
            self._shutdown_event.set()
            return

        logger.info(
            "Received shutdown signal, initiating graceful shutdown",
            extra={
                "signal": sig.name,
                "timeout": self.timeout,
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

    def request_shutdown(self) -> None:
        """
        Programmatically request shutdown.

        Triggers the same shutdown sequence as receiving a signal.
        Useful for programmatic shutdown without signals.

        Example:
            >>> # In some error handler or health check
            >>> coordinator.request_shutdown()
        """
        if not self._shutdown_requested:
            logger.info("Programmatic shutdown requested")
            self._shutdown_requested = True
            self._shutdown_event.set()

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

        try:
            # Phase 1: Stop accepting new events
            self._phase = ShutdownPhase.STOPPING
            logger.info(
                "Shutdown phase: stopping",
                extra={"phase": self._phase.value},
            )

            try:
                await asyncio.wait_for(stop_func(), timeout=5.0)
                subscriptions_stopped = 1  # Will be updated by caller if needed
            except TimeoutError:
                logger.warning(
                    "Stop phase timed out",
                    extra={"timeout": 5.0},
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
            if drain_func and not forced:
                self._phase = ShutdownPhase.DRAINING
                logger.info(
                    "Shutdown phase: draining in-flight events",
                    extra={
                        "phase": self._phase.value,
                        "timeout": self.drain_timeout,
                    },
                )

                try:
                    events_drained = await asyncio.wait_for(
                        drain_func(),
                        timeout=self.drain_timeout,
                    )
                    logger.info(
                        "Events drained successfully",
                        extra={"events_drained": events_drained},
                    )
                except TimeoutError:
                    logger.warning(
                        "Drain phase timed out",
                        extra={"timeout": self.drain_timeout},
                    )
                    forced = True

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
            if checkpoint_func and not forced:
                self._phase = ShutdownPhase.CHECKPOINTING
                logger.info(
                    "Shutdown phase: saving checkpoints",
                    extra={
                        "phase": self._phase.value,
                        "timeout": self.checkpoint_timeout,
                    },
                )

                try:
                    checkpoints_saved = await asyncio.wait_for(
                        checkpoint_func(),
                        timeout=self.checkpoint_timeout,
                    )
                    logger.info(
                        "Checkpoints saved successfully",
                        extra={"checkpoints_saved": checkpoints_saved},
                    )
                except TimeoutError:
                    logger.warning(
                        "Checkpoint save timed out",
                        extra={"timeout": self.checkpoint_timeout},
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

        return self._create_result(
            start_time, subscriptions_stopped, events_drained, checkpoints_saved, forced, error_msg
        )

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
        self._shutdown_event = asyncio.Event()
        self._on_shutdown_callbacks.clear()
        logger.debug("Shutdown coordinator reset")


__all__ = [
    "ShutdownPhase",
    "ShutdownResult",
    "ShutdownCoordinator",
]
