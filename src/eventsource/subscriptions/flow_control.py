"""
Flow control utilities for subscription backpressure.

Provides mechanisms to limit concurrent event processing and
prevent overwhelming downstream systems.

This module provides:
- FlowControlStats: Statistics for flow control monitoring
- FlowController: Controls event processing rate using backpressure
- FlowControlContext: Async context manager for acquire/release pattern

Example:
    >>> controller = FlowController(max_in_flight=1000, backpressure_threshold=0.8)
    >>> async with await controller.acquire():
    ...     await process_event(event)
    >>> print(controller.stats)  # View flow control statistics
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class FlowControlStats:
    """
    Statistics for flow control monitoring.

    Provides metrics for observability and tuning of backpressure settings.

    Attributes:
        events_in_flight: Current number of events being processed
        peak_in_flight: Maximum concurrent events observed
        pause_count: Number of times processing was paused due to backpressure
        total_pause_time_seconds: Cumulative time spent paused
        total_acquisitions: Total number of slot acquisitions
        total_releases: Total number of slot releases
    """

    events_in_flight: int = 0
    peak_in_flight: int = 0
    pause_count: int = 0
    total_pause_time_seconds: float = 0.0
    total_acquisitions: int = 0
    total_releases: int = 0

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation of statistics
        """
        return {
            "events_in_flight": self.events_in_flight,
            "peak_in_flight": self.peak_in_flight,
            "pause_count": self.pause_count,
            "total_pause_time_seconds": self.total_pause_time_seconds,
            "total_acquisitions": self.total_acquisitions,
            "total_releases": self.total_releases,
        }


class FlowController:
    """
    Controls event processing rate using backpressure.

    Uses a semaphore to limit concurrent in-flight events.
    When the limit is reached, processing pauses until capacity
    is available. Supports automatic backpressure detection based
    on configurable thresholds.

    The FlowController tracks:
    - Current in-flight events
    - Peak in-flight (high water mark)
    - Pause/resume events
    - Total pause time

    Example:
        >>> controller = FlowController(max_in_flight=1000)
        >>> async with await controller.acquire():
        ...     await process_event(event)

        >>> # Check backpressure status
        >>> if controller.is_backpressured:
        ...     logger.warning("System under backpressure")

    Attributes:
        max_in_flight: Maximum concurrent events allowed
        backpressure_threshold: Fraction (0-1) at which to signal backpressure
    """

    def __init__(
        self,
        max_in_flight: int = 1000,
        backpressure_threshold: float = 0.8,
    ) -> None:
        """
        Initialize the flow controller.

        Args:
            max_in_flight: Maximum concurrent events allowed.
                When this limit is reached, acquire() will block.
            backpressure_threshold: Fraction (0.0-1.0) of max_in_flight
                at which backpressure is signaled. Default 0.8 means
                backpressure is detected at 80% capacity.

        Raises:
            ValueError: If max_in_flight < 1 or threshold not in [0, 1]
        """
        if max_in_flight < 1:
            raise ValueError(f"max_in_flight must be >= 1, got {max_in_flight}")
        if not 0.0 <= backpressure_threshold <= 1.0:
            raise ValueError(
                f"backpressure_threshold must be between 0.0 and 1.0, got {backpressure_threshold}"
            )

        self.max_in_flight = max_in_flight
        self.backpressure_threshold = backpressure_threshold

        self._semaphore = asyncio.Semaphore(max_in_flight)
        self._in_flight = 0
        self._lock = asyncio.Lock()
        self._stats = FlowControlStats()

        # Pause tracking
        self._paused = False
        self._pause_start: datetime | None = None

        # Drain tracking
        self._drain_event: asyncio.Event = asyncio.Event()
        self._drain_event.set()  # Initially set (no events in flight)

        # Calculate backpressure trigger point
        self._backpressure_trigger = int(max_in_flight * backpressure_threshold)

        logger.debug(
            "FlowController initialized",
            extra={
                "max_in_flight": max_in_flight,
                "backpressure_threshold": backpressure_threshold,
                "backpressure_trigger": self._backpressure_trigger,
            },
        )

    async def acquire(self) -> "FlowControlContext":
        """
        Acquire a slot for processing.

        Blocks if max_in_flight is reached until a slot becomes available.
        When blocked, the controller is considered paused.

        Returns:
            Context manager that releases slot on exit

        Example:
            >>> async with await controller.acquire():
            ...     # Process event while holding a slot
            ...     await handle_event(event)
            >>> # Slot automatically released
        """
        # Acquire semaphore (may block if at capacity)
        await self._semaphore.acquire()

        async with self._lock:
            self._in_flight += 1
            self._stats.total_acquisitions += 1

            # Clear drain event when we have in-flight events
            self._drain_event.clear()

            # Update peak
            if self._in_flight > self._stats.peak_in_flight:
                self._stats.peak_in_flight = self._in_flight

            # Check if we need to enter pause state (at capacity)
            if self._in_flight >= self.max_in_flight and not self._paused:
                self._enter_paused_state()

        return FlowControlContext(self)

    async def release(self) -> None:
        """
        Release a processing slot.

        Called automatically by FlowControlContext on exit.
        Should not normally be called directly.
        """
        self._semaphore.release()

        async with self._lock:
            self._in_flight -= 1
            self._stats.total_releases += 1
            self._stats.events_in_flight = self._in_flight

            # Signal drain complete when no more in-flight events
            if self._in_flight == 0:
                self._drain_event.set()

            # Exit pause state when we drop below capacity
            if self._paused and self._in_flight < self.max_in_flight:
                self._exit_paused_state()

    def _enter_paused_state(self) -> None:
        """Enter paused state due to backpressure."""
        self._paused = True
        self._pause_start = datetime.now(UTC)
        self._stats.pause_count += 1

        logger.info(
            "Backpressure: pausing",
            extra={
                "in_flight": self._in_flight,
                "max_in_flight": self.max_in_flight,
                "pause_count": self._stats.pause_count,
            },
        )

    def _exit_paused_state(self) -> None:
        """Exit paused state as capacity is available."""
        self._paused = False

        if self._pause_start is not None:
            pause_duration = (datetime.now(UTC) - self._pause_start).total_seconds()
            self._stats.total_pause_time_seconds += pause_duration
            self._pause_start = None

            logger.info(
                "Backpressure: resuming",
                extra={
                    "in_flight": self._in_flight,
                    "pause_duration_seconds": pause_duration,
                },
            )

    @property
    def is_paused(self) -> bool:
        """
        Check if processing is paused due to backpressure.

        Returns:
            True if at capacity and blocking on acquire()
        """
        return self._paused

    @property
    def is_backpressured(self) -> bool:
        """
        Check if system is experiencing backpressure.

        Backpressure is detected when in-flight events exceed
        the configured threshold (default 80% of max_in_flight).

        Returns:
            True if at or above backpressure threshold
        """
        return self._in_flight >= self._backpressure_trigger

    @property
    def in_flight(self) -> int:
        """
        Get current in-flight event count.

        Returns:
            Number of events currently being processed
        """
        return self._in_flight

    @property
    def available_capacity(self) -> int:
        """
        Get available capacity for new events.

        Returns:
            Number of additional events that can be processed
        """
        return max(0, self.max_in_flight - self._in_flight)

    @property
    def utilization(self) -> float:
        """
        Get current utilization as a fraction.

        Returns:
            Fraction of capacity in use (0.0 to 1.0+)
        """
        if self.max_in_flight == 0:
            return 0.0
        return self._in_flight / self.max_in_flight

    @property
    def stats(self) -> FlowControlStats:
        """
        Get flow control statistics.

        Returns a snapshot of current statistics for monitoring.
        Each call returns a new snapshot reflecting the current state.

        Returns:
            FlowControlStats with current metrics
        """
        return FlowControlStats(
            events_in_flight=self._in_flight,
            peak_in_flight=self._stats.peak_in_flight,
            pause_count=self._stats.pause_count,
            total_pause_time_seconds=self._stats.total_pause_time_seconds,
            total_acquisitions=self._stats.total_acquisitions,
            total_releases=self._stats.total_releases,
        )

    def reset_stats(self) -> None:
        """
        Reset statistics to initial values.

        Useful for periodic metric collection where you want
        to track stats per interval.
        """
        current_in_flight = self._in_flight
        self._stats = FlowControlStats(events_in_flight=current_in_flight)

    async def wait_for_capacity(self, min_capacity: int = 1) -> None:
        """
        Wait until at least min_capacity slots are available.

        Useful for batch processing where you want to ensure
        capacity before starting a batch.

        Args:
            min_capacity: Minimum slots to wait for (default 1)

        Raises:
            ValueError: If min_capacity > max_in_flight
        """
        if min_capacity > self.max_in_flight:
            raise ValueError(
                f"min_capacity ({min_capacity}) cannot exceed max_in_flight ({self.max_in_flight})"
            )

        while self.available_capacity < min_capacity:
            # Brief sleep to avoid busy-waiting
            await asyncio.sleep(0.001)

    async def wait_for_drain(self, timeout: float) -> int:
        """
        Wait for all in-flight events to complete.

        Blocks until all in-flight events have been processed or the
        timeout expires. This is used during graceful shutdown to ensure
        all active handlers complete before proceeding.

        Args:
            timeout: Maximum seconds to wait for drain to complete

        Returns:
            Number of events still in-flight when wait completed.
            Returns 0 if all events drained successfully.

        Example:
            >>> # During shutdown
            >>> remaining = await controller.wait_for_drain(timeout=10.0)
            >>> if remaining > 0:
            ...     logger.warning(f"{remaining} events did not drain")
        """
        if self._in_flight == 0:
            return 0

        logger.debug(
            "Waiting for drain",
            extra={
                "in_flight": self._in_flight,
                "timeout": timeout,
            },
        )

        try:
            await asyncio.wait_for(
                self._drain_event.wait(),
                timeout=timeout,
            )
            remaining = 0
        except TimeoutError:
            remaining = self._in_flight

        logger.debug(
            "Drain wait completed",
            extra={
                "remaining": remaining,
                "timed_out": remaining > 0,
            },
        )

        return remaining


@dataclass
class FlowControlContext:
    """
    Async context manager for flow control acquire/release.

    Ensures proper release of flow control slots even if
    processing raises an exception.

    Example:
        >>> context = await controller.acquire()
        >>> async with context:
        ...     await process_event(event)
        >>> # Slot automatically released

    Note:
        You typically don't create this directly. Use
        controller.acquire() which returns a FlowControlContext.
    """

    _controller: FlowController = field(repr=False)

    async def __aenter__(self) -> "FlowControlContext":
        """Enter the context (slot already acquired)."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Exit the context and release the slot."""
        await self._controller.release()


__all__ = [
    "FlowController",
    "FlowControlContext",
    "FlowControlStats",
]
