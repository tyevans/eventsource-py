"""
Concurrency accounting and drain tracking for subscription shutdown.

Both subscription runners deliver events sequentially -- each awaits
handle() to completion before the next iteration -- so there is never more
than one event in flight per subscription and nothing here ever blocks.
FlowController exists to give graceful shutdown a place to wait for
in-flight work to finish; it is not a concurrency limiter.

This module provides:
- FlowControlStats: Counters for slot acquisitions/releases
- FlowController: Tracks in-flight events and lets shutdown wait for drain
- FlowControlContext: Async context manager for acquire/release pattern

Example:
    >>> controller = FlowController()
    >>> async with await controller.acquire():
    ...     await process_event(event)
    >>> print(controller.stats)  # View flow control statistics
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class FlowControlStats:
    """
    Counters for flow control monitoring.

    Attributes:
        events_in_flight: Current number of events being processed
        total_acquisitions: Total number of slot acquisitions
        total_releases: Total number of slot releases
    """

    events_in_flight: int = 0
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
            "total_acquisitions": self.total_acquisitions,
            "total_releases": self.total_releases,
        }


class FlowController:
    """
    Tracks in-flight events and lets graceful shutdown wait for drain.

    Both subscription runners await each event's handle() to completion
    before starting the next, so at most one event is ever in flight per
    subscription -- acquire() never blocks. What this class provides is
    the drain latch `wait_for_drain` uses during shutdown, plus simple
    acquisition/release counters.

    Example:
        >>> controller = FlowController()
        >>> async with await controller.acquire():
        ...     await process_event(event)
    """

    def __init__(self) -> None:
        """Initialize the flow controller."""
        self._in_flight = 0
        self._lock = asyncio.Lock()
        self._stats = FlowControlStats()

        # Drain tracking
        self._drain_event: asyncio.Event = asyncio.Event()
        self._drain_event.set()  # Initially set (no events in flight)

        logger.debug("FlowController initialized")

    async def acquire(self) -> "FlowControlContext":
        """
        Acquire a slot for processing.

        Returns:
            Context manager that releases slot on exit

        Example:
            >>> async with await controller.acquire():
            ...     # Process event while holding a slot
            ...     await handle_event(event)
            >>> # Slot automatically released
        """
        async with self._lock:
            self._in_flight += 1
            self._stats.total_acquisitions += 1

            # Clear drain event when we have in-flight events
            self._drain_event.clear()

        return FlowControlContext(self)

    async def release(self) -> None:
        """
        Release a processing slot.

        Called automatically by FlowControlContext on exit.
        Should not normally be called directly.
        """
        async with self._lock:
            self._in_flight -= 1
            self._stats.total_releases += 1
            self._stats.events_in_flight = self._in_flight

            # Signal drain complete when no more in-flight events
            if self._in_flight == 0:
                self._drain_event.set()

    @property
    def in_flight(self) -> int:
        """
        Get current in-flight event count.

        Returns:
            Number of events currently being processed
        """
        return self._in_flight

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
