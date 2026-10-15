"""Concurrency accounting and drain tracking for subscription shutdown."""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class FlowControlStats:
    """Counters for flow control monitoring."""

    events_in_flight: int = 0
    total_acquisitions: int = 0
    total_releases: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "events_in_flight": self.events_in_flight,
            "total_acquisitions": self.total_acquisitions,
            "total_releases": self.total_releases,
        }


class FlowController:
    """Tracks in-flight events and lets graceful shutdown wait for drain."""

    def __init__(self) -> None:
        """Initialize the flow controller."""
        self._in_flight = 0
        self._lock = asyncio.Lock()
        self._stats = FlowControlStats()
        self._drain_event: asyncio.Event = asyncio.Event()
        self._drain_event.set()
        logger.debug("FlowController initialized")

    async def acquire(self) -> "FlowControlContext":
        """Acquire a slot for processing and return a context manager that releases on exit."""
        async with self._lock:
            self._in_flight += 1
            self._stats.total_acquisitions += 1
            self._drain_event.clear()
        return FlowControlContext(self)

    async def release(self) -> None:
        """Release a processing slot."""
        async with self._lock:
            self._in_flight -= 1
            self._stats.total_releases += 1
            self._stats.events_in_flight = self._in_flight
            if self._in_flight == 0:
                self._drain_event.set()

    @property
    def in_flight(self) -> int:
        """Get current in-flight event count."""
        return self._in_flight

    @property
    def stats(self) -> FlowControlStats:
        """Get a snapshot of current flow control statistics."""
        return FlowControlStats(
            events_in_flight=self._in_flight,
            total_acquisitions=self._stats.total_acquisitions,
            total_releases=self._stats.total_releases,
        )

    def reset_stats(self) -> None:
        """Reset statistics to initial values while preserving current in-flight count."""
        self._stats = FlowControlStats(events_in_flight=self._in_flight)

    async def wait_for_drain(self, timeout: float) -> int:
        """Wait for all in-flight events to complete or timeout to expire."""
        if self._in_flight == 0:
            return 0

        logger.debug(
            "Waiting for drain",
            extra={"in_flight": self._in_flight, "timeout": timeout},
        )

        try:
            await asyncio.wait_for(self._drain_event.wait(), timeout=timeout)
            remaining = 0
        except TimeoutError:
            remaining = self._in_flight

        logger.debug(
            "Drain wait completed",
            extra={"remaining": remaining, "timed_out": remaining > 0},
        )
        return remaining


@dataclass
class FlowControlContext:
    """Async context manager for flow control acquire/release."""

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
