"""Progress, result models, and rate limiting for bulk copy operations."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from uuid import UUID

from eventsource.ports import Position


@dataclass(frozen=True)
class BulkCopyProgress:
    """
    Progress information for bulk copy operation.

    Provides real-time metrics about the bulk copy progress including
    events processed, processing rate, and estimated completion time.

    Attributes:
        migration_id: ID of the migration being processed.
        events_copied: Number of events successfully copied.
        events_total: Total events to copy (0 if not yet counted).
        last_source_position: Last processed position in source store.
        last_target_position: Last written position in target store.
        events_per_second: Current processing rate.
        estimated_remaining_seconds: Estimated time to completion (None if unknown).
        is_complete: Whether the copy operation has finished.
    """

    migration_id: UUID
    events_copied: int
    events_total: int
    last_source_position: Position | None
    last_target_position: Position | None
    events_per_second: float
    estimated_remaining_seconds: float | None
    is_complete: bool

    @property
    def progress_percent(self) -> float:
        """
        Calculate progress as percentage (0-100).

        Returns:
            Progress percentage, or 0.0 if total is unknown.
        """
        if self.events_total == 0:
            return 0.0
        return min(100.0, (self.events_copied / self.events_total) * 100)


@dataclass
class BulkCopyResult:
    """
    Result of a completed bulk copy operation.

    Attributes:
        success: Whether the copy completed successfully.
        events_copied: Total number of events copied.
        last_source_position: Final source position processed.
        last_target_position: Final target position written.
        duration_seconds: Total time taken for the operation.
        error_message: Error message if the operation failed.
    """

    success: bool
    events_copied: int
    last_source_position: Position | None
    last_target_position: Position | None
    duration_seconds: float
    error_message: str | None = None


class RateLimiter:
    """
    Simple token bucket rate limiter for controlling event throughput.

    Limits the rate of events processed per second using a token bucket
    algorithm. Tokens are refilled based on elapsed time.

    Attributes:
        _max_rate: Maximum events allowed per second.
        _tokens: Current available tokens.
        _last_update: Time of last token update.
        _lock: Async lock for thread-safe operation.
    """

    def __init__(self, max_rate: int) -> None:
        """
        Initialize rate limiter.

        Args:
            max_rate: Maximum events per second (must be > 0).
        """
        self._max_rate = max_rate
        self._tokens = float(max_rate)
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait(self, count: int) -> None:
        """
        Wait for capacity to process `count` events.

        If insufficient tokens are available, sleeps until enough
        tokens have been accumulated.

        Args:
            count: Number of events to process.
        """
        if self._max_rate <= 0:
            return  # No rate limiting

        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_update
            self._last_update = now

            # Add tokens based on time elapsed
            self._tokens = min(
                self._max_rate,
                self._tokens + elapsed * self._max_rate,
            )

            # Wait if we need more tokens
            if count > self._tokens:
                wait_time = (count - self._tokens) / self._max_rate
                await asyncio.sleep(wait_time)
                self._tokens = 0
            else:
                self._tokens -= count


__all__ = [
    "BulkCopyProgress",
    "BulkCopyResult",
    "RateLimiter",
]
