"""Result models for catch-up operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.ports.positions import Position


@dataclass(frozen=True)
class _BatchOutcome:
    """What one `_process_batch` call did.

    Two numbers, because they answer different questions and the public
    `CatchUpResult.events_processed` is defined by the second: how many
    envelopes the batch read (whether or not the filter passed them), and
    how many events reached the subscriber.
    """

    envelopes_read: int
    events_delivered: int


@dataclass
class CatchUpResult:
    """
    Result of a catch-up operation.

    Provides statistics and outcome information for a catch-up run.

    Attributes:
        events_processed: Number of events successfully processed
        final_position: Last processed global-feed position, None if nothing
            has been processed
        completed: True if caught up to target position
        error: Exception if catch-up failed, None otherwise
    """

    events_processed: int
    final_position: Position | None
    completed: bool
    error: Exception | None = None

    @property
    def success(self) -> bool:
        """Return True if catch-up completed without errors."""
        return self.completed and self.error is None


__all__ = [
    "CatchUpResult",
    "_BatchOutcome",
]
