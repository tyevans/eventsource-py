"""Data models for dual-write failure tracking and statistics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from eventsource.ports import Position


@dataclass
class FailedWrite:
    """
    Records a failed write to the target store.

    Used for tracking and monitoring failed dual-writes, enabling
    background recovery via the BulkCopier catch-up mechanism.

    Attributes:
        timestamp: When the failure occurred.
        aggregate_id: The aggregate that was being written to.
        aggregate_type: Type of the aggregate.
        event_ids: IDs of events that failed to write.
        error_message: The error message from the failed write.
        source_position: Position of the FIRST event of the source append
            (`AppendResult.position`), not the position after the write.
            None when the source store has no global feed.
    """

    timestamp: datetime
    aggregate_id: UUID
    aggregate_type: str
    event_ids: list[UUID]
    error_message: str
    source_position: Position | None


@dataclass
class FailureStats:
    """
    Statistics about dual-write failures.

    Provides aggregate metrics for monitoring dual-write health.

    Attributes:
        total_failures: Total number of failed target writes.
        total_events_failed: Total number of events that failed to write.
        first_failure_at: Timestamp of the first failure.
        last_failure_at: Timestamp of the most recent failure.
        unique_aggregates_affected: Number of unique aggregates affected.
    """

    total_failures: int = 0
    total_events_failed: int = 0
    first_failure_at: datetime | None = None
    last_failure_at: datetime | None = None
    unique_aggregates_affected: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "total_failures": self.total_failures,
            "total_events_failed": self.total_events_failed,
            "first_failure_at": (
                self.first_failure_at.isoformat() if self.first_failure_at else None
            ),
            "last_failure_at": (self.last_failure_at.isoformat() if self.last_failure_at else None),
            "unique_aggregates_affected": self.unique_aggregates_affected,
        }


__all__ = [
    "FailedWrite",
    "FailureStats",
]
