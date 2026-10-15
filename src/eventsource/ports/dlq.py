"""Dead letter queue port (pure boundary interface: stdlib, typing, uuid, datetime, dataclasses)."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@dataclass
class DLQEntry:
    """Data structure for a dead letter queue entry."""

    id: int | str
    event_id: UUID
    projection_name: str
    event_type: str
    event_data: str | dict[str, Any]
    error_message: str
    error_stacktrace: str | None = None
    retry_count: int = 0
    first_failed_at: datetime | None = None
    last_failed_at: datetime | None = None
    status: str = "failed"
    resolved_at: datetime | None = None
    resolved_by: str | None = None


@dataclass(frozen=True)
class DLQStats:
    """Aggregate statistics for the dead letter queue."""

    total_failed: int = 0
    total_retrying: int = 0
    affected_projections: int = 0
    oldest_failure: str | None = None


@dataclass(frozen=True)
class ProjectionFailureCount:
    """Failure count and timestamps for a specific projection."""

    projection_name: str
    failure_count: int = 0
    oldest_failure: str | None = None
    most_recent_failure: str | None = None


@runtime_checkable
class DLQRepository(Protocol):
    """Protocol for dead letter queue repositories storing failed events for review and replay."""

    async def add_failed_event(
        self,
        event_id: UUID,
        projection_name: str,
        event_type: str,
        event_data: dict[str, Any],
        error: Exception,
        retry_count: int = 0,
    ) -> None:
        """Add or update (upsert) a failed event in the DLQ with retry count and error info."""
        ...

    async def get_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """Get failed events from the DLQ filtered by optional projection_name and status."""
        ...

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        """Get a specific failed event by its DLQ ID, or None if not found."""
        ...

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        """Mark a DLQ entry as resolved by the specified user or identifier."""
        ...

    async def mark_retrying(self, dlq_id: int | str) -> None:
        """Mark a DLQ entry as being retried."""
        ...

    async def get_failure_stats(self) -> DLQStats:
        """Get aggregate statistics about DLQ health."""
        ...

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """Get failure counts grouped by projection."""
        ...

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """Delete resolved events older than `datetime.now(UTC) - timedelta(days=older_than_days)`.

        Deletes entries where `status == "resolved"` and `resolved_at` is strictly before the
        rolling cutoff (`older_than_days=0` deletes all already-resolved entries).
        """
        ...


__all__ = [
    "DLQEntry",
    "DLQRepository",
    "DLQStats",
    "ProjectionFailureCount",
]
