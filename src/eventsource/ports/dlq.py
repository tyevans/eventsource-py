"""Dead letter queue port.

Pure boundary interface: stdlib, typing, uuid, datetime, dataclasses only.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@dataclass
class DLQEntry:
    """
    Data structure for a dead letter queue entry.

    Attributes:
        id: Unique DLQ entry identifier
        event_id: Original event ID that failed
        projection_name: Name of the projection that failed
        event_type: Type of the failed event
        event_data: Serialized event data (JSON string or dict)
        error_message: Error message from the failure
        error_stacktrace: Full stack trace of the error
        retry_count: Number of retry attempts made
        first_failed_at: When the event first failed
        last_failed_at: When the event most recently failed
        status: Current status (failed, retrying, resolved)
        resolved_at: When the entry was resolved (if applicable)
        resolved_by: Who resolved the entry (if applicable)
    """

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
    """
    Aggregate statistics for the dead letter queue.

    Attributes:
        total_failed: Total number of entries in failed status
        total_retrying: Total number of entries being retried
        affected_projections: Number of unique projections with failures
        oldest_failure: Timestamp of the oldest failure
    """

    total_failed: int = 0
    total_retrying: int = 0
    affected_projections: int = 0
    oldest_failure: str | None = None


@dataclass(frozen=True)
class ProjectionFailureCount:
    """
    Failure count for a specific projection.

    Attributes:
        projection_name: Name of the projection
        failure_count: Number of failures for this projection
        oldest_failure: Timestamp of oldest failure
        most_recent_failure: Timestamp of most recent failure
    """

    projection_name: str
    failure_count: int = 0
    oldest_failure: str | None = None
    most_recent_failure: str | None = None


@runtime_checkable
class DLQRepository(Protocol):
    """
    Protocol for dead letter queue repositories.

    DLQ repositories store events that failed processing after all retries,
    enabling manual review, replay, and resolution.
    """

    async def add_failed_event(
        self,
        event_id: UUID,
        projection_name: str,
        event_type: str,
        event_data: dict[str, Any],
        error: Exception,
        retry_count: int = 0,
    ) -> None:
        """
        Add or update a failed event in the DLQ.

        Uses UPSERT pattern - if event already exists for this projection,
        updates the retry count and error information.

        Args:
            event_id: Event ID that failed
            projection_name: Name of projection that failed to process it
            event_type: Type of event
            event_data: Event data as dict
            error: Exception that occurred
            retry_count: Number of retry attempts
        """
        ...

    async def get_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """
        Get failed events from the DLQ.

        Args:
            projection_name: Filter by projection name (optional)
            status: Filter by status (default: "failed")
            limit: Maximum number of events to return

        Returns:
            List of DLQEntry instances
        """
        ...

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        """
        Get a specific failed event by its DLQ ID.

        Args:
            dlq_id: DLQ record ID

        Returns:
            DLQEntry instance, or None if not found
        """
        ...

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        """
        Mark a DLQ entry as resolved.

        Args:
            dlq_id: DLQ record ID
            resolved_by: User ID or identifier of resolver
        """
        ...

    async def mark_retrying(self, dlq_id: int | str) -> None:
        """
        Mark a DLQ entry as being retried.

        Args:
            dlq_id: DLQ record ID
        """
        ...

    async def get_failure_stats(self) -> DLQStats:
        """
        Get aggregate statistics about DLQ health.

        Returns:
            DLQStats with failure statistics
        """
        ...

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """
        Get failure counts grouped by projection.

        Returns:
            List of ProjectionFailureCount for each affected projection
        """
        ...

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """
        Delete resolved events older than specified days.

        Useful for periodic cleanup to prevent DLQ table growth.

        Args:
            older_than_days: Delete resolved events older than this many days

        Returns:
            Number of events deleted
        """
        ...


__all__ = [
    "DLQEntry",
    "DLQRepository",
    "DLQStats",
    "ProjectionFailureCount",
]
