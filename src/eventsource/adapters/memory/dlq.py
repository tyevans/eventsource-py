"""In-memory DLQ adapter."""

import asyncio
import traceback
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from eventsource.adapters.serialization import json_dumps
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_DB_SYSTEM,
    ATTR_ERROR_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.ports.dlq import DLQEntry, DLQStats, ProjectionFailureCount


class InMemoryDLQRepository:
    """
    In-memory implementation of DLQ repository for testing.

    Stores failed events in memory. All data is lost when process terminates.

    Example:
        >>> repo = InMemoryDLQRepository()
        >>> await repo.add_failed_event(
        ...     event_id=event_id,
        ...     projection_name="MyProjection",
        ...     event_type="MyEvent",
        ...     event_data={},
        ...     error=Exception("Test error"),
        ... )
    """

    def __init__(
        self,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize an empty in-memory DLQ repository.

        Args:
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._entries: dict[str, DLQEntry] = {}  # key: "{event_id}:{projection_name}"
        self._id_counter: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    def _make_key(self, event_id: UUID, projection_name: str) -> str:
        """Create a unique key for event_id + projection_name combination."""
        return f"{event_id}:{projection_name}"

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

        Args:
            event_id: Event ID that failed
            projection_name: Name of projection that failed to process it
            event_type: Type of event
            event_data: Event data as dict
            error: Exception that occurred
            retry_count: Number of retry attempts
        """
        with self._tracer.span(
            "eventsource.dlq.add",
            {
                ATTR_EVENT_ID: str(event_id),
                ATTR_EVENT_TYPE: event_type,
                ATTR_PROJECTION_NAME: projection_name,
                ATTR_ERROR_TYPE: str(error)[:100],
                ATTR_RETRY_COUNT: retry_count,
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            now = datetime.now(UTC)
            key = self._make_key(event_id, projection_name)

            async with self._lock:
                existing = self._entries.get(key)
                if existing:
                    # Update existing entry
                    existing.retry_count = retry_count
                    existing.last_failed_at = now
                    existing.error_message = str(error)
                    existing.error_stacktrace = traceback.format_exc()
                    existing.status = "failed"
                else:
                    # Create new entry
                    self._id_counter += 1
                    self._entries[key] = DLQEntry(
                        id=self._id_counter,
                        event_id=event_id,
                        projection_name=projection_name,
                        event_type=event_type,
                        event_data=json_dumps(event_data),
                        error_message=str(error),
                        error_stacktrace=traceback.format_exc(),
                        retry_count=retry_count,
                        first_failed_at=now,
                        last_failed_at=now,
                        status="failed",
                    )

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
        span_attributes: dict[str, Any] = {
            "limit": limit,
            "status_filter": status,
            ATTR_DB_SYSTEM: "memory",
        }
        if projection_name:
            span_attributes[ATTR_PROJECTION_NAME] = projection_name

        with self._tracer.span("eventsource.dlq.get", span_attributes):
            async with self._lock:
                entries = list(self._entries.values())

                # Filter by status
                entries = [e for e in entries if e.status == status]

                # Filter by projection name if provided
                if projection_name:
                    entries = [e for e in entries if e.projection_name == projection_name]

                # Sort by first_failed_at descending
                entries.sort(key=lambda e: e.first_failed_at or datetime.min, reverse=True)

                # Apply limit
                return entries[:limit]

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        """
        Get a specific failed event by its DLQ ID.

        Args:
            dlq_id: DLQ record ID

        Returns:
            DLQEntry instance, or None if not found
        """
        with self._tracer.span(
            "eventsource.dlq.get_by_id",
            {
                "dlq.id": str(dlq_id),
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            async with self._lock:
                for entry in self._entries.values():
                    if entry.id == dlq_id:
                        return entry
                return None

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        """
        Mark a DLQ entry as resolved.

        Args:
            dlq_id: DLQ record ID
            resolved_by: User ID or identifier of resolver
        """
        with self._tracer.span(
            "eventsource.dlq.resolve",
            {
                "dlq.id": str(dlq_id),
                "resolved_by": str(resolved_by) if resolved_by else None,
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            now = datetime.now(UTC)
            async with self._lock:
                for entry in self._entries.values():
                    if entry.id == dlq_id:
                        entry.status = "resolved"
                        entry.resolved_at = now
                        entry.resolved_by = str(resolved_by) if resolved_by else None
                        break

    async def mark_retrying(self, dlq_id: int | str) -> None:
        """
        Mark a DLQ entry as being retried.

        Args:
            dlq_id: DLQ record ID
        """
        with self._tracer.span(
            "eventsource.dlq.retry",
            {
                "dlq.id": str(dlq_id),
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            async with self._lock:
                for entry in self._entries.values():
                    if entry.id == dlq_id:
                        entry.status = "retrying"
                        break

    async def get_failure_stats(self) -> DLQStats:
        """
        Get aggregate statistics about DLQ health.

        Returns:
            DLQStats with failure statistics
        """
        with self._tracer.span(
            "eventsource.dlq.get_stats",
            {ATTR_DB_SYSTEM: "memory"},
        ):
            async with self._lock:
                active_entries = [
                    e for e in self._entries.values() if e.status in ("failed", "retrying")
                ]

                total_failed = sum(1 for e in active_entries if e.status == "failed")
                total_retrying = sum(1 for e in active_entries if e.status == "retrying")
                affected_projections = len({e.projection_name for e in active_entries})

                oldest_failure = None
                if active_entries:
                    failures_with_dates = [
                        e.first_failed_at for e in active_entries if e.first_failed_at
                    ]
                    if failures_with_dates:
                        oldest_failure = min(failures_with_dates).isoformat()

                return DLQStats(
                    total_failed=total_failed,
                    total_retrying=total_retrying,
                    affected_projections=affected_projections,
                    oldest_failure=oldest_failure,
                )

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """
        Get failure counts grouped by projection.

        Returns:
            List of ProjectionFailureCount for each affected projection
        """
        with self._tracer.span(
            "eventsource.dlq.get_projection_counts",
            {ATTR_DB_SYSTEM: "memory"},
        ):
            async with self._lock:
                active_entries = [
                    e for e in self._entries.values() if e.status in ("failed", "retrying")
                ]

                # Group by projection
                projection_stats: dict[str, list[DLQEntry]] = {}
                for entry in active_entries:
                    if entry.projection_name not in projection_stats:
                        projection_stats[entry.projection_name] = []
                    projection_stats[entry.projection_name].append(entry)

                result = []
                for projection_name, entries in projection_stats.items():
                    failures_with_first = [e.first_failed_at for e in entries if e.first_failed_at]
                    failures_with_last = [e.last_failed_at for e in entries if e.last_failed_at]

                    result.append(
                        ProjectionFailureCount(
                            projection_name=projection_name,
                            failure_count=len(entries),
                            oldest_failure=(
                                min(failures_with_first).isoformat()
                                if failures_with_first
                                else None
                            ),
                            most_recent_failure=(
                                max(failures_with_last).isoformat() if failures_with_last else None
                            ),
                        )
                    )

                # Sort by failure count descending
                result.sort(key=lambda x: x.failure_count, reverse=True)
                return result

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """
        Delete resolved events older than specified days.

        Args:
            older_than_days: Delete resolved events older than this many days

        Returns:
            Number of events deleted
        """
        with self._tracer.span(
            "eventsource.dlq.delete_resolved",
            {
                "older_than_days": older_than_days,
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            cutoff = datetime.now(UTC) - timedelta(days=older_than_days)

            deleted = 0
            async with self._lock:
                keys_to_delete = []
                for key, entry in self._entries.items():
                    if (
                        entry.status == "resolved"
                        and entry.resolved_at
                        and entry.resolved_at < cutoff
                    ):
                        keys_to_delete.append(key)

                for key in keys_to_delete:
                    del self._entries[key]
                    deleted += 1

            return deleted

    async def clear(self) -> None:
        """Clear all entries. Useful for test setup/teardown."""
        with self._tracer.span(
            "eventsource.dlq.clear",
            {ATTR_DB_SYSTEM: "memory"},
        ):
            async with self._lock:
                self._entries.clear()
                self._id_counter = 0


__all__ = ["InMemoryDLQRepository"]
