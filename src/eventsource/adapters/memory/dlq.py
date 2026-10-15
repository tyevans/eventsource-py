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
    """In-memory implementation of DLQ repository for testing."""

    def __init__(self, tracer: Tracer | None = None, enable_tracing: bool = True) -> None:
        """Initialize an empty in-memory DLQ repository."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._entries: dict[str, DLQEntry] = {}
        self._by_id: dict[int | str, DLQEntry] = {}
        self._id_counter: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    def __len__(self) -> int:
        """Return total number of DLQ entries currently stored."""
        return len(self._entries)

    def _make_key(self, event_id: UUID, projection_name: str) -> str:
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
        """Add or update a failed event in the DLQ."""
        attrs = {
            ATTR_EVENT_ID: str(event_id),
            ATTR_EVENT_TYPE: event_type,
            ATTR_PROJECTION_NAME: projection_name,
            ATTR_ERROR_TYPE: str(error)[:100],
            ATTR_RETRY_COUNT: retry_count,
            ATTR_DB_SYSTEM: "memory",
        }
        with self._tracer.span("eventsource.dlq.add", attrs):
            now = datetime.now(UTC)
            key = self._make_key(event_id, projection_name)
            async with self._lock:
                existing = self._entries.get(key)
                if existing:
                    existing.retry_count = retry_count
                    existing.last_failed_at = now
                    existing.error_message = str(error)
                    existing.error_stacktrace = traceback.format_exc()
                    existing.status = "failed"
                else:
                    self._id_counter += 1
                    entry = DLQEntry(
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
                    self._entries[key] = entry
                    self._by_id[self._id_counter] = entry

    async def get_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """Get failed events from the DLQ."""
        span_attributes: dict[str, Any] = {
            "limit": limit,
            "status_filter": status,
            ATTR_DB_SYSTEM: "memory",
        }
        if projection_name:
            span_attributes[ATTR_PROJECTION_NAME] = projection_name
        with self._tracer.span("eventsource.dlq.get", span_attributes):
            async with self._lock:
                entries = [
                    e
                    for e in self._entries.values()
                    if e.status == status
                    and (not projection_name or e.projection_name == projection_name)
                ]
                entries.sort(key=lambda e: e.first_failed_at or datetime.min, reverse=True)
                return entries[:limit]

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        """Get a specific failed event by its DLQ ID via O(1) index lookup."""
        with self._tracer.span(
            "eventsource.dlq.get_by_id", {"dlq.id": str(dlq_id), ATTR_DB_SYSTEM: "memory"}
        ):
            async with self._lock:
                return self._by_id.get(dlq_id)

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        """Mark a DLQ entry as resolved via O(1) index lookup."""
        attrs = {
            "dlq.id": str(dlq_id),
            "resolved_by": str(resolved_by) if resolved_by else None,
            ATTR_DB_SYSTEM: "memory",
        }
        with self._tracer.span("eventsource.dlq.resolve", attrs):
            now = datetime.now(UTC)
            async with self._lock:
                entry = self._by_id.get(dlq_id)
                if entry is not None:
                    entry.status = "resolved"
                    entry.resolved_at = now
                    entry.resolved_by = str(resolved_by) if resolved_by else None

    async def mark_retrying(self, dlq_id: int | str) -> None:
        """Mark a DLQ entry as being retried via O(1) index lookup."""
        with self._tracer.span(
            "eventsource.dlq.retry", {"dlq.id": str(dlq_id), ATTR_DB_SYSTEM: "memory"}
        ):
            async with self._lock:
                entry = self._by_id.get(dlq_id)
                if entry is not None:
                    entry.status = "retrying"

    async def get_failure_stats(self) -> DLQStats:
        """Get aggregate statistics about DLQ health."""
        with self._tracer.span("eventsource.dlq.get_stats", {ATTR_DB_SYSTEM: "memory"}):
            async with self._lock:
                active = [e for e in self._entries.values() if e.status in ("failed", "retrying")]
                dates = [e.first_failed_at for e in active if e.first_failed_at]
                return DLQStats(
                    total_failed=sum(1 for e in active if e.status == "failed"),
                    total_retrying=sum(1 for e in active if e.status == "retrying"),
                    affected_projections=len({e.projection_name for e in active}),
                    oldest_failure=min(dates).isoformat() if dates else None,
                )

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """Get failure counts grouped by projection."""
        with self._tracer.span("eventsource.dlq.get_projection_counts", {ATTR_DB_SYSTEM: "memory"}):
            async with self._lock:
                projection_stats: dict[str, list[DLQEntry]] = {}
                for entry in self._entries.values():
                    if entry.status in ("failed", "retrying"):
                        projection_stats.setdefault(entry.projection_name, []).append(entry)
                result = []
                for projection_name, entries in projection_stats.items():
                    firsts = [e.first_failed_at for e in entries if e.first_failed_at]
                    lasts = [e.last_failed_at for e in entries if e.last_failed_at]
                    result.append(
                        ProjectionFailureCount(
                            projection_name=projection_name,
                            failure_count=len(entries),
                            oldest_failure=min(firsts).isoformat() if firsts else None,
                            most_recent_failure=max(lasts).isoformat() if lasts else None,
                        )
                    )
                result.sort(key=lambda x: x.failure_count, reverse=True)
                return result

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """Delete resolved events older than specified days."""
        with self._tracer.span(
            "eventsource.dlq.delete_resolved",
            {"older_than_days": older_than_days, ATTR_DB_SYSTEM: "memory"},
        ):
            cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
            async with self._lock:
                keys_to_delete = [
                    k
                    for k, e in self._entries.items()
                    if e.status == "resolved" and e.resolved_at and e.resolved_at < cutoff
                ]
                for key in keys_to_delete:
                    removed = self._entries.pop(key)
                    self._by_id.pop(removed.id, None)
                return len(keys_to_delete)

    async def clear(self) -> None:
        """Clear all entries. Useful for test setup/teardown."""
        with self._tracer.span("eventsource.dlq.clear", {ATTR_DB_SYSTEM: "memory"}):
            async with self._lock:
                self._entries.clear()
                self._by_id.clear()
                self._id_counter = 0


__all__ = ["InMemoryDLQRepository"]
