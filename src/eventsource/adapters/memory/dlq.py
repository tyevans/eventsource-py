"""In-memory DLQ adapter for tests. Not for production use."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from eventsource.adapters._common.dlq_tenant import entry_visible_now, refuse_cleanup_under_scope
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
from eventsource.observability.error_summary import error_summary
from eventsource.observability.stack_trace import frames_only_trace
from eventsource.ports.dlq import DLQEntry, DLQStats, ProjectionFailureCount


class InMemoryDLQRepository:
    """In-memory implementation of DLQ repository for testing."""

    def __init__(self, tracer: Tracer | None = None, enable_tracing: bool = True) -> None:
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._entries: dict[str, DLQEntry] = {}
        self._by_id: dict[int | str, DLQEntry] = {}
        self._id_counter: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    def __len__(self) -> int:
        return len(self._entries)

    def __bool__(self) -> bool:
        return True

    async def add_failed_event(self, event_id: UUID, projection_name: str, event_type: str, event_data: dict[str, Any], error: Exception, retry_count: int = 0, *, tenant_id: UUID | None = None) -> None:  # fmt: skip
        """Add or update a failed event in the DLQ."""
        attrs = {
            ATTR_EVENT_ID: str(event_id), ATTR_EVENT_TYPE: event_type,
            ATTR_PROJECTION_NAME: projection_name, ATTR_ERROR_TYPE: error_summary(error)[:100],
            ATTR_RETRY_COUNT: retry_count, ATTR_DB_SYSTEM: "memory",
        }  # fmt: skip
        with self._tracer.span("eventsource.dlq.add", attrs):
            now, key = datetime.now(UTC), f"{event_id}:{projection_name}"
            async with self._lock:
                if existing := self._entries.get(key):
                    existing.retry_count, existing.last_failed_at = retry_count, now
                    existing.error_message, existing.status = error_summary(error), "failed"
                    existing.error_stacktrace = frames_only_trace()
                else:
                    self._id_counter += 1
                    entry = DLQEntry(
                        self._id_counter, event_id, projection_name, event_type,
                        json_dumps(event_data), error_summary(error), frames_only_trace(),
                        retry_count, now, now, "failed", tenant_id=tenant_id,
                    )  # fmt: skip
                    self._entries[key] = self._by_id[self._id_counter] = entry

    async def get_failed_events(
        self, projection_name: str | None = None, status: str = "failed", limit: int = 100
    ) -> list[DLQEntry]:
        """Get failed events from the DLQ."""
        attrs: dict[str, Any] = {"limit": limit, "status_filter": status, ATTR_DB_SYSTEM: "memory"}
        if projection_name:
            attrs[ATTR_PROJECTION_NAME] = projection_name
        with self._tracer.span("eventsource.dlq.get", attrs):
            async with self._lock:
                entries = [
                    e for e in self._entries.values()
                    if entry_visible_now(e) and e.status == status and (not projection_name or e.projection_name == projection_name)
                ]  # fmt: skip
                entries.sort(key=lambda e: e.first_failed_at or datetime.min, reverse=True)
                return entries[:limit]

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        """Get a specific failed event by its DLQ ID via O(1) index lookup."""
        with self._tracer.span("eventsource.dlq.get_by_id", {"dlq.id": str(dlq_id), ATTR_DB_SYSTEM: "memory"}):  # fmt: skip
            async with self._lock:
                return e if (e := self._by_id.get(dlq_id)) and entry_visible_now(e) else None

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        """Mark a DLQ entry as resolved via O(1) index lookup."""
        by_str = str(resolved_by) if resolved_by else None
        attrs = {"dlq.id": str(dlq_id), "resolved_by": by_str, ATTR_DB_SYSTEM: "memory"}
        with self._tracer.span("eventsource.dlq.resolve", attrs):
            async with self._lock:
                if (entry := self._by_id.get(dlq_id)) is not None and entry_visible_now(entry):
                    entry.status, entry.resolved_by, entry.resolved_at = "resolved", by_str, datetime.now(UTC)  # fmt: skip

    async def mark_retrying(self, dlq_id: int | str) -> None:
        """Mark a DLQ entry as being retried via O(1) index lookup."""
        with self._tracer.span("eventsource.dlq.retry", {"dlq.id": str(dlq_id), ATTR_DB_SYSTEM: "memory"}):  # fmt: skip
            async with self._lock:
                if (entry := self._by_id.get(dlq_id)) is not None and entry_visible_now(entry):
                    entry.status = "retrying"

    async def get_failure_stats(self) -> DLQStats:
        """Get aggregate statistics about DLQ health."""
        with self._tracer.span("eventsource.dlq.get_stats", {ATTR_DB_SYSTEM: "memory"}):
            async with self._lock:
                active = [e for e in self._entries.values() if entry_visible_now(e) and e.status in ("failed", "retrying")]  # fmt: skip
                dates = [e.first_failed_at for e in active if e.first_failed_at]
                failed_cnt = sum(1 for e in active if e.status == "failed")
                retry_cnt = sum(1 for e in active if e.status == "retrying")
                oldest = min(dates).isoformat() if dates else None
                return DLQStats(failed_cnt, retry_cnt, len({e.projection_name for e in active}), oldest)  # fmt: skip

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """Get failure counts grouped by projection."""
        with self._tracer.span("eventsource.dlq.get_projection_counts", {ATTR_DB_SYSTEM: "memory"}):
            async with self._lock:
                by_proj: dict[str, list[DLQEntry]] = {}
                for entry in self._entries.values():
                    if entry.status in ("failed", "retrying") and entry_visible_now(entry):
                        by_proj.setdefault(entry.projection_name, []).append(entry)
                result = []
                for proj, items in by_proj.items():
                    firsts = [e.first_failed_at.isoformat() for e in items if e.first_failed_at]
                    lasts = [e.last_failed_at.isoformat() for e in items if e.last_failed_at]
                    result.append(ProjectionFailureCount(proj, len(items), min(firsts, default=None), max(lasts, default=None)))  # fmt: skip
                result.sort(key=lambda x: x.failure_count, reverse=True)
                return result

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """Delete resolved events older than specified days."""
        attrs = {"older_than_days": older_than_days, ATTR_DB_SYSTEM: "memory"}
        refuse_cleanup_under_scope()
        with self._tracer.span("eventsource.dlq.delete_resolved", attrs):
            cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
            async with self._lock:
                keys = [k for k, e in self._entries.items() if e.status == "resolved" and e.resolved_at and e.resolved_at < cutoff]  # fmt: skip
                for key in keys:
                    self._by_id.pop(self._entries.pop(key).id, None)
                return len(keys)

    async def clear(self) -> None:
        """Clear all entries. Useful for test setup/teardown."""
        with self._tracer.span("eventsource.dlq.clear", {ATTR_DB_SYSTEM: "memory"}):
            async with self._lock:
                self._entries.clear()
                self._by_id.clear()
                self._id_counter = 0


__all__ = ["InMemoryDLQRepository"]
