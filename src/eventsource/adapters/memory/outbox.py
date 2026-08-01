"""
In-memory outbox repository for testing.

The outbox pattern ensures events are reliably published even if the event bus
is temporarily unavailable. Events are persisted transactionally with the
aggregate changes, then published asynchronously by a background worker.
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_DB_SYSTEM,
    ATTR_EVENT_COUNT,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
)
from eventsource.ports.outbox import OutboxEntry, OutboxStats, outbox_event_data


class InMemoryOutboxRepository:
    """
    In-memory implementation of outbox repository for testing.

    Stores outbox events in memory. All data is lost when process terminates.

    Example:
        >>> repo = InMemoryOutboxRepository()
        >>> outbox_id = await repo.add_event(event)
        >>> pending = await repo.get_pending_events()
        >>> await repo.mark_published(pending[0].id)
    """

    def __init__(
        self,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize an empty in-memory outbox repository.

        Args:
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._entries: dict[UUID, OutboxEntry] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def add_event(self, event: DomainEvent) -> UUID:
        """
        Add an event to the outbox for publishing.

        Args:
            event: Domain event to publish

        Returns:
            Outbox record ID
        """
        with self._tracer.span(
            "eventsource.outbox.add",
            {
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_EVENT_TYPE: event.event_type,
                ATTR_AGGREGATE_ID: str(event.aggregate_id),
                ATTR_AGGREGATE_TYPE: event.aggregate_type,
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            outbox_id = uuid4()
            now = datetime.now(UTC)

            # outbox_event_data() is JSON-safe by construction, so no custom
            # encoder is needed here.
            event_data = outbox_event_data(event)

            async with self._lock:
                self._entries[outbox_id] = OutboxEntry(
                    id=outbox_id,
                    event_id=event.event_id,
                    event_type=event.event_type,
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    tenant_id=event.tenant_id,
                    event_data=json.dumps(event_data),
                    created_at=now,
                    status="pending",
                )

            return outbox_id

    async def get_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """
        Get pending events that need to be published.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of OutboxEntry instances
        """
        with self._tracer.span(
            "eventsource.outbox.get_pending",
            {
                "limit": limit,
                ATTR_DB_SYSTEM: "memory",
            },
        ) as span:
            async with self._lock:
                pending = [e for e in self._entries.values() if e.status == "pending"]
                # Sort by created_at ascending (oldest first)
                pending.sort(key=lambda e: e.created_at)
                result = pending[:limit]
            if span:
                span.set_attribute(ATTR_EVENT_COUNT, len(result))
            return result

    async def mark_published(self, outbox_id: UUID) -> None:
        """
        Mark an outbox event as successfully published.

        Args:
            outbox_id: Outbox record ID
        """
        with self._tracer.span(
            "eventsource.outbox.mark_published",
            {
                "outbox.id": str(outbox_id),
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            now = datetime.now(UTC)
            async with self._lock:
                if outbox_id in self._entries:
                    entry = self._entries[outbox_id]
                    entry.status = "published"
                    entry.published_at = now

    async def increment_retry(self, outbox_id: UUID, error: str | None = None) -> None:
        """
        Increment retry count for a failed publishing attempt.

        Args:
            outbox_id: Outbox record ID
            error: Error message (optional)
        """
        with self._tracer.span(
            "eventsource.outbox.increment_retry",
            {
                "outbox.id": str(outbox_id),
                "error": error[:100] if error else None,
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            async with self._lock:
                if outbox_id in self._entries:
                    entry = self._entries[outbox_id]
                    entry.retry_count += 1
                    entry.last_error = error

    async def mark_failed(self, outbox_id: UUID, error: str) -> None:
        """
        Mark an outbox event as permanently failed.

        Args:
            outbox_id: Outbox record ID
            error: Error message
        """
        with self._tracer.span(
            "eventsource.outbox.mark_failed",
            {
                "outbox.id": str(outbox_id),
                "error": error[:100] if error else None,
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            async with self._lock:
                if outbox_id in self._entries:
                    entry = self._entries[outbox_id]
                    entry.status = "failed"
                    entry.last_error = error

    async def cleanup_published(self, days: int = 7) -> int:
        """
        Clean up published events older than specified days.

        Args:
            days: Number of days to retain published events

        Returns:
            Number of records deleted
        """
        with self._tracer.span(
            "eventsource.outbox.cleanup",
            {
                "older_than_days": days,
                ATTR_DB_SYSTEM: "memory",
            },
        ) as span:
            cutoff = datetime.now(UTC) - timedelta(days=days)

            deleted = 0
            async with self._lock:
                ids_to_delete = []
                for id_, entry in self._entries.items():
                    if (
                        entry.status == "published"
                        and entry.published_at
                        and entry.published_at < cutoff
                    ):
                        ids_to_delete.append(id_)

                for id_ in ids_to_delete:
                    del self._entries[id_]
                    deleted += 1

            if span:
                span.set_attribute("deleted_count", deleted)
            return deleted

    async def get_stats(self) -> OutboxStats:
        """
        Get outbox statistics.

        Returns:
            OutboxStats with outbox metrics
        """
        with self._tracer.span(
            "eventsource.outbox.get_stats",
            {ATTR_DB_SYSTEM: "memory"},
        ):
            async with self._lock:
                entries = list(self._entries.values())

                pending = [e for e in entries if e.status == "pending"]
                published = [e for e in entries if e.status == "published"]
                failed = [e for e in entries if e.status == "failed"]

                oldest_pending = None
                if pending:
                    oldest_pending = min(e.created_at for e in pending)

                avg_retries = 0.0
                if pending:
                    avg_retries = sum(e.retry_count for e in pending) / len(pending)

                return OutboxStats(
                    pending_count=len(pending),
                    published_count=len(published),
                    failed_count=len(failed),
                    oldest_pending=oldest_pending,
                    avg_retries=avg_retries,
                )

    async def clear(self) -> None:
        """Clear all entries. Useful for test setup/teardown."""
        with self._tracer.span(
            "eventsource.outbox.clear",
            {ATTR_DB_SYSTEM: "memory"},
        ):
            async with self._lock:
                self._entries.clear()


__all__ = ["InMemoryOutboxRepository"]
