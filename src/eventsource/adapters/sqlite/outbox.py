"""SQLite outbox repository for transactional event publishing (`event_outbox` table)."""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from eventsource.adapters.serialization import json_dumps
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

if TYPE_CHECKING:
    import aiosqlite


class SQLiteOutboxRepository:
    """SQLite implementation of the outbox repository (`event_outbox` table)."""

    def __init__(
        self,
        connection: "aiosqlite.Connection",
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._connection = connection

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime:
        if value is None:
            return datetime.now(UTC)
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return datetime.now(UTC)

    async def add_event(self, event: DomainEvent) -> UUID:
        """Add an event to the outbox for publishing and return its outbox UUID."""
        attrs = {
            ATTR_EVENT_ID: str(event.event_id), ATTR_EVENT_TYPE: event.event_type,
            ATTR_AGGREGATE_ID: str(event.aggregate_id), ATTR_AGGREGATE_TYPE: event.aggregate_type,
            ATTR_DB_SYSTEM: "sqlite",
        }  # fmt: skip
        with self._tracer.span("eventsource.outbox.add", attrs):
            outbox_id = uuid4()
            params = (
                str(outbox_id), str(event.event_id), event.event_type,
                str(event.aggregate_id), event.aggregate_type,
                str(event.tenant_id) if event.tenant_id else None,
                json_dumps(outbox_event_data(event)), datetime.now(UTC).isoformat(),
            )  # fmt: skip
            sql = (
                "INSERT INTO event_outbox (id, event_id, event_type, aggregate_id, aggregate_type, "
                "tenant_id, event_data, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')"
            )
            await self._connection.execute(sql, params)
            await self._connection.commit()
            return outbox_id

    async def get_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """Return up to `limit` pending outbox entries ordered by creation time."""
        attrs = {"limit": limit, ATTR_DB_SYSTEM: "sqlite"}
        with self._tracer.span("eventsource.outbox.get_pending", attrs) as span:
            sql = (
                "SELECT id, event_id, event_type, aggregate_id, aggregate_type, "
                "tenant_id, event_data, created_at, retry_count FROM event_outbox "
                "WHERE status = 'pending' ORDER BY created_at ASC LIMIT ?"
            )
            cursor = await self._connection.execute(sql, (limit,))
            entries = [
                OutboxEntry(
                    UUID(r[0]),
                    UUID(r[1]),
                    r[2],
                    UUID(r[3]),
                    r[4],
                    UUID(r[5]) if r[5] else None,
                    r[6],
                    self._parse_datetime(r[7]),
                    "pending",
                    retry_count=r[8] or 0,
                )  # fmt: skip
                for r in await cursor.fetchall()
            ]
            if span:
                span.set_attribute(ATTR_EVENT_COUNT, len(entries))
            return entries

    async def mark_published(self, outbox_id: UUID) -> None:
        """Mark an outbox event as successfully published."""
        attrs = {"outbox.id": str(outbox_id), ATTR_DB_SYSTEM: "sqlite"}
        with self._tracer.span("eventsource.outbox.mark_published", attrs):
            sql = "UPDATE event_outbox SET status = 'published', published_at = ? WHERE id = ?"
            await self._connection.execute(sql, (datetime.now(UTC).isoformat(), str(outbox_id)))
            await self._connection.commit()

    async def increment_retry(self, outbox_id: UUID, error: str | None = None) -> None:
        """Increment retry count for a failed publishing attempt."""
        attrs = {"outbox.id": str(outbox_id), "error": error[:100] if error else None, ATTR_DB_SYSTEM: "sqlite"}  # fmt: skip
        with self._tracer.span("eventsource.outbox.increment_retry", attrs):
            sql = (
                "UPDATE event_outbox SET retry_count = retry_count + 1, last_error = ? WHERE id = ?"
            )
            await self._connection.execute(sql, (error, str(outbox_id)))
            await self._connection.commit()

    async def mark_failed(self, outbox_id: UUID, error: str) -> None:
        """Mark an outbox event as permanently failed."""
        attrs = {"outbox.id": str(outbox_id), "error": error[:100] if error else None, ATTR_DB_SYSTEM: "sqlite"}  # fmt: skip
        with self._tracer.span("eventsource.outbox.mark_failed", attrs):
            sql = "UPDATE event_outbox SET status = 'failed', last_error = ? WHERE id = ?"
            await self._connection.execute(sql, (error, str(outbox_id)))
            await self._connection.commit()

    async def cleanup_published(self, days: int = 7) -> int:
        """Delete published events older than `days` and return the deleted count."""
        attrs = {"older_than_days": days, ATTR_DB_SYSTEM: "sqlite"}
        with self._tracer.span("eventsource.outbox.cleanup", attrs) as span:
            cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
            sql = "DELETE FROM event_outbox WHERE status = 'published' AND published_at < ?"
            cursor = await self._connection.execute(sql, (cutoff,))
            await self._connection.commit()
            deleted = cursor.rowcount if cursor.rowcount is not None else 0
            if span:
                span.set_attribute("deleted_count", deleted)
            return deleted

    async def get_stats(self) -> OutboxStats:
        """Return aggregate statistics for the outbox."""
        with self._tracer.span("eventsource.outbox.get_stats", {ATTR_DB_SYSTEM: "sqlite"}):
            cursor = await self._connection.execute(
                "SELECT SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status = 'published' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), "
                "MIN(CASE WHEN status = 'pending' THEN created_at END), "
                "AVG(CASE WHEN status = 'pending' THEN retry_count END) FROM event_outbox"
            )
            if (row := await cursor.fetchone()) is None:
                return OutboxStats(0, 0, 0, None, 0.0)
            oldest = None
            if row[3]:
                try:
                    oldest = datetime.fromisoformat(row[3].replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    oldest = None
            return OutboxStats(row[0] or 0, row[1] or 0, row[2] or 0, oldest, float(row[4]) if row[4] else 0.0)  # fmt: skip


__all__ = ["SQLiteOutboxRepository"]
