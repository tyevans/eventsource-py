"""PostgreSQL outbox repository for transactional event publishing."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
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


class PostgreSQLOutboxRepository:
    """PostgreSQL implementation of outbox repository (`event_outbox` table)."""

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self.conn = conn

    async def add_event(self, event: DomainEvent) -> UUID:
        attrs = {
            ATTR_EVENT_ID: str(event.event_id),
            ATTR_EVENT_TYPE: event.event_type,
            ATTR_AGGREGATE_ID: str(event.aggregate_id),
            ATTR_AGGREGATE_TYPE: event.aggregate_type,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.outbox.add", attrs):
            outbox_id = uuid4()
            query = text("""
                INSERT INTO event_outbox
                    (id, event_id, event_type, aggregate_id, aggregate_type,
                     tenant_id, event_data, created_at, status)
                VALUES (:id, :event_id, :event_type, :aggregate_id, :aggregate_type,
                        :tenant_id, :event_data, :created_at, 'pending')
            """)
            params = {
                "id": outbox_id,
                "event_id": event.event_id,
                "event_type": event.event_type,
                "aggregate_id": event.aggregate_id,
                "aggregate_type": event.aggregate_type,
                "tenant_id": event.tenant_id,
                "event_data": json_dumps(outbox_event_data(event)),
                "created_at": datetime.now(UTC),
            }
            async with sql_connection(self.conn, write=True) as conn:
                await conn.execute(query, params)
            return outbox_id

    async def get_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        attrs = {"limit": limit, ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.outbox.get_pending", attrs) as span:
            query = text("""
                SELECT id, event_id, event_type, aggregate_id, aggregate_type,
                       tenant_id, event_data, created_at, retry_count
                FROM event_outbox WHERE status = 'pending'
                ORDER BY created_at ASC LIMIT :limit
            """)
            async with sql_connection(self.conn, write=False) as conn:
                rows = (await conn.execute(query, {"limit": limit})).fetchall()
            entries = [
                OutboxEntry(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], "pending", r[8])
                for r in rows
            ]
            if span:
                span.set_attribute(ATTR_EVENT_COUNT, len(entries))
            return entries

    async def mark_published(self, outbox_id: UUID) -> None:
        attrs = {"outbox.id": str(outbox_id), ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.outbox.mark_published", attrs):
            query = text("UPDATE event_outbox SET status = 'published', published_at = :published_at WHERE id = :id")  # fmt: skip
            async with sql_connection(self.conn, write=True) as conn:
                await conn.execute(query, {"id": outbox_id, "published_at": datetime.now(UTC)})

    async def increment_retry(self, outbox_id: UUID, error: str | None = None) -> None:
        attrs = {
            "outbox.id": str(outbox_id),
            "error": error[:100] if error else None,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.outbox.increment_retry", attrs):
            query = text("UPDATE event_outbox SET retry_count = retry_count + 1, last_error = :error WHERE id = :id")  # fmt: skip
            async with sql_connection(self.conn, write=True) as conn:
                await conn.execute(query, {"id": outbox_id, "error": error})

    async def mark_failed(self, outbox_id: UUID, error: str) -> None:
        attrs = {
            "outbox.id": str(outbox_id),
            "error": error[:100] if error else None,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.outbox.mark_failed", attrs):
            query = text("UPDATE event_outbox SET status = 'failed', last_error = :error WHERE id = :id")  # fmt: skip
            async with sql_connection(self.conn, write=True) as conn:
                await conn.execute(query, {"id": outbox_id, "error": error})

    async def cleanup_published(self, days: int = 7) -> int:
        attrs = {"older_than_days": days, ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.outbox.cleanup", attrs) as span:
            query = text("""
                DELETE FROM event_outbox
                WHERE status = 'published' AND published_at < NOW() - INTERVAL '1 day' * :days
                RETURNING id
            """)
            async with sql_connection(self.conn, write=True) as conn:
                deleted = len((await conn.execute(query, {"days": days})).fetchall())
            if span:
                span.set_attribute("deleted_count", deleted)
            return deleted

    async def get_stats(self) -> OutboxStats:
        with self._tracer.span("eventsource.outbox.get_stats", {ATTR_DB_SYSTEM: "postgresql"}):
            query = text("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
                    COUNT(*) FILTER (WHERE status = 'published') as published_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                    MIN(created_at) FILTER (WHERE status = 'pending') as oldest_pending,
                    AVG(retry_count) FILTER (WHERE status = 'pending') as avg_retries
                FROM event_outbox
            """)
            async with sql_connection(self.conn, write=False) as conn:
                row = (await conn.execute(query)).fetchone()
            if row is None:
                return OutboxStats(0, 0, 0, None, 0.0)
            return OutboxStats(
                row[0] or 0, row[1] or 0, row[2] or 0, row[3], float(row[4]) if row[4] else 0.0
            )


__all__ = ["PostgreSQLOutboxRepository"]
