"""
PostgreSQL outbox repository for transactional event publishing.

The outbox pattern ensures events are reliably published even if the event bus
is temporarily unavailable. Events are persisted transactionally with the
aggregate changes, then published asynchronously by a background worker.

This adapter reads and writes `event_outbox` rows using the schema in
`src/eventsource/migrations/templates/outbox.sql`. The same table is written
in-transaction by `adapters/postgresql/store.py::_write_to_outbox`; that
pairing is the table contract this adapter and the store share.
"""

import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters.serialization import EventSourceJSONEncoder
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
    """
    PostgreSQL implementation of outbox repository.

    Stores outbox events in the `event_outbox` table.

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = PostgreSQLOutboxRepository(conn)
        ...     outbox_id = await repo.add_event(event)
        ...
        >>> # Later, in the publisher worker:
        >>> pending = await repo.get_pending_events(limit=100)
        >>> for entry in pending:
        ...     # Publish to event bus
        ...     await repo.mark_published(entry.id)
    """

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """
        Initialize the outbox repository.

        Args:
            conn: Database connection or engine
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self.conn = conn

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
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            outbox_id = uuid4()
            now = datetime.now(UTC)

            event_data = outbox_event_data(event)

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
                # The encoder is redundant for this JSON-safe payload, but is
                # left in place so the parameter binding here matches every
                # other write path in this module without a second
                # serialization delta in this commit.
                "event_data": json.dumps(event_data, cls=EventSourceJSONEncoder),
                "created_at": now,
            }

            async with sql_connection(self.conn, write=True) as conn:
                await conn.execute(query, params)

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
                ATTR_DB_SYSTEM: "postgresql",
            },
        ) as span:
            query = text("""
                SELECT id, event_id, event_type, aggregate_id, aggregate_type,
                       tenant_id, event_data, created_at, retry_count
                FROM event_outbox
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT :limit
            """)

            async with sql_connection(self.conn, write=False) as conn:
                result = await conn.execute(query, {"limit": limit})
                rows = result.fetchall()

            entries = [
                OutboxEntry(
                    id=row[0],
                    event_id=row[1],
                    event_type=row[2],
                    aggregate_id=row[3],
                    aggregate_type=row[4],
                    tenant_id=row[5],
                    event_data=row[6],
                    created_at=row[7],
                    status="pending",
                    retry_count=row[8],
                )
                for row in rows
            ]
            if span:
                span.set_attribute(ATTR_EVENT_COUNT, len(entries))
            return entries

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
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            now = datetime.now(UTC)
            query = text("""
                UPDATE event_outbox
                SET status = 'published',
                    published_at = :published_at
                WHERE id = :id
            """)

            async with sql_connection(self.conn, write=True) as conn:
                await conn.execute(query, {"id": outbox_id, "published_at": now})

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
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                UPDATE event_outbox
                SET retry_count = retry_count + 1,
                    last_error = :error
                WHERE id = :id
            """)

            async with sql_connection(self.conn, write=True) as conn:
                await conn.execute(query, {"id": outbox_id, "error": error})

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
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                UPDATE event_outbox
                SET status = 'failed',
                    last_error = :error
                WHERE id = :id
            """)

            async with sql_connection(self.conn, write=True) as conn:
                await conn.execute(query, {"id": outbox_id, "error": error})

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
                ATTR_DB_SYSTEM: "postgresql",
            },
        ) as span:
            query = text("""
                DELETE FROM event_outbox
                WHERE status = 'published'
                  AND published_at < NOW() - INTERVAL '1 day' * :days
                RETURNING id
            """)

            async with sql_connection(self.conn, write=True) as conn:
                result = await conn.execute(query, {"days": days})
                deleted = len(result.fetchall())

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
            {ATTR_DB_SYSTEM: "postgresql"},
        ):
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
                result = await conn.execute(query)
                row = result.fetchone()

            # Aggregate query always returns a row
            if row is None:
                return OutboxStats(
                    pending_count=0,
                    published_count=0,
                    failed_count=0,
                    oldest_pending=None,
                    avg_retries=0.0,
                )

            return OutboxStats(
                pending_count=row[0] or 0,
                published_count=row[1] or 0,
                failed_count=row[2] or 0,
                oldest_pending=row[3],
                avg_retries=float(row[4]) if row[4] else 0.0,
            )


__all__ = ["PostgreSQLOutboxRepository"]
