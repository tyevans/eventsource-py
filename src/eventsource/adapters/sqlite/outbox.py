"""
SQLite outbox repository for transactional event publishing.

The outbox pattern ensures events are reliably published even if the event bus
is temporarily unavailable. Events are persisted transactionally with the
aggregate changes, then published asynchronously by a background worker.

This adapter reads and writes `event_outbox` rows using the schema in
`src/eventsource/migrations/templates/sqlite/outbox.sql`.
"""

import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from eventsource.events.base import DomainEvent
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
from eventsource.serialization import EventSourceJSONEncoder

if TYPE_CHECKING:
    import aiosqlite


class SQLiteOutboxRepository:
    """
    SQLite implementation of outbox repository.

    Stores outbox events in the `event_outbox` table.

    SQLite-specific adaptations:
    - UUIDs stored as TEXT (36 characters, hyphenated format)
    - Timestamps stored as TEXT in ISO 8601 format
    - Uses `?` positional parameters instead of named parameters
    - Uses `datetime('now', '-' || ? || ' days')` for interval arithmetic
    - Uses `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` instead of `COUNT(*) FILTER`

    Example:
        >>> async with aiosqlite.connect("events.db") as db:
        ...     repo = SQLiteOutboxRepository(db)
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
        connection: "aiosqlite.Connection",
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the outbox repository.

        Args:
            connection: aiosqlite database connection
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._connection = connection

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime:
        """Parse ISO 8601 timestamp string to datetime."""
        if value is None:
            return datetime.now(UTC)
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return datetime.now(UTC)

    async def add_event(self, event: DomainEvent) -> UUID:
        """
        Add an event to the outbox for publishing.

        Args:
            event: Domain event to publish

        Returns:
            Outbox record ID (generated UUID)
        """
        with self._tracer.span(
            "eventsource.outbox.add",
            {
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_EVENT_TYPE: event.event_type,
                ATTR_AGGREGATE_ID: str(event.aggregate_id),
                ATTR_AGGREGATE_TYPE: event.aggregate_type,
                ATTR_DB_SYSTEM: "sqlite",
            },
        ):
            outbox_id = uuid4()
            now = datetime.now(UTC)

            event_data = outbox_event_data(event)

            await self._connection.execute(
                """
                INSERT INTO event_outbox
                    (id, event_id, event_type, aggregate_id, aggregate_type,
                     tenant_id, event_data, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    str(outbox_id),
                    str(event.event_id),
                    event.event_type,
                    str(event.aggregate_id),
                    event.aggregate_type,
                    str(event.tenant_id) if event.tenant_id else None,
                    # The encoder is redundant for this JSON-safe payload; see
                    # the postgresql adapter for why it is left in place.
                    json.dumps(event_data, cls=EventSourceJSONEncoder),
                    now.isoformat(),
                ),
            )
            await self._connection.commit()

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
                ATTR_DB_SYSTEM: "sqlite",
            },
        ) as span:
            cursor = await self._connection.execute(
                """
                SELECT id, event_id, event_type, aggregate_id, aggregate_type,
                       tenant_id, event_data, created_at, retry_count
                FROM event_outbox
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()

            entries = [
                OutboxEntry(
                    id=UUID(row[0]),
                    event_id=UUID(row[1]),
                    event_type=row[2],
                    aggregate_id=UUID(row[3]),
                    aggregate_type=row[4],
                    tenant_id=UUID(row[5]) if row[5] else None,
                    event_data=row[6],
                    created_at=self._parse_datetime(row[7]),
                    status="pending",
                    retry_count=row[8] or 0,
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
                ATTR_DB_SYSTEM: "sqlite",
            },
        ):
            now = datetime.now(UTC)
            await self._connection.execute(
                """
                UPDATE event_outbox
                SET status = 'published',
                    published_at = ?
                WHERE id = ?
                """,
                (now.isoformat(), str(outbox_id)),
            )
            await self._connection.commit()

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
                ATTR_DB_SYSTEM: "sqlite",
            },
        ):
            await self._connection.execute(
                """
                UPDATE event_outbox
                SET retry_count = retry_count + 1,
                    last_error = ?
                WHERE id = ?
                """,
                (error, str(outbox_id)),
            )
            await self._connection.commit()

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
                ATTR_DB_SYSTEM: "sqlite",
            },
        ):
            await self._connection.execute(
                """
                UPDATE event_outbox
                SET status = 'failed',
                    last_error = ?
                WHERE id = ?
                """,
                (error, str(outbox_id)),
            )
            await self._connection.commit()

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
                ATTR_DB_SYSTEM: "sqlite",
            },
        ) as span:
            # The cutoff is computed in Python and bound as a parameter,
            # not delegated to SQLite's datetime('now', ...) -- that
            # function returns a space-separated, sub-second-less string
            # ("2026-07-31 18:08:33"), while published_at is written as
            # this adapter's own datetime.now(UTC).isoformat() output
            # ('T'-separated, with microseconds and a UTC offset). TEXT
            # comparison between the two formats is not the ordering
            # either format alone would give: 'T' (0x54) sorts after ' '
            # (0x20), so published_at never compares less than
            # datetime('now', ...) for any cutoff computed within the
            # same wall-clock second. Binding the same isoformat() the
            # column was written with keeps both sides of the TEXT
            # comparison in one format.
            cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
            cursor = await self._connection.execute(
                """
                DELETE FROM event_outbox
                WHERE status = 'published'
                  AND published_at < ?
                """,
                (cutoff,),
            )
            await self._connection.commit()
            deleted = cursor.rowcount if cursor.rowcount is not None else 0
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
            {ATTR_DB_SYSTEM: "sqlite"},
        ):
            # SQLite doesn't support FILTER clause, use CASE WHEN instead
            cursor = await self._connection.execute(
                """
                SELECT
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_count,
                    SUM(CASE WHEN status = 'published' THEN 1 ELSE 0 END) as published_count,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count,
                    MIN(CASE WHEN status = 'pending' THEN created_at END) as oldest_pending,
                    AVG(CASE WHEN status = 'pending' THEN retry_count END) as avg_retries
                FROM event_outbox
                """
            )
            row = await cursor.fetchone()

            # Aggregate query always returns a row, but values may be NULL
            if row is None:
                return OutboxStats(
                    pending_count=0,
                    published_count=0,
                    failed_count=0,
                    oldest_pending=None,
                    avg_retries=0.0,
                )

            # Parse oldest_pending from ISO 8601 string to datetime
            oldest_pending = None
            if row[3]:
                try:
                    oldest_pending = datetime.fromisoformat(row[3].replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    oldest_pending = None

            return OutboxStats(
                pending_count=row[0] or 0,
                published_count=row[1] or 0,
                failed_count=row[2] or 0,
                oldest_pending=oldest_pending,
                avg_retries=float(row[4]) if row[4] else 0.0,
            )


__all__ = ["SQLiteOutboxRepository"]
