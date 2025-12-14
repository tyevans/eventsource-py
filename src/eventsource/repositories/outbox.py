"""
Outbox repository for transactional event publishing.

The outbox pattern ensures events are reliably published even if the event bus
is temporarily unavailable. Events are persisted transactionally with the
aggregate changes, then published asynchronously by a background worker.

This enables:
- Guaranteed event delivery (at-least-once semantics)
- Transactional consistency between aggregate state and events
- Decoupled publishing from the main request path
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

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
from eventsource.repositories._connection import execute_with_connection
from eventsource.serialization import EventSourceJSONEncoder, json_dumps

if TYPE_CHECKING:
    import aiosqlite


@dataclass
class OutboxEntry:
    """
    Data structure for an outbox entry.

    Attributes:
        id: Unique outbox entry identifier
        event_id: Event ID being published
        event_type: Type of the event
        aggregate_id: Aggregate ID the event belongs to
        aggregate_type: Type of aggregate
        tenant_id: Tenant ID (optional)
        event_data: Serialized event data (JSON string or dict)
        created_at: When the entry was created
        status: Current status (pending, published, failed)
        published_at: When the event was published (if applicable)
        retry_count: Number of publish retry attempts
        last_error: Last error message (if any)
    """

    id: UUID
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    aggregate_type: str
    tenant_id: UUID | None
    event_data: str | dict[str, Any]
    created_at: datetime
    status: str = "pending"
    published_at: datetime | None = None
    retry_count: int = 0
    last_error: str | None = None


@dataclass(frozen=True)
class OutboxStats:
    """
    Aggregate statistics for the outbox.

    Attributes:
        pending_count: Number of pending events
        published_count: Number of published events
        failed_count: Number of failed events
        oldest_pending: Timestamp of oldest pending event
        avg_retries: Average retry count for pending events
    """

    pending_count: int = 0
    published_count: int = 0
    failed_count: int = 0
    oldest_pending: datetime | None = None
    avg_retries: float = 0.0


@runtime_checkable
class OutboxRepository(Protocol):
    """
    Protocol for outbox repositories.

    Outbox repositories implement the transactional outbox pattern for
    reliable event publishing. Events are stored transactionally with
    aggregate changes and published asynchronously.
    """

    async def add_event(self, event: DomainEvent) -> UUID:
        """
        Add an event to the outbox for publishing.

        This should be called within the same transaction that persists
        the event to the event store.

        Args:
            event: Domain event to publish

        Returns:
            Outbox record ID
        """
        ...

    async def get_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """
        Get pending events that need to be published.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of OutboxEntry instances
        """
        ...

    async def mark_published(self, outbox_id: UUID) -> None:
        """
        Mark an outbox event as successfully published.

        Args:
            outbox_id: Outbox record ID
        """
        ...

    async def mark_failed(self, outbox_id: UUID, error: str) -> None:
        """
        Mark an outbox event as permanently failed.

        Args:
            outbox_id: Outbox record ID
            error: Error message
        """
        ...

    async def increment_retry(self, outbox_id: UUID, error: str | None = None) -> None:
        """
        Increment retry count for a failed publishing attempt.

        Args:
            outbox_id: Outbox record ID
            error: Error message (optional)
        """
        ...

    async def cleanup_published(self, days: int = 7) -> int:
        """
        Clean up published events older than specified days.

        Args:
            days: Number of days to retain published events

        Returns:
            Number of records deleted
        """
        ...

    async def get_stats(self) -> OutboxStats:
        """
        Get outbox statistics.

        Returns:
            OutboxStats with outbox metrics
        """
        ...

    async def list_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """
        List pending events that need to be published.

        This is an alias for get_pending_events() for naming consistency.
        Prefer this method when fetching multiple entries.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of OutboxEntry instances
        """
        ...


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

            # Serialize event data
            event_data = {
                "event_id": str(event.event_id),
                "aggregate_id": str(event.aggregate_id),
                "aggregate_type": event.aggregate_type,
                "tenant_id": str(event.tenant_id) if event.tenant_id else None,
                "occurred_at": event.occurred_at.isoformat(),
                "payload": json.loads(event.model_dump_json()),
            }

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
                "event_data": json.dumps(event_data, cls=EventSourceJSONEncoder),
                "created_at": now,
            }

            async with execute_with_connection(self.conn, transactional=True) as conn:
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

            async with execute_with_connection(self.conn, transactional=False) as conn:
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

            async with execute_with_connection(self.conn, transactional=True) as conn:
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

            async with execute_with_connection(self.conn, transactional=True) as conn:
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

            async with execute_with_connection(self.conn, transactional=True) as conn:
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

            async with execute_with_connection(self.conn, transactional=True) as conn:
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

            async with execute_with_connection(self.conn, transactional=False) as conn:
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

    async def list_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """Alias for get_pending_events() - preferred for naming consistency."""
        return await self.get_pending_events(limit)


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

            # Serialize event data
            event_data = {
                "event_id": str(event.event_id),
                "aggregate_id": str(event.aggregate_id),
                "aggregate_type": event.aggregate_type,
                "tenant_id": str(event.tenant_id) if event.tenant_id else None,
                "occurred_at": event.occurred_at.isoformat(),
                "payload": json.loads(event.model_dump_json()),
            }

            async with self._lock:
                self._entries[outbox_id] = OutboxEntry(
                    id=outbox_id,
                    event_id=event.event_id,
                    event_type=event.event_type,
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    tenant_id=event.tenant_id,
                    event_data=json_dumps(event_data),
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
            from datetime import timedelta

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

    async def list_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """Alias for get_pending_events() - preferred for naming consistency."""
        return await self.get_pending_events(limit)

    async def clear(self) -> None:
        """Clear all entries. Useful for test setup/teardown."""
        with self._tracer.span(
            "eventsource.outbox.clear",
            {ATTR_DB_SYSTEM: "memory"},
        ):
            async with self._lock:
                self._entries.clear()


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

            # Serialize event data
            event_data = {
                "event_id": str(event.event_id),
                "aggregate_id": str(event.aggregate_id),
                "aggregate_type": event.aggregate_type,
                "tenant_id": str(event.tenant_id) if event.tenant_id else None,
                "occurred_at": event.occurred_at.isoformat(),
                "payload": json.loads(event.model_dump_json()),
            }

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
            # SQLite uses different date arithmetic syntax
            cursor = await self._connection.execute(
                """
                DELETE FROM event_outbox
                WHERE status = 'published'
                  AND published_at < datetime('now', '-' || ? || ' days')
                """,
                (days,),
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

    async def list_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """Alias for get_pending_events() - preferred for naming consistency."""
        return await self.get_pending_events(limit)


# Type alias for backwards compatibility
OutboxRepositoryProtocol = OutboxRepository
