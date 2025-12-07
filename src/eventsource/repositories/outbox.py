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

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Lock
from typing import Any, Protocol, runtime_checkable
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.events.base import DomainEvent
from eventsource.repositories._json import EventSourceJSONEncoder, json_dumps


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

    async def get_pending_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        Get pending events that need to be published.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of pending outbox records
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
        ...     await repo.mark_published(entry["id"])
    """

    def __init__(self, conn: AsyncConnection | AsyncEngine):
        """
        Initialize the outbox repository.

        Args:
            conn: Database connection or engine
        """
        self.conn = conn

    async def add_event(self, event: DomainEvent) -> UUID:
        """
        Add an event to the outbox for publishing.

        Args:
            event: Domain event to publish

        Returns:
            Outbox record ID
        """
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

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.begin() as conn:
                await conn.execute(query, params)
        else:
            await self.conn.execute(query, params)

        return outbox_id

    async def get_pending_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        Get pending events that need to be published.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of pending outbox records
        """
        query = text("""
            SELECT id, event_id, event_type, aggregate_id, aggregate_type,
                   tenant_id, event_data, created_at, retry_count
            FROM event_outbox
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT :limit
        """)

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.connect() as conn:
                result = await conn.execute(query, {"limit": limit})
                rows = result.fetchall()
        else:
            result = await self.conn.execute(query, {"limit": limit})
            rows = result.fetchall()

        return [
            {
                "id": str(row[0]),
                "event_id": str(row[1]),
                "event_type": row[2],
                "aggregate_id": str(row[3]),
                "aggregate_type": row[4],
                "tenant_id": str(row[5]) if row[5] else None,
                "event_data": row[6],
                "created_at": row[7],
                "retry_count": row[8],
            }
            for row in rows
        ]

    async def mark_published(self, outbox_id: UUID) -> None:
        """
        Mark an outbox event as successfully published.

        Args:
            outbox_id: Outbox record ID
        """
        now = datetime.now(UTC)
        query = text("""
            UPDATE event_outbox
            SET status = 'published',
                published_at = :published_at
            WHERE id = :id
        """)

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.begin() as conn:
                await conn.execute(query, {"id": outbox_id, "published_at": now})
        else:
            await self.conn.execute(query, {"id": outbox_id, "published_at": now})

    async def increment_retry(self, outbox_id: UUID, error: str | None = None) -> None:
        """
        Increment retry count for a failed publishing attempt.

        Args:
            outbox_id: Outbox record ID
            error: Error message (optional)
        """
        query = text("""
            UPDATE event_outbox
            SET retry_count = retry_count + 1,
                last_error = :error
            WHERE id = :id
        """)

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.begin() as conn:
                await conn.execute(query, {"id": outbox_id, "error": error})
        else:
            await self.conn.execute(query, {"id": outbox_id, "error": error})

    async def mark_failed(self, outbox_id: UUID, error: str) -> None:
        """
        Mark an outbox event as permanently failed.

        Args:
            outbox_id: Outbox record ID
            error: Error message
        """
        query = text("""
            UPDATE event_outbox
            SET status = 'failed',
                last_error = :error
            WHERE id = :id
        """)

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.begin() as conn:
                await conn.execute(query, {"id": outbox_id, "error": error})
        else:
            await self.conn.execute(query, {"id": outbox_id, "error": error})

    async def cleanup_published(self, days: int = 7) -> int:
        """
        Clean up published events older than specified days.

        Args:
            days: Number of days to retain published events

        Returns:
            Number of records deleted
        """
        query = text("""
            DELETE FROM event_outbox
            WHERE status = 'published'
              AND published_at < NOW() - INTERVAL '1 day' * :days
            RETURNING id
        """)

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.begin() as conn:
                result = await conn.execute(query, {"days": days})
                return len(result.fetchall())
        else:
            result = await self.conn.execute(query, {"days": days})
            return len(result.fetchall())

    async def get_stats(self) -> dict[str, Any]:
        """
        Get outbox statistics.

        Returns:
            Dictionary with outbox metrics
        """
        query = text("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
                COUNT(*) FILTER (WHERE status = 'published') as published_count,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                MIN(created_at) FILTER (WHERE status = 'pending') as oldest_pending,
                AVG(retry_count) FILTER (WHERE status = 'pending') as avg_retries
            FROM event_outbox
        """)

        if isinstance(self.conn, AsyncEngine):
            async with self.conn.connect() as conn:
                result = await conn.execute(query)
                row = result.fetchone()
        else:
            result = await self.conn.execute(query)
            row = result.fetchone()

        # Aggregate query always returns a row
        if row is None:
            return {
                "pending_count": 0,
                "published_count": 0,
                "failed_count": 0,
                "oldest_pending": None,
                "avg_retries": 0.0,
            }

        return {
            "pending_count": row[0] or 0,
            "published_count": row[1] or 0,
            "failed_count": row[2] or 0,
            "oldest_pending": row[3],
            "avg_retries": float(row[4]) if row[4] else 0.0,
        }


class InMemoryOutboxRepository:
    """
    In-memory implementation of outbox repository for testing.

    Stores outbox events in memory. All data is lost when process terminates.

    Example:
        >>> repo = InMemoryOutboxRepository()
        >>> outbox_id = await repo.add_event(event)
        >>> pending = await repo.get_pending_events()
        >>> await repo.mark_published(UUID(pending[0]["id"]))
    """

    def __init__(self) -> None:
        """Initialize an empty in-memory outbox repository."""
        self._entries: dict[UUID, OutboxEntry] = {}
        self._lock = Lock()

    async def add_event(self, event: DomainEvent) -> UUID:
        """
        Add an event to the outbox for publishing.

        Args:
            event: Domain event to publish

        Returns:
            Outbox record ID
        """
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

        with self._lock:
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

    async def get_pending_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        Get pending events that need to be published.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of pending outbox records
        """
        with self._lock:
            pending = [e for e in self._entries.values() if e.status == "pending"]
            # Sort by created_at ascending (oldest first)
            pending.sort(key=lambda e: e.created_at)
            pending = pending[:limit]

            return [
                {
                    "id": str(e.id),
                    "event_id": str(e.event_id),
                    "event_type": e.event_type,
                    "aggregate_id": str(e.aggregate_id),
                    "aggregate_type": e.aggregate_type,
                    "tenant_id": str(e.tenant_id) if e.tenant_id else None,
                    "event_data": e.event_data,
                    "created_at": e.created_at,
                    "retry_count": e.retry_count,
                }
                for e in pending
            ]

    async def mark_published(self, outbox_id: UUID) -> None:
        """
        Mark an outbox event as successfully published.

        Args:
            outbox_id: Outbox record ID
        """
        now = datetime.now(UTC)
        with self._lock:
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
        with self._lock:
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
        with self._lock:
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
        from datetime import timedelta
        cutoff = datetime.now(UTC) - timedelta(days=days)

        deleted = 0
        with self._lock:
            ids_to_delete = []
            for id_, entry in self._entries.items():
                if entry.status == "published" and entry.published_at:
                    if entry.published_at < cutoff:
                        ids_to_delete.append(id_)

            for id_ in ids_to_delete:
                del self._entries[id_]
                deleted += 1

        return deleted

    async def get_stats(self) -> dict[str, Any]:
        """
        Get outbox statistics.

        Returns:
            Dictionary with outbox metrics
        """
        with self._lock:
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

            return {
                "pending_count": len(pending),
                "published_count": len(published),
                "failed_count": len(failed),
                "oldest_pending": oldest_pending,
                "avg_retries": avg_retries,
            }

    def clear(self) -> None:
        """Clear all entries. Useful for test setup/teardown."""
        with self._lock:
            self._entries.clear()


# Type alias for backwards compatibility
OutboxRepositoryProtocol = OutboxRepository
