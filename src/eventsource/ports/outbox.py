"""Transactional outbox port.

Pure boundary interface: stdlib, typing, uuid, datetime, dataclasses, and
`DomainEvent` only. No sqlalchemy, no driver types.

The outbox pattern lets events be published reliably even when the broker
is unavailable: the event row and the outbox row commit in one database
transaction, and a drain worker (which this library does not ship — see
`docs/guides/repository-operations.md`) publishes and marks them.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from eventsource.events.base import DomainEvent


@dataclass
class OutboxEntry:
    """One row of the outbox.

    Attributes:
        id: Unique outbox entry identifier. A `UUID` on PostgreSQL and
            in memory; SQLite mints an autoincrement integer, so adapters
            may hand back either.
        event_id: Event ID being published
        event_type: Type of the event
        aggregate_id: Aggregate ID the event belongs to
        aggregate_type: Type of aggregate
        tenant_id: Tenant ID (optional)
        event_data: The `outbox_event_data()` payload, as the JSON string
            the backend stored or as the already-parsed dict
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
    """Aggregate statistics for the outbox.

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
    """Protocol for outbox repositories.

    Implementations store events transactionally with aggregate changes
    and hand them to a drain worker for asynchronous publishing.
    """

    async def add_event(self, event: DomainEvent) -> UUID:
        """Add an event to the outbox for publishing.

        Call this inside the same transaction that persists the event to
        the event store; that atomicity is the point of the pattern.

        Args:
            event: Domain event to publish

        Returns:
            Outbox record ID
        """
        ...

    async def get_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """Get pending events that need to be published, oldest first.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of OutboxEntry instances
        """
        ...

    async def mark_published(self, outbox_id: UUID) -> None:
        """Mark an outbox event as successfully published.

        Args:
            outbox_id: Outbox record ID
        """
        ...

    async def mark_failed(self, outbox_id: UUID, error: str) -> None:
        """Mark an outbox event as permanently failed.

        Args:
            outbox_id: Outbox record ID
            error: Error message
        """
        ...

    async def increment_retry(self, outbox_id: UUID, error: str | None = None) -> None:
        """Increment retry count for a failed publishing attempt.

        Args:
            outbox_id: Outbox record ID
            error: Error message (optional)
        """
        ...

    async def cleanup_published(self, days: int = 7) -> int:
        """Delete published events older than `days`.

        Args:
            days: Number of days to retain published events

        Returns:
            Number of records deleted
        """
        ...

    async def get_stats(self) -> OutboxStats:
        """Get outbox statistics.

        Returns:
            OutboxStats with outbox metrics
        """
        ...


def outbox_event_data(event: DomainEvent) -> dict[str, Any]:
    """Build the JSON-safe payload stored in `event_outbox.event_data`.

    This is the single authority for that shape. Both sides of the outbox
    depend on it: the same-transaction writer
    (`eventsource.adapters.postgresql.store.PostgreSQLEventStore._write_to_outbox`)
    produces it, and every `OutboxRepository` adapter stores it and hands it
    back on `OutboxEntry.event_data`. A drain worker parses it to rebuild the
    event, so adding or renaming a key is a wire-format change, not a
    refactor.

    The result contains only `str`, `None`, and JSON-native values, so
    `json.dumps` serializes it without a custom encoder.
    """
    return {
        "event_id": str(event.event_id),
        "aggregate_id": str(event.aggregate_id),
        "aggregate_type": event.aggregate_type,
        "tenant_id": str(event.tenant_id) if event.tenant_id else None,
        "occurred_at": event.occurred_at.isoformat(),
        "payload": event.model_dump(mode="json"),
    }


__all__ = [
    "OutboxEntry",
    "OutboxRepository",
    "OutboxStats",
    "outbox_event_data",
]
