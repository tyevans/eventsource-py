"""Transactional outbox port (pure boundary interface)."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from eventsource.domain.event import DomainEvent


@dataclass
class OutboxEntry:
    """One row of the outbox."""

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
    """Aggregate statistics for the outbox."""

    pending_count: int = 0
    published_count: int = 0
    failed_count: int = 0
    oldest_pending: datetime | None = None
    avg_retries: float = 0.0


@runtime_checkable
class OutboxRepository(Protocol):
    """Protocol for transactional outbox repositories."""

    async def add_event(self, event: DomainEvent) -> UUID:
        """Add an event to the outbox for publishing inside the same transaction."""
        ...

    async def get_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """Get pending events that need to be published, oldest first."""
        ...

    async def mark_published(self, outbox_id: UUID) -> None:
        """Mark an outbox event as successfully published."""
        ...

    async def mark_failed(self, outbox_id: UUID, error: str) -> None:
        """Mark an outbox event as permanently failed."""
        ...

    async def increment_retry(self, outbox_id: UUID, error: str | None = None) -> None:
        """Increment retry count for a failed publishing attempt."""
        ...

    async def cleanup_published(self, days: int = 7) -> int:
        """Delete published events older than `days` and return count deleted."""
        ...

    async def get_stats(self) -> OutboxStats:
        """Get outbox statistics."""
        ...


def outbox_event_data(event: DomainEvent) -> dict[str, Any]:
    """Build the JSON-safe wire-format payload stored in `event_outbox.event_data`."""
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
