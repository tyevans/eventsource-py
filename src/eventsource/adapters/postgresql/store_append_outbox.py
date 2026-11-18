"""Outbox row insert for PostgreSQL appends, written in the same transaction as the events."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from eventsource.adapters.serialization import json_dumps
from eventsource.domain.event import DomainEvent
from eventsource.ports import outbox_event_data

_INSERT_OUTBOX_SQL = text(
    "INSERT INTO event_outbox (id, event_id, event_type, aggregate_id, aggregate_type, "
    "tenant_id, event_data, created_at, status) VALUES (:id, :event_id, :event_type, "
    ":aggregate_id, :aggregate_type, :tenant_id, :event_data, :created_at, 'pending')"
)


async def write_outbox_row(session: AsyncSession, event: DomainEvent, aggregate_type: str) -> None:
    """Write one outbox row for `event`, on `session`, before commit."""
    await session.execute(
        _INSERT_OUTBOX_SQL,
        {
            "id": uuid4(),
            "event_id": event.event_id,
            "event_type": event.event_type,
            "aggregate_id": event.aggregate_id,
            "aggregate_type": aggregate_type,
            "tenant_id": str(event.tenant_id) if event.tenant_id else None,
            "event_data": json_dumps(outbox_event_data(event)),
            "created_at": datetime.now(UTC),
        },
    )


__all__ = ["write_outbox_row"]
