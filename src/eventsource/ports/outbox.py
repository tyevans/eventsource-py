"""Transactional outbox port.

Pure boundary interface: stdlib, typing, uuid, datetime, dataclasses, and
`DomainEvent` only. No sqlalchemy, no driver types.
"""

from typing import Any

from eventsource.events.base import DomainEvent


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


__all__ = ["outbox_event_data"]
