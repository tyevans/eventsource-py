"""Helper functions for populating and transferring test events."""

from __future__ import annotations

from uuid import UUID, uuid4

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.migration.test_final_integration._events import OrderCreated


async def create_test_events(
    store: InMemoryEventStore,
    tenant_id: UUID,
    count: int = 10,
    aggregate_type: str = "Order",
) -> list[UUID]:
    """Create test events in the store and return aggregate IDs."""
    aggregate_ids = []

    for i in range(count):
        aggregate_id = uuid4()
        aggregate_ids.append(aggregate_id)

        event = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            customer_id=f"customer-{i}",
            amount=100.0 + i,
        )

        await store.append(
            StreamId(aggregate_id=aggregate_id, category=aggregate_type),
            [event],
            ExpectedVersion.no_stream(),
        )

    return aggregate_ids


async def copy_events_between_stores(
    source_store: InMemoryEventStore,
    target_store: InMemoryEventStore,
    tenant_id: UUID,
) -> int:
    """Copy events from source to target store."""
    count = 0
    async for envelope in source_store.read_all():
        if envelope.event.tenant_id == tenant_id:
            event = envelope.event
            await target_store.append(
                StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type),
                [event],
                ExpectedVersion.any_(),
            )
            count += 1
    return count


__all__ = [  # pragma: allowlist secret
    "copy_events_between_stores",
    "create_test_events",
]
