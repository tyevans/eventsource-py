"""Helper functions for Phase 3 integration tests."""

from __future__ import annotations

from uuid import UUID, uuid4

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.migration.test_phase3_integration._events import OrderCreated


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


async def get_all_tenant_events(
    store: InMemoryEventStore,
    tenant_id: UUID,
) -> list[DomainEvent]:
    """Get all events for a tenant from a store."""
    events = []
    async for envelope in store.read_all():
        if envelope.event.tenant_id == tenant_id:
            events.append(envelope.event)
    return events


async def copy_events_with_position_mapping(
    source_store: InMemoryEventStore,
    target_store: InMemoryEventStore,
    tenant_id: UUID,
    migration_id: UUID,
    position_mapper: PositionMapper,
) -> int:
    """Copy events from source to target and record position mappings."""
    envelopes = []
    async for envelope in source_store.read_all():
        if envelope.event.tenant_id == tenant_id:
            envelopes.append(envelope)

    for envelope in envelopes:
        event = envelope.event
        stream = StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type)
        result = await target_store.append(stream, [event], ExpectedVersion.any_())

        # Record position mapping (real positions from each store's feed)
        assert envelope.position is not None
        assert result.position is not None
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=envelope.position,
            target_position=result.position,
            event_id=event.event_id,
        )

    return len(envelopes)


__all__ = [  # pragma: allowlist secret
    "copy_events_with_position_mapping",
    "create_test_events",
    "get_all_tenant_events",
]
