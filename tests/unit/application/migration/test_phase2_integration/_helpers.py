"""Helper functions for phase 2 migration integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from tests.unit.application.migration.test_phase2_integration._events import (
    OrderCreated,
    SampleTestEvent,
)

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.dual_write import DualWriteInterceptor
    from eventsource.domain.event import DomainEvent
    from eventsource.ports import Position


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


async def drive_dual_writes(
    interceptor: DualWriteInterceptor,
    tenant_id: UUID,
    count: int,
) -> None:
    """Push `count` single-event appends through the interceptor."""
    for _i in range(count):
        aggregate_id = uuid4()
        event = SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        await interceptor.append(
            StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )


async def seed_copied_prefix(
    source_store: InMemoryEventStore,
    target_store: InMemoryEventStore,
    tenant_id: UUID,
    count: int,
) -> Position | None:
    """Write `count` events to BOTH stores; return the last source position.

    Stands in for a completed bulk-copy pass.
    """
    last_source_position: Position | None = None
    for _i in range(count):
        aggregate_id = uuid4()
        event = SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="SampleAggregate")
        result = await source_store.append(stream, [event], ExpectedVersion.no_stream())
        last_source_position = result.position
        await target_store.append(stream, [event], ExpectedVersion.no_stream())
    return last_source_position


__all__ = [
    "create_test_events",
    "drive_dual_writes",
    "get_all_tenant_events",
    "seed_copied_prefix",
]
