"""CUTOVER_PAUSED blocks writes even when a dual-write interceptor is still registered."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.router import TenantStoreRouter, WritePausedError
from eventsource.domain import StreamId
from eventsource.ports.migration.models import TenantMigrationState
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.migration.test_cutover_safety._harness import (
    InMemoryRoutingRepository,
)
from tests.unit.application.migration.test_phase3_integration._events import OrderCreated


async def _count_events(store: InMemoryEventStore) -> int:
    return sum([1 async for _ in store.read_all()])


def _router_with_interceptor() -> tuple[TenantStoreRouter, InMemoryRoutingRepository, InMemoryEventStore, InMemoryEventStore, UUID]:  # fmt: skip
    source, interceptor_store = InMemoryEventStore("source"), InMemoryEventStore("interceptor")
    routing = InMemoryRoutingRepository()
    router = TenantStoreRouter(
        default_store=source,
        routing_repo=routing,
        stores={"default": source},
        default_store_id="default",
        enable_tracing=False,
        write_pause_timeout=0.1,
    )
    tenant_id = uuid4()
    router.set_dual_write_interceptor(tenant_id, interceptor_store)
    return router, routing, source, interceptor_store, tenant_id


def _event(tenant_id: UUID) -> OrderCreated:
    return OrderCreated(aggregate_id=uuid4(), tenant_id=tenant_id)


@pytest.mark.asyncio
async def test_paused_tenant_with_interceptor_rejects_write() -> None:
    router, routing, source, interceptor_store, tenant_id = _router_with_interceptor()
    await routing.set_migration_state(tenant_id, TenantMigrationState.CUTOVER_PAUSED, uuid4())
    event = _event(tenant_id)

    with pytest.raises(WritePausedError):
        await router.append(StreamId(aggregate_id=event.aggregate_id, category="Order"), [event], ExpectedVersion.no_stream())  # fmt: skip

    assert await _count_events(interceptor_store) == 0
    assert await _count_events(source) == 0


@pytest.mark.asyncio
async def test_dual_write_tenant_with_interceptor_still_writes_through_interceptor() -> None:
    router, routing, source, interceptor_store, tenant_id = _router_with_interceptor()
    await routing.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE, uuid4())
    event = _event(tenant_id)

    await router.append(StreamId(aggregate_id=event.aggregate_id, category="Order"), [event], ExpectedVersion.no_stream())  # fmt: skip

    assert await _count_events(interceptor_store) == 1
    assert await _count_events(source) == 0
