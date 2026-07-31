"""Tests for the adapter contract using the always-available memory adapters."""

from uuid import uuid4

from bench.adapters.buses import BUS_ADAPTERS, MemoryBusAdapter
from bench.adapters.snapshots import SNAPSHOT_ADAPTERS, MemorySnapshotAdapter
from bench.adapters.stores import STORE_ADAPTERS, MemoryStoreAdapter
from bench.core.domain import make_events


async def test_memory_store_adapter_lifecycle() -> None:
    adapter = MemoryStoreAdapter()
    assert adapter.name == "memory"
    assert await adapter.available() is None
    await adapter.setup()
    store = await adapter.create()
    aggregate_id = uuid4()
    result = await store.append_events(
        aggregate_id, "Bench", make_events(aggregate_id, 2), expected_version=0
    )
    assert result.success
    await adapter.destroy(store)
    await adapter.teardown()


async def test_memory_bus_adapter_delivers() -> None:
    adapter = MemoryBusAdapter()
    assert await adapter.available() is None
    await adapter.setup()
    bus = await adapter.create()
    received: list[object] = []

    async def handler(event: object) -> None:
        received.append(event)

    from bench.core.domain import BenchEvent

    bus.subscribe(BenchEvent, handler)
    await adapter.start_delivery(bus)
    aggregate_id = uuid4()
    await bus.publish(make_events(aggregate_id, 1))
    assert len(received) == 1
    await adapter.stop_delivery(bus)
    await adapter.destroy(bus)
    await adapter.teardown()


async def test_memory_snapshot_adapter_roundtrip() -> None:
    from datetime import UTC, datetime

    from eventsource.ports.snapshots import Snapshot

    adapter = MemorySnapshotAdapter()
    await adapter.setup()
    snapshot_store = await adapter.create()
    aggregate_id = uuid4()
    snapshot = Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="Bench",
        version=1,
        state={"blob": "x"},
        schema_version=1,
        created_at=datetime.now(UTC),
    )
    await snapshot_store.save_snapshot(snapshot)
    loaded = await snapshot_store.get_snapshot(aggregate_id, "Bench")
    assert loaded is not None and loaded.version == 1
    await adapter.destroy(snapshot_store)
    await adapter.teardown()


def test_registries_contain_memory() -> None:
    assert STORE_ADAPTERS["memory"] is MemoryStoreAdapter
    assert BUS_ADAPTERS["memory"] is MemoryBusAdapter
    assert SNAPSHOT_ADAPTERS["memory"] is MemorySnapshotAdapter
