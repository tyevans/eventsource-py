"""Unit tests for InMemorySnapshotStore."""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.ports.snapshots import Snapshot


def _snap(
    aggregate_id=None,
    aggregate_type: str = "Order",
    version: int = 1,
    state: dict | None = None,
    schema_version: int = 1,
) -> Snapshot:
    return Snapshot(
        aggregate_id=aggregate_id or uuid4(),
        aggregate_type=aggregate_type,
        version=version,
        state=state if state is not None else {},
        schema_version=schema_version,
        created_at=datetime.now(UTC),
    )


class TestInMemorySnapshotStore:
    @pytest.fixture
    def store(self):
        return InMemorySnapshotStore()

    @pytest.fixture
    def sample_snapshot(self):
        return _snap(version=100, state={"status": "shipped", "items": ["item1", "item2"]})

    @pytest.mark.asyncio
    async def test_save_and_get_snapshot(self, store, sample_snapshot):
        await store.save_snapshot(sample_snapshot)
        loaded = await store.get_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        assert loaded == sample_snapshot

    @pytest.mark.asyncio
    async def test_get_nonexistent_snapshot_returns_none(self, store):
        assert await store.get_snapshot(uuid4(), "Order") is None

    @pytest.mark.asyncio
    async def test_save_overwrites_existing_snapshot(self, store):
        aggregate_id = uuid4()
        snapshot_v1 = _snap(aggregate_id=aggregate_id, version=50, state={"status": "pending"})
        snapshot_v2 = _snap(aggregate_id=aggregate_id, version=100, state={"status": "shipped"})

        await store.save_snapshot(snapshot_v1)
        await store.save_snapshot(snapshot_v2)

        loaded = await store.get_snapshot(aggregate_id, "Order")
        assert loaded == snapshot_v2
        assert loaded.version == 100

    @pytest.mark.asyncio
    async def test_delete_snapshot_returns_true_when_exists(self, store, sample_snapshot):
        await store.save_snapshot(sample_snapshot)
        agg_id, agg_type = sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        assert await store.delete_snapshot(agg_id, agg_type) is True
        assert await store.get_snapshot(agg_id, agg_type) is None

    @pytest.mark.asyncio
    async def test_delete_snapshot_returns_false_when_missing(self, store):
        assert await store.delete_snapshot(uuid4(), "Order") is False

    @pytest.mark.asyncio
    async def test_snapshot_exists_returns_true(self, store, sample_snapshot):
        await store.save_snapshot(sample_snapshot)
        agg_id, agg_type = sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        assert await store.snapshot_exists(agg_id, agg_type) is True

    @pytest.mark.asyncio
    async def test_snapshot_exists_returns_false(self, store):
        assert await store.snapshot_exists(uuid4(), "Order") is False

    @pytest.mark.asyncio
    async def test_clear_removes_all_snapshots(self, store):
        for _ in range(5):
            await store.save_snapshot(_snap())
        assert store.snapshot_count == 5
        await store.clear()
        assert store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type(self, store):
        for _ in range(3):
            await store.save_snapshot(_snap(aggregate_type="Order"))
        for _ in range(2):
            await store.save_snapshot(_snap(aggregate_type="User"))

        deleted = await store.delete_snapshots_by_type("Order")
        assert deleted == 3
        assert store.snapshot_count == 2

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_with_schema_version_filter(self, store):
        for schema_v in [1, 2, 3]:
            await store.save_snapshot(_snap(schema_version=schema_v))

        deleted = await store.delete_snapshots_by_type("Order", schema_version_below=3)
        assert deleted == 2
        assert store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_no_matches(self, store):
        await store.save_snapshot(_snap(aggregate_type="Order"))
        deleted = await store.delete_snapshots_by_type("User")
        assert deleted == 0
        assert store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, store):
        aggregate_id = uuid4()

        async def save_and_read():
            for i in range(10):
                await store.save_snapshot(
                    _snap(aggregate_id=aggregate_id, version=i, state={"iteration": i})
                )
                await store.get_snapshot(aggregate_id, "Order")

        await asyncio.gather(*[save_and_read() for _ in range(5)])
        assert store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_different_aggregate_types_are_separate(self, store):
        aggregate_id = uuid4()
        order_snapshot = _snap(
            aggregate_id=aggregate_id, aggregate_type="Order", version=10, state={"type": "order"}
        )
        user_snapshot = _snap(
            aggregate_id=aggregate_id, aggregate_type="User", version=20, state={"type": "user"}
        )
        await store.save_snapshot(order_snapshot)
        await store.save_snapshot(user_snapshot)

        order_loaded = await store.get_snapshot(aggregate_id, "Order")
        user_loaded = await store.get_snapshot(aggregate_id, "User")
        assert order_loaded.state["type"] == "order"
        assert user_loaded.state["type"] == "user"

    @pytest.mark.asyncio
    async def test_snapshot_count_property(self, store):
        assert store.snapshot_count == 0
        for i in range(3):
            await store.save_snapshot(_snap(version=i))
        assert store.snapshot_count == 3

    def test_repr(self, store):
        assert "InMemorySnapshotStore" in repr(store)
        assert "snapshots=0" in repr(store)

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_schema_version_edge_case(self, store):
        await store.save_snapshot(_snap(schema_version=5))
        assert await store.delete_snapshots_by_type("Order", schema_version_below=5) == 0
        assert store.snapshot_count == 1

        assert await store.delete_snapshots_by_type("Order", schema_version_below=6) == 1
        assert store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_async_context_manager_support(self, store, sample_snapshot):
        from eventsource.adapters.memory.store import InMemoryEventStore
        from eventsource.ports.lifecycle import SupportsAsyncContextManager, SupportsClose

        assert isinstance(store, SupportsClose)
        assert isinstance(store, SupportsAsyncContextManager)
        async with store as entered:
            assert entered is store
            await entered.save_snapshot(sample_snapshot)

        event_store = InMemoryEventStore()
        assert isinstance(event_store, SupportsClose)
        assert isinstance(event_store, SupportsAsyncContextManager)
        async with event_store as entered_event_store:
            assert entered_event_store is event_store
