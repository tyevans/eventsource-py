"""Tests for SQLiteSnapshotStore CRUD operations."""

from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.adapters.sqlite.snapshots import SQLiteSnapshotStore
    from eventsource.ports.snapshots import Snapshot

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


def _make_snapshot(
    aggregate_id: UUID | None = None,
    aggregate_type: str = "Order",
    version: int = 1,
    state: dict | None = None,
    schema_version: int = 1,
    created_at: datetime | None = None,
) -> Snapshot:
    return Snapshot(
        aggregate_id=aggregate_id or uuid4(),
        aggregate_type=aggregate_type,
        version=version,
        state=state if state is not None else {},
        schema_version=schema_version,
        created_at=created_at or datetime.now(UTC),
    )


class TestSQLiteSnapshotStoreOperations:
    @pytest.fixture
    async def db_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield os.path.join(tmpdir, "test_snapshots.db")

    @pytest.fixture
    async def store(self, db_path):
        store = SQLiteSnapshotStore(db_path)
        yield store
        await store.close()

    @pytest.fixture
    def sample_snapshot(self):
        return _make_snapshot(
            version=100,
            state={"status": "shipped", "items": ["item1", "item2"]},
        )

    @pytest.mark.asyncio
    async def test_save_and_get_snapshot(self, store, sample_snapshot):
        await store.save_snapshot(sample_snapshot)
        loaded = await store.get_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        assert loaded is not None
        assert loaded.aggregate_id == sample_snapshot.aggregate_id
        assert loaded.aggregate_type == sample_snapshot.aggregate_type
        assert loaded.version == sample_snapshot.version
        assert loaded.state == sample_snapshot.state
        assert loaded.schema_version == sample_snapshot.schema_version

    @pytest.mark.asyncio
    async def test_get_nonexistent_snapshot_returns_none(self, store):
        assert await store.get_snapshot(uuid4(), "Order") is None

    @pytest.mark.asyncio
    async def test_save_overwrites_existing_snapshot(self, store):
        aggregate_id = uuid4()
        snapshot_v1 = _make_snapshot(aggregate_id, "Order", 50, {"status": "pending"})
        snapshot_v2 = _make_snapshot(aggregate_id, "Order", 100, {"status": "shipped"})
        await store.save_snapshot(snapshot_v1)
        await store.save_snapshot(snapshot_v2)

        loaded = await store.get_snapshot(aggregate_id, "Order")
        assert loaded is not None
        assert loaded.version == 100
        assert loaded.state["status"] == "shipped"

    @pytest.mark.asyncio
    async def test_delete_snapshot_returns_true_when_exists(self, store, sample_snapshot):
        await store.save_snapshot(sample_snapshot)
        result = await store.delete_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        assert result is True
        assert (
            await store.get_snapshot(sample_snapshot.aggregate_id, sample_snapshot.aggregate_type)
            is None
        )

    @pytest.mark.asyncio
    async def test_delete_snapshot_returns_false_when_missing(self, store):
        assert await store.delete_snapshot(uuid4(), "Order") is False

    @pytest.mark.asyncio
    async def test_snapshot_exists_returns_true(self, store, sample_snapshot):
        await store.save_snapshot(sample_snapshot)
        assert (
            await store.snapshot_exists(
                sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
            )
            is True
        )

    @pytest.mark.asyncio
    async def test_snapshot_exists_returns_false(self, store):
        assert await store.snapshot_exists(uuid4(), "Order") is False

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type(self, store):
        order_ids = [uuid4() for _ in range(3)]
        user_ids = [uuid4() for _ in range(2)]
        for agg_id in order_ids:
            await store.save_snapshot(_make_snapshot(agg_id, "Order"))
        for agg_id in user_ids:
            await store.save_snapshot(_make_snapshot(agg_id, "User"))

        deleted = await store.delete_snapshots_by_type("Order")
        assert deleted == 3
        for agg_id in user_ids:
            assert await store.snapshot_exists(agg_id, "User") is True

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_with_schema_version_filter(self, store):
        agg_ids = []
        for schema_v in [1, 2, 3]:
            agg_id = uuid4()
            agg_ids.append((agg_id, schema_v))
            await store.save_snapshot(_make_snapshot(agg_id, "Order", schema_version=schema_v))

        deleted = await store.delete_snapshots_by_type("Order", schema_version_below=3)
        assert deleted == 2
        for agg_id, schema_v in agg_ids:
            exists = await store.snapshot_exists(agg_id, "Order")
            assert exists is (schema_v >= 3)

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_no_matches(self, store):
        await store.save_snapshot(_make_snapshot(aggregate_type="Order"))
        assert await store.delete_snapshots_by_type("User") == 0

    @pytest.mark.asyncio
    async def test_different_aggregate_types_are_separate(self, store):
        aggregate_id = uuid4()
        await store.save_snapshot(_make_snapshot(aggregate_id, "Order", 10, {"type": "order"}))
        await store.save_snapshot(_make_snapshot(aggregate_id, "User", 20, {"type": "user"}))

        order_loaded = await store.get_snapshot(aggregate_id, "Order")
        user_loaded = await store.get_snapshot(aggregate_id, "User")
        assert order_loaded is not None and user_loaded is not None
        assert order_loaded.state["type"] == "order"
        assert user_loaded.state["type"] == "user"

    @pytest.mark.asyncio
    async def test_complex_state_serialization(self, store):
        aggregate_id = uuid4()
        complex_state = {
            "string": "value",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "null": None,
            "list": [1, 2, 3],
            "nested": {"deep": {"value": "found"}},
        }
        await store.save_snapshot(_make_snapshot(aggregate_id, "Complex", 1, complex_state))
        loaded = await store.get_snapshot(aggregate_id, "Complex")
        assert loaded is not None and loaded.state == complex_state

    @pytest.mark.asyncio
    async def test_datetime_serialization(self, store):
        aggregate_id = uuid4()
        created_time = datetime(2024, 6, 15, 12, 30, 45, tzinfo=UTC)
        await store.save_snapshot(_make_snapshot(aggregate_id, "Order", created_at=created_time))
        loaded = await store.get_snapshot(aggregate_id, "Order")
        assert loaded is not None and loaded.created_at == created_time
