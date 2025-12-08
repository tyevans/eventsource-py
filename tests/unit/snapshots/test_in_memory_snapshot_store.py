"""Unit tests for InMemorySnapshotStore."""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.snapshots import Snapshot
from eventsource.snapshots.in_memory import InMemorySnapshotStore


class TestInMemorySnapshotStore:
    """Tests for the InMemorySnapshotStore implementation."""

    @pytest.fixture
    def store(self):
        """Create a fresh InMemorySnapshotStore for each test."""
        return InMemorySnapshotStore()

    @pytest.fixture
    def sample_snapshot(self):
        """Create a sample snapshot for testing."""
        return Snapshot(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            version=100,
            state={"status": "shipped", "items": ["item1", "item2"]},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_save_and_get_snapshot(self, store, sample_snapshot):
        """Test basic save and retrieve cycle."""
        await store.save_snapshot(sample_snapshot)

        loaded = await store.get_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        assert loaded == sample_snapshot

    @pytest.mark.asyncio
    async def test_get_nonexistent_snapshot_returns_none(self, store):
        """Test that getting missing snapshot returns None."""
        result = await store.get_snapshot(uuid4(), "Order")
        assert result is None

    @pytest.mark.asyncio
    async def test_save_overwrites_existing_snapshot(self, store):
        """Test upsert behavior - new snapshot replaces old."""
        aggregate_id = uuid4()

        snapshot_v1 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=50,
            state={"status": "pending"},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        snapshot_v2 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=100,
            state={"status": "shipped"},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        await store.save_snapshot(snapshot_v1)
        await store.save_snapshot(snapshot_v2)

        loaded = await store.get_snapshot(aggregate_id, "Order")
        assert loaded == snapshot_v2
        assert loaded.version == 100

    @pytest.mark.asyncio
    async def test_delete_snapshot_returns_true_when_exists(self, store, sample_snapshot):
        """Test delete returns True when snapshot exists."""
        await store.save_snapshot(sample_snapshot)

        result = await store.delete_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        assert result is True
        assert (
            await store.get_snapshot(
                sample_snapshot.aggregate_id,
                sample_snapshot.aggregate_type,
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_delete_snapshot_returns_false_when_missing(self, store):
        """Test delete returns False when no snapshot."""
        result = await store.delete_snapshot(uuid4(), "Order")
        assert result is False

    @pytest.mark.asyncio
    async def test_snapshot_exists_returns_true(self, store, sample_snapshot):
        """Test snapshot_exists returns True when present."""
        await store.save_snapshot(sample_snapshot)

        assert (
            await store.snapshot_exists(
                sample_snapshot.aggregate_id,
                sample_snapshot.aggregate_type,
            )
            is True
        )

    @pytest.mark.asyncio
    async def test_snapshot_exists_returns_false(self, store):
        """Test snapshot_exists returns False when missing."""
        assert await store.snapshot_exists(uuid4(), "Order") is False

    @pytest.mark.asyncio
    async def test_clear_removes_all_snapshots(self, store):
        """Test clear() removes all snapshots."""
        for _ in range(5):
            snapshot = Snapshot(
                aggregate_id=uuid4(),
                aggregate_type="Order",
                version=1,
                state={},
                schema_version=1,
                created_at=datetime.now(UTC),
            )
            await store.save_snapshot(snapshot)

        assert store.snapshot_count == 5

        await store.clear()

        assert store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type(self, store):
        """Test bulk deletion by aggregate type."""
        # Create snapshots for different types
        order_ids = [uuid4() for _ in range(3)]
        user_ids = [uuid4() for _ in range(2)]

        for agg_id in order_ids:
            await store.save_snapshot(
                Snapshot(
                    aggregate_id=agg_id,
                    aggregate_type="Order",
                    version=1,
                    state={},
                    schema_version=1,
                    created_at=datetime.now(UTC),
                )
            )

        for agg_id in user_ids:
            await store.save_snapshot(
                Snapshot(
                    aggregate_id=agg_id,
                    aggregate_type="User",
                    version=1,
                    state={},
                    schema_version=1,
                    created_at=datetime.now(UTC),
                )
            )

        # Delete Order snapshots
        deleted = await store.delete_snapshots_by_type("Order")

        assert deleted == 3
        assert store.snapshot_count == 2  # Only User snapshots remain

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_with_schema_version_filter(self, store):
        """Test bulk deletion with schema version filter."""
        # Create snapshots with different schema versions
        for schema_v in [1, 2, 3]:
            await store.save_snapshot(
                Snapshot(
                    aggregate_id=uuid4(),
                    aggregate_type="Order",
                    version=1,
                    state={},
                    schema_version=schema_v,
                    created_at=datetime.now(UTC),
                )
            )

        # Delete only schema_version < 3
        deleted = await store.delete_snapshots_by_type("Order", schema_version_below=3)

        assert deleted == 2
        assert store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_no_matches(self, store):
        """Test bulk deletion when no snapshots match."""
        # Create snapshots for Order type
        await store.save_snapshot(
            Snapshot(
                aggregate_id=uuid4(),
                aggregate_type="Order",
                version=1,
                state={},
                schema_version=1,
                created_at=datetime.now(UTC),
            )
        )

        # Try to delete User snapshots (none exist)
        deleted = await store.delete_snapshots_by_type("User")

        assert deleted == 0
        assert store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, store):
        """Test thread safety with concurrent operations."""
        aggregate_id = uuid4()

        async def save_and_read():
            for i in range(10):
                snapshot = Snapshot(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    version=i,
                    state={"iteration": i},
                    schema_version=1,
                    created_at=datetime.now(UTC),
                )
                await store.save_snapshot(snapshot)
                await store.get_snapshot(aggregate_id, "Order")

        # Run multiple concurrent tasks
        await asyncio.gather(*[save_and_read() for _ in range(5)])

        # Should have one snapshot (last write wins)
        assert store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_different_aggregate_types_are_separate(self, store):
        """Test that same ID with different types are separate."""
        aggregate_id = uuid4()

        order_snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=10,
            state={"type": "order"},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        user_snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="User",
            version=20,
            state={"type": "user"},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        await store.save_snapshot(order_snapshot)
        await store.save_snapshot(user_snapshot)

        order_loaded = await store.get_snapshot(aggregate_id, "Order")
        user_loaded = await store.get_snapshot(aggregate_id, "User")

        assert order_loaded.state["type"] == "order"
        assert user_loaded.state["type"] == "user"

    @pytest.mark.asyncio
    async def test_snapshot_count_property(self, store):
        """Test the snapshot_count property."""
        assert store.snapshot_count == 0

        for i in range(3):
            await store.save_snapshot(
                Snapshot(
                    aggregate_id=uuid4(),
                    aggregate_type="Order",
                    version=i,
                    state={},
                    schema_version=1,
                    created_at=datetime.now(UTC),
                )
            )

        assert store.snapshot_count == 3

    def test_repr(self, store):
        """Test string representation."""
        assert "InMemorySnapshotStore" in repr(store)
        assert "snapshots=0" in repr(store)

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_schema_version_edge_case(self, store):
        """Test schema version filter boundary condition."""
        # Create snapshot with exactly the boundary version
        await store.save_snapshot(
            Snapshot(
                aggregate_id=uuid4(),
                aggregate_type="Order",
                version=1,
                state={},
                schema_version=5,
                created_at=datetime.now(UTC),
            )
        )

        # Delete schema_version < 5 should NOT delete this snapshot
        deleted = await store.delete_snapshots_by_type("Order", schema_version_below=5)

        assert deleted == 0
        assert store.snapshot_count == 1

        # Delete schema_version < 6 SHOULD delete this snapshot
        deleted = await store.delete_snapshots_by_type("Order", schema_version_below=6)

        assert deleted == 1
        assert store.snapshot_count == 0
