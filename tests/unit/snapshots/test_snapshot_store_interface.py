"""Unit tests for the SnapshotStore interface."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.snapshots import Snapshot, SnapshotStore


class TestSnapshotStoreInterface:
    """Tests for the SnapshotStore abstract base class."""

    def test_snapshot_store_is_abstract(self):
        """Test that SnapshotStore cannot be instantiated directly."""
        with pytest.raises(TypeError, match="abstract"):
            SnapshotStore()  # type: ignore

    def test_snapshot_store_requires_save_snapshot(self):
        """Test that save_snapshot must be implemented."""

        class IncompleteStore(SnapshotStore):
            async def get_snapshot(self, aggregate_id, aggregate_type):
                return None

            async def delete_snapshot(self, aggregate_id, aggregate_type):
                return False

        with pytest.raises(TypeError, match="save_snapshot"):
            IncompleteStore()  # type: ignore

    def test_snapshot_store_requires_get_snapshot(self):
        """Test that get_snapshot must be implemented."""

        class IncompleteStore(SnapshotStore):
            async def save_snapshot(self, snapshot):
                pass

            async def delete_snapshot(self, aggregate_id, aggregate_type):
                return False

        with pytest.raises(TypeError, match="get_snapshot"):
            IncompleteStore()  # type: ignore

    def test_snapshot_store_requires_delete_snapshot(self):
        """Test that delete_snapshot must be implemented."""

        class IncompleteStore(SnapshotStore):
            async def save_snapshot(self, snapshot):
                pass

            async def get_snapshot(self, aggregate_id, aggregate_type):
                return None

        with pytest.raises(TypeError, match="delete_snapshot"):
            IncompleteStore()  # type: ignore


class TestSnapshotStoreDefaultMethods:
    """Test default method implementations."""

    @pytest.fixture
    def mock_store(self):
        """Create a minimal concrete implementation for testing defaults."""

        class MockSnapshotStore(SnapshotStore):
            def __init__(self):
                self._snapshots = {}

            async def save_snapshot(self, snapshot: Snapshot) -> None:
                key = (snapshot.aggregate_id, snapshot.aggregate_type)
                self._snapshots[key] = snapshot

            async def get_snapshot(self, aggregate_id, aggregate_type):
                return self._snapshots.get((aggregate_id, aggregate_type))

            async def delete_snapshot(self, aggregate_id, aggregate_type):
                key = (aggregate_id, aggregate_type)
                if key in self._snapshots:
                    del self._snapshots[key]
                    return True
                return False

        return MockSnapshotStore()

    @pytest.mark.asyncio
    async def test_snapshot_exists_returns_true_when_present(self, mock_store):
        """Test snapshot_exists returns True when snapshot exists."""
        aggregate_id = uuid4()
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=1,
            state={},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await mock_store.save_snapshot(snapshot)

        assert await mock_store.snapshot_exists(aggregate_id, "Order") is True

    @pytest.mark.asyncio
    async def test_snapshot_exists_returns_false_when_missing(self, mock_store):
        """Test snapshot_exists returns False when no snapshot."""
        assert await mock_store.snapshot_exists(uuid4(), "Order") is False

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_raises_not_implemented(self, mock_store):
        """Test delete_snapshots_by_type raises NotImplementedError by default."""
        with pytest.raises(NotImplementedError):
            await mock_store.delete_snapshots_by_type("Order")

    @pytest.mark.asyncio
    async def test_snapshot_exists_for_different_types(self, mock_store):
        """Test snapshot_exists distinguishes between aggregate types."""
        aggregate_id = uuid4()

        # Save snapshot for Order
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=1,
            state={},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await mock_store.save_snapshot(snapshot)

        # Should exist for Order but not for User
        assert await mock_store.snapshot_exists(aggregate_id, "Order") is True
        assert await mock_store.snapshot_exists(aggregate_id, "User") is False
