"""Unit tests for SQLiteSnapshotStore."""

import asyncio
import os
import tempfile
import threading
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.adapters.sqlite.snapshots import SQLITE_AVAILABLE, SQLiteSnapshotStore
    from eventsource.ports.lifecycle import SupportsClose
    from eventsource.ports.snapshots import Snapshot


pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


class TestSQLiteSnapshotStoreConfiguration:
    """Tests for SQLiteSnapshotStore configuration."""

    def test_sqlite_available_flag(self):
        """Test that SQLITE_AVAILABLE is True when aiosqlite is installed."""
        assert SQLITE_AVAILABLE is True

    def test_init_with_path(self):
        """Test initialization with a database path."""
        store = SQLiteSnapshotStore("/path/to/db.sqlite")
        assert store.database_path == "/path/to/db.sqlite"

    def test_init_with_memory(self):
        """Test initialization with in-memory database."""
        store = SQLiteSnapshotStore(":memory:")
        assert store.database_path == ":memory:"


class TestSQLiteSnapshotStoreOperations:
    """Tests for SQLiteSnapshotStore CRUD operations."""

    @pytest.fixture
    async def db_path(self):
        """Create temporary database path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield os.path.join(tmpdir, "test_snapshots.db")

    @pytest.fixture
    async def store(self, db_path):
        """Create a store; it applies its own schema on first use."""
        store = SQLiteSnapshotStore(db_path)
        yield store
        await store.close()

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

        assert loaded is not None
        assert loaded.aggregate_id == sample_snapshot.aggregate_id
        assert loaded.aggregate_type == sample_snapshot.aggregate_type
        assert loaded.version == sample_snapshot.version
        assert loaded.state == sample_snapshot.state
        assert loaded.schema_version == sample_snapshot.schema_version

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
        assert loaded is not None
        assert loaded.version == 100
        assert loaded.state["status"] == "shipped"

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
        # Verify User snapshots remain
        for agg_id in user_ids:
            assert await store.snapshot_exists(agg_id, "User") is True

    @pytest.mark.asyncio
    async def test_delete_snapshots_by_type_with_schema_version_filter(self, store):
        """Test bulk deletion with schema version filter."""
        # Create snapshots with different schema versions
        agg_ids = []
        for schema_v in [1, 2, 3]:
            agg_id = uuid4()
            agg_ids.append((agg_id, schema_v))
            await store.save_snapshot(
                Snapshot(
                    aggregate_id=agg_id,
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
        # Verify schema_version=3 snapshot remains
        for agg_id, schema_v in agg_ids:
            exists = await store.snapshot_exists(agg_id, "Order")
            if schema_v < 3:
                assert exists is False
            else:
                assert exists is True

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

        assert order_loaded is not None
        assert user_loaded is not None
        assert order_loaded.state["type"] == "order"
        assert user_loaded.state["type"] == "user"

    @pytest.mark.asyncio
    async def test_complex_state_serialization(self, store):
        """Test that complex nested state is properly serialized/deserialized."""
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

        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Complex",
            version=1,
            state=complex_state,
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        await store.save_snapshot(snapshot)
        loaded = await store.get_snapshot(aggregate_id, "Complex")

        assert loaded is not None
        assert loaded.state == complex_state

    @pytest.mark.asyncio
    async def test_datetime_serialization(self, store):
        """Test that datetime is properly serialized/deserialized."""
        aggregate_id = uuid4()
        created_time = datetime(2024, 6, 15, 12, 30, 45, tzinfo=UTC)

        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=1,
            state={},
            schema_version=1,
            created_at=created_time,
        )

        await store.save_snapshot(snapshot)
        loaded = await store.get_snapshot(aggregate_id, "Order")

        assert loaded is not None
        assert loaded.created_at == created_time


class TestSQLiteSnapshotStoreFilePersistence:
    """Tests for file-based SQLite persistence."""

    @pytest.mark.asyncio
    async def test_file_based_persistence(self):
        """Test that snapshots persist to file and survive restart."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "persist_test.db")

            aggregate_id = uuid4()

            # First session: the store applies its own schema on first use
            store1 = SQLiteSnapshotStore(db_path)
            snapshot = Snapshot(
                aggregate_id=aggregate_id,
                aggregate_type="Order",
                version=100,
                state={"persisted": True},
                schema_version=1,
                created_at=datetime.now(UTC),
            )
            await store1.save_snapshot(snapshot)
            await store1.close()

            # Second session: verify snapshot persisted
            store2 = SQLiteSnapshotStore(db_path)
            try:
                loaded = await store2.get_snapshot(aggregate_id, "Order")
            finally:
                await store2.close()

            assert loaded is not None
            assert loaded.state["persisted"] is True


class TestSQLiteSnapshotStoreLifecycle:
    """Tests for the SupportsClose capability and the shared connection."""

    def test_satisfies_supports_close(self):
        assert isinstance(SQLiteSnapshotStore(":memory:"), SupportsClose)

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        store = SQLiteSnapshotStore(":memory:")
        await store.snapshot_exists(uuid4(), "Order")

        await store.close()
        await store.close()

    @pytest.mark.asyncio
    async def test_close_before_first_use_is_a_no_op(self):
        await SQLiteSnapshotStore(":memory:").close()

    @pytest.mark.asyncio
    async def test_close_releases_the_aiosqlite_worker_thread(self):
        """The reason `close()` exists: aiosqlite's worker is non-daemon.

        A thread nothing releases keeps the interpreter alive at shutdown,
        which is what forced consumers to thread one shared store instance
        through every call site rather than construct a second one.
        """
        before = {t.name for t in threading.enumerate()}

        store = SQLiteSnapshotStore(":memory:")
        await store.snapshot_exists(uuid4(), "Order")
        during = {t.name for t in threading.enumerate()}
        assert during - before, "expected aiosqlite to have started a worker thread"

        await store.close()
        # The worker exits asynchronously once its queue drains.
        for _ in range(100):
            if not ({t.name for t in threading.enumerate()} - before):
                break
            await asyncio.sleep(0.01)

        assert {t.name for t in threading.enumerate()} - before == set()

    @pytest.mark.asyncio
    async def test_memory_database_keeps_one_connection_across_operations(self):
        """A per-operation connection would see a different, empty ":memory:" db."""
        store = SQLiteSnapshotStore(":memory:")
        try:
            aggregate_id = uuid4()
            await store.save_snapshot(
                Snapshot(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    version=1,
                    state={"in": "memory"},
                    schema_version=1,
                    created_at=datetime.now(UTC),
                )
            )

            loaded = await store.get_snapshot(aggregate_id, "Order")
        finally:
            await store.close()

        assert loaded is not None
        assert loaded.state["in"] == "memory"

    @pytest.mark.asyncio
    async def test_reopens_after_close(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteSnapshotStore(os.path.join(tmpdir, "reopen.db"))
            aggregate_id = uuid4()
            await store.save_snapshot(
                Snapshot(
                    aggregate_id=aggregate_id,
                    aggregate_type="Order",
                    version=1,
                    state={"round": "trip"},
                    schema_version=1,
                    created_at=datetime.now(UTC),
                )
            )
            await store.close()

            try:
                loaded = await store.get_snapshot(aggregate_id, "Order")
            finally:
                await store.close()

            assert loaded is not None
            assert loaded.state["round"] == "trip"


class TestSQLiteSnapshotStoreConcurrency:
    """Tests for concurrent operations."""

    @pytest.fixture
    async def db_path(self):
        """Create temporary database path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield os.path.join(tmpdir, "concurrent_test.db")

    @pytest.fixture
    async def store(self, db_path):
        """Create a store; it applies its own schema on first use."""
        store = SQLiteSnapshotStore(db_path)
        yield store
        await store.close()

    @pytest.mark.asyncio
    async def test_concurrent_save_operations(self, store):
        """Test concurrent save operations."""
        aggregate_id = uuid4()

        async def save_snapshot(version: int):
            snapshot = Snapshot(
                aggregate_id=aggregate_id,
                aggregate_type="Order",
                version=version,
                state={"version": version},
                schema_version=1,
                created_at=datetime.now(UTC),
            )
            await store.save_snapshot(snapshot)

        # Run multiple concurrent saves
        await asyncio.gather(*[save_snapshot(i) for i in range(10)])

        # Should have one snapshot (last write wins)
        loaded = await store.get_snapshot(aggregate_id, "Order")
        assert loaded is not None
