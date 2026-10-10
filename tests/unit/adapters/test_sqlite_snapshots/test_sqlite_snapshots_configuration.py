"""Tests for SQLiteSnapshotStore configuration, lifecycle, persistence, and concurrency."""

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.adapters.sqlite.snapshots import (
        AIOSQLITE_AVAILABLE as SNAPSHOTS_AIOSQLITE_AVAILABLE,
    )
    from eventsource.adapters.sqlite.snapshots import SQLiteSnapshotStore
    from eventsource.ports.lifecycle import SupportsClose
    from eventsource.ports.snapshots import Snapshot

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


class TestSQLiteSnapshotStoreConfiguration:
    """Tests for SQLiteSnapshotStore configuration."""

    def test_sqlite_available_flag(self):
        """Test that snapshots.AIOSQLITE_AVAILABLE is True when aiosqlite is installed."""
        assert SNAPSHOTS_AIOSQLITE_AVAILABLE is True

    def test_init_with_path(self):
        """Test initialization with a database path."""
        store = SQLiteSnapshotStore("/path/to/db.sqlite")
        assert store.database_path == "/path/to/db.sqlite"

    def test_init_with_memory(self):
        """Test initialization with in-memory database."""
        store = SQLiteSnapshotStore(":memory:")
        assert store.database_path == ":memory:"


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
