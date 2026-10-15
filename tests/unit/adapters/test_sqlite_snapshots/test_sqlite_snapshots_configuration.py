"""Tests for SQLiteSnapshotStore configuration, lifecycle, persistence, and concurrency."""

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.adapters.sqlite import SQLiteEventStore
    from eventsource.adapters.sqlite.snapshots import (
        AIOSQLITE_AVAILABLE as SNAPSHOTS_AIOSQLITE_AVAILABLE,
    )
    from eventsource.adapters.sqlite.snapshots import SQLiteSnapshotStore
    from eventsource.ports.lifecycle import SupportsAsyncContextManager, SupportsClose
    from eventsource.ports.snapshots import Snapshot

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


def _make_snapshot(aggregate_id: UUID, state: dict[str, object], version: int = 1) -> Snapshot:
    return Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="Order",
        version=version,
        state=state,
        schema_version=1,
        created_at=datetime.now(UTC),
    )


class TestSQLiteSnapshotStoreConfiguration:
    def test_sqlite_available_flag(self):
        assert SNAPSHOTS_AIOSQLITE_AVAILABLE is True

    def test_init_with_path(self):
        store = SQLiteSnapshotStore("/path/to/db.sqlite")
        assert store.database_path == "/path/to/db.sqlite"

    def test_init_with_memory(self):
        store = SQLiteSnapshotStore(":memory:")
        assert store.database_path == ":memory:"


class TestSQLiteSnapshotStoreFilePersistence:
    @pytest.mark.asyncio
    async def test_file_based_persistence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "persist_test.db")
            aggregate_id = uuid4()

            store1 = SQLiteSnapshotStore(db_path)
            await store1.save_snapshot(
                _make_snapshot(aggregate_id, {"persisted": True}, version=100)
            )
            await store1.close()

            store2 = SQLiteSnapshotStore(db_path)
            try:
                loaded = await store2.get_snapshot(aggregate_id, "Order")
            finally:
                await store2.close()

            assert loaded is not None
            assert loaded.state["persisted"] is True


class TestSQLiteSnapshotStoreLifecycle:
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
        before = {t.name for t in threading.enumerate()}
        store = SQLiteSnapshotStore(":memory:")
        await store.snapshot_exists(uuid4(), "Order")
        during = {t.name for t in threading.enumerate()}
        assert during - before, "expected aiosqlite to have started a worker thread"

        await store.close()
        for _ in range(100):
            if not ({t.name for t in threading.enumerate()} - before):
                break
            await asyncio.sleep(0.01)

        assert {t.name for t in threading.enumerate()} - before == set()

    @pytest.mark.asyncio
    async def test_memory_database_keeps_one_connection_across_operations(self):
        store = SQLiteSnapshotStore(":memory:")
        try:
            aggregate_id = uuid4()
            await store.save_snapshot(_make_snapshot(aggregate_id, {"in": "memory"}))
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
            await store.save_snapshot(_make_snapshot(aggregate_id, {"round": "trip"}))
            await store.close()

            try:
                loaded = await store.get_snapshot(aggregate_id, "Order")
            finally:
                await store.close()

            assert loaded is not None
            assert loaded.state["round"] == "trip"

    @pytest.mark.asyncio
    async def test_async_context_manager_closes_connection_on_exit(self):
        store = SQLiteSnapshotStore(":memory:")
        assert isinstance(store, SupportsAsyncContextManager)
        async with store as entered:
            assert entered is store
            await store.snapshot_exists(uuid4(), "Order")
            assert store._connection is not None

        assert store._connection is None

    @pytest.mark.asyncio
    async def test_unclosed_snapshot_store_emits_resource_warning_on_del(self):
        store = SQLiteSnapshotStore(":memory:")
        await store.snapshot_exists(uuid4(), "Order")
        try:
            with pytest.warns(
                ResourceWarning, match=r"Unclosed SQLiteSnapshotStore \(':memory:'\)"
            ):
                store.__del__()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_unclosed_event_store_emits_resource_warning_and_supports_async_with(self):
        async with SQLiteEventStore(":memory:") as event_store:
            assert isinstance(event_store, SupportsAsyncContextManager)
            await event_store.event_exists(uuid4())
            assert event_store._connection is not None

        assert event_store._connection is None

        leaked = SQLiteEventStore(":memory:")
        await leaked.event_exists(uuid4())
        try:
            with pytest.warns(ResourceWarning, match=r"Unclosed SQLiteEventStore \(':memory:'\)"):
                leaked.__del__()
        finally:
            await leaked.close()


class TestSQLiteSnapshotStoreConcurrency:
    @pytest.fixture
    async def db_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield os.path.join(tmpdir, "concurrent_test.db")

    @pytest.fixture
    async def store(self, db_path):
        store = SQLiteSnapshotStore(db_path)
        yield store
        await store.close()

    @pytest.mark.asyncio
    async def test_concurrent_save_operations(self, store):
        aggregate_id = uuid4()

        async def save_snapshot(version: int):
            await store.save_snapshot(
                _make_snapshot(aggregate_id, {"version": version}, version=version)
            )

        await asyncio.gather(*[save_snapshot(i) for i in range(10)])
        loaded = await store.get_snapshot(aggregate_id, "Order")
        assert loaded is not None
