"""SQLite adapters run for real; PostgreSQL adapters are probed only."""

from uuid import uuid4

from bench.adapters._postgres import asyncpg_dsn, postgres_url
from bench.adapters.snapshots import SNAPSHOT_ADAPTERS, SQLiteSnapshotAdapter
from bench.adapters.stores import (
    STORE_ADAPTERS,
    PostgresStoreAdapter,
    SQLiteStoreAdapter,
)
from bench.core.domain import make_events
from eventsource.domain.stream_id import StreamId
from eventsource.ports import ExpectedVersion, collect


def test_registries_contain_sql_backends() -> None:
    assert STORE_ADAPTERS["postgresql"] is PostgresStoreAdapter
    assert STORE_ADAPTERS["sqlite"] is SQLiteStoreAdapter
    assert "postgresql" in SNAPSHOT_ADAPTERS and "sqlite" in SNAPSHOT_ADAPTERS


def test_asyncpg_dsn_strips_driver() -> None:
    assert asyncpg_dsn("postgresql+asyncpg://u:p@h:5/db") == "postgresql://u:p@h:5/db"
    assert postgres_url().startswith("postgresql+asyncpg://")


async def test_sqlite_store_adapter_appends_and_reads() -> None:
    adapter = SQLiteStoreAdapter()
    assert await adapter.available() is None
    await adapter.setup()
    store = await adapter.create()
    aggregate_id = uuid4()
    stream = StreamId(aggregate_id=aggregate_id, category="Bench")
    result = await store.append(stream, make_events(aggregate_id, 3), ExpectedVersion.no_stream())
    assert result.new_version == 3
    envelopes = await collect(store.read_stream(stream))
    assert len(envelopes) == 3
    await adapter.destroy(store)
    await adapter.teardown()


async def test_sqlite_snapshot_adapter_roundtrip() -> None:
    from datetime import UTC, datetime

    from eventsource.ports.snapshots import Snapshot

    adapter = SQLiteSnapshotAdapter()
    await adapter.setup()
    store = await adapter.create()
    aggregate_id = uuid4()
    await store.save_snapshot(
        Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Bench",
            version=1,
            state={"blob": "x"},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
    )
    loaded = await store.get_snapshot(aggregate_id, "Bench")
    assert loaded is not None
    await adapter.destroy(store)
    await adapter.teardown()


async def test_postgres_adapter_unavailable_without_service() -> None:
    adapter = PostgresStoreAdapter(url="postgresql+asyncpg://x:x@localhost:1/nope")
    reason = await adapter.available()
    assert reason is not None and "postgres" in reason.lower()
