"""Conformance tests for PostgreSQLEventStore against the port suites."""

import asyncio
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from ..conftest import skip_if_no_postgres_infra

pytestmark = [pytest.mark.integration, pytest.mark.postgres, skip_if_no_postgres_infra]

from eventsource.adapters.postgresql import (  # noqa: E402
    PostgreSQLEventStore,
    PostgreSQLSnapshotStore,
)
from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository  # noqa: E402
from eventsource.adapters.sql.dlq import SQLDLQRepository  # noqa: E402
from eventsource.adapters.sql.schemas import get_schema  # noqa: E402
from eventsource.domain.exceptions import OptimisticLockError  # noqa: E402
from eventsource.ports import ExpectedVersion, collect  # noqa: E402
from eventsource.testing.conformance_ports import (  # noqa: E402
    AppenderConformance,
    CategoryQueryConformance,
    CheckpointRepositoryConformance,
    DLQRepositoryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    SnapshotStoreConformance,
    StreamReaderConformance,
)
from eventsource.testing.conformance_ports._fixtures import (  # noqa: E402
    make_conformance_registry,
    make_event,
    make_stream,
)


async def _fresh_store(connection_url: str) -> PostgreSQLEventStore:
    engine = create_async_engine(connection_url)
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS events CASCADE"))
    return PostgreSQLEventStore(
        engine, event_registry=make_conformance_registry(), create_schema=True, owns_engine=True
    )


async def _exec_raw_schema(conn: object, schema_name: str) -> None:
    raw = await conn.get_raw_connection()  # type: ignore[attr-defined]
    assert raw.driver_connection is not None
    await raw.driver_connection.execute(get_schema(schema_name, "postgresql"))


class _PGStoreFixture:
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLEventStore]:
        store = await _fresh_store(ports_postgres_connection_url)
        yield store
        await store.close()


class TestPostgreSQLAppender(_PGStoreFixture, AppenderConformance):
    pass


class TestPostgreSQLStreamReader(_PGStoreFixture, StreamReaderConformance):
    pass


class TestPostgreSQLEventLookup(_PGStoreFixture, EventLookupConformance):
    pass


class TestPostgreSQLGlobalFeed(_PGStoreFixture, GlobalFeedConformance):
    @pytest.mark.postgres
    async def test_rows_with_null_txid_are_returned_by_read_all(
        self, store: PostgreSQLEventStore
    ) -> None:
        stream = make_stream()
        await store.append(stream, [make_event(stream.aggregate_id)], ExpectedVersion.no_stream())
        async with store._engine.begin() as conn:  # type: ignore[attr-defined]
            await conn.execute(text("UPDATE events SET txid = NULL"))
        assert len(await collect(store.read_all())) == 1


class TestPostgreSQLCategoryQuery(_PGStoreFixture, CategoryQueryConformance):
    pass


class TestPostgreSQLSnapshotStore(SnapshotStoreConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLSnapshotStore]:
        engine = create_async_engine(ports_postgres_connection_url)
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS snapshots CASCADE"))
            await _exec_raw_schema(conn, "snapshots")
        yield PostgreSQLSnapshotStore(async_sessionmaker(engine, expire_on_commit=False))
        await engine.dispose()


_CHECKPOINTS_DDL = [
    "CREATE TABLE IF NOT EXISTS projection_checkpoints ("
    "projection_name VARCHAR(255) PRIMARY KEY, last_event_id UUID, "
    "last_event_type VARCHAR(255), last_processed_at TIMESTAMPTZ, "
    "events_processed BIGINT NOT NULL DEFAULT 0, global_position BIGINT, position_token TEXT, "
    "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
]

_DLQ_DDL = [
    "CREATE TABLE IF NOT EXISTS dead_letter_queue ("
    "id BIGSERIAL PRIMARY KEY, event_id UUID NOT NULL, projection_name VARCHAR(255) NOT NULL, "
    "event_type VARCHAR(255) NOT NULL, event_data JSONB NOT NULL, "
    "error_message TEXT NOT NULL, error_stacktrace TEXT, retry_count INTEGER NOT NULL DEFAULT 0, "
    "first_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
    "last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
    "status VARCHAR(20) NOT NULL DEFAULT 'failed', resolved_at TIMESTAMPTZ, "
    "resolved_by VARCHAR(255), "
    "CONSTRAINT chk_dlq_status_ports CHECK (status IN ('failed', 'retrying', 'resolved')), "
    "CONSTRAINT uq_dlq_event_projection_ports UNIQUE (event_id, projection_name))"
]


class TestPostgreSQLCheckpointRepository(CheckpointRepositoryConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[SQLCheckpointRepository]:
        engine = create_async_engine(ports_postgres_connection_url)
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS projection_checkpoints CASCADE"))
            await conn.execute(text("DROP TABLE IF EXISTS events CASCADE"))
            for statement in _CHECKPOINTS_DDL:
                await conn.execute(text(statement))
            await _exec_raw_schema(conn, "events")
        yield SQLCheckpointRepository(engine)
        await engine.dispose()

    async def write_legacy_int_row(
        self, store: SQLCheckpointRepository, subscription_id: str, value: int
    ) -> None:
        sql = (
            "INSERT INTO projection_checkpoints "
            "(projection_name, global_position, events_processed) VALUES (:name, :value, 1)"
        )
        async with store._conn.begin() as conn:
            await conn.execute(text(sql), {"name": subscription_id, "value": value})


class TestPostgreSQLDLQRepository(DLQRepositoryConformance):
    @pytest.fixture
    async def store(self, ports_postgres_connection_url: str) -> AsyncIterator[SQLDLQRepository]:
        engine = create_async_engine(ports_postgres_connection_url)
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS dead_letter_queue CASCADE"))
            for statement in _DLQ_DDL:
                await conn.execute(text(statement))
        yield SQLDLQRepository(engine)
        await engine.dispose()

    async def test_postgres_delete_resolved_events_removes_past_cutoff_entries(
        self, store: SQLDLQRepository
    ) -> None:
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        (entry,) = await store.get_failed_events()
        await store.mark_resolved(entry.id, "alice")
        assert await store.delete_resolved_events(older_than_days=0) == 1
        assert await store.get_failed_event_by_id(entry.id) is None


class TestPostgreSQLConcurrency(_PGStoreFixture):
    async def test_concurrent_appends_to_different_streams_all_succeed(
        self, store: PostgreSQLEventStore
    ) -> None:
        streams = [make_stream() for _ in range(5)]

        async def append_one(s: object) -> None:
            await store.append(s, [make_event(s.aggregate_id)], ExpectedVersion.no_stream())

        await asyncio.gather(*[append_one(s) for s in streams])
        for s in streams:
            assert await store.get_stream_version(s) == 1

    async def test_concurrent_appends_to_same_stream_only_one_succeeds(
        self, store: PostgreSQLEventStore
    ) -> None:
        stream = make_stream()

        async def append_at_zero() -> bool:
            try:
                await store.append(
                    stream, [make_event(stream.aggregate_id)], ExpectedVersion.exact(0)
                )
                return True
            except OptimisticLockError:
                return False

        results = await asyncio.gather(
            *[append_at_zero() for _ in range(5)], return_exceptions=True
        )
        assert (sum(r is True for r in results), sum(r is False for r in results)) == (1, 4)


class TestPostgreSQLLargePayloads(_PGStoreFixture):
    async def test_large_metadata_round_trips(self, store: PostgreSQLEventStore) -> None:
        stream = make_stream()
        large_metadata = {f"key_{i}": f"value_{i}" * 100 for i in range(100)}
        event = make_event(stream.aggregate_id).with_metadata(**large_metadata)
        result = await store.append(stream, [event], ExpectedVersion.no_stream())
        assert result.new_version == 1
        envelopes = [e async for e in store.read_stream(stream)]
        assert len(envelopes) == 1 and envelopes[0].event.metadata == large_metadata


async def test_store_id_stable_across_restarts(ports_postgres_connection_url: str) -> None:
    engine_a: AsyncEngine = create_async_engine(ports_postgres_connection_url)
    engine_b: AsyncEngine = create_async_engine(ports_postgres_connection_url)
    try:
        reg = make_conformance_registry()
        store_a = PostgreSQLEventStore(engine_a, event_registry=reg)
        store_b = PostgreSQLEventStore(engine_b, event_registry=reg)
        assert store_a.store_id == store_b.store_id == f"pg:{engine_a.url.database}"
    finally:
        await engine_a.dispose()
        await engine_b.dispose()
