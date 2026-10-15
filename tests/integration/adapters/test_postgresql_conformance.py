"""Conformance tests for PostgreSQLEventStore against the port suites."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from ..conftest import skip_if_no_postgres_infra

pytestmark = [pytest.mark.integration, pytest.mark.postgres, skip_if_no_postgres_infra]

import eventsource.testing.conformance_ports as cp  # noqa: E402
from eventsource.adapters.postgresql import (  # noqa: E402
    PostgreSQLEventStore,
    PostgreSQLSnapshotStore,
)
from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository  # noqa: E402
from eventsource.adapters.sql.dlq import SQLDLQRepository  # noqa: E402
from eventsource.adapters.sql.schemas import get_schema  # noqa: E402
from eventsource.domain.exceptions import OptimisticLockError  # noqa: E402
from eventsource.ports import ExpectedVersion, collect  # noqa: E402
from eventsource.testing.conformance_ports._fixtures import (  # noqa: E402
    make_conformance_registry,
    make_event,
    make_stream,
)


@asynccontextmanager
async def _pg_engine(
    url: str, drop: str, ddl: str = "", raw: str = ""
) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {drop} CASCADE"))
        if ddl:
            await conn.execute(text(ddl))
        if raw:
            raw_conn = await conn.get_raw_connection()
            assert raw_conn.driver_connection is not None
            await raw_conn.driver_connection.execute(get_schema(raw, "postgresql"))
    try:
        yield engine
    finally:
        await engine.dispose()


class _PGStoreFixture:
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLEventStore]:
        engine = create_async_engine(ports_postgres_connection_url)
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS events CASCADE"))
        reg = make_conformance_registry()
        s = PostgreSQLEventStore(engine, event_registry=reg, create_schema=True, owns_engine=True)
        yield s
        await s.close()


class TestPostgreSQLAppender(_PGStoreFixture, cp.AppenderConformance):
    pass


class TestPostgreSQLStreamReader(_PGStoreFixture, cp.StreamReaderConformance):
    pass


class TestPostgreSQLEventLookup(_PGStoreFixture, cp.EventLookupConformance):
    pass


class TestPostgreSQLGlobalFeed(_PGStoreFixture, cp.GlobalFeedConformance):
    @pytest.mark.postgres
    async def test_rows_with_null_txid_are_returned_by_read_all(
        self, store: PostgreSQLEventStore
    ) -> None:
        stream = make_stream()
        await store.append(stream, [make_event(stream.aggregate_id)], ExpectedVersion.no_stream())
        async with store._engine.begin() as conn:  # type: ignore[attr-defined]
            await conn.execute(text("UPDATE events SET txid = NULL"))
        assert len(await collect(store.read_all())) == 1


class TestPostgreSQLCategoryQuery(_PGStoreFixture, cp.CategoryQueryConformance):
    pass


class TestPostgreSQLSnapshotStore(cp.SnapshotStoreConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[PostgreSQLSnapshotStore]:
        async with _pg_engine(ports_postgres_connection_url, "snapshots", raw="snapshots") as eng:
            yield PostgreSQLSnapshotStore(async_sessionmaker(eng, expire_on_commit=False))


_CHECKPOINTS_DDL = """CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name VARCHAR(255) PRIMARY KEY, last_event_id UUID, last_event_type VARCHAR(255), last_processed_at TIMESTAMPTZ,
    events_processed BIGINT NOT NULL DEFAULT 0, global_position BIGINT, position_token TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"""
_DLQ_DDL = """CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id BIGSERIAL PRIMARY KEY, event_id UUID NOT NULL, projection_name VARCHAR(255) NOT NULL, event_type VARCHAR(255) NOT NULL,
    event_data JSONB NOT NULL, error_message TEXT NOT NULL, error_stacktrace TEXT, retry_count INTEGER NOT NULL DEFAULT 0,
    first_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'failed', resolved_at TIMESTAMPTZ, resolved_by VARCHAR(255),
    CONSTRAINT chk_dlq_status_ports CHECK (status IN ('failed', 'retrying', 'resolved')),
    CONSTRAINT uq_dlq_event_projection_ports UNIQUE (event_id, projection_name))"""


class TestPostgreSQLCheckpointRepository(cp.CheckpointRepositoryConformance):
    @pytest.fixture
    async def store(
        self, ports_postgres_connection_url: str
    ) -> AsyncIterator[SQLCheckpointRepository]:
        drop = "projection_checkpoints, events"
        async with _pg_engine(ports_postgres_connection_url, drop, _CHECKPOINTS_DDL, "events") as e:
            yield SQLCheckpointRepository(e)

    async def write_legacy_int_row(
        self, store: SQLCheckpointRepository, sub_id: str, val: int
    ) -> None:
        sql = "INSERT INTO projection_checkpoints (projection_name, global_position, events_processed) VALUES (:n, :v, 1)"
        async with store._conn.begin() as conn:
            await conn.execute(text(sql), {"n": sub_id, "v": val})


class TestPostgreSQLDLQRepository(cp.DLQRepositoryConformance):
    @pytest.fixture
    async def store(self, ports_postgres_connection_url: str) -> AsyncIterator[SQLDLQRepository]:
        async with _pg_engine(ports_postgres_connection_url, "dead_letter_queue", _DLQ_DDL) as eng:
            yield SQLDLQRepository(eng)

    async def test_postgres_delete_resolved_events_removes_past_cutoff_entries(
        self, store: SQLDLQRepository
    ) -> None:
        await store.add_failed_event(uuid4(), "P", "Created", {}, RuntimeError("boom"))
        (entry,) = await store.get_failed_events()
        await store.mark_resolved(entry.id, "alice")
        assert await store.delete_resolved_events(older_than_days=0) == 1
        assert await store.get_failed_event_by_id(entry.id) is None


class TestPostgreSQLConcurrency(_PGStoreFixture):
    async def test_concurrent_appends_to_different_streams_all_succeed(
        self, store: PostgreSQLEventStore
    ) -> None:
        streams, ver = [make_stream() for _ in range(5)], ExpectedVersion.no_stream()
        await asyncio.gather(*[store.append(s, [make_event(s.aggregate_id)], ver) for s in streams])
        for s in streams:
            assert await store.get_stream_version(s) == 1

    async def test_concurrent_appends_to_same_stream_only_one_succeeds(
        self, store: PostgreSQLEventStore
    ) -> None:
        stream, ver = make_stream(), ExpectedVersion.exact(0)

        async def append_at_zero() -> bool:
            try:
                await store.append(stream, [make_event(stream.aggregate_id)], ver)
                return True
            except OptimisticLockError:
                return False

        res = await asyncio.gather(*[append_at_zero() for _ in range(5)], return_exceptions=True)
        assert (sum(r is True for r in res), sum(r is False for r in res)) == (1, 4)


class TestPostgreSQLLargePayloads(_PGStoreFixture):
    async def test_large_metadata_round_trips(self, store: PostgreSQLEventStore) -> None:
        stream, meta = make_stream(), {f"key_{i}": f"value_{i}" * 100 for i in range(100)}
        event = make_event(stream.aggregate_id).with_metadata(**meta)
        assert (await store.append(stream, [event], ExpectedVersion.no_stream())).new_version == 1
        envelopes = [e async for e in store.read_stream(stream)]
        assert len(envelopes) == 1 and envelopes[0].event.metadata == meta


async def test_store_id_stable_across_restarts(ports_postgres_connection_url: str) -> None:
    eng_a = create_async_engine(ports_postgres_connection_url)
    eng_b = create_async_engine(ports_postgres_connection_url)
    try:
        reg = make_conformance_registry()
        s_a = PostgreSQLEventStore(eng_a, event_registry=reg)
        s_b = PostgreSQLEventStore(eng_b, event_registry=reg)
        assert s_a.store_id == s_b.store_id == f"pg:{eng_a.url.database}"
    finally:
        await asyncio.gather(eng_a.dispose(), eng_b.dispose())
