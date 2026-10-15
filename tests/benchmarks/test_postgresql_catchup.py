"""Benchmark and index utilization test suite for PostgreSQL catch-up queries."""

from __future__ import annotations

import asyncio
import contextlib
import subprocess
import time
from collections.abc import AsyncGenerator
from typing import Any
from uuid import uuid4

import pytest
import sqlalchemy.ext.asyncio as sa_async
from sqlalchemy import text

import eventsource.adapters.postgresql.store as pg_store
from eventsource.adapters.postgresql.store import PostgreSQLEventStore
from eventsource.adapters.sql.schemas import get_schema
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.ports import ExpectedVersion, FeedReadOptions

try:
    from testcontainers.postgres import PostgresContainer

    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    PostgresContainer = None  # type: ignore[assignment, misc]
    TESTCONTAINERS_AVAILABLE = False


def _is_docker_available() -> bool:
    with contextlib.suppress(Exception):
        return subprocess.run(["docker", "info"], capture_output=True, timeout=5).returncode == 0
    return False


DOCKER_AVAILABLE = _is_docker_available()
skip_if_no_postgres_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE), reason="PostgreSQL infra not available"
)
pytestmark = [
    pytest.mark.benchmark,
    pytest.mark.postgres,
    pytest.mark.integration,
    skip_if_no_postgres_infra,
]
_BASE_SELECT = f"SELECT {pg_store._SELECT_COLUMNS} FROM events WHERE {pg_store._HORIZON_PREDICATE}"


class BenchmarkCatchupEvent(DomainEvent):
    aggregate_type: str = "BenchmarkEntity"
    data: str = "benchmark_payload_value"


@pytest.fixture(scope="module")
def pg_registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register(BenchmarkCatchupEvent)
    return registry


@pytest.fixture(scope="module")
async def pg_bench_engine() -> AsyncGenerator[sa_async.AsyncEngine]:
    container = PostgresContainer("postgres:16").start()
    url = container.get_connection_url().replace("postgresql://", "postgresql+asyncpg://")
    engine = sa_async.create_async_engine(url.replace("psycopg2", "asyncpg"), pool_size=20)
    seed_sql = (
        "INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, tenant_id, "
        "actor_id, version, timestamp, payload, created_at, txid) SELECT gen_random_uuid(), "
        "'BenchmarkCatchupEvent', CASE WHEN i % 2 = 0 THEN 'BenchmarkEntity' ELSE 'OtherEntity' "
        "END, id, NULL, NULL, 1, NOW(), json_build_object('event_id', gen_random_uuid(), "
        "'event_type', 'BenchmarkCatchupEvent', 'aggregate_id', id, 'aggregate_type', "
        "CASE WHEN i % 2 = 0 THEN 'BenchmarkEntity' ELSE 'OtherEntity' END, 'aggregate_version', "
        "1, 'occurred_at', NOW(), 'data', 'seed_event_data')::jsonb, NOW(), pg_current_xact_id() "
        "FROM (SELECT gen_random_uuid() AS id, i FROM generate_series(1, 50000) AS i) AS s;"
    )
    async with engine.connect() as conn:
        driver_conn = (await conn.get_raw_connection()).driver_connection
        for stmt in (get_schema("events"), seed_sql):
            await driver_conn.execute(stmt)
        await conn.commit()
        await driver_conn.execute("VACUUM ANALYZE events;")
    yield engine
    await engine.dispose()
    container.stop()


async def _run_explain(engine: sa_async.AsyncEngine, query_body: str, **params: Any) -> list[str]:
    async with sa_async.async_sessionmaker(engine, class_=sa_async.AsyncSession)() as session:
        horizon = (await session.execute(text(pg_store._HORIZON_QUERY))).scalar_one()
        sql = f"EXPLAIN (ANALYZE, BUFFERS) {query_body}"
        res = await session.execute(text(sql), {"txid_horizon": horizon, **params})
        return [row[0] for row in res.fetchall()]


class TestPostgreSQLCatchupIndexUtilization:
    async def test_catchup_range_query_plan_uses_primary_index(self, pg_bench_engine) -> None:
        q = f"{_BASE_SELECT} AND global_position > :from_position ORDER BY global_position ASC LIMIT :limit"
        plan = await _run_explain(pg_bench_engine, q, from_position=25000, limit=500)
        plan_text = "\n".join(plan)
        assert "Index Scan using events_pkey on events" in plan_text
        assert "Seq Scan" not in plan_text and "Sort" not in plan_text

    async def test_catchup_from_beginning_plan_uses_index(self, pg_bench_engine) -> None:
        query = f"{_BASE_SELECT} ORDER BY global_position ASC LIMIT :limit"
        plan_text = "\n".join(await _run_explain(pg_bench_engine, query, limit=500))
        assert "Index Scan using events_pkey on events" in plan_text and "Seq Scan" not in plan_text

    async def test_catchup_with_aggregate_type_filter_plan(self, pg_bench_engine) -> None:
        q = f"{_BASE_SELECT} AND aggregate_type = :aggregate_type AND global_position > :from_position ORDER BY global_position ASC LIMIT :limit"
        kw = {"aggregate_type": "BenchmarkEntity", "from_position": 10000, "limit": 500}
        plan_text = "\n".join(await _run_explain(pg_bench_engine, q, **kw))
        scans = ("events_pkey", "idx_events_type_position", "idx_events_aggregate_type")
        assert any(f"Index Scan using {idx} on events" in plan_text for idx in scans)
        assert "Seq Scan" not in plan_text

    async def test_current_position_backward_index_scan(self, pg_bench_engine) -> None:
        query = f"SELECT MAX(global_position) FROM events WHERE {pg_store._HORIZON_PREDICATE}"
        plan_text = "\n".join(await _run_explain(pg_bench_engine, query))
        assert "Index Scan Backward using events_pkey on events" in plan_text
        assert "Seq Scan" not in plan_text

    async def test_catchup_buffer_access_is_bounded(self, pg_bench_engine) -> None:
        q = f"{_BASE_SELECT} AND global_position > :from_position ORDER BY global_position ASC LIMIT 500"
        plan = await _run_explain(pg_bench_engine, q, from_position=20000)
        lines = [ln for ln in plan if "Buffers:" in ln]
        assert lines, "Expected buffer statistics in EXPLAIN output"
        total_buffers = sum(
            int(t[i + 1])
            for t in (ln.replace(",", "").split() for ln in lines)
            for i, tok in enumerate(t[:-1])
            if tok in ("hit=", "read=") and t[i + 1].isdigit()
        )
        assert total_buffers < 100


class TestPostgreSQLCatchupScaleAndConcurrency:
    async def test_catchup_throughput_under_concurrent_writers(
        self, pg_bench_engine, pg_registry
    ) -> None:
        store = PostgreSQLEventStore(pg_bench_engine, event_registry=pg_registry)
        stop_writers = asyncio.Event()

        async def writer_worker(wid: int) -> int:
            wstore, count = PostgreSQLEventStore(pg_bench_engine, event_registry=pg_registry), 0
            while not stop_writers.is_set():
                aid = uuid4()
                ev, s = BenchmarkCatchupEvent(aggregate_id=aid), StreamId(aid, "BenchmarkEntity")
                await wstore.append(s, [ev], ExpectedVersion.no_stream())
                count += 1
                await asyncio.sleep(0.002)
            return count

        writer_tasks = [asyncio.create_task(writer_worker(i)) for i in range(3)]
        events_read, current_pos, start_time = 0, None, time.perf_counter()
        batch_times, opts = [], FeedReadOptions(limit=500)
        while events_read < 25000:
            t0 = time.perf_counter()
            batch = [env async for env in store.read_all(from_position=current_pos, options=opts)]
            batch_times.append(time.perf_counter() - t0)
            if batch:
                events_read, current_pos = events_read + len(batch), batch[-1].position
            else:
                await asyncio.sleep(0.01)

        elapsed = time.perf_counter() - start_time
        stop_writers.set()
        await asyncio.gather(*writer_tasks)
        assert events_read / elapsed > 5000 and sorted(batch_times)[len(batch_times) // 2] < 0.05

    async def test_catchup_monotonic_ordering(self, pg_bench_engine, pg_registry) -> None:
        store = PostgreSQLEventStore(pg_bench_engine, event_registry=pg_registry)
        opts = FeedReadOptions(limit=1000)
        positions = [env.position async for env in store.read_all(options=opts)]
        assert len(positions) == 1000 and positions == sorted(positions)
        assert all(nxt > prev for prev, nxt in zip(positions, positions[1:], strict=False))

    async def test_catchup_safe_horizon_uncommitted_boundary(
        self, pg_bench_engine, pg_registry
    ) -> None:
        store = PostgreSQLEventStore(pg_bench_engine, event_registry=pg_registry)
        conn = await pg_bench_engine.connect()
        insert_sql = (
            "INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, tenant_id, "
            "actor_id, version, timestamp, payload, created_at, txid) VALUES (:eid, "
            "'BenchmarkCatchupEvent', 'BenchmarkEntity', :aid, NULL, NULL, 1, NOW(), '{}'::jsonb, "
            "NOW(), pg_current_xact_id())"
        )
        await conn.execute(text(insert_sql), {"eid": uuid4(), "aid": uuid4()})
        stream2 = StreamId(uuid4(), "BenchmarkEntity")
        ev2 = [BenchmarkCatchupEvent(aggregate_id=stream2.aggregate_id, data="after")]
        res2 = await store.append(stream2, ev2, ExpectedVersion.no_stream())
        pos = await store.current_position()
        assert pos is not None and pos < res2.position
        await conn.commit()
        await conn.close()
        pos_after = await store.current_position()
        assert pos_after is not None and pos_after >= res2.position
