"""Benchmark and index utilization test suite for PostgreSQL catch-up queries."""

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import AsyncGenerator
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from eventsource.adapters.postgresql.store import (
    _HORIZON_PREDICATE,
    _HORIZON_QUERY,
    _SELECT_COLUMNS,
    PostgreSQLEventStore,
)
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
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=5).returncode == 0
    except Exception:
        return False


DOCKER_AVAILABLE = _is_docker_available()
skip_if_no_postgres_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE),
    reason="PostgreSQL test infrastructure (testcontainers + docker) not available",
)
pytestmark = [
    pytest.mark.benchmark,
    pytest.mark.postgres,
    pytest.mark.integration,
    skip_if_no_postgres_infra,
]


class BenchmarkCatchupEvent(DomainEvent):
    aggregate_type: str = "BenchmarkEntity"
    data: str = "benchmark_payload_value"


@pytest.fixture(scope="module")
def pg_registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register(BenchmarkCatchupEvent)
    return registry


@pytest.fixture(scope="module")
async def pg_bench_engine() -> AsyncGenerator[AsyncEngine]:
    """Start PostgreSQL container, initialize schema, and pre-populate 50,000 events."""
    container = PostgresContainer("postgres:16")
    container.start()
    url = container.get_connection_url().replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(url.replace("psycopg2", "asyncpg"), pool_size=20, max_overflow=10)
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
        raw_conn = await conn.get_raw_connection()
        await raw_conn.driver_connection.execute(get_schema("events"))
        await raw_conn.driver_connection.execute(seed_sql)
        await conn.commit()
        await raw_conn.driver_connection.execute("VACUUM ANALYZE events;")
    yield engine
    await engine.dispose()
    container.stop()


async def _run_explain(engine: AsyncEngine, query_body: str, **params: Any) -> list[str]:
    session_factory = async_sessionmaker(engine, class_=AsyncSession)
    async with session_factory() as session:
        horizon = (await session.execute(text(_HORIZON_QUERY))).scalar_one()
        sql = f"EXPLAIN (ANALYZE, BUFFERS) {query_body}"
        result = await session.execute(text(sql), {"txid_horizon": horizon, **params})
        return [row[0] for row in result.fetchall()]


class TestPostgreSQLCatchupIndexUtilization:
    async def test_catchup_range_query_plan_uses_primary_index(
        self, pg_bench_engine: AsyncEngine
    ) -> None:
        query = (
            f"SELECT {_SELECT_COLUMNS} FROM events WHERE {_HORIZON_PREDICATE} "
            "AND global_position > :from_position ORDER BY global_position ASC LIMIT :limit"
        )
        plan_text = "\n".join(
            await _run_explain(pg_bench_engine, query, from_position=25000, limit=500)
        )
        assert "Index Scan using events_pkey on events" in plan_text
        assert "Seq Scan" not in plan_text and "Sort" not in plan_text

    async def test_catchup_from_beginning_plan_uses_index(
        self, pg_bench_engine: AsyncEngine
    ) -> None:
        query = (
            f"SELECT {_SELECT_COLUMNS} FROM events WHERE {_HORIZON_PREDICATE} "
            "ORDER BY global_position ASC LIMIT :limit"
        )
        plan_text = "\n".join(await _run_explain(pg_bench_engine, query, limit=500))
        assert "Index Scan using events_pkey on events" in plan_text
        assert "Seq Scan" not in plan_text

    async def test_catchup_with_aggregate_type_filter_plan(
        self, pg_bench_engine: AsyncEngine
    ) -> None:
        query = (
            f"SELECT {_SELECT_COLUMNS} FROM events WHERE {_HORIZON_PREDICATE} "
            "AND aggregate_type = :aggregate_type AND global_position > :from_position "
            "ORDER BY global_position ASC LIMIT :limit"
        )
        plan = await _run_explain(
            pg_bench_engine, query, aggregate_type="BenchmarkEntity", from_position=10000, limit=500
        )
        plan_text = "\n".join(plan)
        expected_scans = (
            "Index Scan using events_pkey on events",
            "Index Scan using idx_events_type_position on events",
            "Index Scan using idx_events_aggregate_type on events",
        )
        assert any(idx in plan_text for idx in expected_scans) and "Seq Scan" not in plan_text

    async def test_current_position_backward_index_scan(self, pg_bench_engine: AsyncEngine) -> None:
        query = f"SELECT MAX(global_position) FROM events WHERE {_HORIZON_PREDICATE}"
        plan_text = "\n".join(await _run_explain(pg_bench_engine, query))
        assert "Index Scan Backward using events_pkey on events" in plan_text
        assert "Seq Scan" not in plan_text

    async def test_catchup_buffer_access_is_bounded(self, pg_bench_engine: AsyncEngine) -> None:
        query = (
            f"SELECT {_SELECT_COLUMNS} FROM events WHERE {_HORIZON_PREDICATE} "
            "AND global_position > :from_position ORDER BY global_position ASC LIMIT 500"
        )
        plan_lines = await _run_explain(pg_bench_engine, query, from_position=20000)
        buffer_lines = [line for line in plan_lines if "Buffers:" in line]
        assert buffer_lines, "Expected buffer statistics in EXPLAIN output"
        total_buffers = sum(
            int(tokens[i + 1])
            for tokens in (line.replace(",", "").split() for line in buffer_lines)
            for i, token in enumerate(tokens)
            if token in ("hit=", "read=") and i + 1 < len(tokens) and tokens[i + 1].isdigit()
        )
        assert total_buffers < 100, f"Expected < 100 buffers, got {total_buffers}"


class TestPostgreSQLCatchupScaleAndConcurrency:
    async def test_catchup_throughput_under_concurrent_writers(
        self, pg_bench_engine: AsyncEngine, pg_registry: EventRegistry
    ) -> None:
        store = PostgreSQLEventStore(pg_bench_engine, event_registry=pg_registry)
        stop_writers = asyncio.Event()

        async def writer_worker(worker_id: int) -> int:
            worker_store = PostgreSQLEventStore(pg_bench_engine, event_registry=pg_registry)
            count = 0
            while not stop_writers.is_set():
                aid = uuid4()
                ev = BenchmarkCatchupEvent(aggregate_id=aid, data=f"worker_{worker_id}_{count}")
                await worker_store.append(
                    StreamId(aid, "BenchmarkEntity"), [ev], ExpectedVersion.no_stream()
                )
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
        throughput = events_read / elapsed
        median_batch_time_ms = (sorted(batch_times)[len(batch_times) // 2]) * 1000.0
        assert throughput > 5000, f"Expected throughput > 5000 events/s, got {throughput:.1f}"
        assert median_batch_time_ms < 50.0, f"Median batch >= 50ms: {median_batch_time_ms:.2f}ms"

    async def test_catchup_monotonic_ordering(
        self, pg_bench_engine: AsyncEngine, pg_registry: EventRegistry
    ) -> None:
        store = PostgreSQLEventStore(pg_bench_engine, event_registry=pg_registry)
        envelopes = [env async for env in store.read_all(options=FeedReadOptions(limit=1000))]
        assert len(envelopes) == 1000
        positions = [env.position for env in envelopes]
        for prev, next_pos in zip(positions, positions[1:], strict=False):
            assert next_pos > prev, f"Non-monotonic position detected: {prev} >= {next_pos}"
        assert positions == sorted(positions)

    async def test_catchup_safe_horizon_uncommitted_boundary(
        self, pg_bench_engine: AsyncEngine, pg_registry: EventRegistry
    ) -> None:
        store = PostgreSQLEventStore(pg_bench_engine, event_registry=pg_registry)
        conn = await pg_bench_engine.connect()
        insert_sql = (
            "INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id, "
            "tenant_id, actor_id, version, timestamp, payload, created_at, txid) VALUES ("
            ":eid, 'BenchmarkCatchupEvent', 'BenchmarkEntity', :aid, "
            "NULL, NULL, 1, NOW(), '{}'::jsonb, NOW(), pg_current_xact_id())"
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
