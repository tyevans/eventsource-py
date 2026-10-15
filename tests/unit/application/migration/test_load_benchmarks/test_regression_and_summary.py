"""Performance regression tests and overall benchmark summary report."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.bulk_copier import BulkCopier
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion, FeedReadOptions
from eventsource.ports.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
    PositionMapping,
)
from tests.unit.application.migration.test_load_benchmarks._fixtures import (
    _REGISTRY,
    BenchmarkEvent,
    BenchmarkResult,
    LatencyResult,
    create_test_events,
    src_pos,
    tgt_pos,
)
from tests.unit.application.migration.test_load_benchmarks._repositories import (
    InMemoryMigrationRepository,
    InMemoryPositionMappingRepository,
)


async def _measure_appends(store_or_interceptor, tenant_id: UUID, count: int) -> list[float]:
    latencies: list[float] = []
    for _ in range(count):
        aggregate_id = uuid4()
        event = BenchmarkEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        start = time.monotonic()
        await store_or_interceptor.append(
            StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )
        latencies.append((time.monotonic() - start) * 1000)
    return latencies


class TestPerformanceRegression:
    """Regression tests to catch performance degradation."""

    @pytest.mark.asyncio
    async def test_event_store_append_regression(self) -> None:
        store = InMemoryEventStore(event_registry=_REGISTRY)
        latencies = await _measure_appends(store, uuid4(), 1_000)
        result = LatencyResult.from_samples("event_store_append_regression", latencies)
        assert result.mean_ms < 0.5
        assert result.p95_ms < 1.0

    @pytest.mark.asyncio
    async def test_event_store_read_all_regression(self) -> None:
        store = InMemoryEventStore(event_registry=_REGISTRY)
        tenant_id = uuid4()
        await create_test_events(store, tenant_id, 10_000)

        start_time = time.monotonic()
        count = 0
        async for _ in store.read_all(None, FeedReadOptions(tenant_id=tenant_id)):
            count += 1
        end_time = time.monotonic()

        result = BenchmarkResult.from_timing(
            name="read_all_regression",
            start_time=start_time,
            end_time=end_time,
            operations_count=count,
        )
        assert result.operations_per_second > 5_000

    @pytest.mark.asyncio
    async def test_dual_write_regression(self) -> None:
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = InMemoryEventStore(event_registry=_REGISTRY)
        tenant_id = uuid4()
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        latencies = await _measure_appends(interceptor, tenant_id, 500)
        result = LatencyResult.from_samples("dual_write_regression", latencies)
        assert result.mean_ms < 1.0
        assert result.p95_ms < 2.0


class TestBenchmarkSummary:
    """Generates a summary report of all benchmarks."""

    @pytest.mark.asyncio
    async def test_generate_benchmark_summary(self) -> None:
        summary: list[str] = ["\n" + "=" * 60, "Migration System Benchmark Summary", "=" * 60]
        tenant_id = uuid4()
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = InMemoryEventStore(event_registry=_REGISTRY)
        migration_repo = InMemoryMigrationRepository()
        await create_test_events(source_store, tenant_id, 5_000)

        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.BULK_COPY,
            config=MigrationConfig(batch_size=500, max_bulk_copy_rate=1_000_000),
        )
        await migration_repo.create(migration)
        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        start = time.monotonic()
        events_copied = 0
        async for progress in copier.run(migration):
            events_copied = progress.events_copied
        bulk_copy_time = time.monotonic() - start
        summary.extend(
            [
                "\nBulk Copy Performance:",
                f"  Events: {events_copied:,}",
                f"  Duration: {bulk_copy_time:.2f}s",
                f"  Throughput: {events_copied / bulk_copy_time:,.0f} events/s",
            ]
        )

        tenant_id2 = uuid4()
        interceptor = DualWriteInterceptor(
            source_store=InMemoryEventStore(event_registry=_REGISTRY),
            target_store=InMemoryEventStore(event_registry=_REGISTRY),
            tenant_id=tenant_id2,
            enable_tracing=False,
        )
        dual_write_times = await _measure_appends(interceptor, tenant_id2, 200)
        dual_result = LatencyResult.from_samples("dual_write", dual_write_times)
        summary.extend(
            [
                "\nDual-Write Latency:",
                f"  Mean: {dual_result.mean_ms:.3f}ms",
                f"  P50: {dual_result.p50_ms:.3f}ms",
                f"  P95: {dual_result.p95_ms:.3f}ms",
                f"  P99: {dual_result.p99_ms:.3f}ms",
            ]
        )

        pos_repo = InMemoryPositionMappingRepository()
        pos_mapper = PositionMapper(pos_repo, enable_tracing=False)
        pos_migration_id = uuid4()
        now = datetime.now(UTC)
        for i in range(1_000):
            await pos_repo.create(
                PositionMapping(
                    migration_id=pos_migration_id,
                    source_position=src_pos(i * 10),
                    target_position=tgt_pos(i * 5),
                    event_id=uuid4(),
                    mapped_at=now,
                )
            )

        pos_times: list[float] = []
        for i in range(100):
            start = time.monotonic()
            await pos_mapper.translate_position(
                pos_migration_id, src_pos(i * 100), use_nearest=True
            )
            pos_times.append((time.monotonic() - start) * 1000)

        pos_result = LatencyResult.from_samples("position_translation", pos_times)
        summary.extend(
            [
                "\nPosition Translation Latency:",
                f"  Mean: {pos_result.mean_ms:.3f}ms",
                f"  P95: {pos_result.p95_ms:.3f}ms",
                "\n" + "=" * 60,
            ]
        )
        print("\n".join(summary))

        assert events_copied / bulk_copy_time > 1_000
        assert dual_result.p95_ms < 10.0
        assert pos_result.mean_ms < 1.0
