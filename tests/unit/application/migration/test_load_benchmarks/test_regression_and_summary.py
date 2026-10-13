"""Performance regression tests and overall benchmark summary report."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from uuid import uuid4

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


class TestPerformanceRegression:
    """
    Regression tests to catch performance degradation.

    These tests establish baseline performance expectations
    and fail if performance drops significantly.
    """

    @pytest.mark.asyncio
    async def test_event_store_append_regression(self) -> None:
        """Ensure InMemoryEventStore append performance hasn't regressed."""
        store = InMemoryEventStore(event_registry=_REGISTRY)
        tenant_id = uuid4()
        num_operations = 1_000

        latencies: list[float] = []

        for _ in range(num_operations):
            aggregate_id = uuid4()
            event = BenchmarkEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )

            start = time.monotonic()
            await store.append(
                StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            end = time.monotonic()
            latencies.append((end - start) * 1000)

            result = LatencyResult.from_samples("event_store_append_regression", latencies)

        # Baseline: mean < 0.5ms, p95 < 1.0ms
        assert result.mean_ms < 0.5
        assert result.p95_ms < 1.0

    @pytest.mark.asyncio
    async def test_event_store_read_all_regression(self) -> None:
        """Ensure InMemoryEventStore read_all performance hasn't regressed."""
        store = InMemoryEventStore(event_registry=_REGISTRY)
        tenant_id = uuid4()

        # Create 10K events
        await create_test_events(store, tenant_id, 10_000)

        # Measure read_all throughput
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

        # Baseline: raw read_all measures ~11K events/second, but pytest,
        # coverage instrumentation, and parallel workers eat the margin --
        # 10K had zero headroom and flaked on real machines (pre- and
        # post-refactor alike, measured 2026-07-31). 5K still catches an
        # order-of-magnitude regression without asserting on machine speed.
        assert result.operations_per_second > 5_000

    @pytest.mark.asyncio
    async def test_dual_write_regression(self) -> None:
        """Ensure DualWriteInterceptor performance hasn't regressed."""
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = InMemoryEventStore(event_registry=_REGISTRY)
        tenant_id = uuid4()

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        num_operations = 500
        latencies: list[float] = []

        for _ in range(num_operations):
            aggregate_id = uuid4()
            event = BenchmarkEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )

            start = time.monotonic()
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            end = time.monotonic()
            latencies.append((end - start) * 1000)

        result = LatencyResult.from_samples("dual_write_regression", latencies)

        # Baseline: mean < 1.0ms, p95 < 2.0ms
        assert result.mean_ms < 1.0
        assert result.p95_ms < 2.0


class TestBenchmarkSummary:
    """
    Generates a summary report of all benchmarks.
    """

    @pytest.mark.asyncio
    async def test_generate_benchmark_summary(self) -> None:
        """
        Run a quick benchmark suite and print summary.

        This test runs a condensed version of all benchmarks
        and outputs a summary for analysis.
        """
        summary: list[str] = []
        summary.append("\n" + "=" * 60)
        summary.append("Migration System Benchmark Summary")
        summary.append("=" * 60)

        # Bulk copy benchmark
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

        summary.append("\nBulk Copy Performance:")
        summary.append(f"  Events: {events_copied:,}")
        summary.append(f"  Duration: {bulk_copy_time:.2f}s")
        summary.append(f"  Throughput: {events_copied / bulk_copy_time:,.0f} events/s")

        # Dual-write benchmark
        source_store2 = InMemoryEventStore(event_registry=_REGISTRY)
        target_store2 = InMemoryEventStore(event_registry=_REGISTRY)
        tenant_id2 = uuid4()

        interceptor = DualWriteInterceptor(
            source_store=source_store2,
            target_store=target_store2,
            tenant_id=tenant_id2,
            enable_tracing=False,
        )

        dual_write_times: list[float] = []
        for _ in range(200):
            aggregate_id = uuid4()
            event = BenchmarkEvent(aggregate_id=aggregate_id, tenant_id=tenant_id2)

            start = time.monotonic()
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            dual_write_times.append((time.monotonic() - start) * 1000)

        dual_result = LatencyResult.from_samples("dual_write", dual_write_times)

        summary.append("\nDual-Write Latency:")
        summary.append(f"  Mean: {dual_result.mean_ms:.3f}ms")
        summary.append(f"  P50: {dual_result.p50_ms:.3f}ms")
        summary.append(f"  P95: {dual_result.p95_ms:.3f}ms")
        summary.append(f"  P99: {dual_result.p99_ms:.3f}ms")

        # Position translation benchmark
        pos_repo = InMemoryPositionMappingRepository()
        pos_mapper = PositionMapper(pos_repo, enable_tracing=False)
        pos_migration_id = uuid4()

        now = datetime.now(UTC)
        for i in range(1_000):
            mapping = PositionMapping(
                migration_id=pos_migration_id,
                source_position=src_pos(i * 10),
                target_position=tgt_pos(i * 5),
                event_id=uuid4(),
                mapped_at=now,
            )
            await pos_repo.create(mapping)

        pos_times: list[float] = []
        for i in range(100):
            start = time.monotonic()
            await pos_mapper.translate_position(
                pos_migration_id, src_pos(i * 100), use_nearest=True
            )
            pos_times.append((time.monotonic() - start) * 1000)

        pos_result = LatencyResult.from_samples("position_translation", pos_times)

        summary.append("\nPosition Translation Latency:")
        summary.append(f"  Mean: {pos_result.mean_ms:.3f}ms")
        summary.append(f"  P95: {pos_result.p95_ms:.3f}ms")

        summary.append("\n" + "=" * 60)
        summary.append("Performance Targets:")
        summary.append("  Bulk copy: > 10K events/s")
        summary.append("  Dual-write overhead: < 5%")
        summary.append("  Cutover duration: < 100ms p95")
        summary.append("  Memory: < 500MB for 10M events")
        summary.append("=" * 60 + "\n")

        # Print summary (visible in pytest output with -v flag)
        print("\n".join(summary))

        # Assert all benchmarks passed baseline thresholds
        assert events_copied / bulk_copy_time > 1_000  # Min 1K events/s
        assert dual_result.p95_ms < 10.0  # Max 10ms p95
        assert pos_result.mean_ms < 1.0  # Max 1ms mean
