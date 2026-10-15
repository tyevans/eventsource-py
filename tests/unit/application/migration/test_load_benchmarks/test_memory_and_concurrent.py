"""Memory usage and concurrent migration benchmarks."""

from __future__ import annotations

import asyncio
import gc
import time
import tracemalloc
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.bulk_copier import BulkCopier
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
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
    MemoryResult,
    create_test_events,
    src_pos,
    tgt_pos,
)
from tests.unit.application.migration.test_load_benchmarks._repositories import (
    InMemoryMigrationRepository,
    InMemoryPositionMappingRepository,
)


class TestMemoryUsage:
    """Benchmarks for memory usage during migration operations."""

    @pytest.mark.asyncio
    async def test_bulk_copy_memory_usage(self) -> None:
        """Verify memory usage scales linearly with batch size, not total event count."""
        tenant_id = uuid4()
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = InMemoryEventStore(event_registry=_REGISTRY)
        migration_repo = InMemoryMigrationRepository()
        event_count = 10_000

        await create_test_events(source_store, tenant_id, event_count)

        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.BULK_COPY,
            config=MigrationConfig(batch_size=1_000, max_bulk_copy_rate=1_000_000),
        )
        await migration_repo.create(migration)

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        gc.collect()
        tracemalloc.start()
        async for _progress in copier.run(migration):
            pass
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        result = MemoryResult(
            name="bulk_copy_memory",
            peak_memory_mb=peak / (1024 * 1024),
            current_memory_mb=current / (1024 * 1024),
            objects_tracked=len(gc.get_objects()),
            metadata={"event_count": event_count},
        )
        assert result.peak_memory_mb < 100.0

    @pytest.mark.asyncio
    async def test_position_mapping_memory_scaling(self) -> None:
        """Verify position mappings scale linearly with count."""
        migration_id = uuid4()
        repo = InMemoryPositionMappingRepository()
        mapping_counts = [1_000, 5_000, 10_000]
        memory_results: list[tuple[int, float]] = []

        for count in mapping_counts:
            gc.collect()
            tracemalloc.start()
            now = datetime.now(UTC)
            for i in range(count):
                mapping = PositionMapping(
                    migration_id=migration_id,
                    source_position=src_pos(i),
                    target_position=tgt_pos(i // 2),
                    event_id=uuid4(),
                    mapped_at=now,
                )
                await repo.create(mapping)

            _current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            memory_results.append((count, peak / (1024 * 1024)))
            await repo.delete_by_migration(migration_id)

        if len(memory_results) >= 2:
            ratio = memory_results[-1][1] / memory_results[0][1]
            expected_ratio = mapping_counts[-1] / mapping_counts[0]
            assert ratio < expected_ratio * 3


class TestConcurrentMigrations:
    """Benchmarks for handling multiple concurrent migrations."""

    @pytest.mark.asyncio
    async def test_concurrent_bulk_copies(self) -> None:
        """Verify concurrent migrations don't significantly degrade performance."""
        num_migrations = 5
        events_per_migration = 2_000

        async def run_migration(index: int) -> BenchmarkResult:
            tenant_id = uuid4()
            source_store = InMemoryEventStore(event_registry=_REGISTRY)
            target_store = InMemoryEventStore(event_registry=_REGISTRY)
            migration_repo = InMemoryMigrationRepository()

            await create_test_events(source_store, tenant_id, events_per_migration)

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

            start_time = time.monotonic()
            events_copied = 0
            async for progress in copier.run(migration):
                events_copied = progress.events_copied
            end_time = time.monotonic()

            return BenchmarkResult.from_timing(
                name=f"concurrent_migration_{index}",
                start_time=start_time,
                end_time=end_time,
                operations_count=events_copied,
            )

        overall_start = time.monotonic()
        results = await asyncio.gather(*[run_migration(i) for i in range(num_migrations)])
        overall_end = time.monotonic()

        total_events = sum(r.operations_count for r in results)
        overall_throughput = total_events / (overall_end - overall_start)

        for result in results:
            assert result.operations_per_second > 500
        assert overall_throughput > 1_000

    @pytest.mark.asyncio
    async def test_concurrent_dual_writes(self) -> None:
        """Test concurrent dual-write operations."""
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = InMemoryEventStore(event_registry=_REGISTRY)
        num_tenants = 5
        ops_per_tenant = 100

        interceptors = [
            DualWriteInterceptor(
                source_store=source_store,
                target_store=target_store,
                tenant_id=uuid4(),
                enable_tracing=False,
            )
            for _ in range(num_tenants)
        ]

        async def tenant_writes(interceptor: DualWriteInterceptor, count: int) -> list[float]:
            latencies: list[float] = []
            for _ in range(count):
                aggregate_id = uuid4()
                event = BenchmarkEvent(
                    aggregate_id=aggregate_id,
                    tenant_id=interceptor.tenant_id,
                )
                start = time.monotonic()
                await interceptor.append(
                    StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
                    [event],
                    ExpectedVersion.no_stream(),
                )
                end = time.monotonic()
                latencies.append((end - start) * 1000)
            return latencies

        all_latencies = await asyncio.gather(
            *[tenant_writes(i, ops_per_tenant) for i in interceptors]
        )
        combined_latencies = [lat for tenant_lats in all_latencies for lat in tenant_lats]
        result = LatencyResult.from_samples("concurrent_dual_writes", combined_latencies)

        assert result.mean_ms < 5.0
        assert result.p95_ms < 15.0
