"""Bulk copy throughput and dual-write overhead benchmarks."""

from __future__ import annotations

import time
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.bulk_copier import BulkCopier
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import Migration, MigrationConfig, MigrationPhase
from tests.unit.application.migration.test_load_benchmarks._fixtures import (
    _REGISTRY,
    BenchmarkEvent,
    BenchmarkResult,
    LatencyResult,
    create_test_events,
)
from tests.unit.application.migration.test_load_benchmarks._repositories import (
    InMemoryMigrationRepository,
    RaisingTargetStore,
)


class TestBulkCopyThroughput:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "batch_size,event_count",
        [(100, 1_000), (500, 5_000), (1_000, 10_000), (2_000, 10_000), (5_000, 20_000)],
    )
    async def test_bulk_copy_throughput_at_batch_sizes(
        self, batch_size: int, event_count: int
    ) -> None:
        tenant_id = uuid4()
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = InMemoryEventStore(event_registry=_REGISTRY)
        migration_repo = InMemoryMigrationRepository()
        await create_test_events(source_store, tenant_id, event_count)

        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.BULK_COPY,
            config=MigrationConfig(batch_size=batch_size, max_bulk_copy_rate=1_000_000),
        )
        await migration_repo.create(migration)
        copier = BulkCopier(source_store, target_store, migration_repo, enable_tracing=False)

        start_time = time.monotonic()
        events_copied = 0
        async for progress in copier.run(migration):
            events_copied = progress.events_copied
        end_time = time.monotonic()

        result = BenchmarkResult.from_timing(
            name=f"bulk_copy_batch_{batch_size}",
            start_time=start_time,
            end_time=end_time,
            operations_count=events_copied,
            metadata={"batch_size": batch_size, "event_count": event_count},
        )
        assert (
            result.operations_per_second > 1_000
        ), f"Throughput {result.operations_per_second:.0f} events/s below threshold"
        assert events_copied == event_count

    @pytest.mark.asyncio
    async def test_bulk_copy_with_large_payload(self) -> None:
        tenant_id = uuid4()
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = InMemoryEventStore(event_registry=_REGISTRY)
        migration_repo = InMemoryMigrationRepository()
        event_count, payload_size = 5_000, 1_000
        await create_test_events(source_store, tenant_id, event_count, payload_size=payload_size)

        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.BULK_COPY,
            config=MigrationConfig(batch_size=500, max_bulk_copy_rate=1_000_000),
        )
        await migration_repo.create(migration)
        copier = BulkCopier(source_store, target_store, migration_repo, enable_tracing=False)

        start_time = time.monotonic()
        events_copied = 0
        async for progress in copier.run(migration):
            events_copied = progress.events_copied
        end_time = time.monotonic()

        result = BenchmarkResult.from_timing(
            name="bulk_copy_large_payload",
            start_time=start_time,
            end_time=end_time,
            operations_count=events_copied,
            metadata={"payload_size": payload_size, "event_count": event_count},
        )
        assert result.operations_per_second > 500


class TestDualWriteOverhead:
    @pytest.mark.asyncio
    async def test_dual_write_overhead_comparison(self) -> None:
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = InMemoryEventStore(event_registry=_REGISTRY)
        tenant_id = uuid4()
        num_operations = 500
        single_write_times: list[float] = []

        for _i in range(num_operations):
            aggregate_id = uuid4()
            event = BenchmarkEvent(
                aggregate_id=aggregate_id, tenant_id=tenant_id, payload="test_payload"
            )
            start = time.monotonic()
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            single_write_times.append((time.monotonic() - start) * 1000)

        interceptor = DualWriteInterceptor(
            source_store, target_store, tenant_id, enable_tracing=False
        )
        dual_write_times: list[float] = []

        for _i in range(num_operations):
            aggregate_id = uuid4()
            event = BenchmarkEvent(
                aggregate_id=aggregate_id, tenant_id=tenant_id, payload="test_payload"
            )
            start = time.monotonic()
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            dual_write_times.append((time.monotonic() - start) * 1000)

        single_result = LatencyResult.from_samples("single_write", single_write_times)
        dual_result = LatencyResult.from_samples("dual_write", dual_write_times)
        _ = ((dual_result.mean_ms - single_result.mean_ms) / single_result.mean_ms) * 100
        _ = ((dual_result.p95_ms - single_result.p95_ms) / single_result.p95_ms) * 100

        assert (
            dual_result.mean_ms < 5.0
        ), f"Dual-write mean latency {dual_result.mean_ms:.2f}ms too high"
        assert (
            dual_result.p95_ms < 10.0
        ), f"Dual-write p95 latency {dual_result.p95_ms:.2f}ms too high"

    @pytest.mark.asyncio
    async def test_dual_write_with_target_failure(self) -> None:
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        tenant_id = uuid4()
        num_operations = 200
        interceptor = DualWriteInterceptor(
            source_store, RaisingTargetStore(), tenant_id, enable_tracing=False
        )
        latencies: list[float] = []

        for _i in range(num_operations):
            aggregate_id = uuid4()
            event = BenchmarkEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
            start = time.monotonic()
            result = await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            assert result.new_version >= 1
            latencies.append((time.monotonic() - start) * 1000)

        latency_result = LatencyResult.from_samples("dual_write_target_failure", latencies)
        assert latency_result.mean_ms < 10.0
        assert latency_result.p95_ms < 20.0
        assert interceptor.get_failure_stats().total_failures == num_operations

    @pytest.mark.asyncio
    async def test_dual_write_batch_performance(self) -> None:
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = InMemoryEventStore(event_registry=_REGISTRY)
        tenant_id = uuid4()
        interceptor = DualWriteInterceptor(
            source_store, target_store, tenant_id, enable_tracing=False
        )
        results: dict[int, LatencyResult] = {}

        for batch_size in [1, 5, 10, 20]:
            latencies: list[float] = []
            for _ in range(100):
                aggregate_id = uuid4()
                events = [
                    BenchmarkEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
                    for _ in range(batch_size)
                ]
                start = time.monotonic()
                await interceptor.append(
                    StreamId(aggregate_id=aggregate_id, category="BenchmarkAggregate"),
                    events,
                    ExpectedVersion.no_stream(),
                )
                latencies.append((time.monotonic() - start) * 1000)
            results[batch_size] = LatencyResult.from_samples(
                f"dual_write_batch_{batch_size}", latencies
            )

        for batch_size, result in results.items():
            assert (result.mean_ms / batch_size) < 2.0
