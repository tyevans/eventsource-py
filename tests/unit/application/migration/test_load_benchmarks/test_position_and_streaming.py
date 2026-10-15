"""Position translation and status streaming benchmarks."""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.ports.migration.models import (
    MigrationPhase,
    MigrationStatus,
    PositionMapping,
)
from tests.unit.application.migration.test_load_benchmarks._fixtures import (
    BenchmarkResult,
    LatencyResult,
    src_pos,
    tgt_pos,
)
from tests.unit.application.migration.test_load_benchmarks._repositories import (
    InMemoryPositionMappingRepository,
)


class TestPositionTranslation:
    """Benchmarks for position translation performance."""

    @pytest.mark.asyncio
    async def test_position_translation_lookup(self) -> None:
        """Test that nearest-position translation is bounded at O(log n) comparisons."""
        migration_id = uuid4()
        repo = InMemoryPositionMappingRepository()
        mapper = PositionMapper(repo, enable_tracing=False)
        num_mappings = 10_000
        now = datetime.now(UTC)

        for i in range(num_mappings):
            mapping = PositionMapping(
                migration_id=migration_id,
                source_position=src_pos(i * 10),
                target_position=tgt_pos(i * 5),
                event_id=uuid4(),
                mapped_at=now,
            )
            await repo.create(mapping)

        positions_to_lookup = [src_pos(i * 10) for i in range(0, num_mappings, 100)]
        for pos in positions_to_lookup:
            result = await mapper.translate_position(migration_id, pos, use_nearest=False)
            assert result.target_position is not None

        max_comparisons = math.ceil(math.log2(num_mappings)) + 1
        nearest_positions = [src_pos(i * 10 + 5) for i in range(0, num_mappings - 1, 100)]
        for pos in nearest_positions:
            before = repo.nearest_lookup_comparisons
            result = await mapper.translate_position(migration_id, pos, use_nearest=True)
            comparisons = repo.nearest_lookup_comparisons - before
            assert result.target_position is not None
            assert comparisons <= max_comparisons, (
                f"nearest lookup for {pos!r} took {comparisons} comparisons, "
                f"expected at most {max_comparisons} (O(log n) bound)"
            )

    @pytest.mark.asyncio
    async def test_batch_position_translation(self) -> None:
        """Test batch position translation performance."""
        migration_id = uuid4()
        repo = InMemoryPositionMappingRepository()
        mapper = PositionMapper(repo, enable_tracing=False)
        num_mappings = 5_000
        now = datetime.now(UTC)

        for i in range(num_mappings):
            mapping = PositionMapping(
                migration_id=migration_id,
                source_position=src_pos(i * 10),
                target_position=tgt_pos(i * 5),
                event_id=uuid4(),
                mapped_at=now,
            )
            await repo.create(mapping)

        batch_sizes = [10, 50, 100, 500]
        results: dict[int, LatencyResult] = {}
        for batch_size in batch_sizes:
            latencies: list[float] = []
            for _ in range(50):
                positions = [src_pos(i * 10) for i in range(batch_size)]
                start = time.monotonic()
                await mapper.translate_positions_batch(migration_id, positions, use_nearest=True)
                end = time.monotonic()
                latencies.append((end - start) * 1000)
            results[batch_size] = LatencyResult.from_samples(
                f"batch_translation_{batch_size}", latencies
            )

        for batch_size, result in results.items():
            per_position_latency = result.mean_ms / batch_size
            assert per_position_latency < 0.5


class TestStatusStreaming:
    """Benchmarks for status streaming with multiple subscribers."""

    @pytest.mark.asyncio
    async def test_status_update_notification_latency(self) -> None:
        """Test latency of status update notifications."""
        num_subscribers = 10
        num_updates = 100
        queues: list[asyncio.Queue[UUID]] = [
            asyncio.Queue(maxsize=100) for _ in range(num_subscribers)
        ]
        migration_id = uuid4()
        latencies: list[float] = []

        for _ in range(num_updates):
            start = time.monotonic()
            for queue in queues:
                with contextlib.suppress(asyncio.QueueFull):
                    queue.put_nowait(migration_id)
            end = time.monotonic()
            latencies.append((end - start) * 1000)

            for queue in queues:
                while not queue.empty():
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

        result = LatencyResult.from_samples("status_notification_latency", latencies)
        assert result.mean_ms < 1.0
        assert result.p95_ms < 2.0

    @pytest.mark.asyncio
    async def test_status_streaming_throughput(self) -> None:
        """Test throughput of status update streaming."""
        num_updates = 1_000
        migration_id = uuid4()
        tenant_id = uuid4()
        statuses: list[MigrationStatus] = []

        for i in range(num_updates):
            status = MigrationStatus(
                migration_id=migration_id,
                tenant_id=tenant_id,
                phase=MigrationPhase.BULK_COPY,
                progress_percent=i / num_updates * 100,
                events_total=10_000,
                events_copied=i * 10,
                events_remaining=10_000 - i * 10,
                sync_lag_events=0,
                sync_lag_ms=0.0,
                error_count=0,
                started_at=datetime.now(UTC),
                phase_started_at=datetime.now(UTC),
                estimated_completion=None,
                current_rate_events_per_sec=1000.0,
                is_paused=False,
            )
            statuses.append(status)

        start_time = time.monotonic()
        for status in statuses:
            _ = status.to_dict()
        end_time = time.monotonic()

        result = BenchmarkResult.from_timing(
            name="status_serialization",
            start_time=start_time,
            end_time=end_time,
            operations_count=num_updates,
        )
        assert result.operations_per_second > 10_000

    @pytest.mark.asyncio
    async def test_many_subscribers_scaling(self) -> None:
        """Test status streaming with increasing subscriber counts."""
        subscriber_counts = [1, 5, 10, 20, 50]
        num_updates = 100
        migration_id = uuid4()
        results: dict[int, LatencyResult] = {}

        for num_subscribers in subscriber_counts:
            queues: list[asyncio.Queue[UUID]] = [
                asyncio.Queue(maxsize=100) for _ in range(num_subscribers)
            ]
            latencies: list[float] = []

            for _ in range(num_updates):
                start = time.monotonic()
                for queue in queues:
                    with contextlib.suppress(asyncio.QueueFull):
                        queue.put_nowait(migration_id)
                end = time.monotonic()
                latencies.append((end - start) * 1000)

                for queue in queues:
                    while not queue.empty():
                        try:
                            queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break

            results[num_subscribers] = LatencyResult.from_samples(
                f"status_streaming_{num_subscribers}_subs", latencies
            )

        for num_subs, result in results.items():
            if num_subs <= 20:
                assert result.mean_ms < 1.0
