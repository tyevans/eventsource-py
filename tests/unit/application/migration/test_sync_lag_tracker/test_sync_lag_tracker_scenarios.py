"""Tests for SyncLagTracker edge cases, tenant scoping, and integration scenarios."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.ports import ExpectedVersion, Position
from eventsource.ports.migration.models import MigrationConfig, SyncLag
from tests.unit.application.migration.test_sync_lag_tracker._fixtures import (
    LagTestEvent,
    seed_events,
    sid,
)


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_empty_stores(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )
        lag = await tracker.calculate_lag()
        assert lag.events == 0
        assert tracker.is_sync_ready() is True
        assert tracker.is_fully_converged() is True

    @pytest.mark.asyncio
    async def test_large_lag_is_bounded(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=strict_config,
            enable_tracing=False,
        )
        await seed_events(source_store, 1_000)
        lag = await tracker.calculate_lag()
        assert lag.events == strict_config.cutover_max_lag_events + 1
        assert lag.count_is_bounded is True
        assert tracker.is_sync_ready() is False
        with pytest.raises(ValueError, match="exceeds the configured"):
            tracker.is_converged(max_lag=1_000)

    @pytest.mark.asyncio
    async def test_exact_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )
        await seed_events(source_store, 100)
        await tracker.calculate_lag()
        assert tracker.is_converged() is True
        assert tracker.is_sync_ready() is True

    def test_convergence_with_few_samples(self, tracker: SyncLagTracker) -> None:
        for i in range(3):
            lag = SyncLag(
                events=50 - (i * 10),
                source_position=Position(store_id="source", key=(100,)),
                target_position=Position(store_id="target", key=(50 + (i * 10),)),
                timestamp=datetime.now(UTC),
            )
            tracker.record_lag(lag)
        assert tracker.get_lag_stats().is_converging is False


class TestWithTenantId:
    @pytest.mark.asyncio
    async def test_tracker_with_tenant_id(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tenant_id = uuid4()
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        for _ in range(50):
            s = sid()
            await source_store.append(
                s,
                [LagTestEvent(aggregate_id=s.aggregate_id, tenant_id=tenant_id)],
                ExpectedVersion.any_(),
            )
        for _ in range(5):
            s = sid()
            await source_store.append(
                s,
                [LagTestEvent(aggregate_id=s.aggregate_id, tenant_id=uuid4())],
                ExpectedVersion.any_(),
            )
        lag = await tracker.calculate_lag()
        assert tracker.tenant_id == tenant_id
        assert lag.events == 50


class TestIntegrationScenarios:
    @pytest.mark.asyncio
    async def test_typical_migration_scenario(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )
        positions = []
        for _ in range(100):
            result = await source_store.append(
                (stream := sid()),
                [LagTestEvent(aggregate_id=stream.aggregate_id)],
                ExpectedVersion.any_(),
            )
            positions.append(result.position)

        for copied in [0, 20, 40, 60, 80, 90, 95, 99, 100]:
            since = positions[copied - 1] if copied > 0 else None
            await tracker.calculate_lag(since=since)

        assert tracker.is_sync_ready() is True
        assert tracker.is_fully_converged() is True
        stats = tracker.get_lag_stats()
        assert stats.is_converging is True
        assert (stats.current_lag, stats.max_lag, stats.min_lag) == (0, 100, 0)

    @pytest.mark.asyncio
    async def test_fluctuating_lag_scenario(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )
        positions = []
        for _ in range(100):
            result = await source_store.append(
                (stream := sid()),
                [LagTestEvent(aggregate_id=stream.aggregate_id)],
                ExpectedVersion.any_(),
            )
            positions.append(result.position)

        for copied in [0, 30, 25, 50, 45, 70, 65, 90, 95, 100]:
            since = positions[copied - 1] if copied > 0 else None
            await tracker.calculate_lag(since=since)

        assert tracker.get_lag_stats().is_converging is True
