"""Tests for SyncLagTracker lag calculation and convergence detection."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.migration.metrics import (
    clear_metrics_registry,
    get_migration_metrics,
)
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.ports.migration.models import MigrationConfig
from tests.unit.application.migration.test_sync_lag_tracker._fixtures import seed_events


class TestCalculateLag:
    """Tests for calculate_lag method."""

    async def test_calculate_lag_basic(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
    ) -> None:
        await seed_events(source_store, 50)
        lag = await tracker.calculate_lag()
        assert lag.events == 50
        assert lag.source_position == await source_store.current_position()
        assert lag.target_position == await target_store.current_position()
        assert lag.timestamp is not None
        assert lag.count_is_bounded is False

    async def test_calculate_lag_updates_current(
        self, tracker: SyncLagTracker, source_store: InMemoryEventStore
    ) -> None:
        await seed_events(source_store, 5)
        assert tracker.current_lag is None
        lag = await tracker.calculate_lag()
        assert tracker.current_lag is lag

    async def test_calculate_lag_adds_sample(
        self, tracker: SyncLagTracker, source_store: InMemoryEventStore
    ) -> None:
        await seed_events(source_store, 5)
        assert len(tracker._lag_samples) == 0
        await tracker.calculate_lag()
        assert len(tracker._lag_samples) == 1

    async def test_calculate_lag_records_metric_when_migration_id_set(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        clear_metrics_registry()
        migration_id = uuid4()
        tracker = SyncLagTracker(
            source_store,
            target_store,
            config=config,
            migration_id=migration_id,
            enable_tracing=False,
        )
        await seed_events(source_store, 7)
        await tracker.calculate_lag()
        snapshot = get_migration_metrics(str(migration_id), "unknown").get_snapshot()
        assert snapshot.sync_lag_events == 7
        clear_metrics_registry()

    async def test_calculate_lag_skips_metric_without_migration_id(
        self, tracker: SyncLagTracker, source_store: InMemoryEventStore
    ) -> None:
        clear_metrics_registry()
        await seed_events(source_store, 3)
        lag = await tracker.calculate_lag()
        assert lag.events == 3

    async def test_calculate_lag_zero_when_since_is_head(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(source_store, target_store, config=config, enable_tracing=False)
        last_position = await seed_events(source_store, 25)
        lag = await tracker.calculate_lag(since=last_position)
        assert lag.events == 0 and lag.is_converged is True

    async def test_calculate_lag_empty_source(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(source_store, target_store, config=config, enable_tracing=False)
        lag = await tracker.calculate_lag()
        assert lag.events == 0 and lag.count_is_bounded is False

    async def test_calculate_lag_at_bound_reports_and_stops(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(
            source_store, target_store, config=strict_config, enable_tracing=False
        )
        await seed_events(source_store, 15)
        lag = await tracker.calculate_lag()
        assert lag.events == strict_config.cutover_max_lag_events + 1
        assert lag.count_is_bounded is True

    async def test_calculate_lag_multiple_calls(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(source_store, target_store, config=config, enable_tracing=False)
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        await seed_events(source_store, 20)
        lag2 = await tracker.calculate_lag()
        assert lag2.events == 70 and len(tracker._lag_samples) == 2


class TestConvergenceDetection:
    """Tests for convergence detection methods."""

    async def test_is_converged_no_measurement(self, tracker: SyncLagTracker) -> None:
        assert tracker.is_converged() is False

    async def test_is_converged_within_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(source_store, target_store, config=config, enable_tracing=False)
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        assert tracker.is_converged() is True

    async def test_is_converged_exceeds_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(
            source_store, target_store, config=strict_config, enable_tracing=False
        )
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        assert tracker.is_converged() is False

    async def test_is_converged_custom_threshold(
        self, tracker: SyncLagTracker, source_store: InMemoryEventStore
    ) -> None:
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        assert tracker.is_converged(max_lag=100) is True
        assert tracker.is_converged(max_lag=50) is True
        assert tracker.is_converged(max_lag=49) is False
        assert tracker.is_converged(max_lag=10) is False

    async def test_is_converged_rejects_looser_threshold(
        self, tracker: SyncLagTracker, source_store: InMemoryEventStore
    ) -> None:
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        with pytest.raises(ValueError, match="exceeds the configured"):
            tracker.is_converged(max_lag=101)

    async def test_is_sync_ready_no_measurement(self, tracker: SyncLagTracker) -> None:
        assert tracker.is_sync_ready() is False

    async def test_is_sync_ready_within_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(source_store, target_store, config=config, enable_tracing=False)
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        assert tracker.is_sync_ready() is True

    async def test_is_sync_ready_exceeds_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(
            source_store, target_store, config=strict_config, enable_tracing=False
        )
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        assert tracker.is_sync_ready() is False

    async def test_is_fully_converged_no_measurement(self, tracker: SyncLagTracker) -> None:
        assert tracker.is_fully_converged() is False

    async def test_is_fully_converged_with_lag(
        self, tracker: SyncLagTracker, source_store: InMemoryEventStore
    ) -> None:
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        assert tracker.is_fully_converged() is False

    async def test_is_fully_converged_zero_lag(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(source_store, target_store, config=config, enable_tracing=False)
        last_position = await seed_events(source_store, 25)
        await tracker.calculate_lag(since=last_position)
        assert tracker.is_fully_converged() is True
