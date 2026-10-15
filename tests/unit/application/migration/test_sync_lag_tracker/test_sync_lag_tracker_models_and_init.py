"""Tests for LagSample, LagStats, and SyncLagTracker initialization."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.migration.sync_lag_tracker import (
    LagSample,
    LagStats,
    SyncLagTracker,
)
from eventsource.ports import Position
from eventsource.ports.migration.models import MigrationConfig, SyncLag


def _make_lag() -> SyncLag:
    return SyncLag(
        events=50,
        source_position=Position(store_id="source", key=(100,)),
        target_position=Position(store_id="target", key=(50,)),
        timestamp=datetime.now(UTC),
    )


class TestLagSample:
    def test_lag_sample_creation(self) -> None:
        lag = _make_lag()
        sample = LagSample(lag=lag)
        assert sample.lag == lag and sample.sampled_at is not None

    def test_lag_sample_with_custom_timestamp(self) -> None:
        custom_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        sample = LagSample(lag=_make_lag(), sampled_at=custom_time)
        assert sample.sampled_at == custom_time


class TestLagStats:
    def test_lag_stats_creation(self) -> None:
        now = datetime.now(UTC)
        stats = LagStats(
            current_lag=50,
            average_lag=45.5,
            max_lag=100,
            min_lag=10,
            sample_count=20,
            first_sample_at=now - timedelta(hours=1),
            last_sample_at=now,
            is_converging=True,
        )
        assert (
            stats.current_lag,
            stats.average_lag,
            stats.max_lag,
            stats.min_lag,
            stats.sample_count,
            stats.is_converging,
        ) == (50, 45.5, 100, 10, 20, True)

    def test_lag_stats_to_dict(self) -> None:
        now = datetime.now(UTC)
        stats = LagStats(
            current_lag=50,
            average_lag=45.5,
            max_lag=100,
            min_lag=10,
            sample_count=20,
            first_sample_at=now,
            last_sample_at=now,
            is_converging=False,
        )
        result = stats.to_dict()
        assert result["current_lag"] == 50 and result["average_lag"] == 45.5
        assert result["max_lag"] == 100 and result["min_lag"] == 10
        assert result["sample_count"] == 20 and result["is_converging"] is False
        assert result["first_sample_at"] == now.isoformat()
        assert result["last_sample_at"] == now.isoformat()

    def test_lag_stats_to_dict_with_none_timestamps(self) -> None:
        stats = LagStats(
            current_lag=0,
            average_lag=0.0,
            max_lag=0,
            min_lag=0,
            sample_count=0,
            first_sample_at=None,
            last_sample_at=None,
        )
        result = stats.to_dict()
        assert result["first_sample_at"] is None and result["last_sample_at"] is None


class TestSyncLagTrackerInit:
    def test_init_with_defaults(
        self, source_store: InMemoryEventStore, target_store: InMemoryEventStore
    ) -> None:
        tracker = SyncLagTracker(source_store=source_store, target_store=target_store)
        assert tracker.source_store is source_store and tracker.target_store is target_store
        assert tracker.config is not None
        assert tracker.tenant_id is None and tracker.current_lag is None
        assert tracker._max_sample_history == 100

    def test_init_with_config(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        tracker = SyncLagTracker(
            source_store=source_store, target_store=target_store, config=strict_config
        )
        assert tracker.config is strict_config and tracker.sync_threshold == 10

    def test_init_with_tenant_id(
        self, source_store: InMemoryEventStore, target_store: InMemoryEventStore
    ) -> None:
        tenant_id = uuid4()
        tracker = SyncLagTracker(
            source_store=source_store, target_store=target_store, tenant_id=tenant_id
        )
        assert tracker.tenant_id == tenant_id

    def test_init_with_custom_sample_history(
        self, source_store: InMemoryEventStore, target_store: InMemoryEventStore
    ) -> None:
        tracker = SyncLagTracker(
            source_store=source_store, target_store=target_store, max_sample_history=50
        )
        assert tracker._max_sample_history == 50

    def test_init_with_tracing_disabled(
        self, source_store: InMemoryEventStore, target_store: InMemoryEventStore
    ) -> None:
        tracker = SyncLagTracker(
            source_store=source_store, target_store=target_store, enable_tracing=False
        )
        assert tracker._enable_tracing is False

    def test_properties(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        assert tracker.source_store is source_store and tracker.target_store is target_store
        assert tracker.config is config and tracker.sync_threshold == 100
