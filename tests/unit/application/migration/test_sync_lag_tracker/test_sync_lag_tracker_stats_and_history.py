"""Tests for SyncLagTracker stats, history management, and manual lag recording."""

from datetime import UTC, datetime

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


class TestTrackerLagStats:
    """Tests for lag statistics methods."""

    def test_get_lag_stats_no_samples(self, tracker: SyncLagTracker) -> None:
        """Test get_lag_stats with no samples."""
        stats = tracker.get_lag_stats()
        assert stats.current_lag == 0
        assert stats.average_lag == 0.0
        assert stats.max_lag == 0
        assert stats.min_lag == 0
        assert stats.sample_count == 0
        assert stats.first_sample_at is None
        assert stats.last_sample_at is None
        assert stats.is_converging is False

    @pytest.mark.asyncio
    async def test_get_lag_stats_single_sample(
        self, tracker: SyncLagTracker, source_store: InMemoryEventStore
    ) -> None:
        """Test get_lag_stats with single sample."""
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        stats = tracker.get_lag_stats()
        assert stats.current_lag == 50
        assert stats.average_lag == 50.0
        assert stats.max_lag == 50
        assert stats.min_lag == 50
        assert stats.sample_count == 1
        assert stats.first_sample_at is not None
        assert stats.last_sample_at is not None

    @pytest.mark.asyncio
    async def test_get_lag_stats_multiple_samples(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test get_lag_stats with multiple samples as target catches up."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )
        positions = []
        for _ in range(90):
            result = await source_store.append(
                (stream := sid()),
                [LagTestEvent(aggregate_id=stream.aggregate_id)],
                ExpectedVersion.any_(),
            )
            positions.append(result.position)

        await tracker.calculate_lag()
        await tracker.calculate_lag(since=positions[59])
        await tracker.calculate_lag(since=positions[79])

        stats = tracker.get_lag_stats()
        assert stats.current_lag == 10
        assert stats.sample_count == 3
        assert stats.max_lag == 90
        assert stats.min_lag == 10

    @pytest.mark.asyncio
    async def test_convergence_trend_detection(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test that convergence trend is detected correctly."""
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

        for copied in [50, 60, 70, 80, 90, 95]:
            await tracker.calculate_lag(since=positions[copied - 1])

        stats = tracker.get_lag_stats()
        assert stats.is_converging is True

    @pytest.mark.asyncio
    async def test_non_convergence_trend_detection(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test that non-convergence (diverging) is detected."""
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

        for copied in [90, 80, 70, 60, 50, 40]:
            await tracker.calculate_lag(since=positions[copied - 1])

        stats = tracker.get_lag_stats()
        assert stats.is_converging is False


class TestSampleHistoryManagement:
    """Tests for sample history management."""

    @pytest.mark.asyncio
    async def test_get_sample_history(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test getting sample history."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )
        positions = []
        for _ in range(50):
            result = await source_store.append(
                (stream := sid()),
                [LagTestEvent(aggregate_id=stream.aggregate_id)],
                ExpectedVersion.any_(),
            )
            positions.append(result.position)

        await tracker.calculate_lag()
        await tracker.calculate_lag(since=positions[19])
        history = tracker.get_sample_history()

        assert len(history) == 2
        assert all(isinstance(lag, SyncLag) for lag in history)
        assert history[0].events == 50
        assert history[1].events == 30

    def test_clear_history(self, tracker: SyncLagTracker) -> None:
        """Test clearing sample history."""
        lag = SyncLag(
            events=50,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(50,)),
            timestamp=datetime.now(UTC),
        )
        tracker.record_lag(lag)
        assert len(tracker._lag_samples) == 1
        assert tracker.current_lag is not None

        cleared = tracker.clear_history()
        assert cleared == 1
        assert len(tracker._lag_samples) == 0
        assert tracker.current_lag is None

    @pytest.mark.asyncio
    async def test_sample_history_max_size(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test that sample history respects max size."""
        max_history = 5
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
            max_sample_history=max_history,
        )
        for _ in range(max_history + 3):
            await seed_events(source_store, 1)
            await tracker.calculate_lag()

        assert len(tracker._lag_samples) == max_history


class TestManualLagRecording:
    """Tests for manual lag recording."""

    def test_record_lag(self, tracker: SyncLagTracker) -> None:
        """Test manually recording a lag measurement."""
        lag = SyncLag(
            events=25,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(75,)),
            timestamp=datetime.now(UTC),
        )
        tracker.record_lag(lag)
        assert tracker.current_lag is lag
        assert len(tracker._lag_samples) == 1
        assert tracker._lag_samples[0].lag is lag

    def test_record_lag_multiple(self, tracker: SyncLagTracker) -> None:
        """Test recording multiple lag measurements."""
        for i in range(3):
            lag = SyncLag(
                events=50 - (i * 10),
                source_position=Position(store_id="source", key=(100,)),
                target_position=Position(store_id="target", key=(50 + (i * 10),)),
                timestamp=datetime.now(UTC),
            )
            tracker.record_lag(lag)

        assert len(tracker._lag_samples) == 3
        assert tracker.current_lag.events == 30

    def test_record_lag_updates_stats(self, tracker: SyncLagTracker) -> None:
        """Test that recorded lags update statistics."""
        for lag_events in [50, 40, 30, 20, 10]:
            lag = SyncLag(
                events=lag_events,
                source_position=Position(store_id="source", key=(100,)),
                target_position=Position(store_id="target", key=(100 - lag_events,)),
                timestamp=datetime.now(UTC),
            )
            tracker.record_lag(lag)

        stats = tracker.get_lag_stats()
        assert stats.current_lag == 10
        assert stats.average_lag == 30.0
        assert stats.max_lag == 50
        assert stats.min_lag == 10
