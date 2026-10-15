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


async def _append_positions(store: InMemoryEventStore, count: int) -> list[Position]:
    positions: list[Position] = []
    for _ in range(count):
        res = await store.append(
            (s := sid()), [LagTestEvent(aggregate_id=s.aggregate_id)], ExpectedVersion.any_()
        )
        positions.append(res.position)
    return positions


class TestTrackerLagStats:
    """Tests for lag statistics methods."""

    def test_get_lag_stats_no_samples(self, tracker: SyncLagTracker) -> None:
        stats = tracker.get_lag_stats()
        assert (stats.current_lag, stats.average_lag) == (0, 0.0)
        assert (stats.max_lag, stats.min_lag, stats.sample_count) == (0, 0, 0)
        assert stats.first_sample_at is None
        assert stats.last_sample_at is None
        assert stats.is_converging is False

    @pytest.mark.asyncio
    async def test_get_lag_stats_single_sample(
        self, tracker: SyncLagTracker, source_store: InMemoryEventStore
    ) -> None:
        await seed_events(source_store, 50)
        await tracker.calculate_lag()
        stats = tracker.get_lag_stats()
        assert (stats.current_lag, stats.average_lag) == (50, 50.0)
        assert (stats.max_lag, stats.min_lag, stats.sample_count) == (50, 50, 1)
        assert stats.first_sample_at is not None
        assert stats.last_sample_at is not None

    @pytest.mark.asyncio
    async def test_get_lag_stats_multiple_samples(
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
        positions = await _append_positions(source_store, 90)
        await tracker.calculate_lag()
        await tracker.calculate_lag(since=positions[59])
        await tracker.calculate_lag(since=positions[79])

        stats = tracker.get_lag_stats()
        assert (stats.current_lag, stats.sample_count) == (10, 3)
        assert (stats.max_lag, stats.min_lag) == (90, 10)

    @pytest.mark.asyncio
    async def test_convergence_trend_detection(
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
        positions = await _append_positions(source_store, 100)
        for copied in [50, 60, 70, 80, 90, 95]:
            await tracker.calculate_lag(since=positions[copied - 1])
        assert tracker.get_lag_stats().is_converging is True

    @pytest.mark.asyncio
    async def test_non_convergence_trend_detection(
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
        positions = await _append_positions(source_store, 100)
        for copied in [90, 80, 70, 60, 50, 40]:
            await tracker.calculate_lag(since=positions[copied - 1])
        assert tracker.get_lag_stats().is_converging is False


class TestSampleHistoryManagement:
    """Tests for sample history management."""

    @pytest.mark.asyncio
    async def test_get_sample_history(
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
        positions = await _append_positions(source_store, 50)
        await tracker.calculate_lag()
        await tracker.calculate_lag(since=positions[19])
        history = tracker.get_sample_history()

        assert len(history) == 2
        assert all(isinstance(lag, SyncLag) for lag in history)
        assert (history[0].events, history[1].events) == (50, 30)

    def test_clear_history(self, tracker: SyncLagTracker) -> None:
        lag = SyncLag(
            events=50,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(50,)),
            timestamp=datetime.now(UTC),
        )
        tracker.record_lag(lag)
        assert len(tracker._lag_samples) == 1
        assert tracker.current_lag is not None

        assert tracker.clear_history() == 1
        assert len(tracker._lag_samples) == 0
        assert tracker.current_lag is None

    @pytest.mark.asyncio
    async def test_sample_history_max_size(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
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
        for i in range(3):
            tracker.record_lag(
                SyncLag(
                    events=50 - (i * 10),
                    source_position=Position(store_id="source", key=(100,)),
                    target_position=Position(store_id="target", key=(50 + (i * 10),)),
                    timestamp=datetime.now(UTC),
                )
            )
        assert len(tracker._lag_samples) == 3
        assert tracker.current_lag.events == 30

    def test_record_lag_updates_stats(self, tracker: SyncLagTracker) -> None:
        for lag_events in [50, 40, 30, 20, 10]:
            tracker.record_lag(
                SyncLag(
                    events=lag_events,
                    source_position=Position(store_id="source", key=(100,)),
                    target_position=Position(store_id="target", key=(100 - lag_events,)),
                    timestamp=datetime.now(UTC),
                )
            )
        stats = tracker.get_lag_stats()
        assert (stats.current_lag, stats.average_lag) == (10, 30.0)
        assert (stats.max_lag, stats.min_lag) == (50, 10)
