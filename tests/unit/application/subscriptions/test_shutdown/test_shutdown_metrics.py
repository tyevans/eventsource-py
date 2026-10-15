"""Unit tests for shutdown metrics snapshot, recording, and coordinator metrics."""

import asyncio
import signal
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    OTEL_METRICS_AVAILABLE,
    ShutdownCoordinator,
    ShutdownMetricsSnapshot,
    ShutdownPhase,
    get_in_flight_at_shutdown,
    record_drain_duration,
    record_events_drained,
    record_in_flight_at_shutdown,
    record_shutdown_completed,
    record_shutdown_initiated,
    reset_shutdown_metrics,
)


def _sample_snapshot(outcome: str = "clean") -> ShutdownMetricsSnapshot:
    return ShutdownMetricsSnapshot(
        shutdown_duration_seconds=5.5,
        drain_duration_seconds=2.0,
        events_drained=100,
        checkpoints_saved=3,
        in_flight_at_start=150,
        outcome=outcome,
    )


class TestShutdownMetricsSnapshot:
    def test_create_snapshot(self):
        snapshot = _sample_snapshot("clean")
        assert snapshot.shutdown_duration_seconds == 5.5
        assert snapshot.drain_duration_seconds == 2.0
        assert snapshot.events_drained == 100
        assert snapshot.checkpoints_saved == 3
        assert snapshot.in_flight_at_start == 150
        assert snapshot.outcome == "clean"

    def test_snapshot_is_frozen(self):
        snapshot = _sample_snapshot("clean")
        with pytest.raises(AttributeError):
            snapshot.outcome = "forced"  # type: ignore[misc]

    def test_snapshot_to_dict(self):
        d = _sample_snapshot("timeout").to_dict()
        assert d["shutdown_duration_seconds"] == 5.5
        assert d["drain_duration_seconds"] == 2.0
        assert d["events_drained"] == 100
        assert d["checkpoints_saved"] == 3
        assert d["in_flight_at_start"] == 150
        assert d["outcome"] == "timeout"


class TestShutdownMetricsFunctions:
    def setup_method(self):
        reset_shutdown_metrics()

    def teardown_method(self):
        reset_shutdown_metrics()

    def test_record_shutdown_initiated_no_crash(self):
        record_shutdown_initiated()

    def test_record_shutdown_completed_no_crash(self):
        record_shutdown_completed("clean", 5.0)
        record_shutdown_completed("forced", 10.0)
        record_shutdown_completed("timeout", 30.0)

    def test_record_drain_duration_no_crash(self):
        record_drain_duration(2.5)

    def test_record_events_drained_no_crash(self):
        record_events_drained(100)
        record_events_drained(0)

    def test_record_in_flight_at_shutdown(self):
        record_in_flight_at_shutdown(150)
        assert get_in_flight_at_shutdown() == 150

    def test_reset_shutdown_metrics_clears_in_flight(self):
        record_in_flight_at_shutdown(100)
        assert get_in_flight_at_shutdown() == 100
        reset_shutdown_metrics()
        assert get_in_flight_at_shutdown() == 0


class TestShutdownCoordinatorMetrics:
    def setup_method(self):
        reset_shutdown_metrics()

    def teardown_method(self):
        reset_shutdown_metrics()

    @pytest.mark.asyncio
    async def test_shutdown_creates_metrics_snapshot(self):
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock())
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "clean"
        assert coordinator.last_metrics_snapshot.shutdown_duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_clean_shutdown_outcome(self):
        coordinator = ShutdownCoordinator()
        result = await coordinator.shutdown(stop_func=AsyncMock())
        assert result.phase == ShutdownPhase.STOPPED
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "clean"

    @pytest.mark.asyncio
    async def test_timeout_shutdown_outcome(self):
        coordinator = ShutdownCoordinator(drain_timeout=0.1)

        async def slow_drain():
            await asyncio.sleep(10.0)
            return 0

        result = await coordinator.shutdown(stop_func=AsyncMock(), drain_func=slow_drain)
        assert result.forced is True
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "timeout"

    @pytest.mark.asyncio
    async def test_forced_shutdown_outcome(self):
        coordinator = ShutdownCoordinator()

        async def slow_drain():
            await coordinator._handle_signal(signal.SIGTERM)
            await coordinator._handle_signal(signal.SIGINT)
            await asyncio.sleep(1.0)
            return 0

        result = await coordinator.shutdown(stop_func=AsyncMock(), drain_func=slow_drain)
        assert result.forced is True
        assert "double signal" in result.error.lower()
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "forced"

    @pytest.mark.asyncio
    async def test_drain_duration_recorded(self):
        coordinator = ShutdownCoordinator(drain_timeout=5.0)

        async def timed_drain():
            await asyncio.sleep(0.1)
            return 10

        await coordinator.shutdown(stop_func=AsyncMock(), drain_func=timed_drain)
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.drain_duration_seconds >= 0.1

    @pytest.mark.asyncio
    async def test_events_drained_recorded(self):
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock(), drain_func=AsyncMock(return_value=50))
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.events_drained == 50

    @pytest.mark.asyncio
    async def test_checkpoints_saved_recorded(self):
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock(), checkpoint_func=AsyncMock(return_value=5))
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.checkpoints_saved == 5

    @pytest.mark.asyncio
    async def test_set_in_flight_count(self):
        coordinator = ShutdownCoordinator()
        coordinator.set_in_flight_count(150)
        await coordinator.shutdown(stop_func=AsyncMock())
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.in_flight_at_start == 150
        assert get_in_flight_at_shutdown() == 150

    @pytest.mark.asyncio
    async def test_reset_clears_metrics_snapshot(self):
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock())
        assert coordinator.last_metrics_snapshot is not None
        coordinator.reset()
        assert coordinator.last_metrics_snapshot is None

    @pytest.mark.asyncio
    async def test_reset_clears_in_flight_count(self):
        coordinator = ShutdownCoordinator()
        coordinator.set_in_flight_count(100)
        await coordinator.shutdown(stop_func=AsyncMock())
        coordinator.reset()
        assert coordinator._in_flight_at_start == 0

    @pytest.mark.asyncio
    async def test_no_drain_means_zero_drain_duration(self):
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock())
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.drain_duration_seconds == 0.0

    @pytest.mark.asyncio
    async def test_metrics_snapshot_survives_multiple_shutdowns(self):
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()
        await coordinator.shutdown(stop_func=stop_func)
        first_snapshot = coordinator.last_metrics_snapshot
        coordinator.reset()
        await coordinator.shutdown(stop_func=stop_func)
        second_snapshot = coordinator.last_metrics_snapshot
        assert first_snapshot is not None and second_snapshot is not None
        assert first_snapshot is not second_snapshot


class TestMetricsModuleImports:
    def test_import_metrics_snapshot(self):
        assert ShutdownMetricsSnapshot is not None

    def test_import_metrics_functions(self):
        assert all(
            fn is not None
            for fn in (
                record_shutdown_initiated,
                record_shutdown_completed,
                record_drain_duration,
                record_events_drained,
                record_in_flight_at_shutdown,
                get_in_flight_at_shutdown,
                reset_shutdown_metrics,
            )
        )

    def test_import_otel_available_flag(self):
        assert isinstance(OTEL_METRICS_AVAILABLE, bool)
