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
    return ShutdownMetricsSnapshot(5.5, 2.0, 100, 3, 150, outcome)


class TestShutdownMetricsSnapshot:
    def test_create_snapshot(self):
        s = _sample_snapshot("clean")
        assert (s.shutdown_duration_seconds, s.drain_duration_seconds) == (5.5, 2.0)
        assert (s.events_drained, s.checkpoints_saved, s.in_flight_at_start) == (100, 3, 150)
        assert s.outcome == "clean"

    def test_snapshot_is_frozen(self):
        with pytest.raises(AttributeError):
            _sample_snapshot("clean").outcome = "forced"  # type: ignore[misc]

    def test_snapshot_to_dict(self):
        assert _sample_snapshot("timeout").to_dict() == {
            "shutdown_duration_seconds": 5.5,
            "drain_duration_seconds": 2.0,
            "events_drained": 100,
            "checkpoints_saved": 3,
            "in_flight_at_start": 150,
            "outcome": "timeout",
        }


class _ResetMetricsMixin:
    def setup_method(self):
        reset_shutdown_metrics()

    def teardown_method(self):
        reset_shutdown_metrics()


class TestShutdownMetricsFunctions(_ResetMetricsMixin):
    def test_record_shutdown_initiated_no_crash(self):
        record_shutdown_initiated()

    def test_record_shutdown_completed_no_crash(self):
        for outcome, dur in (("clean", 5.0), ("forced", 10.0), ("timeout", 30.0)):
            record_shutdown_completed(outcome, dur)

    def test_record_drain_duration_no_crash(self):
        record_drain_duration(2.5)

    def test_record_events_drained_no_crash(self):
        for count in (100, 0):
            record_events_drained(count)

    def test_record_in_flight_at_shutdown(self):
        record_in_flight_at_shutdown(150)
        assert get_in_flight_at_shutdown() == 150

    def test_reset_shutdown_metrics_clears_in_flight(self):
        record_in_flight_at_shutdown(100)
        assert get_in_flight_at_shutdown() == 100
        reset_shutdown_metrics()
        assert get_in_flight_at_shutdown() == 0


class TestShutdownCoordinatorMetrics(_ResetMetricsMixin):
    @pytest.mark.asyncio
    async def test_shutdown_creates_metrics_snapshot(self):
        coord = ShutdownCoordinator()
        await coord.shutdown(stop_func=AsyncMock())
        snap = coord.last_metrics_snapshot
        assert snap is not None and snap.outcome == "clean" and snap.shutdown_duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_clean_shutdown_outcome(self):
        coord = ShutdownCoordinator()
        result = await coord.shutdown(stop_func=AsyncMock())
        snap = coord.last_metrics_snapshot
        assert (
            result.phase == ShutdownPhase.STOPPED and snap is not None and snap.outcome == "clean"
        )

    @pytest.mark.asyncio
    async def test_timeout_shutdown_outcome(self):
        coord = ShutdownCoordinator(drain_timeout=0.1)

        async def slow_drain():
            await asyncio.sleep(10.0)
            return 0

        result = await coord.shutdown(stop_func=AsyncMock(), drain_func=slow_drain)
        snap = coord.last_metrics_snapshot
        assert result.forced is True and snap is not None and snap.outcome == "timeout"

    @pytest.mark.asyncio
    async def test_forced_shutdown_outcome(self):
        coord = ShutdownCoordinator()

        async def slow_drain():
            await coord._handle_signal(signal.SIGTERM)
            await coord._handle_signal(signal.SIGINT)
            await asyncio.sleep(1.0)
            return 0

        result = await coord.shutdown(stop_func=AsyncMock(), drain_func=slow_drain)
        snap = coord.last_metrics_snapshot
        assert result.forced is True and "double signal" in result.error.lower()
        assert snap is not None and snap.outcome == "forced"

    @pytest.mark.asyncio
    async def test_drain_duration_recorded(self):
        coord = ShutdownCoordinator(drain_timeout=5.0)

        async def timed_drain():
            await asyncio.sleep(0.1)
            return 10

        await coord.shutdown(stop_func=AsyncMock(), drain_func=timed_drain)
        snap = coord.last_metrics_snapshot
        assert snap is not None and snap.drain_duration_seconds >= 0.1

    @pytest.mark.asyncio
    async def test_events_drained_recorded(self):
        coord = ShutdownCoordinator()
        await coord.shutdown(stop_func=AsyncMock(), drain_func=AsyncMock(return_value=50))
        snap = coord.last_metrics_snapshot
        assert snap is not None and snap.events_drained == 50

    @pytest.mark.asyncio
    async def test_checkpoints_saved_recorded(self):
        coord = ShutdownCoordinator()
        await coord.shutdown(stop_func=AsyncMock(), checkpoint_func=AsyncMock(return_value=5))
        snap = coord.last_metrics_snapshot
        assert snap is not None and snap.checkpoints_saved == 5

    @pytest.mark.asyncio
    async def test_set_in_flight_count(self):
        coord = ShutdownCoordinator()
        coord.set_in_flight_count(150)
        await coord.shutdown(stop_func=AsyncMock())
        snap = coord.last_metrics_snapshot
        assert snap is not None and snap.in_flight_at_start == 150
        assert get_in_flight_at_shutdown() == 150

    @pytest.mark.asyncio
    async def test_reset_clears_metrics_snapshot(self):
        coord = ShutdownCoordinator()
        await coord.shutdown(stop_func=AsyncMock())
        coord.reset()
        assert coord.last_metrics_snapshot is None

    @pytest.mark.asyncio
    async def test_reset_clears_in_flight_count(self):
        coord = ShutdownCoordinator()
        coord.set_in_flight_count(100)
        await coord.shutdown(stop_func=AsyncMock())
        coord.reset()
        assert coord._in_flight_at_start == 0

    @pytest.mark.asyncio
    async def test_no_drain_means_zero_drain_duration(self):
        coord = ShutdownCoordinator()
        await coord.shutdown(stop_func=AsyncMock())
        snap = coord.last_metrics_snapshot
        assert snap is not None and snap.drain_duration_seconds == 0.0

    @pytest.mark.asyncio
    async def test_metrics_snapshot_survives_multiple_shutdowns(self):
        coord, stop_func = ShutdownCoordinator(), AsyncMock()
        await coord.shutdown(stop_func=stop_func)
        first = coord.last_metrics_snapshot
        coord.reset()
        await coord.shutdown(stop_func=stop_func)
        second = coord.last_metrics_snapshot
        assert first is not None and second is not None and first is not second


class TestMetricsModuleImports:
    def test_import_metrics_snapshot(self):
        assert ShutdownMetricsSnapshot is not None

    def test_import_metrics_functions(self):
        fns = (
            record_shutdown_initiated,
            record_shutdown_completed,
            record_drain_duration,
            record_events_drained,
            record_in_flight_at_shutdown,
            get_in_flight_at_shutdown,
            reset_shutdown_metrics,
        )
        assert all(fn is not None for fn in fns)

    def test_import_otel_available_flag(self):
        assert isinstance(OTEL_METRICS_AVAILABLE, bool)
