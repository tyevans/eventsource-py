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
        d = _sample_snapshot("timeout").to_dict()
        assert (d["shutdown_duration_seconds"], d["drain_duration_seconds"]) == (5.5, 2.0)
        assert (d["events_drained"], d["checkpoints_saved"], d["in_flight_at_start"]) == (
            100,
            3,
            150,
        )
        assert d["outcome"] == "timeout"


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


async def _shutdown_with_snap(coord: ShutdownCoordinator | None = None, **kw):
    c = coord or ShutdownCoordinator()
    res = await c.shutdown(stop_func=AsyncMock(), **kw)
    assert c.last_metrics_snapshot is not None
    return c, res, c.last_metrics_snapshot


class TestShutdownCoordinatorMetrics(_ResetMetricsMixin):
    @pytest.mark.asyncio
    async def test_shutdown_creates_metrics_snapshot(self):
        _, _, snap = await _shutdown_with_snap()
        assert snap.outcome == "clean" and snap.shutdown_duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_clean_shutdown_outcome(self):
        _, result, snap = await _shutdown_with_snap()
        assert result.phase == ShutdownPhase.STOPPED and snap.outcome == "clean"

    @pytest.mark.asyncio
    async def test_timeout_shutdown_outcome(self):
        async def slow_drain():
            await asyncio.sleep(10.0)
            return 0

        _, result, snap = await _shutdown_with_snap(
            ShutdownCoordinator(drain_timeout=0.1), drain_func=slow_drain
        )
        assert result.forced is True and snap.outcome == "timeout"

    @pytest.mark.asyncio
    async def test_forced_shutdown_outcome(self):
        coord = ShutdownCoordinator()

        async def slow_drain():
            await coord._handle_signal(signal.SIGTERM)
            await coord._handle_signal(signal.SIGINT)
            await asyncio.sleep(1.0)
            return 0

        _, result, snap = await _shutdown_with_snap(coord, drain_func=slow_drain)
        assert result.forced is True and "double signal" in result.error.lower()
        assert snap.outcome == "forced"

    @pytest.mark.asyncio
    async def test_drain_duration_recorded(self):
        async def timed_drain():
            await asyncio.sleep(0.1)
            return 10

        _, _, snap = await _shutdown_with_snap(
            ShutdownCoordinator(drain_timeout=5.0), drain_func=timed_drain
        )
        assert snap.drain_duration_seconds >= 0.1

    @pytest.mark.asyncio
    async def test_events_drained_recorded(self):
        _, _, snap = await _shutdown_with_snap(drain_func=AsyncMock(return_value=50))
        assert snap.events_drained == 50

    @pytest.mark.asyncio
    async def test_checkpoints_saved_recorded(self):
        _, _, snap = await _shutdown_with_snap(checkpoint_func=AsyncMock(return_value=5))
        assert snap.checkpoints_saved == 5

    @pytest.mark.asyncio
    async def test_set_in_flight_count(self):
        coord = ShutdownCoordinator()
        coord.set_in_flight_count(150)
        _, _, snap = await _shutdown_with_snap(coord)
        assert snap.in_flight_at_start == 150 and get_in_flight_at_shutdown() == 150

    @pytest.mark.asyncio
    async def test_reset_clears_metrics_snapshot(self):
        coord, _, _ = await _shutdown_with_snap()
        coord.reset()
        assert coord.last_metrics_snapshot is None

    @pytest.mark.asyncio
    async def test_reset_clears_in_flight_count(self):
        coord = ShutdownCoordinator()
        coord.set_in_flight_count(100)
        await _shutdown_with_snap(coord)
        coord.reset()
        assert coord._in_flight_at_start == 0

    @pytest.mark.asyncio
    async def test_no_drain_means_zero_drain_duration(self):
        _, _, snap = await _shutdown_with_snap()
        assert snap.drain_duration_seconds == 0.0

    @pytest.mark.asyncio
    async def test_metrics_snapshot_survives_multiple_shutdowns(self):
        coord, _, first = await _shutdown_with_snap()
        coord.reset()
        _, _, second = await _shutdown_with_snap(coord)
        assert first is not second


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
