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


class TestShutdownMetricsSnapshot:
    """Tests for ShutdownMetricsSnapshot dataclass."""

    def test_create_snapshot(self):
        """Test creating a metrics snapshot."""
        snapshot = ShutdownMetricsSnapshot(
            shutdown_duration_seconds=5.5,
            drain_duration_seconds=2.0,
            events_drained=100,
            checkpoints_saved=3,
            in_flight_at_start=150,
            outcome="clean",
        )
        assert snapshot.shutdown_duration_seconds == 5.5
        assert snapshot.drain_duration_seconds == 2.0
        assert snapshot.events_drained == 100
        assert snapshot.checkpoints_saved == 3
        assert snapshot.in_flight_at_start == 150
        assert snapshot.outcome == "clean"

    def test_snapshot_is_frozen(self):
        """Test snapshot is immutable."""
        snapshot = ShutdownMetricsSnapshot(
            shutdown_duration_seconds=5.0,
            drain_duration_seconds=2.0,
            events_drained=50,
            checkpoints_saved=2,
            in_flight_at_start=100,
            outcome="clean",
        )
        with pytest.raises(AttributeError):
            snapshot.outcome = "forced"  # type: ignore[misc]

    def test_snapshot_to_dict(self):
        """Test to_dict conversion."""
        snapshot = ShutdownMetricsSnapshot(
            shutdown_duration_seconds=5.5,
            drain_duration_seconds=2.0,
            events_drained=100,
            checkpoints_saved=3,
            in_flight_at_start=150,
            outcome="timeout",
        )
        d = snapshot.to_dict()
        assert d["shutdown_duration_seconds"] == 5.5
        assert d["drain_duration_seconds"] == 2.0
        assert d["events_drained"] == 100
        assert d["checkpoints_saved"] == 3
        assert d["in_flight_at_start"] == 150
        assert d["outcome"] == "timeout"


class TestShutdownMetricsFunctions:
    """Tests for shutdown metrics recording functions."""

    def setup_method(self):
        reset_shutdown_metrics()

    def teardown_method(self):
        reset_shutdown_metrics()

    def test_record_shutdown_initiated_no_crash(self):
        """Test record_shutdown_initiated doesn't crash."""
        record_shutdown_initiated()

    def test_record_shutdown_completed_no_crash(self):
        """Test record_shutdown_completed doesn't crash."""
        record_shutdown_completed("clean", 5.0)
        record_shutdown_completed("forced", 10.0)
        record_shutdown_completed("timeout", 30.0)

    def test_record_drain_duration_no_crash(self):
        """Test record_drain_duration doesn't crash."""
        record_drain_duration(2.5)

    def test_record_events_drained_no_crash(self):
        """Test record_events_drained doesn't crash."""
        record_events_drained(100)
        record_events_drained(0)

    def test_record_in_flight_at_shutdown(self):
        """Test record_in_flight_at_shutdown records value."""
        record_in_flight_at_shutdown(150)
        assert get_in_flight_at_shutdown() == 150

    def test_reset_shutdown_metrics_clears_in_flight(self):
        """Test reset_shutdown_metrics clears in-flight count."""
        record_in_flight_at_shutdown(100)
        assert get_in_flight_at_shutdown() == 100
        reset_shutdown_metrics()
        assert get_in_flight_at_shutdown() == 0


class TestShutdownCoordinatorMetrics:
    """Tests for metrics integration in ShutdownCoordinator."""

    def setup_method(self):
        reset_shutdown_metrics()

    def teardown_method(self):
        reset_shutdown_metrics()

    @pytest.mark.asyncio
    async def test_shutdown_creates_metrics_snapshot(self):
        """Test shutdown creates a metrics snapshot."""
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock())
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "clean"
        assert coordinator.last_metrics_snapshot.shutdown_duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_clean_shutdown_outcome(self):
        """Test clean shutdown has correct outcome."""
        coordinator = ShutdownCoordinator()
        result = await coordinator.shutdown(stop_func=AsyncMock())
        assert result.phase == ShutdownPhase.STOPPED
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "clean"

    @pytest.mark.asyncio
    async def test_timeout_shutdown_outcome(self):
        """Test timeout shutdown has correct outcome."""
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
        """Test forced shutdown (double signal) has correct outcome."""
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
        """Test drain duration is recorded in metrics."""
        coordinator = ShutdownCoordinator(drain_timeout=5.0)

        async def timed_drain():
            await asyncio.sleep(0.1)
            return 10

        await coordinator.shutdown(stop_func=AsyncMock(), drain_func=timed_drain)
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.drain_duration_seconds >= 0.1

    @pytest.mark.asyncio
    async def test_events_drained_recorded(self):
        """Test events drained count is recorded in metrics."""
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock(), drain_func=AsyncMock(return_value=50))
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.events_drained == 50

    @pytest.mark.asyncio
    async def test_checkpoints_saved_recorded(self):
        """Test checkpoints saved count is recorded in metrics."""
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock(), checkpoint_func=AsyncMock(return_value=5))
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.checkpoints_saved == 5

    @pytest.mark.asyncio
    async def test_set_in_flight_count(self):
        """Test set_in_flight_count records value."""
        coordinator = ShutdownCoordinator()
        coordinator.set_in_flight_count(150)
        await coordinator.shutdown(stop_func=AsyncMock())
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.in_flight_at_start == 150
        assert get_in_flight_at_shutdown() == 150

    @pytest.mark.asyncio
    async def test_reset_clears_metrics_snapshot(self):
        """Test reset clears the metrics snapshot."""
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock())
        assert coordinator.last_metrics_snapshot is not None
        coordinator.reset()
        assert coordinator.last_metrics_snapshot is None

    @pytest.mark.asyncio
    async def test_reset_clears_in_flight_count(self):
        """Test reset clears the in-flight count."""
        coordinator = ShutdownCoordinator()
        coordinator.set_in_flight_count(100)
        await coordinator.shutdown(stop_func=AsyncMock())
        coordinator.reset()
        assert coordinator._in_flight_at_start == 0

    @pytest.mark.asyncio
    async def test_no_drain_means_zero_drain_duration(self):
        """Test shutdown without drain has zero drain duration."""
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown(stop_func=AsyncMock())
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.drain_duration_seconds == 0.0

    @pytest.mark.asyncio
    async def test_metrics_snapshot_survives_multiple_shutdowns(self):
        """Test metrics snapshot is updated on each shutdown."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()
        await coordinator.shutdown(stop_func=stop_func)
        first_snapshot = coordinator.last_metrics_snapshot
        coordinator.reset()
        await coordinator.shutdown(stop_func=stop_func)
        second_snapshot = coordinator.last_metrics_snapshot
        assert first_snapshot is not None
        assert second_snapshot is not None
        assert first_snapshot is not second_snapshot


class TestMetricsModuleImports:
    """Tests for metrics module imports."""

    def test_import_metrics_snapshot(self):
        """Test ShutdownMetricsSnapshot import."""
        assert ShutdownMetricsSnapshot is not None

    def test_import_metrics_functions(self):
        """Test metrics function imports."""
        assert record_shutdown_initiated is not None
        assert record_shutdown_completed is not None
        assert record_drain_duration is not None
        assert record_events_drained is not None
        assert record_in_flight_at_shutdown is not None
        assert get_in_flight_at_shutdown is not None
        assert reset_shutdown_metrics is not None

    def test_import_otel_available_flag(self):
        """Test OTEL_METRICS_AVAILABLE import."""
        assert isinstance(OTEL_METRICS_AVAILABLE, bool)
