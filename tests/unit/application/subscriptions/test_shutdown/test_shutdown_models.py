"""Unit tests for shutdown models and enums."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownReason,
    ShutdownResult,
)


class TestShutdownPhaseEnum:
    """Tests for ShutdownPhase enum."""

    def test_running_value(self):
        """Test RUNNING has correct value."""
        assert ShutdownPhase.RUNNING.value == "running"

    def test_stopping_value(self):
        """Test STOPPING has correct value."""
        assert ShutdownPhase.STOPPING.value == "stopping"

    def test_draining_value(self):
        """Test DRAINING has correct value."""
        assert ShutdownPhase.DRAINING.value == "draining"

    def test_checkpointing_value(self):
        """Test CHECKPOINTING has correct value."""
        assert ShutdownPhase.CHECKPOINTING.value == "checkpointing"

    def test_stopped_value(self):
        """Test STOPPED has correct value."""
        assert ShutdownPhase.STOPPED.value == "stopped"

    def test_forced_value(self):
        """Test FORCED has correct value."""
        assert ShutdownPhase.FORCED.value == "forced"

    def test_all_phases_exist(self):
        """Test all expected phases exist."""
        phases = list(ShutdownPhase)
        assert len(phases) == 6
        assert ShutdownPhase.RUNNING in phases
        assert ShutdownPhase.STOPPING in phases
        assert ShutdownPhase.DRAINING in phases
        assert ShutdownPhase.CHECKPOINTING in phases
        assert ShutdownPhase.STOPPED in phases
        assert ShutdownPhase.FORCED in phases


class TestShutdownReasonEnum:
    """Tests for ShutdownReason enum."""

    def test_shutdown_reason_values(self):
        """Test all ShutdownReason enum values."""
        assert ShutdownReason.SIGNAL_SIGTERM.value == "signal_sigterm"
        assert ShutdownReason.SIGNAL_SIGINT.value == "signal_sigint"
        assert ShutdownReason.PROGRAMMATIC.value == "programmatic"
        assert ShutdownReason.HEALTH_CHECK.value == "health_check"
        assert ShutdownReason.TIMEOUT.value == "timeout"
        assert ShutdownReason.DOUBLE_SIGNAL.value == "double_signal"

    def test_all_reasons_exist(self):
        """Test all expected reasons exist."""
        reasons = list(ShutdownReason)
        assert len(reasons) == 6
        assert ShutdownReason.SIGNAL_SIGTERM in reasons
        assert ShutdownReason.SIGNAL_SIGINT in reasons
        assert ShutdownReason.PROGRAMMATIC in reasons
        assert ShutdownReason.HEALTH_CHECK in reasons
        assert ShutdownReason.TIMEOUT in reasons
        assert ShutdownReason.DOUBLE_SIGNAL in reasons


class TestShutdownResultNewFields:
    """Tests for new ShutdownResult fields."""

    def test_in_flight_at_start_field(self):
        """Test in_flight_at_start field."""
        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
            in_flight_at_start=100,
        )
        assert result.in_flight_at_start == 100
        assert result.to_dict()["in_flight_at_start"] == 100

    def test_events_not_drained_field(self):
        """Test events_not_drained field."""
        result = ShutdownResult(
            phase=ShutdownPhase.FORCED,
            duration_seconds=30.0,
            subscriptions_stopped=1,
            events_drained=50,
            in_flight_at_start=100,
            events_not_drained=50,
        )
        assert result.events_not_drained == 50
        assert result.to_dict()["events_not_drained"] == 50

    def test_default_values(self):
        """Test default values for new fields."""
        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )
        assert result.reason is None
        assert result.in_flight_at_start == 0
        assert result.events_not_drained == 0

    @pytest.mark.asyncio
    async def test_events_not_drained_calculated(self):
        """Test events_not_drained is calculated from in_flight and drained."""
        coordinator = ShutdownCoordinator(drain_timeout=0.1)
        coordinator.set_in_flight_count(100)

        stop_func = AsyncMock()

        async def slow_drain():
            await asyncio.sleep(10.0)  # Will timeout
            return 0

        coordinator.request_shutdown()
        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
        )

        # Should have 100 not drained (in_flight=100, drained=0)
        assert result.in_flight_at_start == 100
        assert result.events_not_drained == 100

    @pytest.mark.asyncio
    async def test_events_not_drained_partial_drain(self):
        """Test events_not_drained with partial drain."""
        coordinator = ShutdownCoordinator()
        coordinator.set_in_flight_count(100)

        stop_func = AsyncMock()
        drain_func = AsyncMock(return_value=60)

        coordinator.request_shutdown()
        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
        )

        # Should have 40 not drained (in_flight=100, drained=60)
        assert result.in_flight_at_start == 100
        assert result.events_drained == 60
        assert result.events_not_drained == 40


class TestShutdownResult:
    """Tests for ShutdownResult dataclass."""

    def test_create_result(self):
        """Test creating a shutdown result."""
        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.5,
            subscriptions_stopped=3,
            events_drained=100,
            checkpoints_saved=3,
            forced=False,
        )
        assert result.phase == ShutdownPhase.STOPPED
        assert result.duration_seconds == 5.5
        assert result.subscriptions_stopped == 3
        assert result.events_drained == 100
        assert result.checkpoints_saved == 3
        assert result.forced is False
        assert result.error is None

    def test_create_result_with_error(self):
        """Test creating a shutdown result with error."""
        result = ShutdownResult(
            phase=ShutdownPhase.FORCED,
            duration_seconds=30.0,
            subscriptions_stopped=2,
            events_drained=50,
            checkpoints_saved=1,
            forced=True,
            error="Timeout exceeded",
        )
        assert result.phase == ShutdownPhase.FORCED
        assert result.forced is True
        assert result.error == "Timeout exceeded"

    def test_result_is_frozen(self):
        """Test result is immutable."""
        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )
        with pytest.raises(AttributeError):
            result.phase = ShutdownPhase.FORCED  # type: ignore[misc]

    def test_to_dict(self):
        """Test to_dict conversion."""
        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.5,
            subscriptions_stopped=3,
            events_drained=100,
            checkpoints_saved=3,
            forced=False,
            reason=ShutdownReason.PROGRAMMATIC,
            in_flight_at_start=150,
            events_not_drained=50,
        )
        d = result.to_dict()

        assert d["phase"] == "stopped"
        assert d["duration_seconds"] == 5.5
        assert d["subscriptions_stopped"] == 3
        assert d["events_drained"] == 100
        assert d["checkpoints_saved"] == 3
        assert d["forced"] is False
        assert d["error"] is None
        assert d["reason"] == "programmatic"
        assert d["in_flight_at_start"] == 150
        assert d["events_not_drained"] == 50

    def test_to_dict_with_error(self):
        """Test to_dict includes error."""
        result = ShutdownResult(
            phase=ShutdownPhase.FORCED,
            duration_seconds=30.0,
            subscriptions_stopped=2,
            events_drained=50,
            forced=True,
            error="Timeout exceeded",
        )
        d = result.to_dict()
        assert d["error"] == "Timeout exceeded"


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_shutdown_module(self):
        """Test imports from shutdown module."""
        from eventsource.application.subscriptions.shutdown import (
            ShutdownCoordinator,
            ShutdownPhase,
            ShutdownReason,
            ShutdownResult,
        )

        assert ShutdownCoordinator is not None
        assert ShutdownPhase is not None
        assert ShutdownReason is not None
        assert ShutdownResult is not None

    def test_import_from_subscriptions_package(self):
        """Test imports from subscriptions package."""
        from eventsource.application.subscriptions import (
            ShutdownCoordinator,
            ShutdownPhase,
            ShutdownResult,
        )

        assert ShutdownCoordinator is not None
        assert ShutdownPhase is not None
        assert ShutdownResult is not None

    def test_shutdown_reason_exported(self):
        """Test ShutdownReason is exported in __all__."""
        from eventsource.application.subscriptions.shutdown import __all__

        assert "ShutdownReason" in __all__
