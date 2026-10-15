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
    def test_running_value(self):
        assert ShutdownPhase.RUNNING.value == "running"

    def test_stopping_value(self):
        assert ShutdownPhase.STOPPING.value == "stopping"

    def test_draining_value(self):
        assert ShutdownPhase.DRAINING.value == "draining"

    def test_checkpointing_value(self):
        assert ShutdownPhase.CHECKPOINTING.value == "checkpointing"

    def test_stopped_value(self):
        assert ShutdownPhase.STOPPED.value == "stopped"

    def test_forced_value(self):
        assert ShutdownPhase.FORCED.value == "forced"

    def test_all_phases_exist(self):
        phases = list(ShutdownPhase)
        assert len(phases) == 6
        assert set(phases) == {
            ShutdownPhase.RUNNING,
            ShutdownPhase.STOPPING,
            ShutdownPhase.DRAINING,
            ShutdownPhase.CHECKPOINTING,
            ShutdownPhase.STOPPED,
            ShutdownPhase.FORCED,
        }


class TestShutdownReasonEnum:
    def test_shutdown_reason_values(self):
        assert ShutdownReason.SIGNAL_SIGTERM.value == "signal_sigterm"
        assert ShutdownReason.SIGNAL_SIGINT.value == "signal_sigint"
        assert ShutdownReason.PROGRAMMATIC.value == "programmatic"
        assert ShutdownReason.HEALTH_CHECK.value == "health_check"
        assert ShutdownReason.TIMEOUT.value == "timeout"
        assert ShutdownReason.DOUBLE_SIGNAL.value == "double_signal"

    def test_all_reasons_exist(self):
        reasons = list(ShutdownReason)
        assert len(reasons) == 6
        assert set(reasons) == {
            ShutdownReason.SIGNAL_SIGTERM,
            ShutdownReason.SIGNAL_SIGINT,
            ShutdownReason.PROGRAMMATIC,
            ShutdownReason.HEALTH_CHECK,
            ShutdownReason.TIMEOUT,
            ShutdownReason.DOUBLE_SIGNAL,
        }


class TestShutdownResultNewFields:
    def test_in_flight_at_start_field(self):
        result = ShutdownResult(ShutdownPhase.STOPPED, 5.0, 1, 10, in_flight_at_start=100)
        assert result.in_flight_at_start == 100 and result.to_dict()["in_flight_at_start"] == 100

    def test_events_not_drained_field(self):
        result = ShutdownResult(
            ShutdownPhase.FORCED, 30.0, 1, 50, in_flight_at_start=100, events_not_drained=50
        )
        assert result.events_not_drained == 50 and result.to_dict()["events_not_drained"] == 50

    def test_default_values(self):
        result = ShutdownResult(ShutdownPhase.STOPPED, 5.0, 1, 10)
        assert (result.reason, result.in_flight_at_start, result.events_not_drained) == (None, 0, 0)

    @pytest.mark.asyncio
    async def test_events_not_drained_calculated(self):
        coordinator = ShutdownCoordinator(drain_timeout=0.1)
        coordinator.set_in_flight_count(100)

        async def slow_drain():
            await asyncio.sleep(10.0)
            return 0

        coordinator.request_shutdown()
        result = await coordinator.shutdown(stop_func=AsyncMock(), drain_func=slow_drain)
        assert (result.in_flight_at_start, result.events_not_drained) == (100, 100)

    @pytest.mark.asyncio
    async def test_events_not_drained_partial_drain(self):
        coordinator = ShutdownCoordinator()
        coordinator.set_in_flight_count(100)
        coordinator.request_shutdown()
        result = await coordinator.shutdown(
            stop_func=AsyncMock(), drain_func=AsyncMock(return_value=60)
        )
        assert (result.in_flight_at_start, result.events_drained, result.events_not_drained) == (100, 60, 40)  # fmt: skip


class TestShutdownResult:
    def test_create_result(self):
        result = ShutdownResult(ShutdownPhase.STOPPED, 5.5, 3, 100, checkpoints_saved=3)
        assert (result.phase, result.duration_seconds) == (ShutdownPhase.STOPPED, 5.5)
        assert (result.subscriptions_stopped, result.events_drained, result.checkpoints_saved) == (3, 100, 3)  # fmt: skip
        assert (result.forced, result.error) == (False, None)

    def test_create_result_with_error(self):
        result = ShutdownResult(
            ShutdownPhase.FORCED, 30.0, 2, 50, 1, forced=True, error="Timeout exceeded"
        )
        assert (result.phase, result.forced, result.error) == (ShutdownPhase.FORCED, True, "Timeout exceeded")  # fmt: skip

    def test_result_is_frozen(self):
        result = ShutdownResult(ShutdownPhase.STOPPED, 5.0, 1, 10)
        with pytest.raises(AttributeError):
            result.phase = ShutdownPhase.FORCED  # type: ignore[misc]

    def test_to_dict(self):
        result = ShutdownResult(
            ShutdownPhase.STOPPED,
            5.5,
            3,
            100,
            checkpoints_saved=3,
            forced=False,
            reason=ShutdownReason.PROGRAMMATIC,
            in_flight_at_start=150,
            events_not_drained=50,
        )
        assert result.to_dict() == {
            "phase": "stopped",
            "duration_seconds": 5.5,
            "subscriptions_stopped": 3,
            "events_drained": 100,
            "checkpoints_saved": 3,
            "forced": False,
            "error": None,
            "reason": "programmatic",
            "in_flight_at_start": 150,
            "events_not_drained": 50,
        }

    def test_to_dict_with_error(self):
        result = ShutdownResult(
            ShutdownPhase.FORCED, 30.0, 2, 50, forced=True, error="Timeout exceeded"
        )
        assert result.to_dict()["error"] == "Timeout exceeded"


class TestModuleImports:
    def test_import_from_shutdown_module(self):
        from eventsource.application.subscriptions import shutdown as sm

        assert all(
            getattr(sm, n) is not None
            for n in ("ShutdownCoordinator", "ShutdownPhase", "ShutdownReason", "ShutdownResult")
        )

    def test_import_from_subscriptions_package(self):
        from eventsource.application import subscriptions as sp

        assert all(
            getattr(sp, n) is not None
            for n in ("ShutdownCoordinator", "ShutdownPhase", "ShutdownResult")
        )

    def test_shutdown_reason_exported(self):
        from eventsource.application.subscriptions.shutdown import __all__

        assert "ShutdownReason" in __all__
