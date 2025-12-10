"""
Unit tests for graceful shutdown coordination.

Tests for:
- ShutdownPhase enum
- ShutdownResult dataclass
- ShutdownCoordinator class
- Signal handling
- Timeout handling
- Shutdown phases
- Shutdown metrics
"""

import asyncio
import signal
import sys
from unittest.mock import AsyncMock

import pytest

from eventsource.subscriptions.shutdown import (
    OTEL_METRICS_AVAILABLE,
    ShutdownCoordinator,
    ShutdownMetricsSnapshot,
    ShutdownPhase,
    ShutdownReason,
    ShutdownResult,
    get_in_flight_at_shutdown,
    record_drain_duration,
    record_events_drained,
    record_in_flight_at_shutdown,
    record_shutdown_completed,
    record_shutdown_initiated,
    reset_shutdown_metrics,
)

# ShutdownPhase Enum Tests


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


# ShutdownReason Enum Tests


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


# ShutdownReason Tracking Tests


class TestShutdownReasonTracking:
    """Tests for ShutdownReason tracking in ShutdownCoordinator."""

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Signal handling tests not supported on Windows",
    )
    async def test_sigterm_sets_reason(self):
        """Test SIGTERM sets correct reason."""
        coordinator = ShutdownCoordinator()
        await coordinator._handle_signal(signal.SIGTERM)
        assert coordinator._shutdown_reason == ShutdownReason.SIGNAL_SIGTERM

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Signal handling tests not supported on Windows",
    )
    async def test_sigint_sets_reason(self):
        """Test SIGINT sets correct reason."""
        coordinator = ShutdownCoordinator()
        await coordinator._handle_signal(signal.SIGINT)
        assert coordinator._shutdown_reason == ShutdownReason.SIGNAL_SIGINT

    def test_programmatic_shutdown_default_reason(self):
        """Test programmatic shutdown uses default reason."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()
        assert coordinator._shutdown_reason == ShutdownReason.PROGRAMMATIC

    def test_programmatic_shutdown_custom_reason(self):
        """Test programmatic shutdown with custom reason."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown(ShutdownReason.HEALTH_CHECK)
        assert coordinator._shutdown_reason == ShutdownReason.HEALTH_CHECK

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Signal handling tests not supported on Windows",
    )
    async def test_double_signal_sets_reason(self):
        """Test double signal sets DOUBLE_SIGNAL reason."""
        coordinator = ShutdownCoordinator()
        await coordinator._handle_signal(signal.SIGTERM)
        await coordinator._handle_signal(signal.SIGTERM)  # Second signal
        assert coordinator._shutdown_reason == ShutdownReason.DOUBLE_SIGNAL

    def test_shutdown_result_includes_reason(self):
        """Test ShutdownResult includes reason in to_dict."""
        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
            reason=ShutdownReason.SIGNAL_SIGTERM,
        )
        result_dict = result.to_dict()
        assert result_dict["reason"] == "signal_sigterm"

    def test_shutdown_result_none_reason(self):
        """Test ShutdownResult handles None reason."""
        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
            reason=None,
        )
        result_dict = result.to_dict()
        assert result_dict["reason"] is None

    def test_reset_clears_reason(self):
        """Test reset clears shutdown reason."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()
        coordinator.reset()
        assert coordinator._shutdown_reason is None

    @pytest.mark.asyncio
    async def test_programmatic_shutdown_reason_in_result(self):
        """Test programmatic shutdown reason is included in result."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown(ShutdownReason.HEALTH_CHECK)

        stop_func = AsyncMock()
        result = await coordinator.shutdown(stop_func=stop_func)

        assert result.reason == ShutdownReason.HEALTH_CHECK

    @pytest.mark.asyncio
    async def test_timeout_sets_reason(self):
        """Test timeout sets TIMEOUT reason."""
        coordinator = ShutdownCoordinator(drain_timeout=0.1)
        stop_func = AsyncMock()

        async def slow_drain():
            await asyncio.sleep(10.0)  # Will timeout
            return 0

        coordinator.request_shutdown()  # PROGRAMMATIC reason
        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
        )

        # Reason should be TIMEOUT because drain timed out
        assert result.reason == ShutdownReason.TIMEOUT
        assert result.forced is True


# ShutdownResult New Fields Tests


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


# ShutdownResult Tests


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


# ShutdownCoordinator Creation Tests


class TestShutdownCoordinatorCreation:
    """Tests for ShutdownCoordinator creation."""

    def test_create_with_defaults(self):
        """Test creating coordinator with default values."""
        coordinator = ShutdownCoordinator()
        assert coordinator.timeout == 30.0
        assert coordinator.drain_timeout == 10.0
        assert coordinator.checkpoint_timeout == 5.0
        assert coordinator.checkpoint_interval == 5.0
        assert coordinator.phase == ShutdownPhase.RUNNING
        assert coordinator.is_shutting_down is False

    def test_create_with_custom_timeouts(self):
        """Test creating coordinator with custom timeouts."""
        coordinator = ShutdownCoordinator(
            timeout=60.0,
            drain_timeout=20.0,
            checkpoint_timeout=10.0,
            checkpoint_interval=2.0,
        )
        assert coordinator.timeout == 60.0
        assert coordinator.drain_timeout == 20.0
        assert coordinator.checkpoint_timeout == 10.0
        assert coordinator.checkpoint_interval == 2.0


# ShutdownCoordinator Properties Tests


class TestShutdownCoordinatorProperties:
    """Tests for ShutdownCoordinator properties."""

    def test_is_shutting_down_initially_false(self):
        """Test is_shutting_down is False initially."""
        coordinator = ShutdownCoordinator()
        assert coordinator.is_shutting_down is False

    def test_phase_initially_running(self):
        """Test phase is RUNNING initially."""
        coordinator = ShutdownCoordinator()
        assert coordinator.phase == ShutdownPhase.RUNNING

    def test_is_forced_initially_false(self):
        """Test is_forced is False initially."""
        coordinator = ShutdownCoordinator()
        assert coordinator.is_forced is False


# Request Shutdown Tests


class TestRequestShutdown:
    """Tests for programmatic shutdown request."""

    def test_request_shutdown_sets_flag(self):
        """Test request_shutdown sets is_shutting_down."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()
        assert coordinator.is_shutting_down is True

    @pytest.mark.asyncio
    async def test_request_shutdown_triggers_event(self):
        """Test request_shutdown triggers wait_for_shutdown."""
        coordinator = ShutdownCoordinator()

        async def wait_and_check():
            await coordinator.wait_for_shutdown()
            return True

        # Start waiting
        task = asyncio.create_task(wait_and_check())

        # Small delay to ensure task is waiting
        await asyncio.sleep(0.01)

        # Request shutdown
        coordinator.request_shutdown()

        # Wait should complete
        result = await asyncio.wait_for(task, timeout=1.0)
        assert result is True

    def test_request_shutdown_idempotent(self):
        """Test multiple request_shutdown calls are safe."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()
        coordinator.request_shutdown()
        coordinator.request_shutdown()
        assert coordinator.is_shutting_down is True


# Callback Tests


class TestShutdownCallbacks:
    """Tests for shutdown callbacks."""

    def test_on_shutdown_registers_callback(self):
        """Test on_shutdown registers a callback."""
        coordinator = ShutdownCoordinator()
        callback = AsyncMock()
        coordinator.on_shutdown(callback)
        assert callback in coordinator._on_shutdown_callbacks

    def test_remove_callback(self):
        """Test remove_callback removes a callback."""
        coordinator = ShutdownCoordinator()
        callback = AsyncMock()
        coordinator.on_shutdown(callback)
        result = coordinator.remove_callback(callback)
        assert result is True
        assert callback not in coordinator._on_shutdown_callbacks

    def test_remove_callback_not_found(self):
        """Test remove_callback returns False if not found."""
        coordinator = ShutdownCoordinator()
        callback = AsyncMock()
        result = coordinator.remove_callback(callback)
        assert result is False


# Shutdown Execution Tests


class TestShutdownExecution:
    """Tests for shutdown execution."""

    @pytest.mark.asyncio
    async def test_shutdown_stops_accepting_events(self):
        """Test shutdown calls stop_func."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        result = await coordinator.shutdown(stop_func=stop_func)

        stop_func.assert_called_once()
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_shutdown_drains_events(self):
        """Test shutdown calls drain_func."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()
        drain_func = AsyncMock(return_value=100)

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
        )

        drain_func.assert_called_once()
        assert result.events_drained == 100

    @pytest.mark.asyncio
    async def test_shutdown_saves_checkpoints(self):
        """Test shutdown calls checkpoint_func."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=5)

        result = await coordinator.shutdown(
            stop_func=stop_func,
            checkpoint_func=checkpoint_func,
        )

        checkpoint_func.assert_called_once()
        assert result.checkpoints_saved == 5

    @pytest.mark.asyncio
    async def test_shutdown_closes_connections(self):
        """Test shutdown calls close_func."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()
        close_func = AsyncMock()

        await coordinator.shutdown(
            stop_func=stop_func,
            close_func=close_func,
        )

        close_func.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_phase_progression(self):
        """Test shutdown progresses through phases correctly."""
        coordinator = ShutdownCoordinator()
        phases_seen = []

        async def record_phase_stop():
            phases_seen.append(coordinator.phase)

        async def record_phase_drain():
            phases_seen.append(coordinator.phase)
            return 10

        async def record_phase_checkpoint():
            phases_seen.append(coordinator.phase)
            return 3

        await coordinator.shutdown(
            stop_func=record_phase_stop,
            drain_func=record_phase_drain,
            checkpoint_func=record_phase_checkpoint,
        )

        assert ShutdownPhase.STOPPING in phases_seen
        assert ShutdownPhase.DRAINING in phases_seen
        assert ShutdownPhase.CHECKPOINTING in phases_seen

    @pytest.mark.asyncio
    async def test_shutdown_result_includes_duration(self):
        """Test shutdown result includes duration."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        result = await coordinator.shutdown(stop_func=stop_func)

        assert result.duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_shutdown_without_optional_funcs(self):
        """Test shutdown works with only stop_func."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        result = await coordinator.shutdown(stop_func=stop_func)

        assert result.phase == ShutdownPhase.STOPPED
        assert result.events_drained == 0
        assert result.checkpoints_saved == 0


# Timeout Handling Tests


class TestTimeoutHandling:
    """Tests for shutdown timeout handling."""

    @pytest.mark.asyncio
    async def test_stop_timeout_forces_shutdown(self):
        """Test stop phase timeout forces shutdown."""
        coordinator = ShutdownCoordinator()

        async def slow_stop():
            await asyncio.sleep(10.0)  # Will timeout

        result = await coordinator.shutdown(stop_func=slow_stop)

        assert result.forced is True

    @pytest.mark.asyncio
    async def test_drain_timeout_forces_shutdown(self):
        """Test drain phase timeout forces shutdown."""
        coordinator = ShutdownCoordinator(drain_timeout=0.1)
        stop_func = AsyncMock()

        async def slow_drain():
            await asyncio.sleep(10.0)  # Will timeout
            return 0

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
        )

        assert result.forced is True

    @pytest.mark.asyncio
    async def test_checkpoint_timeout_forces_shutdown(self):
        """Test checkpoint phase timeout forces shutdown."""
        coordinator = ShutdownCoordinator(checkpoint_timeout=0.1)
        stop_func = AsyncMock()

        async def slow_checkpoint():
            await asyncio.sleep(10.0)  # Will timeout
            return 0

        result = await coordinator.shutdown(
            stop_func=stop_func,
            checkpoint_func=slow_checkpoint,
        )

        assert result.forced is True


# Error Handling Tests


class TestErrorHandling:
    """Tests for shutdown error handling."""

    @pytest.mark.asyncio
    async def test_stop_func_error_forces_shutdown(self):
        """Test error in stop_func forces shutdown."""
        coordinator = ShutdownCoordinator()

        async def failing_stop():
            raise RuntimeError("Stop failed")

        result = await coordinator.shutdown(stop_func=failing_stop)

        assert result.forced is True
        assert result.phase == ShutdownPhase.FORCED
        assert "Stop failed" in result.error

    @pytest.mark.asyncio
    async def test_drain_func_error_forces_shutdown(self):
        """Test error in drain_func forces shutdown."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        async def failing_drain():
            raise RuntimeError("Drain failed")

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=failing_drain,
        )

        assert result.forced is True
        assert result.phase == ShutdownPhase.FORCED

    @pytest.mark.asyncio
    async def test_checkpoint_func_error_forces_shutdown(self):
        """Test error in checkpoint_func forces shutdown."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        async def failing_checkpoint():
            raise RuntimeError("Checkpoint failed")

        result = await coordinator.shutdown(
            stop_func=stop_func,
            checkpoint_func=failing_checkpoint,
        )

        assert result.forced is True
        assert result.phase == ShutdownPhase.FORCED

    @pytest.mark.asyncio
    async def test_close_func_error_does_not_affect_result(self):
        """Test error in close_func doesn't affect shutdown result."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        async def failing_close():
            raise RuntimeError("Close failed")

        result = await coordinator.shutdown(
            stop_func=stop_func,
            close_func=failing_close,
        )

        # Should still be marked as stopped, not forced
        assert result.phase == ShutdownPhase.STOPPED


# Signal Handling Tests


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Signal handling tests not supported on Windows",
)
class TestSignalHandling:
    """Tests for signal handling."""

    @pytest.mark.asyncio
    async def test_register_signals(self):
        """Test signal handlers are registered."""
        coordinator = ShutdownCoordinator()
        loop = asyncio.get_running_loop()

        coordinator.register_signals(loop)

        # Handlers should be registered (we can't easily test this directly)
        assert coordinator._signal_handlers_registered is True

        # Cleanup
        coordinator.unregister_signals(loop)

    @pytest.mark.asyncio
    async def test_unregister_signals(self):
        """Test signal handlers are unregistered."""
        coordinator = ShutdownCoordinator()
        loop = asyncio.get_running_loop()

        coordinator.register_signals(loop)
        coordinator.unregister_signals(loop)

        assert coordinator._signal_handlers_registered is False

    @pytest.mark.asyncio
    async def test_signal_triggers_shutdown(self):
        """Test signal triggers shutdown sequence."""
        coordinator = ShutdownCoordinator()

        # Simulate signal handling
        await coordinator._handle_signal(signal.SIGTERM)

        assert coordinator.is_shutting_down is True

    @pytest.mark.asyncio
    async def test_double_signal_forces_shutdown(self):
        """Test second signal forces immediate shutdown."""
        coordinator = ShutdownCoordinator()

        # First signal initiates graceful shutdown
        await coordinator._handle_signal(signal.SIGTERM)
        assert coordinator.is_shutting_down is True
        assert coordinator.phase == ShutdownPhase.RUNNING

        # Second signal forces shutdown
        await coordinator._handle_signal(signal.SIGINT)
        assert coordinator.phase == ShutdownPhase.FORCED

    @pytest.mark.asyncio
    async def test_signal_invokes_callbacks(self):
        """Test signal invokes registered callbacks."""
        coordinator = ShutdownCoordinator()
        callback = AsyncMock()
        coordinator.on_shutdown(callback)

        await coordinator._handle_signal(signal.SIGTERM)

        callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_callback_error_does_not_stop_shutdown(self):
        """Test callback error doesn't prevent shutdown."""
        coordinator = ShutdownCoordinator()
        failing_callback = AsyncMock(side_effect=RuntimeError("Callback failed"))
        coordinator.on_shutdown(failing_callback)

        await coordinator._handle_signal(signal.SIGTERM)

        # Shutdown should still proceed
        assert coordinator.is_shutting_down is True

    @pytest.mark.asyncio
    async def test_register_signals_twice_warns(self):
        """Test registering signals twice logs warning."""
        coordinator = ShutdownCoordinator()
        loop = asyncio.get_running_loop()

        coordinator.register_signals(loop)
        coordinator.register_signals(loop)  # Should warn, not error

        assert coordinator._signal_handlers_registered is True

        # Cleanup
        coordinator.unregister_signals(loop)


# Reset Tests


class TestReset:
    """Tests for coordinator reset."""

    def test_reset_clears_state(self):
        """Test reset clears all state."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()

        coordinator.reset()

        assert coordinator.is_shutting_down is False
        assert coordinator.phase == ShutdownPhase.RUNNING

    def test_reset_clears_callbacks(self):
        """Test reset clears callbacks."""
        coordinator = ShutdownCoordinator()
        callback = AsyncMock()
        coordinator.on_shutdown(callback)

        coordinator.reset()

        assert len(coordinator._on_shutdown_callbacks) == 0

    @pytest.mark.asyncio
    async def test_reset_allows_reuse(self):
        """Test coordinator can be reused after reset."""
        coordinator = ShutdownCoordinator()

        # First shutdown
        stop_func = AsyncMock()
        await coordinator.shutdown(stop_func=stop_func)
        assert coordinator.phase == ShutdownPhase.STOPPED

        # Reset
        coordinator.reset()
        assert coordinator.phase == ShutdownPhase.RUNNING

        # Second shutdown
        stop_func.reset_mock()
        result = await coordinator.shutdown(stop_func=stop_func)
        stop_func.assert_called_once()
        assert result.phase == ShutdownPhase.STOPPED


# Wait For Shutdown Tests


class TestWaitForShutdown:
    """Tests for wait_for_shutdown."""

    @pytest.mark.asyncio
    async def test_wait_blocks_until_shutdown(self):
        """Test wait_for_shutdown blocks until shutdown requested."""
        coordinator = ShutdownCoordinator()
        wait_completed = False

        async def wait_task():
            nonlocal wait_completed
            await coordinator.wait_for_shutdown()
            wait_completed = True

        # Start waiting
        task = asyncio.create_task(wait_task())

        # Small delay to ensure task is waiting
        await asyncio.sleep(0.01)
        assert wait_completed is False

        # Request shutdown
        coordinator.request_shutdown()

        # Wait should complete
        await asyncio.wait_for(task, timeout=1.0)
        assert wait_completed is True

    @pytest.mark.asyncio
    async def test_wait_returns_immediately_if_already_shutdown(self):
        """Test wait returns immediately if already shutting down."""
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()

        # Should return immediately
        await asyncio.wait_for(
            coordinator.wait_for_shutdown(),
            timeout=0.1,
        )


# Integration Tests


class TestIntegration:
    """Integration tests for shutdown coordinator."""

    @pytest.mark.asyncio
    async def test_full_shutdown_sequence(self):
        """Test complete shutdown sequence."""
        coordinator = ShutdownCoordinator(
            timeout=30.0,
            drain_timeout=10.0,
            checkpoint_timeout=5.0,
        )

        stop_called = False
        drain_called = False
        checkpoint_called = False
        close_called = False

        async def stop():
            nonlocal stop_called
            stop_called = True

        async def drain():
            nonlocal drain_called
            drain_called = True
            return 50

        async def checkpoint():
            nonlocal checkpoint_called
            checkpoint_called = True
            return 3

        async def close():
            nonlocal close_called
            close_called = True

        result = await coordinator.shutdown(
            stop_func=stop,
            drain_func=drain,
            checkpoint_func=checkpoint,
            close_func=close,
        )

        assert stop_called
        assert drain_called
        assert checkpoint_called
        assert close_called
        assert result.phase == ShutdownPhase.STOPPED
        assert result.events_drained == 50
        assert result.checkpoints_saved == 3
        assert result.forced is False

    @pytest.mark.asyncio
    async def test_partial_shutdown_on_error(self):
        """Test shutdown continues on partial failure."""
        coordinator = ShutdownCoordinator()

        stop_called = False
        checkpoint_called = False

        async def stop():
            nonlocal stop_called
            stop_called = True

        async def failing_drain():
            raise RuntimeError("Drain failed")

        async def checkpoint():
            nonlocal checkpoint_called
            checkpoint_called = True
            return 1

        result = await coordinator.shutdown(
            stop_func=stop,
            drain_func=failing_drain,
            checkpoint_func=checkpoint,
        )

        assert stop_called
        # Checkpoint should NOT be called because drain failed with exception
        assert checkpoint_called is False
        assert result.forced is True


# Module Import Tests


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_shutdown_module(self):
        """Test imports from shutdown module."""
        from eventsource.subscriptions.shutdown import (
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
        from eventsource.subscriptions import (
            ShutdownCoordinator,
            ShutdownPhase,
            ShutdownResult,
        )

        assert ShutdownCoordinator is not None
        assert ShutdownPhase is not None
        assert ShutdownResult is not None

    def test_shutdown_reason_exported(self):
        """Test ShutdownReason is exported in __all__."""
        from eventsource.subscriptions.shutdown import __all__

        assert "ShutdownReason" in __all__


# Periodic Checkpoint Tests


class TestPeriodicCheckpointDuringDrain:
    """Tests for periodic checkpoint saves during drain phase."""

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_during_drain(self):
        """Test checkpoints are saved periodically during drain."""
        coordinator = ShutdownCoordinator(
            drain_timeout=1.0,
            checkpoint_interval=0.1,  # Fast interval for testing
        )
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=2)

        # Drain takes some time
        async def slow_drain():
            await asyncio.sleep(0.35)  # Should allow 3 periodic saves
            return 10

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
            checkpoint_func=checkpoint_func,
        )

        # Periodic checkpoints during drain + final checkpoint
        # At least 3 periodic saves during 0.35s with 0.1s interval
        assert checkpoint_func.call_count >= 3
        assert result.phase == ShutdownPhase.STOPPED
        assert result.events_drained == 10

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_disabled_when_zero(self):
        """Test periodic checkpoints disabled with interval=0."""
        coordinator = ShutdownCoordinator(
            drain_timeout=1.0,
            checkpoint_interval=0,  # Disabled
        )
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=1)

        async def slow_drain():
            await asyncio.sleep(0.1)
            return 5

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
            checkpoint_func=checkpoint_func,
        )

        # Only final checkpoint, no periodic during drain
        assert checkpoint_func.call_count == 1  # Just final checkpoint
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_not_started_without_checkpoint_func(self):
        """Test periodic checkpoints not started if no checkpoint_func."""
        coordinator = ShutdownCoordinator(
            drain_timeout=1.0,
            checkpoint_interval=0.1,
        )
        stop_func = AsyncMock()

        async def slow_drain():
            await asyncio.sleep(0.15)
            return 3

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
            # No checkpoint_func
        )

        # No crash, drain completes
        assert result.phase == ShutdownPhase.STOPPED
        assert result.events_drained == 3
        assert coordinator._periodic_checkpoint_task is None

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_stops_when_drain_completes(self):
        """Test periodic checkpoint task is cancelled when drain completes."""
        coordinator = ShutdownCoordinator(
            drain_timeout=5.0,
            checkpoint_interval=0.05,
        )
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=1)

        async def quick_drain():
            await asyncio.sleep(0.15)
            return 2

        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=quick_drain,
            checkpoint_func=checkpoint_func,
        )

        # Task should be cleaned up
        assert coordinator._periodic_checkpoint_task is None

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_error_does_not_crash(self):
        """Test errors in periodic checkpoint don't crash the loop."""
        coordinator = ShutdownCoordinator(
            drain_timeout=2.0,
            checkpoint_interval=0.05,
        )
        stop_func = AsyncMock()

        call_count = 0

        async def failing_checkpoint():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Checkpoint failed")
            return 1

        async def slow_drain():
            # Enough time for: 0.05 (interval) + error + 0.5 (retry delay) + 0.05 (interval) + success
            await asyncio.sleep(0.7)
            return 5

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
            checkpoint_func=failing_checkpoint,
        )

        # Should have retried after errors: 1 failed + at least 1 success during drain + final
        assert call_count >= 2
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_with_drain_timeout(self):
        """Test periodic checkpoints stop when drain times out."""
        coordinator = ShutdownCoordinator(
            drain_timeout=0.2,
            checkpoint_interval=0.05,
        )
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=1)

        async def very_slow_drain():
            await asyncio.sleep(10.0)  # Will timeout
            return 100

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=very_slow_drain,
            checkpoint_func=checkpoint_func,
        )

        # Drain timed out, but periodic checkpoints should have run
        assert result.forced is True
        # Some periodic checkpoints should have been saved before timeout
        assert checkpoint_func.call_count >= 1
        assert coordinator._periodic_checkpoint_task is None

    @pytest.mark.asyncio
    async def test_final_checkpoint_after_drain_with_periodic(self):
        """Test final checkpoint is still saved after periodic checkpoints during drain."""
        coordinator = ShutdownCoordinator(
            drain_timeout=1.0,
            checkpoint_interval=0.05,
        )
        stop_func = AsyncMock()
        checkpoint_call_times = []

        async def checkpoint_with_timing():
            checkpoint_call_times.append(asyncio.get_event_loop().time())
            return 1

        async def slow_drain():
            await asyncio.sleep(0.15)
            return 3

        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
            checkpoint_func=checkpoint_with_timing,
        )

        # Should have periodic checkpoints during drain AND final checkpoint after
        assert len(checkpoint_call_times) >= 3  # At least 2 periodic + 1 final

    @pytest.mark.asyncio
    async def test_reset_clears_periodic_checkpoint_state(self):
        """Test reset clears periodic checkpoint state."""
        coordinator = ShutdownCoordinator(
            checkpoint_interval=0.1,
        )

        # Simulate some state
        coordinator._periodic_checkpoints_saved = 5

        coordinator.reset()

        assert coordinator._periodic_checkpoint_task is None
        assert coordinator._periodic_checkpoints_saved == 0

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_task_has_name(self):
        """Test periodic checkpoint task has descriptive name."""
        coordinator = ShutdownCoordinator(
            drain_timeout=1.0,
            checkpoint_interval=0.1,
        )
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=1)

        task_name = None

        async def drain_that_captures_task():
            nonlocal task_name
            await asyncio.sleep(0.05)
            if coordinator._periodic_checkpoint_task:
                task_name = coordinator._periodic_checkpoint_task.get_name()
            return 1

        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_that_captures_task,
            checkpoint_func=checkpoint_func,
        )

        assert task_name == "periodic-checkpoint-loop"

    @pytest.mark.asyncio
    async def test_negative_checkpoint_interval_disables_periodic(self):
        """Test negative checkpoint_interval disables periodic checkpoints."""
        coordinator = ShutdownCoordinator(
            drain_timeout=1.0,
            checkpoint_interval=-1.0,  # Negative value
        )
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=1)

        async def slow_drain():
            await asyncio.sleep(0.1)
            return 5

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
            checkpoint_func=checkpoint_func,
        )

        # Only final checkpoint, no periodic during drain
        assert checkpoint_func.call_count == 1
        assert result.phase == ShutdownPhase.STOPPED


# Shutdown Metrics Snapshot Tests


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


# Shutdown Metrics Recording Functions Tests


class TestShutdownMetricsFunctions:
    """Tests for shutdown metrics recording functions."""

    def setup_method(self):
        """Reset metrics state before each test."""
        reset_shutdown_metrics()

    def teardown_method(self):
        """Reset metrics state after each test."""
        reset_shutdown_metrics()

    def test_record_shutdown_initiated_no_crash(self):
        """Test record_shutdown_initiated doesn't crash."""
        # Should not raise even without OpenTelemetry configured
        record_shutdown_initiated()

    def test_record_shutdown_completed_no_crash(self):
        """Test record_shutdown_completed doesn't crash."""
        # Should not raise even without OpenTelemetry configured
        record_shutdown_completed("clean", 5.0)
        record_shutdown_completed("forced", 10.0)
        record_shutdown_completed("timeout", 30.0)

    def test_record_drain_duration_no_crash(self):
        """Test record_drain_duration doesn't crash."""
        record_drain_duration(2.5)

    def test_record_events_drained_no_crash(self):
        """Test record_events_drained doesn't crash."""
        record_events_drained(100)
        record_events_drained(0)  # Zero should be safe too

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


# Shutdown Coordinator Metrics Integration Tests


class TestShutdownCoordinatorMetrics:
    """Tests for metrics integration in ShutdownCoordinator."""

    def setup_method(self):
        """Reset metrics state before each test."""
        reset_shutdown_metrics()

    def teardown_method(self):
        """Reset metrics state after each test."""
        reset_shutdown_metrics()

    @pytest.mark.asyncio
    async def test_shutdown_creates_metrics_snapshot(self):
        """Test shutdown creates a metrics snapshot."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        await coordinator.shutdown(stop_func=stop_func)

        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "clean"
        assert coordinator.last_metrics_snapshot.shutdown_duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_clean_shutdown_outcome(self):
        """Test clean shutdown has correct outcome."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        result = await coordinator.shutdown(stop_func=stop_func)

        assert result.phase == ShutdownPhase.STOPPED
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "clean"

    @pytest.mark.asyncio
    async def test_timeout_shutdown_outcome(self):
        """Test timeout shutdown has correct outcome."""
        coordinator = ShutdownCoordinator(drain_timeout=0.1)
        stop_func = AsyncMock()

        async def slow_drain():
            await asyncio.sleep(10.0)
            return 0

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
        )

        assert result.forced is True
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "timeout"

    @pytest.mark.asyncio
    async def test_forced_shutdown_outcome(self):
        """Test forced shutdown (double signal) has correct outcome."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        # Set up a slow drain so we can trigger double signal during shutdown
        async def slow_drain():
            # First signal to initiate
            await coordinator._handle_signal(signal.SIGTERM)
            # Second signal forces
            await coordinator._handle_signal(signal.SIGINT)
            await asyncio.sleep(1.0)
            return 0

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=slow_drain,
        )

        assert result.forced is True
        assert "double signal" in result.error.lower()
        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.outcome == "forced"

    @pytest.mark.asyncio
    async def test_drain_duration_recorded(self):
        """Test drain duration is recorded in metrics."""
        coordinator = ShutdownCoordinator(drain_timeout=5.0)
        stop_func = AsyncMock()

        async def timed_drain():
            await asyncio.sleep(0.1)
            return 10

        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=timed_drain,
        )

        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.drain_duration_seconds >= 0.1

    @pytest.mark.asyncio
    async def test_events_drained_recorded(self):
        """Test events drained count is recorded in metrics."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()
        drain_func = AsyncMock(return_value=50)

        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
        )

        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.events_drained == 50

    @pytest.mark.asyncio
    async def test_checkpoints_saved_recorded(self):
        """Test checkpoints saved count is recorded in metrics."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=5)

        await coordinator.shutdown(
            stop_func=stop_func,
            checkpoint_func=checkpoint_func,
        )

        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.checkpoints_saved == 5

    @pytest.mark.asyncio
    async def test_set_in_flight_count(self):
        """Test set_in_flight_count records value."""
        coordinator = ShutdownCoordinator()
        coordinator.set_in_flight_count(150)

        stop_func = AsyncMock()
        await coordinator.shutdown(stop_func=stop_func)

        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.in_flight_at_start == 150
        assert get_in_flight_at_shutdown() == 150

    @pytest.mark.asyncio
    async def test_reset_clears_metrics_snapshot(self):
        """Test reset clears the metrics snapshot."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        await coordinator.shutdown(stop_func=stop_func)
        assert coordinator.last_metrics_snapshot is not None

        coordinator.reset()
        assert coordinator.last_metrics_snapshot is None

    @pytest.mark.asyncio
    async def test_reset_clears_in_flight_count(self):
        """Test reset clears the in-flight count."""
        coordinator = ShutdownCoordinator()
        coordinator.set_in_flight_count(100)

        stop_func = AsyncMock()
        await coordinator.shutdown(stop_func=stop_func)

        coordinator.reset()
        assert coordinator._in_flight_at_start == 0

    @pytest.mark.asyncio
    async def test_no_drain_means_zero_drain_duration(self):
        """Test shutdown without drain has zero drain duration."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        await coordinator.shutdown(stop_func=stop_func)

        assert coordinator.last_metrics_snapshot is not None
        assert coordinator.last_metrics_snapshot.drain_duration_seconds == 0.0

    @pytest.mark.asyncio
    async def test_metrics_snapshot_survives_multiple_shutdowns(self):
        """Test metrics snapshot is updated on each shutdown."""
        coordinator = ShutdownCoordinator()
        stop_func = AsyncMock()

        # First shutdown
        await coordinator.shutdown(stop_func=stop_func)
        first_snapshot = coordinator.last_metrics_snapshot

        # Reset and second shutdown
        coordinator.reset()
        await coordinator.shutdown(stop_func=stop_func)
        second_snapshot = coordinator.last_metrics_snapshot

        assert first_snapshot is not None
        assert second_snapshot is not None
        assert first_snapshot is not second_snapshot


# Module Import Tests for Metrics


class TestMetricsModuleImports:
    """Tests for metrics module imports."""

    def test_import_metrics_snapshot(self):
        """Test ShutdownMetricsSnapshot import."""
        from eventsource.subscriptions.shutdown import ShutdownMetricsSnapshot

        assert ShutdownMetricsSnapshot is not None

    def test_import_metrics_functions(self):
        """Test metrics function imports."""
        from eventsource.subscriptions.shutdown import (
            get_in_flight_at_shutdown,
            record_drain_duration,
            record_events_drained,
            record_in_flight_at_shutdown,
            record_shutdown_completed,
            record_shutdown_initiated,
            reset_shutdown_metrics,
        )

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


# Pre-Shutdown Hooks Tests


class TestPreShutdownHooks:
    """Tests for pre-shutdown hook functionality."""

    def test_register_hook(self):
        """Test registering a pre-shutdown hook."""
        coordinator = ShutdownCoordinator()

        async def hook():
            pass

        coordinator.on_pre_shutdown(hook, timeout=5.0)
        assert len(coordinator._pre_shutdown_hooks) == 1

    def test_register_hook_default_timeout(self):
        """Test registering a hook with default timeout."""
        coordinator = ShutdownCoordinator()

        async def hook():
            pass

        coordinator.on_pre_shutdown(hook)
        assert len(coordinator._pre_shutdown_hooks) == 1
        # Default timeout is 5.0
        assert coordinator._pre_shutdown_hooks[0][1] == 5.0

    def test_register_hook_validates_async(self):
        """Test that non-async hooks are rejected."""
        coordinator = ShutdownCoordinator()

        def sync_hook():
            pass

        with pytest.raises(TypeError, match="must be async function"):
            coordinator.on_pre_shutdown(sync_hook)  # type: ignore[arg-type]

    def test_register_hook_validates_timeout_zero(self):
        """Test that zero timeout is rejected."""
        coordinator = ShutdownCoordinator()

        async def hook():
            pass

        with pytest.raises(ValueError, match="must be positive"):
            coordinator.on_pre_shutdown(hook, timeout=0)

    def test_register_hook_validates_timeout_negative(self):
        """Test that negative timeout is rejected."""
        coordinator = ShutdownCoordinator()

        async def hook():
            pass

        with pytest.raises(ValueError, match="must be positive"):
            coordinator.on_pre_shutdown(hook, timeout=-1)

    def test_register_multiple_hooks(self):
        """Test registering multiple hooks."""
        coordinator = ShutdownCoordinator()

        async def hook1():
            pass

        async def hook2():
            pass

        async def hook3():
            pass

        coordinator.on_pre_shutdown(hook1, timeout=5.0)
        coordinator.on_pre_shutdown(hook2, timeout=10.0)
        coordinator.on_pre_shutdown(hook3, timeout=2.0)

        assert len(coordinator._pre_shutdown_hooks) == 3
        assert coordinator._pre_shutdown_hooks[0] == (hook1, 5.0)
        assert coordinator._pre_shutdown_hooks[1] == (hook2, 10.0)
        assert coordinator._pre_shutdown_hooks[2] == (hook3, 2.0)

    @pytest.mark.asyncio
    async def test_hooks_execute_in_order(self):
        """Test hooks execute in registration order."""
        coordinator = ShutdownCoordinator()
        execution_order = []

        async def hook1():
            execution_order.append(1)

        async def hook2():
            execution_order.append(2)

        async def hook3():
            execution_order.append(3)

        coordinator.on_pre_shutdown(hook1)
        coordinator.on_pre_shutdown(hook2)
        coordinator.on_pre_shutdown(hook3)

        await coordinator._execute_pre_shutdown_hooks()

        assert execution_order == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_hook_timeout(self):
        """Test hook timeout is enforced."""
        coordinator = ShutdownCoordinator()
        completed = False

        async def slow_hook():
            nonlocal completed
            await asyncio.sleep(10.0)
            completed = True

        coordinator.on_pre_shutdown(slow_hook, timeout=0.1)

        start = asyncio.get_event_loop().time()
        await coordinator._execute_pre_shutdown_hooks()
        duration = asyncio.get_event_loop().time() - start

        assert not completed
        assert duration < 0.5  # Much less than 10s

    @pytest.mark.asyncio
    async def test_hook_exception_continues_execution(self):
        """Test that hook exception doesn't stop other hooks."""
        coordinator = ShutdownCoordinator()
        hook3_executed = False

        async def failing_hook():
            raise RuntimeError("Hook failed")

        async def success_hook():
            nonlocal hook3_executed
            hook3_executed = True

        coordinator.on_pre_shutdown(failing_hook)
        coordinator.on_pre_shutdown(success_hook)

        await coordinator._execute_pre_shutdown_hooks()

        assert hook3_executed

    @pytest.mark.asyncio
    async def test_hooks_execute_before_stopping_phase(self):
        """Test hooks run before STOPPING phase."""
        coordinator = ShutdownCoordinator()
        phase_at_hook_execution = None

        async def capture_phase():
            nonlocal phase_at_hook_execution
            phase_at_hook_execution = coordinator.phase

        coordinator.on_pre_shutdown(capture_phase)

        async def stop_func():
            pass

        await coordinator.shutdown(stop_func=stop_func)

        assert phase_at_hook_execution == ShutdownPhase.RUNNING

    def test_reset_clears_hooks(self):
        """Test reset clears registered hooks."""
        coordinator = ShutdownCoordinator()

        async def hook():
            pass

        coordinator.on_pre_shutdown(hook)
        assert len(coordinator._pre_shutdown_hooks) == 1

        coordinator.reset()
        assert len(coordinator._pre_shutdown_hooks) == 0

    @pytest.mark.asyncio
    async def test_no_hooks_execution_is_safe(self):
        """Test executing with no hooks is safe."""
        coordinator = ShutdownCoordinator()

        # Should not raise
        await coordinator._execute_pre_shutdown_hooks()

    @pytest.mark.asyncio
    async def test_hooks_run_during_shutdown_sequence(self):
        """Test hooks are called as part of shutdown() sequence."""
        coordinator = ShutdownCoordinator()
        hook_executed = False

        async def pre_hook():
            nonlocal hook_executed
            hook_executed = True

        coordinator.on_pre_shutdown(pre_hook)

        async def stop_func():
            pass

        await coordinator.shutdown(stop_func=stop_func)

        assert hook_executed

    @pytest.mark.asyncio
    async def test_hook_failure_does_not_prevent_shutdown(self):
        """Test that hook failure does not prevent shutdown from proceeding."""
        coordinator = ShutdownCoordinator()
        stop_called = False

        async def failing_hook():
            raise RuntimeError("Hook exploded")

        async def stop_func():
            nonlocal stop_called
            stop_called = True

        coordinator.on_pre_shutdown(failing_hook)
        result = await coordinator.shutdown(stop_func=stop_func)

        assert stop_called
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_hook_timeout_does_not_prevent_shutdown(self):
        """Test that hook timeout does not prevent shutdown from proceeding."""
        coordinator = ShutdownCoordinator()
        stop_called = False

        async def slow_hook():
            await asyncio.sleep(10.0)

        async def stop_func():
            nonlocal stop_called
            stop_called = True

        coordinator.on_pre_shutdown(slow_hook, timeout=0.1)
        result = await coordinator.shutdown(stop_func=stop_func)

        assert stop_called
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_multiple_hooks_with_different_timeouts(self):
        """Test multiple hooks with different individual timeouts."""
        coordinator = ShutdownCoordinator()
        results = []

        async def fast_hook():
            results.append("fast")

        async def slow_hook():
            await asyncio.sleep(10.0)
            results.append("slow")

        async def medium_hook():
            await asyncio.sleep(0.05)
            results.append("medium")

        coordinator.on_pre_shutdown(fast_hook, timeout=1.0)
        coordinator.on_pre_shutdown(slow_hook, timeout=0.1)  # Will timeout
        coordinator.on_pre_shutdown(medium_hook, timeout=1.0)

        await coordinator._execute_pre_shutdown_hooks()

        assert "fast" in results
        assert "slow" not in results  # Timed out
        assert "medium" in results

    @pytest.mark.asyncio
    async def test_hooks_execute_before_drain_and_checkpoint(self):
        """Test hooks execute before drain and checkpoint phases."""
        coordinator = ShutdownCoordinator()
        hook_time = None
        drain_time = None
        checkpoint_time = None

        async def pre_hook():
            nonlocal hook_time
            hook_time = asyncio.get_event_loop().time()

        async def stop_func():
            pass

        async def drain_func():
            nonlocal drain_time
            drain_time = asyncio.get_event_loop().time()
            return 0

        async def checkpoint_func():
            nonlocal checkpoint_time
            checkpoint_time = asyncio.get_event_loop().time()
            return 0

        coordinator.on_pre_shutdown(pre_hook)
        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
            checkpoint_func=checkpoint_func,
        )

        assert hook_time is not None
        assert drain_time is not None
        assert checkpoint_time is not None
        assert hook_time < drain_time
        assert drain_time < checkpoint_time


# Pre-Shutdown Hooks Import Tests


class TestPreShutdownHookImports:
    """Tests for pre-shutdown hook type imports."""

    def test_import_pre_shutdown_hook_type(self):
        """Test PreShutdownHook type alias import."""
        from eventsource.subscriptions.shutdown import PreShutdownHook

        assert PreShutdownHook is not None

    def test_pre_shutdown_hook_in_all(self):
        """Test PreShutdownHook is in __all__."""
        from eventsource.subscriptions.shutdown import __all__

        assert "PreShutdownHook" in __all__


# Post-Shutdown Hooks Tests


class TestPostShutdownHooks:
    """Tests for post-shutdown hook functionality."""

    def test_register_hook(self):
        """Test registering a post-shutdown hook."""
        coordinator = ShutdownCoordinator()

        async def hook(result: ShutdownResult):
            pass

        coordinator.on_post_shutdown(hook)
        assert len(coordinator._post_shutdown_hooks) == 1

    def test_register_hook_validates_async(self):
        """Test that non-async hooks are rejected."""
        coordinator = ShutdownCoordinator()

        def sync_hook(result):
            pass

        with pytest.raises(TypeError):
            coordinator.on_post_shutdown(sync_hook)

    def test_register_multiple_hooks(self):
        """Test registering multiple hooks."""
        coordinator = ShutdownCoordinator()

        async def hook1(result):
            pass

        async def hook2(result):
            pass

        coordinator.on_post_shutdown(hook1)
        coordinator.on_post_shutdown(hook2)
        assert len(coordinator._post_shutdown_hooks) == 2

    @pytest.mark.asyncio
    async def test_hooks_receive_result(self):
        """Test hooks receive ShutdownResult."""
        coordinator = ShutdownCoordinator()
        received_result = None

        async def capture_result(result: ShutdownResult):
            nonlocal received_result
            received_result = result

        coordinator.on_post_shutdown(capture_result)

        test_result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        await coordinator._execute_post_shutdown_hooks(test_result)

        assert received_result is test_result

    @pytest.mark.asyncio
    async def test_hooks_execute_in_order(self):
        """Test hooks execute in registration order."""
        coordinator = ShutdownCoordinator()
        execution_order = []

        async def hook1(result):
            execution_order.append(1)

        async def hook2(result):
            execution_order.append(2)

        coordinator.on_post_shutdown(hook1)
        coordinator.on_post_shutdown(hook2)

        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        await coordinator._execute_post_shutdown_hooks(result)

        assert execution_order == [1, 2]

    @pytest.mark.asyncio
    async def test_hook_exception_continues_execution(self):
        """Test that hook exception doesn't stop other hooks."""
        coordinator = ShutdownCoordinator()
        hook2_executed = False

        async def failing_hook(result):
            raise RuntimeError("Hook failed")

        async def success_hook(result):
            nonlocal hook2_executed
            hook2_executed = True

        coordinator.on_post_shutdown(failing_hook)
        coordinator.on_post_shutdown(success_hook)

        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        await coordinator._execute_post_shutdown_hooks(result)

        assert hook2_executed

    @pytest.mark.asyncio
    async def test_hook_timeout_continues_execution(self):
        """Test that hook timeout doesn't stop other hooks."""
        coordinator = ShutdownCoordinator()
        hook2_executed = False

        async def slow_hook(result):
            await asyncio.sleep(10.0)  # Will timeout (5s limit)

        async def success_hook(result):
            nonlocal hook2_executed
            hook2_executed = True

        coordinator.on_post_shutdown(slow_hook)
        coordinator.on_post_shutdown(success_hook)

        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        await coordinator._execute_post_shutdown_hooks(result)

        assert hook2_executed

    @pytest.mark.asyncio
    async def test_hooks_execute_after_forced_shutdown(self):
        """Test hooks run even after forced shutdown."""
        coordinator = ShutdownCoordinator()
        hook_executed = False
        received_forced = None

        async def check_forced(result: ShutdownResult):
            nonlocal hook_executed, received_forced
            hook_executed = True
            received_forced = result.forced

        coordinator.on_post_shutdown(check_forced)

        result = ShutdownResult(
            phase=ShutdownPhase.FORCED,
            duration_seconds=10.0,
            subscriptions_stopped=1,
            events_drained=0,
            forced=True,
        )

        await coordinator._execute_post_shutdown_hooks(result)

        assert hook_executed
        assert received_forced is True

    @pytest.mark.asyncio
    async def test_hooks_execute_via_shutdown(self):
        """Test hooks are called from shutdown() method."""
        coordinator = ShutdownCoordinator()
        hook_called = False
        received_result = None

        async def track_call(result: ShutdownResult):
            nonlocal hook_called, received_result
            hook_called = True
            received_result = result

        coordinator.on_post_shutdown(track_call)

        async def stop_func():
            pass

        result = await coordinator.shutdown(stop_func=stop_func)

        assert hook_called
        assert received_result is result

    @pytest.mark.asyncio
    async def test_hooks_receive_full_result_info(self):
        """Test hooks receive complete ShutdownResult info."""
        coordinator = ShutdownCoordinator()
        received_result = None

        async def capture_result(result: ShutdownResult):
            nonlocal received_result
            received_result = result

        coordinator.on_post_shutdown(capture_result)

        async def stop_func():
            pass

        async def drain_func():
            return 42

        async def checkpoint_func():
            return 5

        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
            checkpoint_func=checkpoint_func,
        )

        assert received_result is not None
        assert received_result.events_drained == 42
        assert received_result.checkpoints_saved == 5
        assert received_result.phase == ShutdownPhase.STOPPED

    def test_reset_clears_hooks(self):
        """Test reset clears registered hooks."""
        coordinator = ShutdownCoordinator()

        async def hook(result):
            pass

        coordinator.on_post_shutdown(hook)
        assert len(coordinator._post_shutdown_hooks) == 1

        coordinator.reset()
        assert len(coordinator._post_shutdown_hooks) == 0

    @pytest.mark.asyncio
    async def test_no_hooks_execution_is_safe(self):
        """Test that executing with no hooks registered is safe."""
        coordinator = ShutdownCoordinator()

        result = ShutdownResult(
            phase=ShutdownPhase.STOPPED,
            duration_seconds=5.0,
            subscriptions_stopped=1,
            events_drained=10,
        )

        # Should not raise
        await coordinator._execute_post_shutdown_hooks(result)

    @pytest.mark.asyncio
    async def test_hooks_execute_after_all_phases(self):
        """Test hooks execute after all shutdown phases complete."""
        coordinator = ShutdownCoordinator()
        phase_order = []

        async def stop_func():
            phase_order.append("stop")

        async def drain_func():
            phase_order.append("drain")
            return 0

        async def checkpoint_func():
            phase_order.append("checkpoint")
            return 0

        async def close_func():
            phase_order.append("close")

        async def post_hook(result):
            phase_order.append("post_hook")

        coordinator.on_post_shutdown(post_hook)

        await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
            checkpoint_func=checkpoint_func,
            close_func=close_func,
        )

        # Post-shutdown hooks should be last
        assert phase_order == ["stop", "drain", "checkpoint", "close", "post_hook"]

    @pytest.mark.asyncio
    async def test_hooks_can_access_result_reason(self):
        """Test hooks can access ShutdownResult.reason."""
        coordinator = ShutdownCoordinator()
        received_reason = None

        async def check_reason(result: ShutdownResult):
            nonlocal received_reason
            received_reason = result.reason

        coordinator.on_post_shutdown(check_reason)
        coordinator.request_shutdown(ShutdownReason.PROGRAMMATIC)

        async def stop_func():
            pass

        await coordinator.shutdown(stop_func=stop_func)

        assert received_reason == ShutdownReason.PROGRAMMATIC

    @pytest.mark.asyncio
    async def test_hooks_execute_even_when_shutdown_errors(self):
        """Test hooks execute even when shutdown has errors."""
        coordinator = ShutdownCoordinator()
        hook_executed = False

        async def track_hook(result: ShutdownResult):
            nonlocal hook_executed
            hook_executed = True

        coordinator.on_post_shutdown(track_hook)

        async def failing_stop():
            raise RuntimeError("Stop failed")

        result = await coordinator.shutdown(stop_func=failing_stop)

        assert hook_executed
        assert result.forced
        assert result.error is not None


# Post-Shutdown Hooks Import Tests


class TestPostShutdownHookImports:
    """Tests for post-shutdown hook type imports."""

    def test_import_post_shutdown_hook_type(self):
        """Test PostShutdownHook type alias import."""
        from eventsource.subscriptions.shutdown import PostShutdownHook

        assert PostShutdownHook is not None

    def test_post_shutdown_hook_in_all(self):
        """Test PostShutdownHook is in __all__."""
        from eventsource.subscriptions.shutdown import __all__

        assert "PostShutdownHook" in __all__


# Shutdown Deadline Tests


class TestShutdownDeadline:
    """Tests for shutdown deadline functionality."""

    def test_set_deadline(self):
        """Test setting shutdown deadline."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) + timedelta(seconds=30)

        coordinator.set_shutdown_deadline(deadline)

        assert coordinator.deadline == deadline

    def test_get_remaining_time_with_deadline(self):
        """Test remaining time calculation with deadline."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) + timedelta(seconds=10)
        coordinator.set_shutdown_deadline(deadline)

        remaining = coordinator.get_remaining_shutdown_time()

        assert 9 < remaining <= 10

    def test_get_remaining_time_without_deadline(self):
        """Test remaining time returns configured timeout when no deadline."""
        coordinator = ShutdownCoordinator(timeout=30.0)

        remaining = coordinator.get_remaining_shutdown_time()

        assert remaining == 30.0

    def test_get_remaining_time_past_deadline(self):
        """Test remaining time returns 0 when deadline passed."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) - timedelta(seconds=10)
        coordinator.set_shutdown_deadline(deadline)

        remaining = coordinator.get_remaining_shutdown_time()

        assert remaining == 0.0

    def test_adjusted_timeouts_plenty_of_time(self):
        """Test timeouts unchanged when plenty of time."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator(drain_timeout=10.0, checkpoint_timeout=5.0)
        # Total configured: 5 + 10 + 5 = 20 seconds
        deadline = datetime.now(UTC) + timedelta(seconds=60)
        coordinator.set_shutdown_deadline(deadline)

        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()

        assert stop == 5.0
        assert drain == 10.0
        assert checkpoint == 5.0

    def test_adjusted_timeouts_limited_time(self):
        """Test timeouts scale down with limited time."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator(drain_timeout=10.0, checkpoint_timeout=5.0)
        deadline = datetime.now(UTC) + timedelta(seconds=10)  # Half of total
        coordinator.set_shutdown_deadline(deadline)

        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()

        assert stop + drain + checkpoint <= 10.0

    def test_adjusted_timeouts_critical_time(self):
        """Test drain skipped when time is critical."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) + timedelta(seconds=2)  # Very short
        coordinator.set_shutdown_deadline(deadline)

        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()

        assert drain == 0.0  # Drain should be skipped
        assert checkpoint > 0  # Checkpoint should still have time

    def test_adjusted_timeouts_zero_remaining(self):
        """Test all timeouts are zero when no time remaining."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) - timedelta(seconds=10)  # Past deadline
        coordinator.set_shutdown_deadline(deadline)

        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()

        assert stop == 0.0
        assert drain == 0.0
        assert checkpoint == 0.0

    def test_reset_clears_deadline(self):
        """Test reset clears the deadline."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator()
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=30))

        coordinator.reset()

        assert coordinator.deadline is None

    def test_deadline_initially_none(self):
        """Test deadline is None when not set."""
        coordinator = ShutdownCoordinator()
        assert coordinator.deadline is None

    def test_set_deadline_naive_datetime_warns(self, caplog):
        """Test setting naive datetime logs warning."""
        import logging
        from datetime import datetime, timedelta

        coordinator = ShutdownCoordinator()
        naive_deadline = datetime.now() + timedelta(seconds=30)

        with caplog.at_level(logging.WARNING):
            coordinator.set_shutdown_deadline(naive_deadline)

        assert "not timezone-aware" in caplog.text

    @pytest.mark.asyncio
    async def test_shutdown_uses_adjusted_timeouts(self):
        """Test shutdown() uses deadline-adjusted timeouts."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator(drain_timeout=10.0)
        deadline = datetime.now(UTC) + timedelta(seconds=5)
        coordinator.set_shutdown_deadline(deadline)

        async def mock_drain():
            await asyncio.sleep(0.1)
            return 0

        async def mock_stop():
            pass

        result = await coordinator.shutdown(
            stop_func=mock_stop,
            drain_func=mock_drain,
        )

        # Should complete quickly with adjusted timeouts
        assert result.duration_seconds < 5.0

    @pytest.mark.asyncio
    async def test_shutdown_skips_drain_when_time_critical(self):
        """Test shutdown skips drain phase when time is critical."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator(drain_timeout=10.0)
        # Very short deadline - should skip drain
        deadline = datetime.now(UTC) + timedelta(seconds=2)
        coordinator.set_shutdown_deadline(deadline)

        drain_called = False

        async def mock_drain():
            nonlocal drain_called
            drain_called = True
            await asyncio.sleep(0.1)
            return 5

        async def mock_stop():
            pass

        async def mock_checkpoint():
            return 1

        result = await coordinator.shutdown(
            stop_func=mock_stop,
            drain_func=mock_drain,
            checkpoint_func=mock_checkpoint,
        )

        # Drain should be skipped due to time constraint
        assert not drain_called
        assert result.events_drained == 0

    @pytest.mark.asyncio
    async def test_shutdown_always_attempts_checkpoint(self):
        """Test shutdown always attempts checkpoint if time remains."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator(drain_timeout=10.0)
        # Short deadline but enough for checkpoint
        deadline = datetime.now(UTC) + timedelta(seconds=3)
        coordinator.set_shutdown_deadline(deadline)

        checkpoint_called = False

        async def mock_stop():
            pass

        async def mock_checkpoint():
            nonlocal checkpoint_called
            checkpoint_called = True
            return 1

        result = await coordinator.shutdown(
            stop_func=mock_stop,
            checkpoint_func=mock_checkpoint,
        )

        assert checkpoint_called
        assert result.checkpoints_saved == 1

    @pytest.mark.asyncio
    async def test_shutdown_no_deadline_uses_defaults(self):
        """Test shutdown uses default timeouts when no deadline set."""
        coordinator = ShutdownCoordinator(
            drain_timeout=10.0,
            checkpoint_timeout=5.0,
        )

        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()

        assert stop == 5.0  # Default stop timeout
        assert drain == 10.0
        assert checkpoint == 5.0

    @pytest.mark.asyncio
    async def test_deadline_with_checkpoint_after_forced_drain(self):
        """Test checkpoint still attempted after drain is forced."""
        from datetime import UTC, datetime, timedelta

        # Set up coordinator with short drain timeout that will timeout
        coordinator = ShutdownCoordinator(drain_timeout=0.1)
        # Deadline with enough time for checkpoint after drain timeout
        deadline = datetime.now(UTC) + timedelta(seconds=10)
        coordinator.set_shutdown_deadline(deadline)

        checkpoint_called = False

        async def mock_stop():
            pass

        async def mock_drain():
            await asyncio.sleep(10.0)  # Will timeout
            return 100

        async def mock_checkpoint():
            nonlocal checkpoint_called
            checkpoint_called = True
            return 2

        result = await coordinator.shutdown(
            stop_func=mock_stop,
            drain_func=mock_drain,
            checkpoint_func=mock_checkpoint,
        )

        # Checkpoint should still be called even after drain timeout
        assert checkpoint_called
        assert result.checkpoints_saved == 2

    @pytest.mark.asyncio
    async def test_shutdown_logs_deadline_adjusted_timeouts(self, caplog):
        """Test shutdown logs when using deadline-adjusted timeouts."""
        import logging
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) + timedelta(seconds=10)
        coordinator.set_shutdown_deadline(deadline)

        async def mock_stop():
            pass

        with caplog.at_level(logging.INFO):
            await coordinator.shutdown(stop_func=mock_stop)

        assert "deadline-adjusted timeouts" in caplog.text.lower()


class TestShutdownDeadlineIntegration:
    """Integration tests for shutdown deadline with cloud scenarios."""

    @pytest.mark.asyncio
    async def test_kubernetes_termination_scenario(self):
        """Test Kubernetes terminationGracePeriodSeconds scenario."""
        from datetime import UTC, datetime, timedelta

        # Kubernetes terminationGracePeriodSeconds: 30
        # Account for preStop hook time (5 seconds buffer)
        coordinator = ShutdownCoordinator(
            drain_timeout=20.0,
            checkpoint_timeout=5.0,
        )
        deadline = datetime.now(UTC) + timedelta(seconds=25)
        coordinator.set_shutdown_deadline(deadline)

        events_processed = 0

        async def mock_stop():
            pass

        async def mock_drain():
            nonlocal events_processed
            await asyncio.sleep(0.1)
            events_processed = 50
            return 50

        async def mock_checkpoint():
            return 3

        result = await coordinator.shutdown(
            stop_func=mock_stop,
            drain_func=mock_drain,
            checkpoint_func=mock_checkpoint,
        )

        assert result.phase == ShutdownPhase.STOPPED
        assert result.events_drained == 50
        assert result.checkpoints_saved == 3
        assert result.forced is False

    @pytest.mark.asyncio
    async def test_aws_spot_termination_scenario(self):
        """Test AWS spot termination 2-minute warning scenario."""
        from datetime import UTC, datetime, timedelta

        # AWS gives 2-minute warning
        coordinator = ShutdownCoordinator(
            drain_timeout=60.0,
            checkpoint_timeout=10.0,
        )
        deadline = datetime.now(UTC) + timedelta(seconds=120)
        coordinator.set_shutdown_deadline(deadline)

        async def mock_stop():
            pass

        async def mock_drain():
            await asyncio.sleep(0.1)
            return 100

        async def mock_checkpoint():
            return 5

        result = await coordinator.shutdown(
            stop_func=mock_stop,
            drain_func=mock_drain,
            checkpoint_func=mock_checkpoint,
        )

        assert result.phase == ShutdownPhase.STOPPED
        assert result.events_drained == 100
        assert result.forced is False

    @pytest.mark.asyncio
    async def test_very_short_deadline_prioritizes_checkpoint(self):
        """Test very short deadline prioritizes checkpoint over drain."""
        from datetime import UTC, datetime, timedelta

        coordinator = ShutdownCoordinator(
            drain_timeout=30.0,
            checkpoint_timeout=10.0,
        )
        # Only 2 seconds - very critical
        deadline = datetime.now(UTC) + timedelta(seconds=2)
        coordinator.set_shutdown_deadline(deadline)

        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()

        # Drain should be skipped
        assert drain == 0.0
        # Checkpoint should get most of the time
        assert checkpoint > stop
