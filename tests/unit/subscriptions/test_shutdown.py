"""
Unit tests for graceful shutdown coordination.

Tests for:
- ShutdownPhase enum
- ShutdownResult dataclass
- ShutdownCoordinator class
- Signal handling
- Timeout handling
- Shutdown phases
"""

import asyncio
import signal
import sys
from unittest.mock import AsyncMock

import pytest

from eventsource.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownResult,
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
        )
        d = result.to_dict()

        assert d["phase"] == "stopped"
        assert d["duration_seconds"] == 5.5
        assert d["subscriptions_stopped"] == 3
        assert d["events_drained"] == 100
        assert d["checkpoints_saved"] == 3
        assert d["forced"] is False
        assert d["error"] is None

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
        assert coordinator.phase == ShutdownPhase.RUNNING
        assert coordinator.is_shutting_down is False

    def test_create_with_custom_timeouts(self):
        """Test creating coordinator with custom timeouts."""
        coordinator = ShutdownCoordinator(
            timeout=60.0,
            drain_timeout=20.0,
            checkpoint_timeout=10.0,
        )
        assert coordinator.timeout == 60.0
        assert coordinator.drain_timeout == 20.0
        assert coordinator.checkpoint_timeout == 10.0


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
            ShutdownResult,
        )

        assert ShutdownCoordinator is not None
        assert ShutdownPhase is not None
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
