"""Unit tests for signal handling, reset, and wait_for_shutdown."""

import asyncio
import signal
import sys
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
)


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
