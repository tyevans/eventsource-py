"""Unit tests for timeout handling, error handling, and integration flows."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
)


class TestTimeoutHandling:
    """Tests for shutdown timeout handling."""

    @pytest.mark.asyncio
    async def test_stop_timeout_forces_shutdown(self):
        coordinator = ShutdownCoordinator()

        async def slow_stop():
            await asyncio.sleep(10.0)

        result = await coordinator.shutdown(stop_func=slow_stop)
        assert result.forced is True

    @pytest.mark.asyncio
    async def test_drain_timeout_forces_shutdown(self):
        coordinator = ShutdownCoordinator(drain_timeout=0.1)

        async def slow_drain():
            await asyncio.sleep(10.0)
            return 0

        result = await coordinator.shutdown(stop_func=AsyncMock(), drain_func=slow_drain)
        assert result.forced is True

    @pytest.mark.asyncio
    async def test_checkpoint_timeout_forces_shutdown(self):
        coordinator = ShutdownCoordinator(checkpoint_timeout=0.1)

        async def slow_checkpoint():
            await asyncio.sleep(10.0)
            return 0

        result = await coordinator.shutdown(stop_func=AsyncMock(), checkpoint_func=slow_checkpoint)
        assert result.forced is True


class TestErrorHandling:
    """Tests for shutdown error handling."""

    @pytest.mark.asyncio
    async def test_stop_func_error_forces_shutdown(self):
        coordinator = ShutdownCoordinator()

        async def failing_stop():
            raise RuntimeError("Stop failed")

        result = await coordinator.shutdown(stop_func=failing_stop)
        assert result.forced is True
        assert result.phase == ShutdownPhase.FORCED
        assert "Stop failed" in result.error

    @pytest.mark.asyncio
    async def test_drain_func_error_forces_shutdown(self):
        coordinator = ShutdownCoordinator()

        async def failing_drain():
            raise RuntimeError("Drain failed")

        result = await coordinator.shutdown(stop_func=AsyncMock(), drain_func=failing_drain)
        assert result.forced is True
        assert result.phase == ShutdownPhase.FORCED

    @pytest.mark.asyncio
    async def test_checkpoint_func_error_forces_shutdown(self):
        coordinator = ShutdownCoordinator()

        async def failing_checkpoint():
            raise RuntimeError("Checkpoint failed")

        result = await coordinator.shutdown(
            stop_func=AsyncMock(), checkpoint_func=failing_checkpoint
        )
        assert result.forced is True
        assert result.phase == ShutdownPhase.FORCED

    @pytest.mark.asyncio
    async def test_close_func_error_does_not_affect_result(self):
        coordinator = ShutdownCoordinator()

        async def failing_close():
            raise RuntimeError("Close failed")

        result = await coordinator.shutdown(stop_func=AsyncMock(), close_func=failing_close)
        assert result.phase == ShutdownPhase.STOPPED


class TestIntegration:
    """Integration tests for shutdown coordinator."""

    @pytest.mark.asyncio
    async def test_full_shutdown_sequence(self):
        coordinator = ShutdownCoordinator(timeout=30.0, drain_timeout=10.0, checkpoint_timeout=5.0)
        stop_called = drain_called = checkpoint_called = close_called = False

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
            stop_func=stop, drain_func=drain, checkpoint_func=checkpoint, close_func=close
        )

        assert stop_called and drain_called and checkpoint_called and close_called
        assert result.phase == ShutdownPhase.STOPPED
        assert result.events_drained == 50
        assert result.checkpoints_saved == 3
        assert result.forced is False

    @pytest.mark.asyncio
    async def test_partial_shutdown_on_error(self):
        coordinator = ShutdownCoordinator()
        stop_called = checkpoint_called = False

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
            stop_func=stop, drain_func=failing_drain, checkpoint_func=checkpoint
        )

        assert stop_called
        assert checkpoint_called is False
        assert result.forced is True
