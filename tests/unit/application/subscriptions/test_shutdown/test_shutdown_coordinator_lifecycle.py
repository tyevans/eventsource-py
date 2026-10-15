"""Unit tests for ShutdownCoordinator lifecycle, callbacks, and execution."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
)


class TestShutdownCoordinatorCreation:
    def test_create_with_defaults(self):
        coordinator = ShutdownCoordinator()
        assert (
            coordinator.timeout,
            coordinator.drain_timeout,
            coordinator.checkpoint_timeout,
            coordinator.checkpoint_interval,
        ) == (30.0, 10.0, 5.0, 5.0)
        assert coordinator.phase == ShutdownPhase.RUNNING
        assert coordinator.is_shutting_down is False

    def test_create_with_custom_timeouts(self):
        coordinator = ShutdownCoordinator(
            timeout=60.0,
            drain_timeout=20.0,
            checkpoint_timeout=10.0,
            checkpoint_interval=2.0,
        )
        assert (
            coordinator.timeout,
            coordinator.drain_timeout,
            coordinator.checkpoint_timeout,
            coordinator.checkpoint_interval,
        ) == (60.0, 20.0, 10.0, 2.0)


class TestShutdownCoordinatorProperties:
    def test_is_shutting_down_initially_false(self):
        assert ShutdownCoordinator().is_shutting_down is False

    def test_phase_initially_running(self):
        assert ShutdownCoordinator().phase == ShutdownPhase.RUNNING

    def test_is_forced_initially_false(self):
        assert ShutdownCoordinator().is_forced is False


class TestRequestShutdown:
    def test_request_shutdown_sets_flag(self):
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()
        assert coordinator.is_shutting_down is True

    @pytest.mark.asyncio
    async def test_request_shutdown_triggers_event(self):
        coordinator = ShutdownCoordinator()

        async def wait_and_check():
            await coordinator.wait_for_shutdown()
            return True

        task = asyncio.create_task(wait_and_check())
        await asyncio.sleep(0.01)
        coordinator.request_shutdown()
        assert await asyncio.wait_for(task, timeout=1.0) is True

    def test_request_shutdown_idempotent(self):
        coordinator = ShutdownCoordinator()
        coordinator.request_shutdown()
        coordinator.request_shutdown()
        coordinator.request_shutdown()
        assert coordinator.is_shutting_down is True


class TestShutdownCallbacks:
    def test_on_shutdown_registers_callback(self):
        coordinator = ShutdownCoordinator()
        callback = AsyncMock()
        coordinator.on_shutdown(callback)
        assert callback in coordinator._on_shutdown_callbacks

    def test_remove_callback(self):
        coordinator = ShutdownCoordinator()
        callback = AsyncMock()
        coordinator.on_shutdown(callback)
        assert coordinator.remove_callback(callback) is True
        assert callback not in coordinator._on_shutdown_callbacks

    def test_remove_callback_not_found(self):
        assert ShutdownCoordinator().remove_callback(AsyncMock()) is False


class TestShutdownExecution:
    @pytest.mark.asyncio
    async def test_shutdown_stops_accepting_events(self):
        stop_func = AsyncMock()
        result = await ShutdownCoordinator().shutdown(stop_func=stop_func)
        stop_func.assert_called_once()
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_shutdown_drains_events(self):
        drain_func = AsyncMock(return_value=100)
        result = await ShutdownCoordinator().shutdown(stop_func=AsyncMock(), drain_func=drain_func)
        drain_func.assert_called_once()
        assert result.events_drained == 100

    @pytest.mark.asyncio
    async def test_shutdown_saves_checkpoints(self):
        checkpoint_func = AsyncMock(return_value=5)
        result = await ShutdownCoordinator().shutdown(
            stop_func=AsyncMock(), checkpoint_func=checkpoint_func
        )
        checkpoint_func.assert_called_once()
        assert result.checkpoints_saved == 5

    @pytest.mark.asyncio
    async def test_shutdown_closes_connections(self):
        close_func = AsyncMock()
        await ShutdownCoordinator().shutdown(stop_func=AsyncMock(), close_func=close_func)
        close_func.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_phase_progression(self):
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
        result = await ShutdownCoordinator().shutdown(stop_func=AsyncMock())
        assert result.duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_shutdown_without_optional_funcs(self):
        result = await ShutdownCoordinator().shutdown(stop_func=AsyncMock())
        assert (result.phase, result.events_drained, result.checkpoints_saved) == (
            ShutdownPhase.STOPPED,
            0,
            0,
        )
