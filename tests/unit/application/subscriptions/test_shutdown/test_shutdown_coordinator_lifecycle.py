"""Unit tests for ShutdownCoordinator lifecycle, callbacks, and execution."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
)


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
