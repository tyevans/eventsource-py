"""Unit tests for periodic checkpoint saves during drain phase."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
)


class TestPeriodicCheckpointDuringDrain:
    """Tests for periodic checkpoint saves during drain phase."""

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_during_drain(self):
        coordinator = ShutdownCoordinator(drain_timeout=1.0, checkpoint_interval=0.1)
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=2)

        async def slow_drain():
            await asyncio.sleep(0.35)
            return 10

        result = await coordinator.shutdown(
            stop_func=stop_func, drain_func=slow_drain, checkpoint_func=checkpoint_func
        )
        assert checkpoint_func.call_count >= 3
        assert result.phase == ShutdownPhase.STOPPED
        assert result.events_drained == 10

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_disabled_when_zero(self):
        coordinator = ShutdownCoordinator(drain_timeout=1.0, checkpoint_interval=0)
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=1)

        async def slow_drain():
            await asyncio.sleep(0.1)
            return 5

        result = await coordinator.shutdown(
            stop_func=stop_func, drain_func=slow_drain, checkpoint_func=checkpoint_func
        )
        assert checkpoint_func.call_count == 1
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_not_started_without_checkpoint_func(self):
        coordinator = ShutdownCoordinator(drain_timeout=1.0, checkpoint_interval=0.1)
        stop_func = AsyncMock()

        async def slow_drain():
            await asyncio.sleep(0.15)
            return 3

        result = await coordinator.shutdown(stop_func=stop_func, drain_func=slow_drain)
        assert result.phase == ShutdownPhase.STOPPED
        assert result.events_drained == 3
        assert coordinator._periodic_checkpoint_task is None

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_stops_when_drain_completes(self):
        coordinator = ShutdownCoordinator(drain_timeout=5.0, checkpoint_interval=0.05)
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=1)

        async def quick_drain():
            await asyncio.sleep(0.15)
            return 2

        await coordinator.shutdown(
            stop_func=stop_func, drain_func=quick_drain, checkpoint_func=checkpoint_func
        )
        assert coordinator._periodic_checkpoint_task is None

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_error_does_not_crash(self):
        coordinator = ShutdownCoordinator(drain_timeout=2.0, checkpoint_interval=0.05)
        stop_func = AsyncMock()
        call_count = 0

        async def failing_checkpoint():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Checkpoint failed")
            return 1

        async def slow_drain():
            await asyncio.sleep(0.7)
            return 5

        result = await coordinator.shutdown(
            stop_func=stop_func, drain_func=slow_drain, checkpoint_func=failing_checkpoint
        )
        assert call_count >= 2
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_with_drain_timeout(self):
        coordinator = ShutdownCoordinator(drain_timeout=0.2, checkpoint_interval=0.05)
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=1)

        async def very_slow_drain():
            await asyncio.sleep(10.0)
            return 100

        result = await coordinator.shutdown(
            stop_func=stop_func, drain_func=very_slow_drain, checkpoint_func=checkpoint_func
        )
        assert result.forced is True
        assert checkpoint_func.call_count >= 1
        assert coordinator._periodic_checkpoint_task is None

    @pytest.mark.asyncio
    async def test_final_checkpoint_after_drain_with_periodic(self):
        coordinator = ShutdownCoordinator(drain_timeout=1.0, checkpoint_interval=0.05)
        stop_func = AsyncMock()
        checkpoint_call_times = []

        async def checkpoint_with_timing():
            checkpoint_call_times.append(asyncio.get_event_loop().time())
            return 1

        async def slow_drain():
            await asyncio.sleep(0.15)
            return 3

        await coordinator.shutdown(
            stop_func=stop_func, drain_func=slow_drain, checkpoint_func=checkpoint_with_timing
        )
        assert len(checkpoint_call_times) >= 3

    @pytest.mark.asyncio
    async def test_reset_clears_periodic_checkpoint_state(self):
        coordinator = ShutdownCoordinator(checkpoint_interval=0.1)
        coordinator._periodic_checkpoints_saved = 5
        coordinator.reset()
        assert coordinator._periodic_checkpoint_task is None
        assert coordinator._periodic_checkpoints_saved == 0

    @pytest.mark.asyncio
    async def test_periodic_checkpoint_task_has_name(self):
        coordinator = ShutdownCoordinator(drain_timeout=1.0, checkpoint_interval=0.1)
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
        coordinator = ShutdownCoordinator(drain_timeout=1.0, checkpoint_interval=-1.0)
        stop_func = AsyncMock()
        checkpoint_func = AsyncMock(return_value=1)

        async def slow_drain():
            await asyncio.sleep(0.1)
            return 5

        result = await coordinator.shutdown(
            stop_func=stop_func, drain_func=slow_drain, checkpoint_func=checkpoint_func
        )
        assert checkpoint_func.call_count == 1
        assert result.phase == ShutdownPhase.STOPPED
