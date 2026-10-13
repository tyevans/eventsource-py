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
