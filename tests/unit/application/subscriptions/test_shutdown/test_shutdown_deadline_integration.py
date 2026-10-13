"""Integration tests for shutdown deadline with cloud scenarios."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
)


class TestShutdownDeadlineIntegration:
    """Integration tests for shutdown deadline with cloud scenarios."""

    @pytest.mark.asyncio
    async def test_kubernetes_termination_scenario(self):
        """Test Kubernetes terminationGracePeriodSeconds scenario."""
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
