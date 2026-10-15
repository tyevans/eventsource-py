"""Unit tests for shutdown deadline functionality."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import pytest

from eventsource.application.subscriptions.shutdown import ShutdownCoordinator


class TestShutdownDeadline:
    """Tests for shutdown deadline functionality."""

    def test_set_deadline(self):
        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) + timedelta(seconds=30)
        coordinator.set_shutdown_deadline(deadline)
        assert coordinator.deadline == deadline

    def test_get_remaining_time_with_deadline(self):
        coordinator = ShutdownCoordinator()
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=10))
        assert 9 < coordinator.get_remaining_shutdown_time() <= 10

    def test_get_remaining_time_without_deadline(self):
        coordinator = ShutdownCoordinator(timeout=30.0)
        assert coordinator.get_remaining_shutdown_time() == 30.0

    def test_get_remaining_time_past_deadline(self):
        coordinator = ShutdownCoordinator()
        coordinator.set_shutdown_deadline(datetime.now(UTC) - timedelta(seconds=10))
        assert coordinator.get_remaining_shutdown_time() == 0.0

    def test_adjusted_timeouts_plenty_of_time(self):
        coordinator = ShutdownCoordinator(drain_timeout=10.0, checkpoint_timeout=5.0)
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=60))
        assert coordinator._get_adjusted_timeouts() == (5.0, 10.0, 5.0)

    def test_adjusted_timeouts_limited_time(self):
        coordinator = ShutdownCoordinator(drain_timeout=10.0, checkpoint_timeout=5.0)
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=10))
        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()
        assert stop + drain + checkpoint <= 10.0

    def test_adjusted_timeouts_critical_time(self):
        coordinator = ShutdownCoordinator()
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=2))
        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()
        assert drain == 0.0 and checkpoint > 0

    def test_adjusted_timeouts_zero_remaining(self):
        coordinator = ShutdownCoordinator()
        coordinator.set_shutdown_deadline(datetime.now(UTC) - timedelta(seconds=10))
        assert coordinator._get_adjusted_timeouts() == (0.0, 0.0, 0.0)

    def test_reset_clears_deadline(self):
        coordinator = ShutdownCoordinator()
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=30))
        coordinator.reset()
        assert coordinator.deadline is None

    def test_deadline_initially_none(self):
        assert ShutdownCoordinator().deadline is None

    def test_set_deadline_naive_datetime_warns(self, caplog):
        coordinator = ShutdownCoordinator()
        with caplog.at_level(logging.WARNING):
            coordinator.set_shutdown_deadline(datetime.now() + timedelta(seconds=30))
        assert "not timezone-aware" in caplog.text

    @pytest.mark.asyncio
    async def test_shutdown_uses_adjusted_timeouts(self):
        coordinator = ShutdownCoordinator(drain_timeout=10.0)
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=5))

        async def mock_drain():
            await asyncio.sleep(0.1)
            return 0

        async def mock_stop():
            pass

        result = await coordinator.shutdown(stop_func=mock_stop, drain_func=mock_drain)
        assert result.duration_seconds < 5.0

    @pytest.mark.asyncio
    async def test_shutdown_skips_drain_when_time_critical(self):
        coordinator = ShutdownCoordinator(drain_timeout=10.0)
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=2))
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
            stop_func=mock_stop, drain_func=mock_drain, checkpoint_func=mock_checkpoint
        )
        assert not drain_called and result.events_drained == 0

    @pytest.mark.asyncio
    async def test_shutdown_always_attempts_checkpoint(self):
        coordinator = ShutdownCoordinator(drain_timeout=10.0)
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=3))
        checkpoint_called = False

        async def mock_stop():
            pass

        async def mock_checkpoint():
            nonlocal checkpoint_called
            checkpoint_called = True
            return 1

        result = await coordinator.shutdown(stop_func=mock_stop, checkpoint_func=mock_checkpoint)
        assert checkpoint_called and result.checkpoints_saved == 1

    @pytest.mark.asyncio
    async def test_shutdown_no_deadline_uses_defaults(self):
        coordinator = ShutdownCoordinator(drain_timeout=10.0, checkpoint_timeout=5.0)
        assert coordinator._get_adjusted_timeouts() == (5.0, 10.0, 5.0)

    @pytest.mark.asyncio
    async def test_deadline_with_checkpoint_after_forced_drain(self):
        coordinator = ShutdownCoordinator(drain_timeout=0.1)
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=10))
        checkpoint_called = False

        async def mock_stop():
            pass

        async def mock_drain():
            await asyncio.sleep(10.0)
            return 100

        async def mock_checkpoint():
            nonlocal checkpoint_called
            checkpoint_called = True
            return 2

        result = await coordinator.shutdown(
            stop_func=mock_stop, drain_func=mock_drain, checkpoint_func=mock_checkpoint
        )
        assert checkpoint_called and result.checkpoints_saved == 2

    @pytest.mark.asyncio
    async def test_shutdown_logs_deadline_adjusted_timeouts(self, caplog):
        coordinator = ShutdownCoordinator()
        coordinator.set_shutdown_deadline(datetime.now(UTC) + timedelta(seconds=10))

        async def mock_stop():
            pass

        with caplog.at_level(logging.INFO):
            await coordinator.shutdown(stop_func=mock_stop)
        assert "deadline-adjusted timeouts" in caplog.text.lower()
