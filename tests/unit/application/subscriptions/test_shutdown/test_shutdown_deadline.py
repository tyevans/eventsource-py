"""Unit tests for shutdown deadline functionality."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import pytest

from eventsource.application.subscriptions.shutdown import ShutdownCoordinator


class TestShutdownDeadline:
    """Tests for shutdown deadline functionality."""

    def test_set_deadline(self):
        """Test setting shutdown deadline."""
        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) + timedelta(seconds=30)

        coordinator.set_shutdown_deadline(deadline)

        assert coordinator.deadline == deadline

    def test_get_remaining_time_with_deadline(self):
        """Test remaining time calculation with deadline."""
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
        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) - timedelta(seconds=10)
        coordinator.set_shutdown_deadline(deadline)

        remaining = coordinator.get_remaining_shutdown_time()

        assert remaining == 0.0

    def test_adjusted_timeouts_plenty_of_time(self):
        """Test timeouts unchanged when plenty of time."""
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
        coordinator = ShutdownCoordinator(drain_timeout=10.0, checkpoint_timeout=5.0)
        deadline = datetime.now(UTC) + timedelta(seconds=10)  # Half of total
        coordinator.set_shutdown_deadline(deadline)

        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()

        assert stop + drain + checkpoint <= 10.0

    def test_adjusted_timeouts_critical_time(self):
        """Test drain skipped when time is critical."""
        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) + timedelta(seconds=2)  # Very short
        coordinator.set_shutdown_deadline(deadline)

        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()

        assert drain == 0.0  # Drain should be skipped
        assert checkpoint > 0  # Checkpoint should still have time

    def test_adjusted_timeouts_zero_remaining(self):
        """Test all timeouts are zero when no time remaining."""
        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) - timedelta(seconds=10)  # Past deadline
        coordinator.set_shutdown_deadline(deadline)

        stop, drain, checkpoint = coordinator._get_adjusted_timeouts()

        assert stop == 0.0
        assert drain == 0.0
        assert checkpoint == 0.0

    def test_reset_clears_deadline(self):
        """Test reset clears the deadline."""
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
        coordinator = ShutdownCoordinator()
        naive_deadline = datetime.now() + timedelta(seconds=30)

        with caplog.at_level(logging.WARNING):
            coordinator.set_shutdown_deadline(naive_deadline)

        assert "not timezone-aware" in caplog.text

    @pytest.mark.asyncio
    async def test_shutdown_uses_adjusted_timeouts(self):
        """Test shutdown() uses deadline-adjusted timeouts."""
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
        coordinator = ShutdownCoordinator()
        deadline = datetime.now(UTC) + timedelta(seconds=10)
        coordinator.set_shutdown_deadline(deadline)

        async def mock_stop():
            pass

        with caplog.at_level(logging.INFO):
            await coordinator.shutdown(stop_func=mock_stop)

        assert "deadline-adjusted timeouts" in caplog.text.lower()
