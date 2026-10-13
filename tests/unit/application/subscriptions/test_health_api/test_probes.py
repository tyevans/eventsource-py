"""Unit tests for SubscriptionManager.readiness_check() and liveness_check()."""

import pytest

from eventsource.application.subscriptions import (
    LivenessStatus,
    ReadinessStatus,
    SubscriptionState,
)


class TestReadinessCheck:
    """Tests for SubscriptionManager.readiness_check() method."""

    @pytest.mark.asyncio
    async def test_readiness_not_running(self, manager):
        """Test readiness check when manager not running."""
        readiness = await manager.readiness_check()

        assert isinstance(readiness, ReadinessStatus)
        assert readiness.ready is False
        assert "not running" in readiness.reason

    @pytest.mark.asyncio
    async def test_readiness_when_running(self, manager, mock_subscriber):
        """Test readiness check when manager is running."""
        await manager.subscribe(mock_subscriber, name="TestSub")

        manager._running = True

        subscription = manager.get_subscription("TestSub")
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.LIVE)

        readiness = await manager.readiness_check()

        assert readiness.ready is True
        assert "ready" in readiness.reason.lower()

    @pytest.mark.asyncio
    async def test_readiness_with_error_subscription(self, manager, mock_subscriber):
        """Test readiness check with subscription in error state."""
        await manager.subscribe(mock_subscriber, name="ErrorSub")

        manager._running = True

        subscription = manager.get_subscription("ErrorSub")
        subscription.last_error = RuntimeError("Test error")
        await subscription.transition_to(SubscriptionState.ERROR)

        readiness = await manager.readiness_check()

        assert readiness.ready is False
        assert "error state" in readiness.reason.lower()
        assert "ErrorSub" in readiness.reason

    @pytest.mark.asyncio
    async def test_readiness_with_starting_subscription(self, manager, mock_subscriber):
        """Test readiness check with subscription still starting."""
        await manager.subscribe(mock_subscriber, name="StartingSub")

        manager._running = True

        readiness = await manager.readiness_check()

        assert readiness.ready is False
        assert "starting" in readiness.reason.lower()

    @pytest.mark.asyncio
    async def test_readiness_when_shutting_down(self, manager, mock_subscriber):
        """Test readiness check when shutting down."""
        await manager.subscribe(mock_subscriber, name="TestSub")
        manager._running = True

        manager.request_shutdown()

        readiness = await manager.readiness_check()

        assert readiness.ready is False
        assert "shutting down" in readiness.reason.lower()

    @pytest.mark.asyncio
    async def test_readiness_to_dict(self, manager):
        """Test readiness check serialization."""
        readiness = await manager.readiness_check()
        data = readiness.to_dict()

        assert "ready" in data
        assert "reason" in data
        assert "details" in data
        assert "timestamp" in data


class TestLivenessCheck:
    """Tests for SubscriptionManager.liveness_check() method."""

    @pytest.mark.asyncio
    async def test_liveness_alive(self, manager):
        """Test liveness check when manager is alive."""
        liveness = await manager.liveness_check()

        assert isinstance(liveness, LivenessStatus)
        assert liveness.alive is True
        assert "alive" in liveness.reason.lower()

    @pytest.mark.asyncio
    async def test_liveness_to_dict(self, manager):
        """Test liveness check serialization."""
        liveness = await manager.liveness_check()
        data = liveness.to_dict()

        assert "alive" in data
        assert "reason" in data
        assert "details" in data
        assert "timestamp" in data

    @pytest.mark.asyncio
    async def test_liveness_with_subscriptions(self, manager, mock_subscriber):
        """Test liveness check with subscriptions."""
        await manager.subscribe(mock_subscriber, name="Sub1")
        await manager.subscribe(mock_subscriber, name="Sub2")

        liveness = await manager.liveness_check()

        assert liveness.alive is True
        assert liveness.details["subscription_count"] == 2
