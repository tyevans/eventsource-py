"""Unit tests for SubscriptionManager.health_check(), get_subscription_health(), and uptime_seconds."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    HealthCheckConfig,
    ManagerHealth,
    SubscriptionHealth,
    SubscriptionManager,
    SubscriptionState,
)


class TestHealthCheck:
    """Tests for SubscriptionManager.health_check() method."""

    @pytest.mark.asyncio
    async def test_health_check_no_subscriptions(self, manager):
        """Test health check with no subscriptions."""
        health = await manager.health_check()

        assert isinstance(health, ManagerHealth)
        assert health.status == "unknown"
        assert health.running is False
        assert health.subscription_count == 0
        assert health.healthy_count == 0
        assert health.degraded_count == 0
        assert health.unhealthy_count == 0
        assert health.total_events_processed == 0
        assert health.total_events_failed == 0
        assert health.total_lag_events == 0

    @pytest.mark.asyncio
    async def test_health_check_with_healthy_subscription(
        self,
        manager,
        mock_subscriber,
    ):
        """Test health check with a healthy subscription."""
        subscription = await manager.subscribe(mock_subscriber, name="TestSub")

        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.LIVE)

        await subscription.record_event_processed(100, uuid4(), "TestEvent")

        health = await manager.health_check()

        assert health.subscription_count == 1
        assert len(health.subscriptions) == 1
        assert health.subscriptions[0].name == "TestSub"
        assert health.subscriptions[0].state == "live"

    @pytest.mark.asyncio
    async def test_health_check_with_unhealthy_subscription(
        self,
        manager,
        mock_subscriber,
    ):
        """Test health check with an unhealthy subscription."""
        subscription = await manager.subscribe(mock_subscriber, name="FailingSub")

        subscription.last_error = RuntimeError("Test error")
        await subscription.transition_to(SubscriptionState.ERROR)

        health = await manager.health_check()

        assert health.subscription_count == 1
        assert health.unhealthy_count >= 1
        assert health.subscriptions[0].state == "error"
        assert health.subscriptions[0].last_error is not None

    @pytest.mark.asyncio
    async def test_health_check_with_mixed_subscriptions(
        self,
        manager,
        mock_subscriber,
    ):
        """Test health check with mixed healthy/unhealthy subscriptions."""
        sub1 = await manager.subscribe(mock_subscriber, name="HealthySub")
        await sub1.transition_to(SubscriptionState.CATCHING_UP)
        await sub1.transition_to(SubscriptionState.LIVE)

        mock_subscriber2 = MagicMock()
        mock_subscriber2.subscribed_to.return_value = []
        sub2 = await manager.subscribe(mock_subscriber2, name="UnhealthySub")
        sub2.last_error = RuntimeError("Error")
        await sub2.transition_to(SubscriptionState.ERROR)

        health = await manager.health_check()

        assert health.subscription_count == 2
        assert health.status == "unhealthy"
        assert len(health.subscriptions) == 2

    @pytest.mark.asyncio
    async def test_health_check_to_dict(self, manager, mock_subscriber):
        """Test health check result serialization."""
        await manager.subscribe(mock_subscriber, name="TestSub")

        health = await manager.health_check()
        data = health.to_dict()

        assert "status" in data
        assert "running" in data
        assert "subscription_count" in data
        assert "healthy_count" in data
        assert "degraded_count" in data
        assert "unhealthy_count" in data
        assert "total_events_processed" in data
        assert "total_events_failed" in data
        assert "total_lag_events" in data
        assert "uptime_seconds" in data
        assert "timestamp" in data
        assert "subscriptions" in data

    @pytest.mark.asyncio
    async def test_health_check_with_lag(self, manager, mock_subscriber):
        """Test health check with subscription lag."""
        subscription = await manager.subscribe(mock_subscriber, name="LaggySub")
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.LIVE)

        await subscription.record_events_seen(4900)

        health = await manager.health_check()

        assert health.total_lag_events == 4900
        assert health.subscriptions[0].lag_events == 4900


class TestGetSubscriptionHealth:
    """Tests for SubscriptionManager.get_subscription_health() method."""

    @pytest.mark.asyncio
    async def test_get_subscription_health_not_found(self, manager):
        """Test getting health for non-existent subscription."""
        health = manager.get_subscription_health("NonExistent")
        assert health is None

    @pytest.mark.asyncio
    async def test_get_subscription_health_found(self, manager, mock_subscriber):
        """Test getting health for existing subscription."""
        await manager.subscribe(mock_subscriber, name="TestSub")

        health = manager.get_subscription_health("TestSub")

        assert isinstance(health, SubscriptionHealth)
        assert health.name == "TestSub"
        assert health.state == "starting"
        assert health.events_processed == 0
        assert health.events_failed == 0

    @pytest.mark.asyncio
    async def test_get_subscription_health_with_metrics(
        self,
        manager,
        mock_subscriber,
    ):
        """Test getting health with recorded metrics."""
        subscription = await manager.subscribe(mock_subscriber, name="MetricSub")
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.LIVE)

        for i in range(10):
            await subscription.record_event_processed(i, uuid4(), "TestEvent")

        await subscription.record_event_failed(RuntimeError("Test error"))

        health = manager.get_subscription_health("MetricSub")

        assert health.events_processed == 10
        assert health.events_failed == 1
        assert health.state == "live"


class TestUptimeSeconds:
    """Tests for SubscriptionManager.uptime_seconds property."""

    def test_uptime_before_start(self, manager):
        """Test uptime is 0 before manager starts."""
        assert manager.uptime_seconds == 0.0

    @pytest.mark.asyncio
    async def test_uptime_after_start(self, manager, mock_subscriber):
        """Test uptime is non-zero after manager starts."""
        await manager.subscribe(mock_subscriber, name="TestSub")

        manager._health_provider.set_started(datetime.now(UTC) - timedelta(seconds=10))

        uptime = manager.uptime_seconds
        assert uptime >= 10.0
        assert uptime < 15.0


class TestHealthCheckWithConfig:
    """Tests for health check with custom configuration."""

    @pytest.fixture
    def manager_with_config(self, mock_event_store, mock_event_bus, mock_checkpoint_repo):
        """Create manager with custom health check config."""
        config = HealthCheckConfig(
            max_lag_events_warning=50,
            max_lag_events_critical=100,
            max_errors_warning=5,
            max_errors_critical=10,
        )
        return SubscriptionManager(
            event_store=mock_event_store,
            event_bus=mock_event_bus,
            checkpoint_repo=mock_checkpoint_repo,
            health_check_config=config,
        )

    @pytest.mark.asyncio
    async def test_health_check_with_custom_lag_thresholds(
        self,
        manager_with_config,
        mock_subscriber,
    ):
        """Test health check respects custom lag thresholds."""
        subscription = await manager_with_config.subscribe(
            mock_subscriber,
            name="LaggySub",
        )
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.LIVE)

        await subscription.record_events_seen(75)

        health = await manager_with_config.health_check()

        assert health.subscriptions[0].lag_events == 75
