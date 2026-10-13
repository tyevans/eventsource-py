"""Fixtures and test doubles for subscription health tests."""

from unittest.mock import MagicMock

import pytest

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.health import (
    HealthCheckResult,
    HealthIndicator,
    HealthStatus,
    ManagerHealth,
    SubscriptionHealth,
)
from eventsource.application.subscriptions.subscription import Subscription


@pytest.fixture
def mock_subscriber():
    """Create mock subscriber."""
    subscriber = MagicMock()
    subscriber.subscribed_to.return_value = []
    return subscriber


@pytest.fixture
def subscription(mock_subscriber) -> Subscription:
    """Create test subscription."""
    return Subscription(
        name="TestSubscription",
        config=SubscriptionConfig(),
        subscriber=mock_subscriber,
    )


@pytest.fixture
def subscriptions(mock_subscriber) -> list[Subscription]:
    """Create test subscriptions."""
    return [
        Subscription(
            name="Subscription1",
            config=SubscriptionConfig(),
            subscriber=mock_subscriber,
        ),
        Subscription(
            name="Subscription2",
            config=SubscriptionConfig(),
            subscriber=mock_subscriber,
        ),
    ]


@pytest.fixture
def sample_health_indicator() -> HealthIndicator:
    """Create sample health indicator."""
    return HealthIndicator(
        name="test_indicator",
        status=HealthStatus.HEALTHY,
        message="All good",
        details={"key": "value"},
    )


@pytest.fixture
def sample_health_result(sample_health_indicator) -> HealthCheckResult:
    """Create sample health check result."""
    return HealthCheckResult(
        overall_status=HealthStatus.HEALTHY,
        indicators=[sample_health_indicator],
        subscription_name="TestSubscription",
        uptime_seconds=3600.0,
    )


@pytest.fixture
def sample_subscription_health() -> SubscriptionHealth:
    """Create sample subscription health."""
    return SubscriptionHealth(
        name="TestSub",
        status="healthy",
        state="live",
        events_processed=100,
        events_failed=2,
        lag_events=5,
        error_rate=0.1,
        last_error=None,
        uptime_seconds=3600.0,
    )


@pytest.fixture
def sample_manager_health(sample_subscription_health) -> ManagerHealth:
    """Create sample manager health."""
    return ManagerHealth(
        status="healthy",
        running=True,
        subscription_count=1,
        healthy_count=1,
        degraded_count=0,
        unhealthy_count=0,
        total_events_processed=100,
        total_events_failed=2,
        total_lag_events=5,
        uptime_seconds=7200.0,
        subscriptions=[sample_subscription_health],
    )
