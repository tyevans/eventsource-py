"""Shared fixtures and test helpers for subscription state tests."""

import pytest

from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
)
from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import EventSubscriber
from eventsource.ports.positions import Position


def pos(n: int) -> Position:
    """Build a test-store position token standing in for an int position."""
    return Position(store_id="test", key=(n,))


class MockSubscriber(EventSubscriber):
    """Mock subscriber for testing."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return []

    async def handle(self, event: DomainEvent) -> None:
        pass


@pytest.fixture
def mock_subscriber() -> MockSubscriber:
    """Create a mock subscriber."""
    return MockSubscriber()


@pytest.fixture
def config() -> SubscriptionConfig:
    """Create a default configuration."""
    return SubscriptionConfig()


@pytest.fixture
def subscription(mock_subscriber: MockSubscriber, config: SubscriptionConfig) -> Subscription:
    """Create a subscription for testing."""
    return Subscription(
        name="TestSubscription",
        config=config,
        subscriber=mock_subscriber,
    )
