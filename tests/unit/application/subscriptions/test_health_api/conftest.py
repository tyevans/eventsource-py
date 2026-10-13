"""Shared fixtures for SubscriptionManager Health Check API unit tests."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.application.subscriptions import SubscriptionManager


@pytest.fixture
def mock_event_store():
    """Create mock event store."""
    store = MagicMock()
    store.get_global_position = AsyncMock(return_value=1000)
    store.read_all = AsyncMock(return_value=iter([]))
    return store


@pytest.fixture
def mock_event_bus():
    """Create mock event bus."""
    bus = MagicMock()
    bus.subscribe = AsyncMock()
    bus.unsubscribe = AsyncMock()
    return bus


@pytest.fixture
def mock_checkpoint_repo():
    """Create mock checkpoint repository."""
    repo = MagicMock()
    repo.get = AsyncMock(return_value=None)
    repo.save = AsyncMock()
    return repo


@pytest.fixture
def mock_subscriber():
    """Create mock subscriber."""
    subscriber = MagicMock()
    subscriber.subscribed_to.return_value = []
    subscriber.handle = AsyncMock()
    return subscriber


@pytest.fixture
def manager(mock_event_store, mock_event_bus, mock_checkpoint_repo):
    """Create subscription manager for testing."""
    return SubscriptionManager(
        event_store=mock_event_store,
        event_bus=mock_event_bus,
        checkpoint_repo=mock_checkpoint_repo,
    )
