"""Shared fixtures and mocks for shutdown coordination tests."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.application.subscriptions.shutdown import ShutdownCoordinator


@pytest.fixture
def coordinator() -> ShutdownCoordinator:
    """Create a default ShutdownCoordinator instance."""
    return ShutdownCoordinator()


@pytest.fixture
def dummy_async_hook() -> AsyncMock:
    """Create a dummy async hook callable."""
    return AsyncMock()


@pytest.fixture
def mock_subscriber() -> MagicMock:
    """Create a mock subscriber with lifecycle async methods."""
    subscriber = MagicMock()
    subscriber.stop = AsyncMock()
    subscriber.drain = AsyncMock(return_value=0)
    subscriber.checkpoint = AsyncMock()
    subscriber.close = AsyncMock()
    return subscriber


@pytest.fixture
def signal_mock() -> MagicMock:
    """Create a mock signal callback."""
    return MagicMock()
