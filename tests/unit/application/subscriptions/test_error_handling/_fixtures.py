"""Shared fixtures and helper functions for error handling tests."""

from unittest.mock import AsyncMock

import pytest

from eventsource.ports.positions import Position


def pos(n: int) -> Position:
    """Build a test-store position token standing in for an int position."""
    return Position(store_id="test", key=(n,))


@pytest.fixture
def mock_dlq_repo() -> AsyncMock:
    """Create mock DLQ repository."""
    repo = AsyncMock()
    repo.add_failed_event = AsyncMock()
    return repo
