"""Pytest fixtures for projection base unit tests."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create an in-memory checkpoint repository."""
    return InMemoryCheckpointRepository()


@pytest.fixture
def dlq_repo() -> InMemoryDLQRepository:
    """Create an in-memory DLQ repository."""
    return InMemoryDLQRepository()


@pytest.fixture
def mock_session_factory():
    """Create a mock async session factory."""
    # Create mock connection
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()

    # Create mock session
    mock_session = AsyncMock()
    mock_session.connection = AsyncMock(return_value=mock_conn)

    # Mock the context managers
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)

    # Create mock transaction context manager
    mock_begin = MagicMock()
    mock_begin.__aenter__ = AsyncMock(return_value=None)
    mock_begin.__aexit__ = AsyncMock(return_value=None)
    mock_session.begin = MagicMock(return_value=mock_begin)

    # Create factory that returns the mock session
    mock_factory = MagicMock()
    mock_factory.return_value = mock_session
    mock_factory.__call__ = MagicMock(return_value=mock_session)

    return mock_factory, mock_session, mock_conn
