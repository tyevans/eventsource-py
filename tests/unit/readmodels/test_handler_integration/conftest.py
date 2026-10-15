"""Shared test events, models, and fixtures for ReadModelProjection handler integration tests."""

from unittest.mock import MagicMock

import pytest
from pydantic import Field

from eventsource.domain.event import DomainEvent
from eventsource.ports.readmodels import ReadModel


class HandlerIntegrationEvent(DomainEvent):
    """Test event for handler integration tests."""

    __test__ = False

    aggregate_type: str = "Test"
    value: str = Field(..., description="Test value")


class SecondHandlerIntegrationEvent(DomainEvent):
    """Second test event for multiple handler tests."""

    __test__ = False

    aggregate_type: str = "Test"
    data: str = Field(default="default", description="Test data")


class UnhandledIntegrationEvent(DomainEvent):
    """Event type with no handler for testing unregistered events."""

    __test__ = False

    aggregate_type: str = "Test"


class HandlerIntegrationModel(ReadModel):
    """Test read model for handler integration tests."""

    __test__ = False

    name: str


@pytest.fixture
def mock_session_factory():
    """Create a mock async session factory."""
    # Create mock connection with PostgreSQL dialect
    mock_conn = MagicMock()
    mock_conn.dialect = MagicMock()
    mock_conn.dialect.name = "postgresql"

    # Create mock session
    mock_session = MagicMock()
    mock_session.connection = MagicMock(return_value=mock_conn)

    # Mock the context managers
    mock_session.__aenter__ = MagicMock(return_value=mock_session)
    mock_session.__aexit__ = MagicMock(return_value=None)

    # Create mock transaction context manager
    mock_begin = MagicMock()
    mock_begin.__aenter__ = MagicMock(return_value=None)
    mock_begin.__aexit__ = MagicMock(return_value=None)
    mock_session.begin = MagicMock(return_value=mock_begin)

    # Create factory that returns the mock session
    mock_factory = MagicMock()
    mock_factory.return_value = mock_session
    mock_factory.__call__ = MagicMock(return_value=mock_session)

    return mock_factory
