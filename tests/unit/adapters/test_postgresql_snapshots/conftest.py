"""Pytest fixtures for PostgreSQLSnapshotStore unit tests."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore
from eventsource.ports.snapshots import Snapshot


@pytest.fixture
def mock_session() -> AsyncMock:
    """Create a mock async session."""
    session = AsyncMock(spec=AsyncSession)
    return session


@pytest.fixture
def mock_session_factory(mock_session: AsyncMock) -> MagicMock:
    """Create a mock session factory that returns the mock session."""
    factory = MagicMock(spec=async_sessionmaker)

    # Create async context manager for session
    session_context = AsyncMock()
    session_context.__aenter__.return_value = mock_session
    session_context.__aexit__.return_value = None
    factory.return_value = session_context

    # Create async context manager for begin()
    begin_context = AsyncMock()
    begin_context.__aenter__.return_value = None
    begin_context.__aexit__.return_value = None
    mock_session.begin.return_value = begin_context

    return factory


@pytest.fixture
def store(mock_session_factory: MagicMock) -> PostgreSQLSnapshotStore:
    """Create a PostgreSQLSnapshotStore with mocked session factory."""
    return PostgreSQLSnapshotStore(
        session_factory=mock_session_factory,
        enable_tracing=False,
    )


@pytest.fixture
def aggregate_id() -> UUID:
    """Create a random aggregate ID."""
    return uuid4()


@pytest.fixture
def sample_snapshot(aggregate_id: UUID) -> Snapshot:
    """Create a sample snapshot for testing."""
    return Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="TestAggregate",
        version=10,
        schema_version=1,
        state={"value": "test", "count": 5, "items": ["a", "b"]},
        created_at=datetime.now(UTC),
    )
