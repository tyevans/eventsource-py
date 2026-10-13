"""Shared fixtures and helper functions for PositionMapper tests."""

from unittest.mock import MagicMock

import pytest

from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.ports.positions import Position


def pos(n: int, store: str = "src") -> Position:
    """Build a Position for a given store, keyed by a single int."""
    return Position(store_id=store, key=(n,))


class PositionMapperTestBase:
    """Shared base class providing common mock repository and mapper fixtures."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        return MagicMock()

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        """Create a mapper with mock repository."""
        return PositionMapper(mock_repo, enable_tracing=False)
