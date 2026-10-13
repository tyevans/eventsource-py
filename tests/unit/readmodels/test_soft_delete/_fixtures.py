"""Test fixtures and model definitions for soft delete tests."""

from decimal import Decimal

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels.model import ReadModel


class TestModel(ReadModel):
    """Test model for soft delete tests."""

    __test__ = False

    name: str
    status: str = "active"
    amount: Decimal = Decimal("0")


@pytest.fixture
def repo() -> InMemoryReadModelRepository[TestModel]:
    """Create a fresh in-memory repository for each test."""
    return InMemoryReadModelRepository(TestModel, enable_tracing=False)


__all__ = ["TestModel", "repo"]
