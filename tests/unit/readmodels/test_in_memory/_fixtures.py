"""Fixtures and test models for InMemoryReadModelRepository tests."""

from decimal import Decimal

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels.model import ReadModel


class OrderSummary(ReadModel):
    """Test model for repository tests."""

    order_number: str
    status: str
    total_amount: Decimal


@pytest.fixture
def repo() -> InMemoryReadModelRepository[OrderSummary]:
    """Create a fresh in-memory repository for each test."""
    return InMemoryReadModelRepository(OrderSummary, enable_tracing=False)
