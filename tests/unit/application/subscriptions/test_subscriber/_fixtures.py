"""Shared fixtures and domain events for subscriber unit tests."""

from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class OrderCancelled(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Order"
    reason: str = Field(default="", description="Cancellation reason")


@pytest.fixture
def order_created() -> OrderCreated:
    """Fixture providing a sample OrderCreated event."""
    return OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")


@pytest.fixture
def order_shipped() -> OrderShipped:
    """Fixture providing a sample OrderShipped event."""
    return OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")


@pytest.fixture
def order_cancelled() -> OrderCancelled:
    """Fixture providing a sample OrderCancelled event."""
    return OrderCancelled(aggregate_id=uuid4(), reason="Customer request")


__all__ = [  # pragma: allowlist secret
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "order_cancelled",
    "order_created",
    "order_shipped",
]
