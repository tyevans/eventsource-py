"""Shared sample events for handler protocol unit tests."""

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
