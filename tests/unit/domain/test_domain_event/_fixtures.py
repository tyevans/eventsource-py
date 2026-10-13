"""Fixtures and sample event models for domain event tests."""

from uuid import UUID

from pydantic import Field

from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    """Sample event for testing - represents an order creation."""

    aggregate_type: str = "Order"

    # Event-specific fields
    order_number: str = Field(..., description="Unique order number")
    customer_id: UUID = Field(..., description="Customer who placed the order")
    total_amount: float = Field(..., ge=0, description="Order total")


class OrderShipped(DomainEvent):
    """Sample event for testing - represents an order being shipped."""

    aggregate_type: str = "Order"

    tracking_number: str = Field(..., description="Shipping tracking number")
    carrier: str = Field(..., description="Shipping carrier name")
