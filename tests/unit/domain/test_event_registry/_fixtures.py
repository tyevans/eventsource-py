"""Shared test event fixtures for event registry tests."""

from uuid import UUID

from pydantic import Field

from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    """Sample event with explicit event_type default."""

    aggregate_type: str = "Order"

    order_number: str = Field(..., description="Unique order number")
    customer_id: UUID = Field(..., description="Customer who placed the order")


class OrderShipped(DomainEvent):
    """Sample event with explicit event_type default."""

    aggregate_type: str = "Order"

    tracking_number: str = Field(..., description="Shipping tracking number")


class PaymentReceived(DomainEvent):
    """Sample event with explicit event_type default."""

    aggregate_type: str = "Payment"

    amount: float = Field(..., description="Payment amount")


class EventWithoutTypeDefault(DomainEvent):
    """Event that doesn't set event_type default - will use class name."""

    aggregate_type: str = "Test"


__all__ = [
    "EventWithoutTypeDefault",
    "OrderCreated",
    "OrderShipped",
    "PaymentReceived",
]
