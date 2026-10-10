"""
Order domain events and aggregate for projection example.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    register_event,
)


@register_event
class OrderPlaced(DomainEvent):
    """Event emitted when an order is placed."""

    aggregate_type: str = "Order"

    customer_id: UUID
    customer_name: str
    total_amount: float
    item_count: int


@register_event
class OrderShipped(DomainEvent):
    """Event emitted when an order is shipped."""

    aggregate_type: str = "Order"

    tracking_number: str
    carrier: str


@register_event
class OrderDelivered(DomainEvent):
    """Event emitted when an order is delivered."""

    aggregate_type: str = "Order"

    delivered_at: datetime


@register_event
class OrderCancelled(DomainEvent):
    """Event emitted when an order is cancelled."""

    aggregate_type: str = "Order"

    reason: str


class OrderState(BaseModel):
    """Order aggregate state."""

    order_id: UUID
    customer_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
    item_count: int = 0
    status: str = "draft"
    tracking_number: str | None = None
    carrier: str | None = None


class OrderAggregate(AggregateRoot[OrderState]):
    """Simple order aggregate."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                customer_name=event.customer_name,
                total_amount=event.total_amount,
                item_count=event.item_count,
                status="placed",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                        "carrier": event.carrier,
                    }
                )
        elif isinstance(event, OrderDelivered):
            if self._state:
                self._state = self._state.model_copy(update={"status": "delivered"})
        elif isinstance(event, OrderCancelled) and self._state:
            self._state = self._state.model_copy(update={"status": "cancelled"})

    def place(
        self, customer_id: UUID, customer_name: str, total_amount: float, item_count: int
    ) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        event = OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            customer_name=customer_name,
            total_amount=total_amount,
            item_count=item_count,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def ship(self, tracking_number: str, carrier: str) -> None:
        if not self.state or self.state.status != "placed":
            raise ValueError("Order must be placed to ship")
        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            carrier=carrier,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def deliver(self) -> None:
        if not self.state or self.state.status != "shipped":
            raise ValueError("Order must be shipped to deliver")
        event = OrderDelivered(
            aggregate_id=self.aggregate_id,
            delivered_at=datetime.now(),
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def cancel(self, reason: str) -> None:
        if not self.state or self.state.status in ("delivered", "cancelled"):
            raise ValueError("Cannot cancel this order")
        event = OrderCancelled(
            aggregate_id=self.aggregate_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


__all__ = [
    "OrderAggregate",
    "OrderCancelled",
    "OrderDelivered",
    "OrderPlaced",
    "OrderShipped",
    "OrderState",
]
