"""Fixtures and test aggregate models for create_event tests."""

from uuid import UUID

from pydantic import BaseModel

from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent


class OrderState(BaseModel):
    """State model for Order aggregate."""

    order_id: UUID
    status: str = "pending"
    customer_id: UUID | None = None
    tracking_number: str | None = None


class OrderCreated(DomainEvent):
    """Event for order creation."""

    aggregate_type: str = "Order"
    customer_id: UUID


class OrderShipped(DomainEvent):
    """Event for shipping order."""

    aggregate_type: str = "Order"
    tracking_number: str


class OrderCancelled(DomainEvent):
    """Event for cancelling order."""

    aggregate_type: str = "Order"
    reason: str


class OrderAggregate(AggregateRoot[OrderState]):
    """Order aggregate for testing create_event()."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=event.aggregate_id,
                customer_id=event.customer_id,
                status="created",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={"status": "shipped", "tracking_number": event.tracking_number}
                )
        elif isinstance(event, OrderCancelled) and self._state:
            self._state = self._state.model_copy(update={"status": "cancelled"})

    # Command methods using create_event
    def create(self, customer_id: UUID) -> OrderCreated:
        """Create order using create_event convenience method."""
        return self.create_event(OrderCreated, customer_id=customer_id)

    def ship(self, tracking_number: str) -> OrderShipped:
        """Ship order using create_event convenience method."""
        return self.create_event(OrderShipped, tracking_number=tracking_number)

    def cancel(self, reason: str) -> OrderCancelled:
        """Cancel order using create_event convenience method."""
        return self.create_event(OrderCancelled, reason=reason)


class DeclarativeOrderAggregate(DeclarativeAggregate[OrderState]):
    """Declarative order aggregate for testing create_event() with @handles decorator."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    @handles(OrderCreated)
    def _on_order_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=event.aggregate_id,
            customer_id=event.customer_id,
            status="created",
        )

    @handles(OrderShipped)
    def _on_order_shipped(self, event: OrderShipped) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={"status": "shipped", "tracking_number": event.tracking_number}
            )

    def create(self, customer_id: UUID) -> OrderCreated:
        return self.create_event(OrderCreated, customer_id=customer_id)

    def ship(self, tracking_number: str) -> OrderShipped:
        return self.create_event(OrderShipped, tracking_number=tracking_number)
