"""Shared fixtures and models for aggregate snapshot method tests."""

from datetime import datetime

from pydantic import BaseModel, Field

from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent

# =============================================================================
# Test State Models
# =============================================================================


class OrderItem(BaseModel):
    """Nested model for testing complex serialization."""

    product_id: str
    quantity: int
    price: float


class OrderState(BaseModel):
    """Complex state with nested models, lists, and optional fields."""

    order_id: str
    customer_id: str
    status: str = "pending"
    items: list[OrderItem] = Field(default_factory=list)
    total: float = 0.0
    created_at: datetime | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class SimpleState(BaseModel):
    """Simple state for basic tests."""

    value: str = ""
    count: int = 0


# =============================================================================
# Test Events
# =============================================================================


class OrderCreated(DomainEvent):
    """Event for order creation."""

    aggregate_type: str = "Order"
    customer_id: str


class ItemAdded(DomainEvent):
    """Event for adding item to order."""

    aggregate_type: str = "Order"
    product_id: str
    quantity: int
    price: float


class StatusChanged(DomainEvent):
    """Event for status change."""

    aggregate_type: str = "Order"
    new_status: str


class ValueSet(DomainEvent):
    """Simple event for value setting."""

    aggregate_type: str = "Simple"
    value: str


# =============================================================================
# Test Aggregates
# =============================================================================


class OrderAggregate(AggregateRoot[OrderState]):
    """Order aggregate for testing complex state serialization."""

    aggregate_type = "Order"
    schema_version = 1

    def _get_initial_state(self) -> OrderState:
        return OrderState(
            order_id=str(self.aggregate_id),
            customer_id="",
        )

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=str(self.aggregate_id),
                customer_id=event.customer_id,
                status="created",
                created_at=event.occurred_at,
            )
        elif isinstance(event, ItemAdded):
            if self._state:
                item = OrderItem(
                    product_id=event.product_id,
                    quantity=event.quantity,
                    price=event.price,
                )
                self._state = self._state.model_copy(
                    update={
                        "items": [*self._state.items, item],
                        "total": self._state.total + (event.quantity * event.price),
                    }
                )
        elif isinstance(event, StatusChanged) and self._state:
            self._state = self._state.model_copy(update={"status": event.new_status})


class SimpleAggregate(AggregateRoot[SimpleState]):
    """Simple aggregate for basic tests."""

    aggregate_type = "Simple"

    def _get_initial_state(self) -> SimpleState:
        return SimpleState()

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, ValueSet):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(
                update={
                    "value": event.value,
                    "count": self._state.count + 1,
                }
            )


class VersionedAggregate(AggregateRoot[SimpleState]):
    """Aggregate with custom schema version."""

    aggregate_type = "Versioned"
    schema_version = 5

    def _get_initial_state(self) -> SimpleState:
        return SimpleState()

    def _apply(self, event: DomainEvent) -> None:
        pass


class DeclarativeOrderAggregate(DeclarativeAggregate[OrderState]):
    """Declarative aggregate for testing _get_state_type with inheritance."""

    aggregate_type = "Order"
    schema_version = 2

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id="", customer_id="")

    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=str(self.aggregate_id),
            customer_id=event.customer_id,
            status="created",
        )
