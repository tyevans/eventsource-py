"""Shared fixtures, events, and aggregate implementations for test_aggregate_root."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field

from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent

# =============================================================================
# Test fixtures: State models and Events
# =============================================================================


class CounterState(BaseModel):
    """Simple state for testing."""

    counter_id: UUID
    value: int = 0
    name: str = ""


class OrderState(BaseModel):
    """More complex state model for comprehensive testing."""

    order_id: UUID
    customer_id: UUID | None = None
    status: str = "draft"
    total: float = 0.0
    items: list[str] = Field(default_factory=list)


class CounterIncremented(DomainEvent):
    """Event for incrementing counter."""

    aggregate_type: str = "Counter"
    increment: int = 1


class CounterDecremented(DomainEvent):
    """Event for decrementing counter."""

    aggregate_type: str = "Counter"
    decrement: int = 1


class CounterNamed(DomainEvent):
    """Event for naming counter."""

    aggregate_type: str = "Counter"
    name: str


class CounterReset(DomainEvent):
    """Event for resetting counter."""

    aggregate_type: str = "Counter"


class OrderCreated(DomainEvent):
    """Event for order creation."""

    aggregate_type: str = "Order"
    customer_id: UUID


class OrderItemAdded(DomainEvent):
    """Event for adding item to order."""

    aggregate_type: str = "Order"
    item_name: str
    price: float


class OrderShipped(DomainEvent):
    """Event for shipping order."""

    aggregate_type: str = "Order"
    tracking_number: str


# =============================================================================
# Test Aggregate Implementations
# =============================================================================


class CounterAggregate(AggregateRoot[CounterState]):
    """Simple counter aggregate for testing basic functionality."""

    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(
                update={"value": self._state.value + event.increment}
            )
        elif isinstance(event, CounterDecremented):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(
                update={"value": self._state.value - event.decrement}
            )
        elif isinstance(event, CounterNamed):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(update={"name": event.name})
        elif isinstance(event, CounterReset):
            self._state = self._get_initial_state()

    def increment(self, amount: int = 1) -> None:
        """Command: Increment the counter."""
        event = CounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            increment=amount,
        )
        self.apply_event(event)

    def decrement(self, amount: int = 1) -> None:
        """Command: Decrement the counter."""
        event = CounterDecremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            decrement=amount,
        )
        self.apply_event(event)

    def set_name(self, name: str) -> None:
        """Command: Name the counter."""
        event = CounterNamed(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            name=name,
        )
        self.apply_event(event)

    def reset(self) -> None:
        """Command: Reset the counter."""
        event = CounterReset(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


class OrderAggregate(AggregateRoot[OrderState]):
    """Order aggregate for testing complex state management."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                status="created",
            )
        elif isinstance(event, OrderItemAdded):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "items": [*self._state.items, event.item_name],
                        "total": self._state.total + event.price,
                    }
                )
        elif isinstance(event, OrderShipped) and self._state:
            self._state = self._state.model_copy(update={"status": "shipped"})

    def create(self, customer_id: UUID) -> None:
        """Command: Create the order."""
        if self.version > 0:
            raise ValueError("Order already exists")
        event = OrderCreated(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            customer_id=customer_id,
        )
        self.apply_event(event)

    def add_item(self, item_name: str, price: float) -> None:
        """Command: Add an item to the order."""
        if not self._state or self._state.status == "shipped":
            raise ValueError("Cannot add items to this order")
        event = OrderItemAdded(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            item_name=item_name,
            price=price,
        )
        self.apply_event(event)

    def ship(self, tracking_number: str) -> None:
        """Command: Ship the order."""
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot ship order in current state")
        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            tracking_number=tracking_number,
        )
        self.apply_event(event)


# =============================================================================
# DeclarativeAggregate Test Implementation
# =============================================================================


class DeclarativeCounterAggregate(DeclarativeAggregate[CounterState]):
    """Counter using declarative pattern with @handles decorator."""

    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    @handles(CounterIncremented)
    def _on_counter_incremented(self, event: CounterIncremented) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"value": self._state.value + event.increment})

    @handles(CounterDecremented)
    def _on_counter_decremented(self, event: CounterDecremented) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"value": self._state.value - event.decrement})

    @handles(CounterNamed)
    def _on_counter_named(self, event: CounterNamed) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"name": event.name})

    @handles(CounterReset)
    def _on_counter_reset(self, event: CounterReset) -> None:
        self._state = self._get_initial_state()

    def increment(self, amount: int = 1) -> None:
        """Command: Increment the counter."""
        event = CounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            increment=amount,
        )
        self._raise_event(event)

    def decrement(self, amount: int = 1) -> None:
        """Command: Decrement the counter."""
        event = CounterDecremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            decrement=amount,
        )
        self._raise_event(event)


# =============================================================================
# Lenient Aggregate Implementation (TD-004)
# =============================================================================


class LenientCounterAggregate(CounterAggregate):
    """Counter aggregate with version validation disabled for testing."""

    validate_versions = False
