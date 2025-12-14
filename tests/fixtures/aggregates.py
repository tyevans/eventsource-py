"""
Shared test aggregate implementations and state models.

This module provides reusable aggregate implementations for testing:
- CounterAggregate: Simple aggregate with increment/decrement operations
- DeclarativeCounterAggregate: Same as CounterAggregate using @handles decorator
- OrderAggregate: Complex aggregate with lifecycle (create, add items, ship)

Also provides state models:
- CounterState: State for counter aggregates
- OrderState: State for order aggregates
"""

from uuid import UUID

from pydantic import BaseModel, Field

from eventsource.aggregates.base import AggregateRoot, DeclarativeAggregate
from eventsource.events.base import DomainEvent
from eventsource.handlers import handles
from tests.fixtures.events import (
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
)

# =============================================================================
# State Models
# =============================================================================


class CounterState(BaseModel):
    """State model for counter aggregates."""

    counter_id: UUID
    value: int = 0
    name: str = ""


class OrderState(BaseModel):
    """State model for order aggregates with complex nested data."""

    order_id: UUID
    customer_id: UUID | None = None
    status: str = "draft"
    total: float = 0.0
    items: list[str] = Field(default_factory=list)


# =============================================================================
# Counter Aggregate (AggregateRoot-based)
# =============================================================================


class CounterAggregate(AggregateRoot[CounterState]):
    """
    Simple counter aggregate for testing basic event sourcing patterns.

    Uses manual event dispatch via isinstance checks.
    Good for testing AggregateRoot base class functionality.
    """

    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        """Return the initial state for a new counter."""
        return CounterState(counter_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        """Apply an event to update aggregate state."""
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
        """Command: Increment the counter by the specified amount."""
        event = CounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            increment=amount,
        )
        self.apply_event(event)

    def decrement(self, amount: int = 1) -> None:
        """Command: Decrement the counter by the specified amount."""
        event = CounterDecremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            decrement=amount,
        )
        self.apply_event(event)

    def set_name(self, name: str) -> None:
        """Command: Set the counter's name."""
        event = CounterNamed(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            name=name,
        )
        self.apply_event(event)

    def reset(self) -> None:
        """Command: Reset the counter to zero."""
        event = CounterReset(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Declarative Counter Aggregate (using @handles decorator)
# =============================================================================


class DeclarativeCounterAggregate(DeclarativeAggregate[CounterState]):
    """
    Counter aggregate using declarative pattern with @handles decorator.

    Good for testing DeclarativeAggregate functionality and handler registration.
    """

    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        """Return the initial state for a new counter."""
        return CounterState(counter_id=self.aggregate_id)

    @handles(CounterIncremented)
    def _on_counter_incremented(self, event: CounterIncremented) -> None:
        """Handle counter increment event."""
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"value": self._state.value + event.increment})

    @handles(CounterDecremented)
    def _on_counter_decremented(self, event: CounterDecremented) -> None:
        """Handle counter decrement event."""
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"value": self._state.value - event.decrement})

    @handles(CounterNamed)
    def _on_counter_named(self, event: CounterNamed) -> None:
        """Handle counter named event."""
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"name": event.name})

    @handles(CounterReset)
    def _on_counter_reset(self, event: CounterReset) -> None:
        """Handle counter reset event."""
        self._state = self._get_initial_state()

    def increment(self, amount: int = 1) -> None:
        """Command: Increment the counter by the specified amount."""
        event = CounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            increment=amount,
        )
        self._raise_event(event)

    def decrement(self, amount: int = 1) -> None:
        """Command: Decrement the counter by the specified amount."""
        event = CounterDecremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            decrement=amount,
        )
        self._raise_event(event)


# =============================================================================
# Order Aggregate (Complex state management)
# =============================================================================


class OrderAggregate(AggregateRoot[OrderState]):
    """
    Order aggregate for testing complex state management scenarios.

    Features:
    - Lifecycle: draft -> created -> shipped
    - Nested state: items list, running total
    - Business rule validation
    """

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        """Return the initial state for a new order."""
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        """Apply an event to update aggregate state."""
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
        """Command: Create the order for a customer."""
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
        """Command: Ship the order with a tracking number."""
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot ship order in current state")
        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            tracking_number=tracking_number,
        )
        self.apply_event(event)
