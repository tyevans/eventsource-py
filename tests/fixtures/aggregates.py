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

from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.command import DomainCommand
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
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
# Order Aggregate (Complex state management, decider style)
# =============================================================================


class CreateOrder(DomainCommand):
    """Request to create an order for a customer."""

    customer_id: UUID


class AddOrderItem(DomainCommand):
    """Request to add an item to an order."""

    item_name: str
    price: float


class ShipOrder(DomainCommand):
    """Request to ship an order with a tracking number."""

    tracking_number: str


class OrderAggregate(DeciderAggregate[OrderState]):
    """
    Order aggregate for testing complex state management scenarios.

    Features:
    - Lifecycle: draft -> created -> shipped
    - Nested state: items list, running total
    - Business rule validation

    Implemented in the decider style (``decide``/``evolve``); the original
    ``create``/``add_item``/``ship`` methods survive as thin wrappers over
    ``execute`` so existing tests are unaffected.
    """

    aggregate_type = "Order"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> OrderState:
        """Return the initial state for a new order."""
        return OrderState(order_id=aggregate_id)

    @staticmethod
    def decide(command: object, state: OrderState) -> list[DomainEvent]:
        """Given current state, return the events a command produces, or raise."""
        match command, state:
            case CreateOrder(customer_id=customer_id), OrderState(status="draft"):
                return [
                    OrderCreated(
                        aggregate_id=state.order_id,
                        customer_id=customer_id,
                    )
                ]
            case CreateOrder(), _:
                # Behavior-preserving: original guard was `self.version > 0`.
                raise ValueError("Order already exists")
            case AddOrderItem(item_name=item_name, price=price), OrderState(status=status) if (
                status not in ("draft", "shipped")
            ):
                return [
                    OrderItemAdded(
                        aggregate_id=state.order_id,
                        item_name=item_name,
                        price=price,
                    )
                ]
            case AddOrderItem(), _:
                # Behavior-preserving: original guard was
                # `not self._state or self._state.status == "shipped"`.
                raise ValueError("Cannot add items to this order")
            case ShipOrder(tracking_number=tracking_number), OrderState(status="created"):
                return [
                    OrderShipped(
                        aggregate_id=state.order_id,
                        tracking_number=tracking_number,
                    )
                ]
            case ShipOrder(), _:
                # Behavior-preserving: original guard was
                # `not self._state or self._state.status != "created"`.
                raise ValueError("Cannot ship order in current state")
            case _:
                raise ValueError(f"unknown command: {command!r}")

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        """Return the next state after an event."""
        match event:
            case OrderCreated(customer_id=customer_id):
                return state.model_copy(update={"customer_id": customer_id, "status": "created"})
            case OrderItemAdded(item_name=item_name, price=price):
                return state.model_copy(
                    update={
                        "items": [*state.items, item_name],
                        "total": state.total + price,
                    }
                )
            case OrderShipped():
                return state.model_copy(update={"status": "shipped"})
            case _:
                return state

    def create(self, customer_id: UUID) -> None:
        """Command: Create the order for a customer."""
        self.execute(CreateOrder(customer_id=customer_id))

    def add_item(self, item_name: str, price: float) -> None:
        """Command: Add an item to the order."""
        self.execute(AddOrderItem(item_name=item_name, price=price))

    def ship(self, tracking_number: str) -> None:
        """Command: Ship the order with a tracking number."""
        self.execute(ShipOrder(tracking_number=tracking_number))
