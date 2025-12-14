"""
Comprehensive unit tests for AggregateRoot and DeclarativeAggregate.

Tests cover:
- Basic aggregate initialization and properties
- Event application (new and historical)
- Version tracking and optimistic concurrency support
- State management
- Uncommitted event tracking
- Event sourcing (reconstituting from events)
- DeclarativeAggregate with @handles decorator
- Edge cases and error conditions
- Event version validation (TD-004)
"""

import logging
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, Field

from eventsource.aggregates.base import (
    AggregateRoot,
    DeclarativeAggregate,
)
from eventsource.events.base import DomainEvent
from eventsource.exceptions import EventVersionError, UnhandledEventError

# Use canonical import for @handles (TD-006)
from eventsource.handlers import handles

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

    event_type: str = "CounterIncremented"
    aggregate_type: str = "Counter"
    increment: int = 1


class CounterDecremented(DomainEvent):
    """Event for decrementing counter."""

    event_type: str = "CounterDecremented"
    aggregate_type: str = "Counter"
    decrement: int = 1


class CounterNamed(DomainEvent):
    """Event for naming counter."""

    event_type: str = "CounterNamed"
    aggregate_type: str = "Counter"
    name: str


class CounterReset(DomainEvent):
    """Event for resetting counter."""

    event_type: str = "CounterReset"
    aggregate_type: str = "Counter"


class OrderCreated(DomainEvent):
    """Event for order creation."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID


class OrderItemAdded(DomainEvent):
    """Event for adding item to order."""

    event_type: str = "OrderItemAdded"
    aggregate_type: str = "Order"
    item_name: str
    price: float


class OrderShipped(DomainEvent):
    """Event for shipping order."""

    event_type: str = "OrderShipped"
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
# Test Classes
# =============================================================================


class TestAggregateRootInitialization:
    """Tests for aggregate initialization."""

    def test_new_aggregate_has_correct_id(self) -> None:
        """New aggregate should have the ID passed to constructor."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        assert aggregate.aggregate_id == agg_id

    def test_new_aggregate_has_version_zero(self) -> None:
        """New aggregate should start at version 0."""
        aggregate = CounterAggregate(uuid4())
        assert aggregate.version == 0

    def test_new_aggregate_has_no_uncommitted_events(self) -> None:
        """New aggregate should have no uncommitted events."""
        aggregate = CounterAggregate(uuid4())
        assert aggregate.uncommitted_events == []
        assert not aggregate.has_uncommitted_events

    def test_new_aggregate_has_none_state(self) -> None:
        """New aggregate should have None state until events are applied."""
        aggregate = CounterAggregate(uuid4())
        assert aggregate.state is None

    def test_aggregate_type_is_set(self) -> None:
        """Aggregate should have correct aggregate_type class attribute."""
        aggregate = CounterAggregate(uuid4())
        assert aggregate.aggregate_type == "Counter"

        order_aggregate = OrderAggregate(uuid4())
        assert order_aggregate.aggregate_type == "Order"


class TestEventApplication:
    """Tests for applying events to aggregates."""

    def test_apply_event_updates_version(self) -> None:
        """Applying event should update version to match event's aggregate_version."""
        aggregate = CounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        aggregate.apply_event(event)
        assert aggregate.version == 1

    def test_apply_event_updates_state(self) -> None:
        """Applying event should update aggregate state."""
        aggregate = CounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        aggregate.apply_event(event)
        assert aggregate.state is not None
        assert aggregate.state.value == 5

    def test_apply_event_tracks_uncommitted_by_default(self) -> None:
        """Applying event with is_new=True should track it as uncommitted."""
        aggregate = CounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        aggregate.apply_event(event, is_new=True)
        assert len(aggregate.uncommitted_events) == 1
        assert aggregate.uncommitted_events[0] == event
        assert aggregate.has_uncommitted_events

    def test_apply_event_does_not_track_historical(self) -> None:
        """Applying event with is_new=False should not track it as uncommitted."""
        aggregate = CounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        aggregate.apply_event(event, is_new=False)
        assert len(aggregate.uncommitted_events) == 0
        assert not aggregate.has_uncommitted_events

    def test_apply_multiple_events(self) -> None:
        """Applying multiple events should update state correctly."""
        aggregate = CounterAggregate(uuid4())

        for i in range(1, 4):
            event = CounterIncremented(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Counter",
                aggregate_version=i,
                increment=10,
            )
            aggregate.apply_event(event)

        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 30
        assert len(aggregate.uncommitted_events) == 3

    def test_uncommitted_events_returns_copy(self) -> None:
        """uncommitted_events should return a copy to prevent external modification."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        events = aggregate.uncommitted_events
        events.clear()  # Modify the returned list

        # Original should be unchanged
        assert len(aggregate.uncommitted_events) == 1


class TestVersionTracking:
    """Tests for version tracking and optimistic concurrency support."""

    def test_get_next_version_for_new_aggregate(self) -> None:
        """get_next_version should return 1 for new aggregate."""
        aggregate = CounterAggregate(uuid4())
        assert aggregate.get_next_version() == 1

    def test_get_next_version_increments(self) -> None:
        """get_next_version should return version + 1."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        assert aggregate.version == 1
        assert aggregate.get_next_version() == 2

        aggregate.increment(3)
        assert aggregate.version == 2
        assert aggregate.get_next_version() == 3

    def test_version_matches_event_count(self) -> None:
        """Version should match the number of events applied."""
        aggregate = CounterAggregate(uuid4())

        for _i in range(5):
            aggregate.increment(1)

        assert aggregate.version == 5


class TestUncommittedEventManagement:
    """Tests for uncommitted event tracking and clearing."""

    def test_mark_events_as_committed_clears_uncommitted(self) -> None:
        """mark_events_as_committed should clear all uncommitted events."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)
        assert len(aggregate.uncommitted_events) == 2

        aggregate.mark_events_as_committed()

        assert len(aggregate.uncommitted_events) == 0
        assert not aggregate.has_uncommitted_events

    def test_mark_events_as_committed_preserves_state(self) -> None:
        """mark_events_as_committed should not affect aggregate state or version."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        state_before = aggregate.state
        version_before = aggregate.version

        aggregate.mark_events_as_committed()

        assert aggregate.state == state_before
        assert aggregate.version == version_before

    def test_clear_uncommitted_events_returns_events(self) -> None:
        """clear_uncommitted_events should return the events before clearing."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        events = aggregate.clear_uncommitted_events()

        assert len(events) == 2
        assert len(aggregate.uncommitted_events) == 0

    def test_can_add_new_events_after_commit(self) -> None:
        """After committing, new events should be tracked normally."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.mark_events_as_committed()

        aggregate.increment(3)

        assert len(aggregate.uncommitted_events) == 1
        assert aggregate.version == 2


class TestLoadFromHistory:
    """Tests for reconstituting aggregate from event history."""

    def test_load_from_history_replays_events(self) -> None:
        """load_from_history should replay all events to rebuild state."""
        agg_id = uuid4()

        # Create events as if they were from event store
        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=1,
                increment=10,
            ),
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=2,
                increment=5,
            ),
            CounterDecremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=3,
                decrement=3,
            ),
        ]

        # Create new aggregate and load history
        aggregate = CounterAggregate(agg_id)
        aggregate.load_from_history(events)

        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 12  # 10 + 5 - 3

    def test_load_from_history_does_not_create_uncommitted_events(self) -> None:
        """Events loaded from history should not be uncommitted."""
        agg_id = uuid4()

        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=1,
                increment=10,
            ),
        ]

        aggregate = CounterAggregate(agg_id)
        aggregate.load_from_history(events)

        assert len(aggregate.uncommitted_events) == 0
        assert not aggregate.has_uncommitted_events

    def test_load_from_empty_history(self) -> None:
        """Loading empty history should leave aggregate at initial state."""
        aggregate = CounterAggregate(uuid4())
        aggregate.load_from_history([])

        assert aggregate.version == 0
        assert aggregate.state is None

    def test_can_apply_new_events_after_loading_history(self) -> None:
        """After loading history, new events should continue from correct version."""
        agg_id = uuid4()

        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=1,
                increment=10,
            ),
        ]

        aggregate = CounterAggregate(agg_id)
        aggregate.load_from_history(events)

        # Now apply new event
        aggregate.increment(5)

        assert aggregate.version == 2
        assert aggregate.state is not None
        assert aggregate.state.value == 15
        assert len(aggregate.uncommitted_events) == 1


class TestCommandMethods:
    """Tests for aggregate command methods."""

    def test_command_creates_and_applies_event(self) -> None:
        """Command methods should create and apply events."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(7)

        assert aggregate.version == 1
        assert aggregate.state is not None
        assert aggregate.state.value == 7

    def test_command_uses_correct_version(self) -> None:
        """Commands should use get_next_version for event creation."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        events = aggregate.uncommitted_events
        assert events[0].aggregate_version == 1
        assert events[1].aggregate_version == 2

    def test_command_validation(self) -> None:
        """Commands should validate business rules."""
        aggregate = OrderAggregate(uuid4())

        # Cannot create twice
        aggregate.create(uuid4())
        with pytest.raises(ValueError, match="Order already exists"):
            aggregate.create(uuid4())

    def test_command_state_validation(self) -> None:
        """Commands should validate state preconditions."""
        aggregate = OrderAggregate(uuid4())

        # Cannot add items without creating first
        with pytest.raises(ValueError, match="Cannot add items"):
            aggregate.add_item("Widget", 10.0)


class TestComplexStateManagement:
    """Tests for complex state with nested structures."""

    def test_order_lifecycle(self) -> None:
        """Test complete order lifecycle with multiple event types."""
        customer_id = uuid4()
        aggregate = OrderAggregate(uuid4())

        # Create order
        aggregate.create(customer_id)
        assert aggregate.state is not None
        assert aggregate.state.status == "created"
        assert aggregate.state.customer_id == customer_id

        # Add items
        aggregate.add_item("Widget A", 10.0)
        aggregate.add_item("Widget B", 15.0)
        assert aggregate.state.items == ["Widget A", "Widget B"]
        assert aggregate.state.total == 25.0

        # Ship order
        aggregate.ship("TRACK123")
        assert aggregate.state.status == "shipped"

        # Verify final state
        assert aggregate.version == 4
        assert len(aggregate.uncommitted_events) == 4

    def test_order_reconstitution(self) -> None:
        """Test reconstituting complex order from history."""
        agg_id = uuid4()
        customer_id = uuid4()

        events: list[DomainEvent] = [
            OrderCreated(
                aggregate_id=agg_id,
                aggregate_type="Order",
                aggregate_version=1,
                customer_id=customer_id,
            ),
            OrderItemAdded(
                aggregate_id=agg_id,
                aggregate_type="Order",
                aggregate_version=2,
                item_name="Widget",
                price=25.0,
            ),
            OrderShipped(
                aggregate_id=agg_id,
                aggregate_type="Order",
                aggregate_version=3,
                tracking_number="TRACK456",
            ),
        ]

        aggregate = OrderAggregate(agg_id)
        aggregate.load_from_history(events)

        assert aggregate.state is not None
        assert aggregate.state.customer_id == customer_id
        assert aggregate.state.status == "shipped"
        assert aggregate.state.items == ["Widget"]
        assert aggregate.state.total == 25.0


class TestRaiseEventMethod:
    """Tests for the _raise_event convenience method."""

    def test_raise_event_applies_and_tracks(self) -> None:
        """_raise_event should apply event and track as uncommitted."""
        aggregate = DeclarativeCounterAggregate(uuid4())
        aggregate.increment(5)

        assert aggregate.version == 1
        assert aggregate.state is not None
        assert aggregate.state.value == 5
        assert len(aggregate.uncommitted_events) == 1


class TestDeclarativeAggregate:
    """Tests for DeclarativeAggregate with @handles decorator."""

    def test_handlers_are_registered(self) -> None:
        """Handlers marked with @handles should be registered."""
        aggregate = DeclarativeCounterAggregate(uuid4())

        # Check that handlers are registered
        assert CounterIncremented in aggregate._event_handlers
        assert CounterDecremented in aggregate._event_handlers
        assert CounterNamed in aggregate._event_handlers
        assert CounterReset in aggregate._event_handlers

    def test_declarative_apply_calls_correct_handler(self) -> None:
        """_apply should dispatch to the correct handler based on event type."""
        aggregate = DeclarativeCounterAggregate(uuid4())

        # Apply increment
        event1 = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=10,
        )
        aggregate.apply_event(event1)
        assert aggregate.state is not None
        assert aggregate.state.value == 10

        # Apply decrement
        event2 = CounterDecremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=2,
            decrement=3,
        )
        aggregate.apply_event(event2)
        assert aggregate.state.value == 7

    def test_declarative_aggregate_command_methods(self) -> None:
        """Declarative aggregate command methods should work correctly."""
        aggregate = DeclarativeCounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)
        aggregate.decrement(2)

        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 6

    def test_unhandled_event_is_ignored(self) -> None:
        """Events without handlers should be silently ignored."""
        aggregate = DeclarativeCounterAggregate(uuid4())

        # Create an event type that has no handler registered
        class UnhandledEvent(DomainEvent):
            event_type: str = "UnhandledEvent"
            aggregate_type: str = "Counter"

        event = UnhandledEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        # Should not raise, just ignore
        aggregate.apply_event(event)
        assert aggregate.version == 1

    def test_declarative_aggregate_reconstitution(self) -> None:
        """Declarative aggregate should reconstitute from history correctly."""
        agg_id = uuid4()

        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=1,
                increment=10,
            ),
            CounterDecremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=2,
                decrement=3,
            ),
            CounterNamed(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=3,
                name="MyCounter",
            ),
        ]

        aggregate = DeclarativeCounterAggregate(agg_id)
        aggregate.load_from_history(events)

        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 7
        assert aggregate.state.name == "MyCounter"


class TestAggregateEquality:
    """Tests for aggregate equality and hashing."""

    def test_equality_based_on_id(self) -> None:
        """Aggregates with same ID should be equal."""
        agg_id = uuid4()
        agg1 = CounterAggregate(agg_id)
        agg2 = CounterAggregate(agg_id)

        assert agg1 == agg2

    def test_inequality_for_different_ids(self) -> None:
        """Aggregates with different IDs should not be equal."""
        agg1 = CounterAggregate(uuid4())
        agg2 = CounterAggregate(uuid4())

        assert agg1 != agg2

    def test_inequality_with_non_aggregate(self) -> None:
        """Aggregate should not be equal to non-aggregate objects."""
        aggregate = CounterAggregate(uuid4())

        assert aggregate != "not an aggregate"
        assert aggregate != 123
        assert aggregate is not None

    def test_hash_based_on_id(self) -> None:
        """Aggregates with same ID should have same hash."""
        agg_id = uuid4()
        agg1 = CounterAggregate(agg_id)
        agg2 = CounterAggregate(agg_id)

        assert hash(agg1) == hash(agg2)

    def test_can_use_aggregate_in_set(self) -> None:
        """Aggregates should be usable in sets."""
        agg_id = uuid4()
        agg1 = CounterAggregate(agg_id)
        agg2 = CounterAggregate(agg_id)
        agg3 = CounterAggregate(uuid4())

        aggregate_set = {agg1, agg2, agg3}
        assert len(aggregate_set) == 2


class TestAggregateRepr:
    """Tests for aggregate string representation."""

    def test_repr_shows_key_info(self) -> None:
        """__repr__ should show class name, id, version, and uncommitted count."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)

        repr_str = repr(aggregate)

        assert "CounterAggregate" in repr_str
        assert str(agg_id) in repr_str
        assert "version=1" in repr_str
        assert "uncommitted=1" in repr_str

    def test_repr_for_new_aggregate(self) -> None:
        """__repr__ for new aggregate should show version=0, uncommitted=0."""
        aggregate = CounterAggregate(uuid4())
        repr_str = repr(aggregate)

        assert "version=0" in repr_str
        assert "uncommitted=0" in repr_str


class TestImmutableStatePatterns:
    """Tests demonstrating immutable state update patterns."""

    def test_state_uses_model_copy_for_updates(self) -> None:
        """State updates should use model_copy to maintain immutability."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        state1 = aggregate.state
        aggregate.increment(3)
        state2 = aggregate.state

        # States should be different objects
        assert state1 is not None
        assert state2 is not None
        assert state1 is not state2
        assert state1.value == 5
        assert state2.value == 8

    def test_list_updates_create_new_lists(self) -> None:
        """List updates should create new lists, not mutate."""
        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())
        aggregate.add_item("Widget A", 10.0)

        assert aggregate.state is not None
        items1 = aggregate.state.items
        aggregate.add_item("Widget B", 15.0)
        assert aggregate.state is not None
        items2 = aggregate.state.items

        # Lists should be different objects
        assert items1 is not items2
        assert items1 == ["Widget A"]
        assert items2 == ["Widget A", "Widget B"]


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_large_number_of_events(self) -> None:
        """Aggregate should handle many events."""
        aggregate = CounterAggregate(uuid4())

        for _ in range(1000):
            aggregate.increment(1)

        assert aggregate.version == 1000
        assert aggregate.state is not None
        assert aggregate.state.value == 1000

    def test_reconstitute_large_history(self) -> None:
        """load_from_history should handle many events efficiently."""
        agg_id = uuid4()
        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=i + 1,
                increment=1,
            )
            for i in range(1000)
        ]

        aggregate = CounterAggregate(agg_id)
        aggregate.load_from_history(events)

        assert aggregate.version == 1000
        assert aggregate.state is not None
        assert aggregate.state.value == 1000

    def test_reset_aggregate_state(self) -> None:
        """Aggregate should support state reset via events."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(100)
        aggregate.increment(50)
        assert aggregate.state is not None
        assert aggregate.state.value == 150

        aggregate.reset()
        assert aggregate.state is not None
        assert aggregate.state.value == 0
        assert aggregate.version == 3


class TestIntegrationWithDomainEvent:
    """Tests for integration with DomainEvent features."""

    def test_events_have_correct_metadata(self) -> None:
        """Events created by commands should have correct aggregate metadata."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        event = aggregate.uncommitted_events[0]
        assert event.aggregate_id == aggregate.aggregate_id
        assert event.aggregate_type == "Counter"
        assert event.aggregate_version == 1

    def test_events_have_unique_event_ids(self) -> None:
        """Each event should have a unique event_id."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        events = aggregate.uncommitted_events
        assert events[0].event_id != events[1].event_id

    def test_events_have_occurred_at_timestamps(self) -> None:
        """Events should have occurred_at timestamps."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        event = aggregate.uncommitted_events[0]
        assert event.occurred_at is not None


# =============================================================================
# TD-004: Event Version Validation Tests
# =============================================================================


class LenientCounterAggregate(CounterAggregate):
    """Counter aggregate with version validation disabled for testing."""

    validate_versions = False


class TestEventVersionValidation:
    """Tests for event version validation feature (TD-004).

    These tests verify:
    - AC1: Events with version gaps raise EventVersionError
    - AC2: Events with version regression raise EventVersionError
    - AC3: Historical replay (is_new=False) works without validation
    - AC4: validate_versions=False disables validation
    - AC5: Error messages include expected vs actual version
    - AC6: Warning logged when validation disabled but versions mismatch
    """

    def test_version_gap_raises_error(self) -> None:
        """AC1: Skipping versions raises EventVersionError."""
        aggregate = CounterAggregate(uuid4())
        # Create event with version 3, but aggregate is at version 0, so expected is 1
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=3,  # Expected: 1
            increment=5,
        )

        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(event)

        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 3
        assert exc_info.value.aggregate_id == aggregate.aggregate_id
        assert exc_info.value.event_id == event.event_id

    def test_version_regression_raises_error(self) -> None:
        """AC2: Going backward in versions raises EventVersionError."""
        aggregate = CounterAggregate(uuid4())
        # First, apply a valid event at version 1
        event1 = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        aggregate.apply_event(event1)
        assert aggregate.version == 1

        # Try to apply an event with the same version (regression)
        event2 = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,  # Expected: 2
            increment=3,
        )

        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(event2)

        assert exc_info.value.expected_version == 2
        assert exc_info.value.actual_version == 1

    def test_version_zero_event_raises_error(self) -> None:
        """Version 0 event should raise EventVersionError (version must be >= 1)."""
        aggregate = CounterAggregate(uuid4())
        # This test demonstrates that version 0 would be caught as wrong
        # because expected version for a new aggregate is 1
        # Note: DomainEvent validates aggregate_version >= 1, so we test with version 2
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=2,  # Expected: 1
            increment=5,
        )

        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(event)

        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 2

    def test_historical_replay_skips_validation(self) -> None:
        """AC3: Events applied with is_new=False skip validation."""
        aggregate = CounterAggregate(uuid4())
        # Create events with gaps and regressions - would fail validation
        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Counter",
                aggregate_version=1,
                increment=10,
            ),
            CounterIncremented(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Counter",
                aggregate_version=5,  # Gap! Expected would be 2
                increment=5,
            ),
            CounterIncremented(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Counter",
                aggregate_version=3,  # Regression! Expected would be 6
                increment=3,
            ),
        ]

        # Should not raise - validation skipped for replay
        aggregate.load_from_history(events)

        # State should reflect all events applied
        assert aggregate.version == 3  # Last event's version
        assert aggregate.state is not None
        assert aggregate.state.value == 18  # 10 + 5 + 3

    def test_validation_disabled_accepts_any_version(self) -> None:
        """AC4: validate_versions=False allows any version."""
        aggregate = LenientCounterAggregate(uuid4())
        # Create event with wrong version
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=99,  # Expected: 1
            increment=5,
        )

        # Should not raise
        aggregate.apply_event(event)

        assert aggregate.version == 99
        assert aggregate.state is not None
        assert aggregate.state.value == 5

    def test_error_message_includes_version_details(self) -> None:
        """AC5: Error messages include expected vs actual version."""
        aggregate = CounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=5,
            increment=5,
        )

        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(event)

        error_message = str(exc_info.value)
        assert "expected version 1" in error_message
        assert "got 5" in error_message
        assert str(aggregate.aggregate_id) in error_message
        assert str(event.event_id) in error_message

    def test_validation_disabled_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """AC6: Mismatched versions log warning when validation disabled."""
        aggregate = LenientCounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=99,  # Expected: 1
            increment=5,
        )

        with caplog.at_level(logging.WARNING):
            aggregate.apply_event(event)

        # Verify warning was logged
        assert "Version mismatch" in caplog.text
        assert "validation disabled" in caplog.text
        assert "expected 1" in caplog.text
        assert "got 99" in caplog.text

    def test_correct_version_does_not_log_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Correct version should not log any warning even with validation disabled."""
        aggregate = LenientCounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,  # Correct version
            increment=5,
        )

        with caplog.at_level(logging.WARNING):
            aggregate.apply_event(event)

        # No warning should be logged for correct version
        assert "Version mismatch" not in caplog.text

    def test_validation_enabled_by_default(self) -> None:
        """Version validation should be enabled by default."""
        aggregate = CounterAggregate(uuid4())
        assert aggregate.validate_versions is True

    def test_command_methods_work_with_validation(self) -> None:
        """Command methods should work correctly with version validation enabled."""
        aggregate = CounterAggregate(uuid4())

        # Commands should automatically use correct versions
        aggregate.increment(5)
        assert aggregate.version == 1

        aggregate.increment(3)
        assert aggregate.version == 2

        aggregate.decrement(2)
        assert aggregate.version == 3

        assert aggregate.state is not None
        assert aggregate.state.value == 6

    def test_declarative_aggregate_validation(self) -> None:
        """DeclarativeAggregate should also support version validation."""
        aggregate = DeclarativeCounterAggregate(uuid4())

        # Test that validation works on declarative aggregates
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=5,  # Wrong version
            increment=5,
        )

        with pytest.raises(EventVersionError):
            aggregate.apply_event(event)

    def test_version_validation_after_history_load(self) -> None:
        """Version validation should work correctly after loading history."""
        agg_id = uuid4()
        # Load some history
        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=1,
                increment=10,
            ),
            CounterIncremented(
                aggregate_id=agg_id,
                aggregate_type="Counter",
                aggregate_version=2,
                increment=5,
            ),
        ]

        aggregate = CounterAggregate(agg_id)
        aggregate.load_from_history(events)
        assert aggregate.version == 2

        # Now apply a new event with wrong version
        bad_event = CounterIncremented(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            aggregate_version=5,  # Expected: 3
            increment=1,
        )

        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(bad_event)

        assert exc_info.value.expected_version == 3
        assert exc_info.value.actual_version == 5

    def test_version_validation_with_multiple_event_types(self) -> None:
        """Version validation should work with different event types."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)

        # Apply first event
        event1 = CounterIncremented(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=10,
        )
        aggregate.apply_event(event1)

        # Try applying different event type with wrong version
        event2 = CounterNamed(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            aggregate_version=5,  # Expected: 2
            name="TestCounter",
        )

        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(event2)

        assert exc_info.value.expected_version == 2
        assert exc_info.value.actual_version == 5


class TestEventVersionErrorException:
    """Tests for EventVersionError exception class."""

    def test_exception_attributes(self) -> None:
        """EventVersionError should have all expected attributes."""
        event_id = uuid4()
        aggregate_id = uuid4()

        error = EventVersionError(
            expected_version=5,
            actual_version=3,
            event_id=event_id,
            aggregate_id=aggregate_id,
        )

        assert error.expected_version == 5
        assert error.actual_version == 3
        assert error.event_id == event_id
        assert error.aggregate_id == aggregate_id

    def test_exception_message_format(self) -> None:
        """EventVersionError message should be informative."""
        event_id = uuid4()
        aggregate_id = uuid4()

        error = EventVersionError(
            expected_version=5,
            actual_version=3,
            event_id=event_id,
            aggregate_id=aggregate_id,
        )

        message = str(error)
        assert f"aggregate {aggregate_id}" in message
        assert "expected version 5" in message
        assert "got 3" in message
        assert f"event_id: {event_id}" in message

    def test_exception_is_subclass_of_eventsource_error(self) -> None:
        """EventVersionError should be a subclass of EventSourceError."""
        from eventsource.exceptions import EventSourceError

        error = EventVersionError(
            expected_version=1,
            actual_version=2,
            event_id=uuid4(),
            aggregate_id=uuid4(),
        )

        assert isinstance(error, EventSourceError)
        assert isinstance(error, Exception)


# =============================================================================
# TD-012: Unregistered Event Handling Tests
# =============================================================================


class TestUnregisteredEventHandling:
    """Tests for configurable unregistered event handling feature (TD-012).

    These tests verify:
    - AC1: Unhandled events raise UnhandledEventError when mode is "error"
    - AC2: Setting unregistered_event_handling="ignore" silently ignores events
    - AC3: Setting unregistered_event_handling="warn" logs warning
    - AC4: Error message lists available handlers
    - AC5: Default mode is "ignore" for backwards compatibility
    """

    def test_default_mode_is_ignore(self) -> None:
        """AC5: Default mode should be 'ignore' for backwards compatibility."""
        assert DeclarativeCounterAggregate.unregistered_event_handling == "ignore"

    def test_ignore_mode_silently_accepts_unknown_events(self) -> None:
        """AC2: ignore mode allows unknown events without raising or logging."""

        class IgnoreAggregate(DeclarativeAggregate[CounterState]):
            unregistered_event_handling = "ignore"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

            @handles(CounterIncremented)
            def _on_counter_incremented(self, event: CounterIncremented) -> None:
                if self._state is None:
                    self._state = self._get_initial_state()
                self._state = self._state.model_copy(
                    update={"value": self._state.value + event.increment}
                )

        aggregate = IgnoreAggregate(uuid4())

        # Create an event type with no handler
        class UnknownEvent(DomainEvent):
            event_type: str = "UnknownEvent"
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        # Should not raise - just silently ignore
        aggregate.apply_event(event)
        assert aggregate.version == 1

    def test_error_mode_raises_unhandled_event_error(self) -> None:
        """AC1: error mode raises UnhandledEventError for unhandled events."""

        class StrictAggregate(DeclarativeAggregate[CounterState]):
            unregistered_event_handling = "error"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

            @handles(CounterIncremented)
            def _on_counter_incremented(self, event: CounterIncremented) -> None:
                if self._state is None:
                    self._state = self._get_initial_state()
                self._state = self._state.model_copy(
                    update={"value": self._state.value + event.increment}
                )

        aggregate = StrictAggregate(uuid4())

        # Create an event type with no handler
        class UnhandledEvent(DomainEvent):
            event_type: str = "UnhandledEvent"
            aggregate_type: str = "Counter"

        event = UnhandledEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        with pytest.raises(UnhandledEventError) as exc_info:
            aggregate.apply_event(event)

        assert "UnhandledEvent" in str(exc_info.value)
        assert "StrictAggregate" in str(exc_info.value)

    def test_warn_mode_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """AC3: warn mode logs warning for unhandled events."""

        class WarnAggregate(DeclarativeAggregate[CounterState]):
            unregistered_event_handling = "warn"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

            @handles(CounterIncremented)
            def _on_counter_incremented(self, event: CounterIncremented) -> None:
                if self._state is None:
                    self._state = self._get_initial_state()
                self._state = self._state.model_copy(
                    update={"value": self._state.value + event.increment}
                )

        aggregate = WarnAggregate(uuid4())

        class UnknownEvent(DomainEvent):
            event_type: str = "UnknownEvent"
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        with caplog.at_level(logging.WARNING):
            aggregate.apply_event(event)

        # Check warning was logged
        assert "No handler registered" in caplog.text
        assert "UnknownEvent" in caplog.text
        assert "WarnAggregate" in caplog.text
        # Event should still be applied (version updated)
        assert aggregate.version == 1

    def test_error_message_includes_available_handlers(self) -> None:
        """AC4: Error message lists handlers that ARE available."""

        class MultiHandlerAggregate(DeclarativeAggregate[CounterState]):
            unregistered_event_handling = "error"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

            @handles(CounterIncremented)
            def _on_counter_incremented(self, event: CounterIncremented) -> None:
                pass

            @handles(CounterDecremented)
            def _on_counter_decremented(self, event: CounterDecremented) -> None:
                pass

        aggregate = MultiHandlerAggregate(uuid4())

        class UnknownEvent(DomainEvent):
            event_type: str = "UnknownEvent"
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        with pytest.raises(UnhandledEventError) as exc_info:
            aggregate.apply_event(event)

        error = exc_info.value
        assert "CounterIncremented" in error.available_handlers
        assert "CounterDecremented" in error.available_handlers
        assert error.event_type == "UnknownEvent"
        assert error.handler_class == "MultiHandlerAggregate"

        # Check error message format
        error_msg = str(error)
        assert "CounterIncremented" in error_msg
        assert "CounterDecremented" in error_msg

    def test_error_message_shows_no_handlers_when_empty(self) -> None:
        """Error message shows 'none' when no handlers are registered."""

        class EmptyAggregate(DeclarativeAggregate[CounterState]):
            unregistered_event_handling = "error"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

        aggregate = EmptyAggregate(uuid4())

        class UnknownEvent(DomainEvent):
            event_type: str = "UnknownEvent"
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        with pytest.raises(UnhandledEventError) as exc_info:
            aggregate.apply_event(event)

        error = exc_info.value
        assert error.available_handlers == []
        assert "none" in str(error)

    def test_subclass_can_override_handling_mode(self) -> None:
        """Subclasses can override the handling mode."""

        class BaseAggregate(DeclarativeAggregate[CounterState]):
            unregistered_event_handling = "ignore"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

        class StrictSubclass(BaseAggregate):
            unregistered_event_handling = "error"

        assert BaseAggregate.unregistered_event_handling == "ignore"
        assert StrictSubclass.unregistered_event_handling == "error"

    def test_handled_event_is_not_affected_by_mode(self) -> None:
        """Events with handlers work regardless of handling mode."""

        class StrictAggregate(DeclarativeAggregate[CounterState]):
            unregistered_event_handling = "error"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

            @handles(CounterIncremented)
            def _on_counter_incremented(self, event: CounterIncremented) -> None:
                if self._state is None:
                    self._state = self._get_initial_state()
                self._state = self._state.model_copy(
                    update={"value": self._state.value + event.increment}
                )

        aggregate = StrictAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )

        # Should work fine - handler exists
        aggregate.apply_event(event)
        assert aggregate.state is not None
        assert aggregate.state.value == 5

    def test_backwards_compatibility_with_existing_code(self) -> None:
        """Existing code should work without changes (ignore mode is default)."""
        # DeclarativeCounterAggregate is defined without explicit mode setting
        aggregate = DeclarativeCounterAggregate(uuid4())

        # Create an event type with no handler
        class FutureEvent(DomainEvent):
            event_type: str = "FutureEvent"
            aggregate_type: str = "Counter"

        event = FutureEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        # Should not raise - backwards compatible behavior
        aggregate.apply_event(event)
        assert aggregate.version == 1


class TestUnhandledEventErrorException:
    """Tests for UnhandledEventError exception class."""

    def test_exception_attributes(self) -> None:
        """UnhandledEventError should have all expected attributes."""
        event_id = uuid4()

        error = UnhandledEventError(
            event_type="OrderShipped",
            event_id=event_id,
            handler_class="OrderAggregate",
            available_handlers=["OrderCreated", "OrderCancelled"],
        )

        assert error.event_type == "OrderShipped"
        assert error.event_id == event_id
        assert error.handler_class == "OrderAggregate"
        assert error.available_handlers == ["OrderCreated", "OrderCancelled"]

    def test_exception_message_format(self) -> None:
        """UnhandledEventError message should be informative."""
        event_id = uuid4()

        error = UnhandledEventError(
            event_type="OrderShipped",
            event_id=event_id,
            handler_class="OrderAggregate",
            available_handlers=["OrderCreated", "OrderCancelled"],
        )

        message = str(error)
        assert "OrderShipped" in message
        assert "OrderAggregate" in message
        assert "OrderCreated" in message
        assert "OrderCancelled" in message
        assert "@handles" in message

    def test_exception_message_with_no_handlers(self) -> None:
        """UnhandledEventError message handles empty handler list."""
        error = UnhandledEventError(
            event_type="OrderShipped",
            event_id=uuid4(),
            handler_class="OrderAggregate",
            available_handlers=[],
        )

        message = str(error)
        assert "none" in message

    def test_exception_is_subclass_of_eventsource_error(self) -> None:
        """UnhandledEventError should be a subclass of EventSourceError."""
        from eventsource.exceptions import EventSourceError

        error = UnhandledEventError(
            event_type="OrderShipped",
            event_id=uuid4(),
            handler_class="OrderAggregate",
            available_handlers=[],
        )

        assert isinstance(error, EventSourceError)
        assert isinstance(error, Exception)
