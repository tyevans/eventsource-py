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
"""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, Field

from eventsource.aggregates.base import (
    AggregateRoot,
    DeclarativeAggregate,
    handles,
)
from eventsource.events.base import DomainEvent

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
        self._state = self._state.model_copy(
            update={"value": self._state.value + event.increment}
        )

    @handles(CounterDecremented)
    def _on_counter_decremented(self, event: CounterDecremented) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(
            update={"value": self._state.value - event.decrement}
        )

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
        assert state1 is not state2
        assert state1.value == 5
        assert state2.value == 8

    def test_list_updates_create_new_lists(self) -> None:
        """List updates should create new lists, not mutate."""
        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())
        aggregate.add_item("Widget A", 10.0)

        items1 = aggregate.state.items
        aggregate.add_item("Widget B", 15.0)
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
        assert aggregate.state.value == 150

        aggregate.reset()
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
