"""
Tests for the shared test fixtures.

These tests verify that the fixtures work correctly and provide
the expected functionality for other tests to use.
"""

from collections.abc import Callable
from uuid import UUID

import pytest

from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository
from eventsource.stores.in_memory import InMemoryEventStore
from tests.conftest import MockEventPublisher
from tests.fixtures import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    CounterState,
    DeclarativeCounterAggregate,
    OrderAggregate,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    OrderState,
    SampleEvent,
    create_event,
)

# =============================================================================
# Event Factory Tests
# =============================================================================


class TestEventFactory:
    """Tests for the create_event factory function."""

    def test_creates_event_with_defaults(self) -> None:
        """Factory creates valid event with sensible defaults."""
        event = create_event()

        assert event.aggregate_id is not None
        assert isinstance(event.aggregate_id, UUID)
        assert event.aggregate_type == "TestAggregate"
        assert event.aggregate_version == 1
        assert isinstance(event, SampleEvent)

    def test_accepts_custom_aggregate_id(self) -> None:
        """Factory uses provided aggregate_id."""
        from uuid import uuid4

        custom_id = uuid4()
        event = create_event(aggregate_id=custom_id)

        assert event.aggregate_id == custom_id

    def test_accepts_custom_version(self) -> None:
        """Factory uses provided version."""
        event = create_event(aggregate_version=5)

        assert event.aggregate_version == 5

    def test_creates_counter_event(self) -> None:
        """Factory can create CounterIncremented events."""
        event = create_event(CounterIncremented, increment=10)

        assert isinstance(event, CounterIncremented)
        assert event.increment == 10
        assert event.aggregate_type == "Counter"

    def test_creates_order_event(self) -> None:
        """Factory can create OrderCreated events."""
        from uuid import uuid4

        customer_id = uuid4()
        event = create_event(OrderCreated, customer_id=customer_id)

        assert isinstance(event, OrderCreated)
        assert event.customer_id == customer_id
        assert event.aggregate_type == "Order"

    def test_accepts_tenant_id(self) -> None:
        """Factory can set tenant_id for multi-tenant tests."""
        from uuid import uuid4

        tenant_id = uuid4()
        event = create_event(tenant_id=tenant_id)

        assert event.tenant_id == tenant_id


# =============================================================================
# Event Type Tests
# =============================================================================


class TestCounterEvents:
    """Tests for counter event types."""

    def test_counter_incremented_has_correct_defaults(self) -> None:
        """CounterIncremented has correct default values."""
        from uuid import uuid4

        event = CounterIncremented(aggregate_id=uuid4(), aggregate_version=1)

        assert event.event_type == "CounterIncremented"
        assert event.aggregate_type == "Counter"
        assert event.increment == 1

    def test_counter_decremented_has_correct_defaults(self) -> None:
        """CounterDecremented has correct default values."""
        from uuid import uuid4

        event = CounterDecremented(aggregate_id=uuid4(), aggregate_version=1)

        assert event.event_type == "CounterDecremented"
        assert event.aggregate_type == "Counter"
        assert event.decrement == 1

    def test_counter_named_requires_name(self) -> None:
        """CounterNamed requires a name."""
        from uuid import uuid4

        event = CounterNamed(aggregate_id=uuid4(), aggregate_version=1, name="MyCounter")

        assert event.name == "MyCounter"


class TestOrderEvents:
    """Tests for order event types."""

    def test_order_created_requires_customer_id(self) -> None:
        """OrderCreated requires a customer_id."""
        from uuid import uuid4

        customer_id = uuid4()
        event = OrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            customer_id=customer_id,
        )

        assert event.customer_id == customer_id

    def test_order_item_added_requires_item_details(self) -> None:
        """OrderItemAdded requires item_name and price."""
        from uuid import uuid4

        event = OrderItemAdded(
            aggregate_id=uuid4(),
            aggregate_version=1,
            item_name="Widget",
            price=10.50,
        )

        assert event.item_name == "Widget"
        assert event.price == 10.50

    def test_order_shipped_requires_tracking_number(self) -> None:
        """OrderShipped requires a tracking_number."""
        from uuid import uuid4

        event = OrderShipped(
            aggregate_id=uuid4(),
            aggregate_version=1,
            tracking_number="TRACK123",
        )

        assert event.tracking_number == "TRACK123"


# =============================================================================
# State Model Tests
# =============================================================================


class TestCounterState:
    """Tests for CounterState model."""

    def test_counter_state_creation(self) -> None:
        """CounterState can be created with required fields."""
        from uuid import uuid4

        state = CounterState(counter_id=uuid4())

        assert state.value == 0
        assert state.name == ""

    def test_counter_state_with_values(self) -> None:
        """CounterState can be created with all fields."""
        from uuid import uuid4

        counter_id = uuid4()
        state = CounterState(counter_id=counter_id, value=42, name="MyCounter")

        assert state.counter_id == counter_id
        assert state.value == 42
        assert state.name == "MyCounter"


class TestOrderState:
    """Tests for OrderState model."""

    def test_order_state_creation(self) -> None:
        """OrderState can be created with required fields."""
        from uuid import uuid4

        state = OrderState(order_id=uuid4())

        assert state.status == "draft"
        assert state.total == 0.0
        assert state.items == []

    def test_order_state_with_items(self) -> None:
        """OrderState can have items."""
        from uuid import uuid4

        state = OrderState(
            order_id=uuid4(),
            customer_id=uuid4(),
            status="created",
            total=25.0,
            items=["Widget A", "Widget B"],
        )

        assert len(state.items) == 2
        assert state.total == 25.0


# =============================================================================
# Aggregate Tests
# =============================================================================


class TestCounterAggregate:
    """Tests for CounterAggregate fixture class."""

    def test_counter_aggregate_initialization(self) -> None:
        """CounterAggregate initializes correctly."""
        from uuid import uuid4

        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)

        assert aggregate.aggregate_id == agg_id
        assert aggregate.version == 0
        assert aggregate.state is None

    def test_counter_aggregate_increment(self) -> None:
        """CounterAggregate.increment creates event and updates state."""
        from uuid import uuid4

        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        assert aggregate.version == 1
        assert aggregate.state is not None
        assert aggregate.state.value == 5

    def test_counter_aggregate_multiple_operations(self) -> None:
        """CounterAggregate handles multiple operations correctly."""
        from uuid import uuid4

        aggregate = CounterAggregate(uuid4())
        aggregate.increment(10)
        aggregate.increment(5)
        aggregate.decrement(3)

        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 12

    def test_counter_aggregate_reset(self) -> None:
        """CounterAggregate.reset resets state to zero."""
        from uuid import uuid4

        aggregate = CounterAggregate(uuid4())
        aggregate.increment(100)
        aggregate.reset()

        assert aggregate.version == 2
        assert aggregate.state is not None
        assert aggregate.state.value == 0


class TestDeclarativeCounterAggregate:
    """Tests for DeclarativeCounterAggregate fixture class."""

    def test_handlers_are_registered(self) -> None:
        """DeclarativeCounterAggregate registers handlers via @handles."""
        from uuid import uuid4

        aggregate = DeclarativeCounterAggregate(uuid4())

        assert CounterIncremented in aggregate._event_handlers
        assert CounterDecremented in aggregate._event_handlers
        assert CounterNamed in aggregate._event_handlers
        assert CounterReset in aggregate._event_handlers

    def test_increment_works(self) -> None:
        """DeclarativeCounterAggregate.increment works correctly."""
        from uuid import uuid4

        aggregate = DeclarativeCounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.decrement(2)

        assert aggregate.version == 2
        assert aggregate.state is not None
        assert aggregate.state.value == 3


class TestOrderAggregate:
    """Tests for OrderAggregate fixture class."""

    def test_order_lifecycle(self) -> None:
        """OrderAggregate supports full order lifecycle."""
        from uuid import uuid4

        aggregate = OrderAggregate(uuid4())
        customer_id = uuid4()

        aggregate.create(customer_id)
        aggregate.add_item("Widget A", 10.0)
        aggregate.add_item("Widget B", 15.0)
        aggregate.ship("TRACK123")

        assert aggregate.version == 4
        assert aggregate.state is not None
        assert aggregate.state.status == "shipped"
        assert aggregate.state.items == ["Widget A", "Widget B"]
        assert aggregate.state.total == 25.0

    def test_cannot_create_twice(self) -> None:
        """OrderAggregate.create raises if already created."""
        from uuid import uuid4

        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())

        with pytest.raises(ValueError, match="already exists"):
            aggregate.create(uuid4())

    def test_cannot_add_items_to_shipped_order(self) -> None:
        """OrderAggregate.add_item raises for shipped orders."""
        from uuid import uuid4

        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())
        aggregate.ship("TRACK123")

        with pytest.raises(ValueError, match="Cannot add items"):
            aggregate.add_item("Widget", 10.0)


# =============================================================================
# Fixture Integration Tests
# =============================================================================


class TestFixtureIntegration:
    """Tests for fixture integration with pytest fixtures."""

    def test_event_factory_fixture(self, event_factory: Callable[..., DomainEvent]) -> None:
        """event_factory fixture returns the factory function."""
        event = event_factory(CounterIncremented, increment=7)

        assert isinstance(event, CounterIncremented)
        assert event.increment == 7

    def test_sample_event_fixture(self, sample_event: SampleEvent) -> None:
        """sample_event fixture provides a valid event."""
        assert isinstance(sample_event, SampleEvent)
        assert sample_event.aggregate_version == 1

    def test_event_stream_fixture(self, event_stream: list[DomainEvent]) -> None:
        """event_stream fixture provides ordered events."""
        assert len(event_stream) == 3
        assert all(isinstance(e, DomainEvent) for e in event_stream)

        # All events should have the same aggregate_id
        agg_id = event_stream[0].aggregate_id
        assert all(e.aggregate_id == agg_id for e in event_stream)

        # Versions should be sequential
        assert [e.aggregate_version for e in event_stream] == [1, 2, 3]

    def test_in_memory_store_fixture(self, in_memory_store: InMemoryEventStore) -> None:
        """in_memory_store fixture provides a fresh store."""
        assert isinstance(in_memory_store, InMemoryEventStore)

    @pytest.mark.asyncio
    async def test_populated_store_fixture(self, populated_store: InMemoryEventStore) -> None:
        """populated_store fixture has events pre-loaded."""
        count = await populated_store.get_event_count()
        assert count == 3

    def test_checkpoint_repo_fixture(self, checkpoint_repo: InMemoryCheckpointRepository) -> None:
        """checkpoint_repo fixture provides a fresh repository."""
        assert isinstance(checkpoint_repo, InMemoryCheckpointRepository)

    def test_dlq_repo_fixture(self, dlq_repo: InMemoryDLQRepository) -> None:
        """dlq_repo fixture provides a fresh repository."""
        assert isinstance(dlq_repo, InMemoryDLQRepository)

    def test_counter_aggregate_fixture(self, counter_aggregate: CounterAggregate) -> None:
        """counter_aggregate fixture provides a fresh aggregate."""
        assert isinstance(counter_aggregate, CounterAggregate)
        assert counter_aggregate.version == 0

    def test_populated_counter_aggregate_fixture(
        self, populated_counter_aggregate: CounterAggregate
    ) -> None:
        """populated_counter_aggregate fixture has state."""
        assert populated_counter_aggregate.version == 1
        assert populated_counter_aggregate.state is not None
        assert populated_counter_aggregate.state.value == 10

    def test_order_aggregate_fixture(self, order_aggregate: OrderAggregate) -> None:
        """order_aggregate fixture provides a fresh aggregate."""
        assert isinstance(order_aggregate, OrderAggregate)
        assert order_aggregate.version == 0

    def test_populated_order_aggregate_fixture(
        self, populated_order_aggregate: OrderAggregate
    ) -> None:
        """populated_order_aggregate fixture has items."""
        assert populated_order_aggregate.version == 3
        assert populated_order_aggregate.state is not None
        assert len(populated_order_aggregate.state.items) == 2

    @pytest.mark.asyncio
    async def test_mock_event_publisher_fixture(
        self, mock_event_publisher: MockEventPublisher
    ) -> None:
        """mock_event_publisher captures published events."""
        from uuid import uuid4

        events = [create_event(aggregate_id=uuid4())]
        await mock_event_publisher.publish(events)

        assert len(mock_event_publisher.published_events) == 1
        assert mock_event_publisher.published_events[0] == events[0]
