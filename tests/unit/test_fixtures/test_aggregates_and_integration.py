"""Tests for aggregate fixtures and pytest fixture integration."""

from collections.abc import Callable
from decimal import Decimal

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.domain.event import DomainEvent
from tests.conftest import MockEventPublisher
from tests.fixtures import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    DeclarativeCounterAggregate,
    OrderAggregate,
    SampleEvent,
    create_event,
)

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
        aggregate.add_item("Widget A", Decimal("10.00"))
        aggregate.add_item("Widget B", Decimal("15.00"))
        aggregate.ship("TRACK123")

        assert aggregate.version == 4
        assert aggregate.state is not None
        assert aggregate.state.status == "shipped"
        assert aggregate.state.items == ["Widget A", "Widget B"]
        assert aggregate.state.total == Decimal("25.00")

    def test_running_total_accumulates_without_precision_loss(self) -> None:
        """Repeated Decimal additions stay exact where float would drift.

        Guards the reason OrderState.total is Decimal rather than float: the
        error only shows up on accumulation, not on a single round trip. See
        docs/guides/money-and-precision.md.
        """
        from uuid import uuid4

        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())

        for _ in range(10):
            aggregate.add_item("Dime", Decimal("0.10"))

        assert aggregate.state is not None
        assert aggregate.state.total == Decimal("1.00")

        # The same arithmetic in float does not land on 1.0.
        naive = 0.0
        for _ in range(10):
            naive += 0.10
        assert naive != 1.0

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
            aggregate.add_item("Widget", Decimal("10.00"))


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
        envelopes = [e async for e in populated_store.read_all()]
        assert len(envelopes) == 3

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
