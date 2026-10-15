"""Tests for aggregate fixtures and pytest fixture integration."""

from collections.abc import Callable
from decimal import Decimal
from uuid import uuid4

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


class TestCounterAggregate:
    """Tests for CounterAggregate fixture class."""

    def test_counter_aggregate_initialization(self) -> None:
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        assert aggregate.aggregate_id == agg_id
        assert aggregate.version == 0 and aggregate.state is None

    def test_counter_aggregate_increment(self) -> None:
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        assert aggregate.version == 1
        assert aggregate.state is not None and aggregate.state.value == 5

    def test_counter_aggregate_multiple_operations(self) -> None:
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(10)
        aggregate.increment(5)
        aggregate.decrement(3)
        assert aggregate.version == 3
        assert aggregate.state is not None and aggregate.state.value == 12

    def test_counter_aggregate_reset(self) -> None:
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(100)
        aggregate.reset()
        assert aggregate.version == 2
        assert aggregate.state is not None and aggregate.state.value == 0


class TestDeclarativeCounterAggregate:
    """Tests for DeclarativeCounterAggregate fixture class."""

    def test_handlers_are_registered(self) -> None:
        aggregate = DeclarativeCounterAggregate(uuid4())
        assert CounterIncremented in aggregate._event_handlers
        assert CounterDecremented in aggregate._event_handlers
        assert CounterNamed in aggregate._event_handlers
        assert CounterReset in aggregate._event_handlers

    def test_increment_works(self) -> None:
        aggregate = DeclarativeCounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.decrement(2)
        assert aggregate.version == 2
        assert aggregate.state is not None and aggregate.state.value == 3


class TestOrderAggregate:
    """Tests for OrderAggregate fixture class."""

    def test_order_lifecycle(self) -> None:
        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())
        aggregate.add_item("Widget A", Decimal("10.00"))
        aggregate.add_item("Widget B", Decimal("15.00"))
        aggregate.ship("TRACK123")
        assert aggregate.version == 4 and aggregate.state is not None
        assert aggregate.state.status == "shipped"
        assert aggregate.state.items == ["Widget A", "Widget B"]
        assert aggregate.state.total == Decimal("25.00")

    def test_running_total_accumulates_without_precision_loss(self) -> None:
        """Repeated Decimal additions stay exact where float would drift."""
        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())
        for _ in range(10):
            aggregate.add_item("Dime", Decimal("0.10"))
        assert aggregate.state is not None and aggregate.state.total == Decimal("1.00")
        naive = 0.0
        for _ in range(10):
            naive += 0.10
        assert naive != 1.0

    def test_cannot_create_twice(self) -> None:
        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())
        with pytest.raises(ValueError, match="already exists"):
            aggregate.create(uuid4())

    def test_cannot_add_items_to_shipped_order(self) -> None:
        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())
        aggregate.ship("TRACK123")
        with pytest.raises(ValueError, match="Cannot add items"):
            aggregate.add_item("Widget", Decimal("10.00"))


class TestFixtureIntegration:
    """Tests for fixture integration with pytest fixtures."""

    def test_event_factory_fixture(self, event_factory: Callable[..., DomainEvent]) -> None:
        event = event_factory(CounterIncremented, increment=7)
        assert isinstance(event, CounterIncremented) and event.increment == 7

    def test_sample_event_fixture(self, sample_event: SampleEvent) -> None:
        assert isinstance(sample_event, SampleEvent) and sample_event.aggregate_version == 1

    def test_event_stream_fixture(self, event_stream: list[DomainEvent]) -> None:
        assert len(event_stream) == 3
        assert all(isinstance(e, DomainEvent) for e in event_stream)
        agg_id = event_stream[0].aggregate_id
        assert all(e.aggregate_id == agg_id for e in event_stream)
        assert [e.aggregate_version for e in event_stream] == [1, 2, 3]

    def test_in_memory_store_fixture(self, in_memory_store: InMemoryEventStore) -> None:
        assert isinstance(in_memory_store, InMemoryEventStore)

    @pytest.mark.asyncio
    async def test_populated_store_fixture(self, populated_store: InMemoryEventStore) -> None:
        envelopes = [e async for e in populated_store.read_all()]
        assert len(envelopes) == 3

    def test_checkpoint_repo_fixture(self, checkpoint_repo: InMemoryCheckpointRepository) -> None:
        assert isinstance(checkpoint_repo, InMemoryCheckpointRepository)

    def test_dlq_repo_fixture(self, dlq_repo: InMemoryDLQRepository) -> None:
        assert isinstance(dlq_repo, InMemoryDLQRepository)

    def test_counter_aggregate_fixture(self, counter_aggregate: CounterAggregate) -> None:
        assert isinstance(counter_aggregate, CounterAggregate) and counter_aggregate.version == 0

    def test_populated_counter_aggregate_fixture(
        self, populated_counter_aggregate: CounterAggregate
    ) -> None:
        assert populated_counter_aggregate.version == 1
        assert populated_counter_aggregate.state is not None
        assert populated_counter_aggregate.state.value == 10

    def test_order_aggregate_fixture(self, order_aggregate: OrderAggregate) -> None:
        assert isinstance(order_aggregate, OrderAggregate) and order_aggregate.version == 0

    def test_populated_order_aggregate_fixture(
        self, populated_order_aggregate: OrderAggregate
    ) -> None:
        assert populated_order_aggregate.version == 3
        assert populated_order_aggregate.state is not None
        assert len(populated_order_aggregate.state.items) == 2

    @pytest.mark.asyncio
    async def test_mock_event_publisher_fixture(
        self, mock_event_publisher: MockEventPublisher
    ) -> None:
        events = [create_event(aggregate_id=uuid4())]
        await mock_event_publisher.publish(events)
        assert mock_event_publisher.published_events == [events[0]]
