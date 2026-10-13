"""Tests for edge cases and performance of create_event."""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from tests.unit.domain.test_create_event._fixtures import (
    OrderAggregate,
    OrderCreated,
)

__all__ = [
    "TestCreateEventEdgeCases",
    "TestCreateEventPerformance",
]


class TestCreateEventEdgeCases:
    """Tests for edge cases and potential error conditions."""

    @pytest.fixture
    def order(self) -> OrderAggregate:
        return OrderAggregate(uuid4())

    def test_event_id_is_unique(self, order: OrderAggregate) -> None:
        """Each created event has a unique event_id."""
        event1 = order.create(customer_id=uuid4())
        event2 = order.ship(tracking_number="TRACK1")
        event3 = order.cancel(reason="Test")

        event_ids = {event1.event_id, event2.event_id, event3.event_id}
        assert len(event_ids) == 3  # All unique

    def test_correlation_id_is_set(self, order: OrderAggregate) -> None:
        """Created events have correlation_id set."""
        event = order.create(customer_id=uuid4())
        assert event.correlation_id is not None

    def test_occurred_at_is_set(self, order: OrderAggregate) -> None:
        """Created events have occurred_at timestamp."""
        event = order.create(customer_id=uuid4())
        assert event.occurred_at is not None

    def test_empty_kwargs(self) -> None:
        """Event with only auto-populated fields works (when event has defaults)."""

        class SimpleEvent(DomainEvent):
            aggregate_type: str = "Simple"

        class SimpleState(BaseModel):
            id: UUID

        class SimpleAggregate(AggregateRoot[SimpleState]):
            aggregate_type = "Simple"

            def _get_initial_state(self) -> SimpleState:
                return SimpleState(id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        agg = SimpleAggregate(uuid4())
        event = agg.create_event(SimpleEvent)

        assert event.aggregate_id == agg.aggregate_id
        assert event.aggregate_type == "Simple"
        assert event.aggregate_version == 1

    def test_create_event_with_metadata(self, order: OrderAggregate) -> None:
        """create_event can include metadata."""
        event = order.create_event(
            OrderCreated,
            customer_id=uuid4(),
            metadata={"source": "api", "request_id": "req-123"},
        )
        assert event.metadata == {"source": "api", "request_id": "req-123"}

    def test_create_event_with_actor_id(self, order: OrderAggregate) -> None:
        """create_event can include actor_id."""
        event = order.create_event(
            OrderCreated,
            customer_id=uuid4(),
            actor_id="user-456",
        )
        assert event.actor_id == "user-456"

    def test_create_event_with_causation_id(self, order: OrderAggregate) -> None:
        """create_event can include causation_id."""
        causation = uuid4()
        event = order.create_event(
            OrderCreated,
            customer_id=uuid4(),
            causation_id=causation,
        )
        assert event.causation_id == causation

    def test_multiple_aggregates_independent(self) -> None:
        """Events created on different aggregates are independent."""
        order1 = OrderAggregate(uuid4())
        order2 = OrderAggregate(uuid4())

        event1 = order1.create(customer_id=uuid4())
        event2 = order2.create(customer_id=uuid4())

        assert event1.aggregate_id != event2.aggregate_id
        assert event1.aggregate_version == 1
        assert event2.aggregate_version == 1
        assert event1 not in order2.uncommitted_events
        assert event2 not in order1.uncommitted_events


class TestCreateEventPerformance:
    """Lightweight performance tests."""

    def test_many_events(self) -> None:
        """Can create many events in sequence."""

        class IncrementEvent(DomainEvent):
            aggregate_type: str = "Counter"
            event_type: str = "Incremented"

        class CounterState(BaseModel):
            value: int = 0

        class Counter(AggregateRoot[CounterState]):
            aggregate_type = "Counter"

            def _get_initial_state(self) -> CounterState:
                return CounterState()

            def _apply(self, event: DomainEvent) -> None:
                if self._state is None:
                    self._state = CounterState()
                self._state = self._state.model_copy(update={"value": self._state.value + 1})

            def increment(self) -> IncrementEvent:
                return self.create_event(IncrementEvent)

        counter = Counter(uuid4())

        for i in range(100):
            event = counter.increment()
            assert event.aggregate_version == i + 1

        assert counter.version == 100
        assert len(counter.uncommitted_events) == 100
        assert counter.state is not None
        assert counter.state.value == 100
