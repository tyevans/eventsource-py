"""Unit tests for aggregate commands, history replay, and complex state management."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent
from tests.unit.domain.test_aggregate_root._fixtures import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    OrderAggregate,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
)


class TestLoadFromHistory:
    """Tests for reconstituting aggregate from event history."""

    def test_load_from_history_replays_events(self) -> None:
        agg_id = uuid4()
        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=1, increment=10
            ),
            CounterIncremented(
                aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=2, increment=5
            ),
            CounterDecremented(
                aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=3, decrement=3
            ),
        ]
        aggregate = CounterAggregate(agg_id)
        aggregate.load_from_history(events)

        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 12

    def test_load_from_history_does_not_create_uncommitted_events(self) -> None:
        agg_id = uuid4()
        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=1, increment=10
            ),
        ]
        aggregate = CounterAggregate(agg_id)
        aggregate.load_from_history(events)

        assert len(aggregate.uncommitted_events) == 0
        assert not aggregate.has_uncommitted_events

    def test_load_from_empty_history(self) -> None:
        aggregate = CounterAggregate(uuid4())
        aggregate.load_from_history([])

        assert aggregate.version == 0
        assert aggregate.state is None

    def test_can_apply_new_events_after_loading_history(self) -> None:
        agg_id = uuid4()
        events: list[DomainEvent] = [
            CounterIncremented(
                aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=1, increment=10
            ),
        ]
        aggregate = CounterAggregate(agg_id)
        aggregate.load_from_history(events)
        aggregate.increment(5)

        assert aggregate.version == 2
        assert aggregate.state is not None
        assert aggregate.state.value == 15
        assert len(aggregate.uncommitted_events) == 1


class TestCommandMethods:
    """Tests for aggregate command methods."""

    def test_command_creates_and_applies_event(self) -> None:
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(7)

        assert aggregate.version == 1
        assert aggregate.state is not None
        assert aggregate.state.value == 7

    def test_command_uses_correct_version(self) -> None:
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        events = aggregate.uncommitted_events
        assert events[0].aggregate_version == 1
        assert events[1].aggregate_version == 2

    def test_command_validation(self) -> None:
        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())
        with pytest.raises(ValueError, match="Order already exists"):
            aggregate.create(uuid4())

    def test_command_state_validation(self) -> None:
        aggregate = OrderAggregate(uuid4())
        with pytest.raises(ValueError, match="Cannot add items"):
            aggregate.add_item("Widget", Decimal("10.00"))


class TestComplexStateManagement:
    """Tests for complex state with nested structures."""

    def test_order_lifecycle(self) -> None:
        customer_id = uuid4()
        aggregate = OrderAggregate(uuid4())

        aggregate.create(customer_id)
        assert aggregate.state is not None
        assert aggregate.state.status == "created"
        assert aggregate.state.customer_id == customer_id

        aggregate.add_item("Widget A", Decimal("10.00"))
        aggregate.add_item("Widget B", Decimal("15.00"))
        assert aggregate.state.items == ["Widget A", "Widget B"]
        assert aggregate.state.total == 25.0

        aggregate.ship("TRACK123")
        assert aggregate.state.status == "shipped"
        assert aggregate.version == 4
        assert len(aggregate.uncommitted_events) == 4

    def test_order_reconstitution(self) -> None:
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
