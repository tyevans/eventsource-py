"""Unit tests for AggregateRoot initialization, equality, repr, and state invariants."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import AggregateTypeNotSetError
from tests.unit.domain.test_aggregate_root._fixtures import (
    CounterAggregate,
    CounterIncremented,
    OrderAggregate,
)


class TestAggregateRootInitialization:
    """Tests for aggregate initialization."""

    def test_new_aggregate_has_correct_id(self) -> None:
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        assert aggregate.aggregate_id == agg_id

    def test_new_aggregate_has_version_zero(self) -> None:
        aggregate = CounterAggregate(uuid4())
        assert aggregate.version == 0

    def test_new_aggregate_has_no_uncommitted_events(self) -> None:
        aggregate = CounterAggregate(uuid4())
        assert aggregate.uncommitted_events == []
        assert not aggregate.has_uncommitted_events

    def test_new_aggregate_has_none_state(self) -> None:
        aggregate = CounterAggregate(uuid4())
        assert aggregate.state is None

    def test_aggregate_type_is_set(self) -> None:
        aggregate = CounterAggregate(uuid4())
        assert aggregate.aggregate_type == "Counter"
        order_aggregate = OrderAggregate(uuid4())
        assert order_aggregate.aggregate_type == "Order"


class TestAggregateEquality:
    """Tests for aggregate equality and hashing."""

    def test_equality_based_on_id(self) -> None:
        agg_id = uuid4()
        assert CounterAggregate(agg_id) == CounterAggregate(agg_id)

    def test_inequality_for_different_ids(self) -> None:
        assert CounterAggregate(uuid4()) != CounterAggregate(uuid4())

    def test_inequality_with_non_aggregate(self) -> None:
        aggregate = CounterAggregate(uuid4())
        assert aggregate != "not an aggregate"
        assert aggregate != 123
        assert aggregate is not None

    def test_hash_based_on_id(self) -> None:
        agg_id = uuid4()
        assert hash(CounterAggregate(agg_id)) == hash(CounterAggregate(agg_id))

    def test_can_use_aggregate_in_set(self) -> None:
        agg_id = uuid4()
        agg1 = CounterAggregate(agg_id)
        agg2 = CounterAggregate(agg_id)
        agg3 = CounterAggregate(uuid4())
        assert len({agg1, agg2, agg3}) == 2


class TestAggregateRepr:
    """Tests for aggregate string representation."""

    def test_repr_shows_key_info(self) -> None:
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        repr_str = repr(aggregate)

        assert "CounterAggregate" in repr_str
        assert str(agg_id) in repr_str
        assert "version=1" in repr_str
        assert "uncommitted=1" in repr_str

    def test_repr_for_new_aggregate(self) -> None:
        repr_str = repr(CounterAggregate(uuid4()))
        assert "version=0" in repr_str
        assert "uncommitted=0" in repr_str


class TestImmutableStatePatterns:
    """Tests demonstrating immutable state update patterns."""

    def test_state_uses_model_copy_for_updates(self) -> None:
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        state1 = aggregate.state
        aggregate.increment(3)
        state2 = aggregate.state

        assert state1 is not None and state2 is not None
        assert state1 is not state2
        assert state1.value == 5
        assert state2.value == 8

    def test_list_updates_create_new_lists(self) -> None:
        aggregate = OrderAggregate(uuid4())
        aggregate.create(uuid4())
        aggregate.add_item("Widget A", Decimal("10.00"))
        assert aggregate.state is not None
        items1 = aggregate.state.items
        aggregate.add_item("Widget B", Decimal("15.00"))
        assert aggregate.state is not None
        items2 = aggregate.state.items

        assert items1 is not items2
        assert items1 == ["Widget A"]
        assert items2 == ["Widget A", "Widget B"]


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_large_number_of_events(self) -> None:
        aggregate = CounterAggregate(uuid4())
        for _ in range(1000):
            aggregate.increment(1)

        assert aggregate.version == 1000
        assert aggregate.state is not None
        assert aggregate.state.value == 1000

    def test_reconstitute_large_history(self) -> None:
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
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(100)
        aggregate.increment(50)
        assert aggregate.state is not None and aggregate.state.value == 150

        aggregate.reset()
        assert aggregate.state is not None and aggregate.state.value == 0
        assert aggregate.version == 3


class TestAggregateTypeRequired:
    def test_subclass_without_aggregate_type_raises_on_construction(self) -> None:
        class ForgotType(AggregateRoot[dict]):
            def _apply(self, event: DomainEvent) -> None: ...
            def _get_initial_state(self) -> dict | None:
                return None

        with pytest.raises(AggregateTypeNotSetError, match="ForgotType"):
            ForgotType(uuid4())

    def test_subclass_with_aggregate_type_constructs(self) -> None:
        class HasType(AggregateRoot[dict]):
            aggregate_type = "HasType"

            def _apply(self, event: DomainEvent) -> None: ...
            def _get_initial_state(self) -> dict | None:
                return None

        assert HasType(uuid4()).aggregate_type == "HasType"
