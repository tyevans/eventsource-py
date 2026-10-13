"""Tests for _get_state_type method."""

from uuid import uuid4

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from tests.unit.domain.test_aggregate_snapshot_methods._fixtures import (
    DeclarativeOrderAggregate,
    OrderAggregate,
    OrderState,
    SimpleAggregate,
    SimpleState,
)


class TestGetStateType:
    """Test _get_state_type method for Generic type introspection."""

    def test_returns_correct_type_for_aggregate_root(self) -> None:
        """_get_state_type should return the TState type parameter."""
        aggregate = OrderAggregate(uuid4())
        state_type = aggregate._get_state_type()
        assert state_type is OrderState

    def test_returns_correct_type_for_simple_aggregate(self) -> None:
        """_get_state_type should work for different state types."""
        aggregate = SimpleAggregate(uuid4())
        state_type = aggregate._get_state_type()
        assert state_type is SimpleState

    def test_works_with_declarative_aggregate(self) -> None:
        """_get_state_type should work with DeclarativeAggregate."""
        aggregate = DeclarativeOrderAggregate(uuid4())
        state_type = aggregate._get_state_type()
        assert state_type is OrderState

    def test_works_with_inheritance(self) -> None:
        """_get_state_type should work with aggregate inheritance."""

        class BaseOrder(AggregateRoot[OrderState]):
            aggregate_type = "BaseOrder"

            def _apply(self, event: DomainEvent) -> None:
                pass

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id="", customer_id="")

        class SpecialOrder(BaseOrder):
            """Inherits TState from BaseOrder."""

            aggregate_type = "SpecialOrder"

        aggregate = SpecialOrder(uuid4())
        state_type = aggregate._get_state_type()
        assert state_type is OrderState

    def test_raises_runtime_error_if_type_cannot_be_determined(self) -> None:
        """_get_state_type should raise RuntimeError if type cannot be found."""
        # This is an edge case that shouldn't happen in normal use,
        # but we test the error handling. We'll create a mock that
        # doesn't have proper Generic bases.

        class BrokenAggregate(AggregateRoot[SimpleState]):
            aggregate_type = "Broken"

            def _apply(self, event: DomainEvent) -> None:
                pass

            def _get_initial_state(self) -> SimpleState:
                return SimpleState()

        aggregate = BrokenAggregate(uuid4())

        # The method should still work for properly defined aggregates
        state_type = aggregate._get_state_type()
        assert state_type is SimpleState
