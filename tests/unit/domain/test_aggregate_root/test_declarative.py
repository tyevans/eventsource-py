"""Unit tests for DeclarativeAggregate and @handles decorator."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.domain.aggregate import DeclarativeAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import UnhandledEventError
from tests.unit.domain.test_aggregate_root._fixtures import (
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    CounterState,
    DeclarativeCounterAggregate,
)


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

    def test_unhandled_event_is_ignored_with_explicit_opt_down(self) -> None:
        """Events without handlers are silently ignored when the aggregate
        explicitly opts down to unregistered_event_handling="ignore"."""

        class OptDownCounterAggregate(DeclarativeAggregate[CounterState]):
            aggregate_type = "Counter"
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

        aggregate = OptDownCounterAggregate(uuid4())

        # Create an event type that has no handler registered
        class UnhandledEvent(DomainEvent):
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


class TestStrictUnregisteredDefault:
    def test_unhandled_event_raises_by_default(self) -> None:
        class Surprise(DomainEvent):
            aggregate_type: str = "Strict"

        class StrictAggregate(DeclarativeAggregate[dict]):
            aggregate_type = "Strict"

        agg = StrictAggregate(uuid4())
        with pytest.raises(UnhandledEventError):
            agg.apply_event(Surprise(aggregate_id=agg.aggregate_id), is_new=True)

    def test_explicit_ignore_still_available(self) -> None:
        class Surprise2(DomainEvent):
            aggregate_type: str = "Lenient"

        class LenientAggregate(DeclarativeAggregate[dict]):
            aggregate_type = "Lenient"
            unregistered_event_handling = "ignore"

        agg = LenientAggregate(uuid4())
        agg.apply_event(Surprise2(aggregate_id=agg.aggregate_id), is_new=True)
        assert agg.version == 1
