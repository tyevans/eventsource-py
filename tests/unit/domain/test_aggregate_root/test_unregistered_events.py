"""Unit tests for configurable unregistered event handling feature (TD-012)."""

from __future__ import annotations

import logging
from uuid import uuid4

import pytest

from eventsource.domain.aggregate import DeclarativeAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import UnhandledEventError
from tests.unit.domain.test_aggregate_root._fixtures import (
    CounterDecremented,
    CounterIncremented,
    CounterState,
    DeclarativeCounterAggregate,
)


class _CounterBase(DeclarativeAggregate[CounterState]):
    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)


class _IncrementCounterBase(_CounterBase):
    @handles(CounterIncremented)
    def _on_counter_incremented(self, event: CounterIncremented) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"value": self._state.value + event.increment})


class TestUnregisteredEventHandling:
    def test_default_mode_is_error(self) -> None:
        assert DeclarativeCounterAggregate.unregistered_event_handling == "error"

    def test_ignore_mode_silently_accepts_unknown_events(self) -> None:
        class IgnoreAggregate(_IncrementCounterBase):
            aggregate_type = "IgnoreAggregate"
            unregistered_event_handling = "ignore"

        aggregate = IgnoreAggregate(uuid4())

        class UnknownEvent(DomainEvent):
            aggregate_type: str = "Counter"

        aggregate.apply_event(
            UnknownEvent(
                aggregate_id=aggregate.aggregate_id, aggregate_type="Counter", aggregate_version=1
            )
        )
        assert aggregate.version == 1

    def test_error_mode_raises_unhandled_event_error(self) -> None:
        class StrictAggregate(_IncrementCounterBase):
            aggregate_type = "StrictAggregate"
            unregistered_event_handling = "error"

        aggregate = StrictAggregate(uuid4())

        class UnhandledEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = UnhandledEvent(
            aggregate_id=aggregate.aggregate_id, aggregate_type="Counter", aggregate_version=1
        )
        with pytest.raises(UnhandledEventError) as exc_info:
            aggregate.apply_event(event)
        assert "UnhandledEvent" in str(exc_info.value) and "StrictAggregate" in str(exc_info.value)

    def test_warn_mode_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        class WarnAggregate(_IncrementCounterBase):
            aggregate_type = "WarnAggregate"
            unregistered_event_handling = "warn"

        aggregate = WarnAggregate(uuid4())

        class UnknownEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id, aggregate_type="Counter", aggregate_version=1
        )
        with caplog.at_level(logging.WARNING):
            aggregate.apply_event(event)
        assert "No handler registered" in caplog.text
        assert "UnknownEvent" in caplog.text and "WarnAggregate" in caplog.text
        assert aggregate.version == 1

    def test_error_message_includes_available_handlers(self) -> None:
        class MultiHandlerAggregate(_CounterBase):
            aggregate_type = "MultiHandlerAggregate"
            unregistered_event_handling = "error"

            @handles(CounterIncremented)
            def _on_counter_incremented(self, event: CounterIncremented) -> None:
                pass

            @handles(CounterDecremented)
            def _on_counter_decremented(self, event: CounterDecremented) -> None:
                pass

        aggregate = MultiHandlerAggregate(uuid4())

        class UnknownEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id, aggregate_type="Counter", aggregate_version=1
        )
        with pytest.raises(UnhandledEventError) as exc_info:
            aggregate.apply_event(event)
        error = exc_info.value
        assert "CounterIncremented" in error.available_handlers
        assert "CounterDecremented" in error.available_handlers
        assert (error.event_type, error.handler_class) == ("UnknownEvent", "MultiHandlerAggregate")
        error_msg = str(error)
        assert "CounterIncremented" in error_msg and "CounterDecremented" in error_msg

    def test_error_message_shows_no_handlers_when_empty(self) -> None:
        class EmptyAggregate(_CounterBase):
            aggregate_type = "EmptyAggregate"
            unregistered_event_handling = "error"

        aggregate = EmptyAggregate(uuid4())

        class UnknownEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id, aggregate_type="Counter", aggregate_version=1
        )
        with pytest.raises(UnhandledEventError) as exc_info:
            aggregate.apply_event(event)
        assert exc_info.value.available_handlers == [] and "none" in str(exc_info.value)

    def test_subclass_can_override_handling_mode(self) -> None:
        class BaseAggregate(_CounterBase):
            unregistered_event_handling = "ignore"

        class StrictSubclass(BaseAggregate):
            unregistered_event_handling = "error"

        assert BaseAggregate.unregistered_event_handling == "ignore"
        assert StrictSubclass.unregistered_event_handling == "error"

    def test_handled_event_is_not_affected_by_mode(self) -> None:
        class StrictAggregate(_IncrementCounterBase):
            aggregate_type = "StrictAggregate"
            unregistered_event_handling = "error"

        aggregate = StrictAggregate(uuid4())
        aggregate.apply_event(
            CounterIncremented(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Counter",
                aggregate_version=1,
                increment=5,
            )
        )
        assert aggregate.state is not None and aggregate.state.value == 5

    def test_unregistered_event_raises_by_default_without_opt_down(self) -> None:
        aggregate = DeclarativeCounterAggregate(uuid4())

        class FutureEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = FutureEvent(
            aggregate_id=aggregate.aggregate_id, aggregate_type="Counter", aggregate_version=1
        )
        with pytest.raises(UnhandledEventError):
            aggregate.apply_event(event)
