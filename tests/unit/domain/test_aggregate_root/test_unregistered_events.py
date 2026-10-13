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


class TestUnregisteredEventHandling:
    """Tests for configurable unregistered event handling feature (TD-012).

    These tests verify:
    - AC1: Unhandled events raise UnhandledEventError when mode is "error"
    - AC2: Setting unregistered_event_handling="ignore" silently ignores events
    - AC3: Setting unregistered_event_handling="warn" logs warning
    - AC4: Error message lists available handlers
    - AC5: Default mode is "error" (strict by default; ignore/warn are explicit opt-downs)
    """

    def test_default_mode_is_error(self) -> None:
        """AC5: Default mode is 'error' -- an aggregate is the write model, so a
        silently unapplied event would leave command handlers reasoning over
        divergent state."""
        assert DeclarativeCounterAggregate.unregistered_event_handling == "error"

    def test_ignore_mode_silently_accepts_unknown_events(self) -> None:
        """AC2: ignore mode allows unknown events without raising or logging."""

        class IgnoreAggregate(DeclarativeAggregate[CounterState]):
            aggregate_type = "IgnoreAggregate"
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

        aggregate = IgnoreAggregate(uuid4())

        # Create an event type with no handler
        class UnknownEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        # Should not raise - just silently ignore
        aggregate.apply_event(event)
        assert aggregate.version == 1

    def test_error_mode_raises_unhandled_event_error(self) -> None:
        """AC1: error mode raises UnhandledEventError for unhandled events."""

        class StrictAggregate(DeclarativeAggregate[CounterState]):
            aggregate_type = "StrictAggregate"
            unregistered_event_handling = "error"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

            @handles(CounterIncremented)
            def _on_counter_incremented(self, event: CounterIncremented) -> None:
                if self._state is None:
                    self._state = self._get_initial_state()
                self._state = self._state.model_copy(
                    update={"value": self._state.value + event.increment}
                )

        aggregate = StrictAggregate(uuid4())

        # Create an event type with no handler
        class UnhandledEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = UnhandledEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        with pytest.raises(UnhandledEventError) as exc_info:
            aggregate.apply_event(event)

        assert "UnhandledEvent" in str(exc_info.value)
        assert "StrictAggregate" in str(exc_info.value)

    def test_warn_mode_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """AC3: warn mode logs warning for unhandled events."""

        class WarnAggregate(DeclarativeAggregate[CounterState]):
            aggregate_type = "WarnAggregate"
            unregistered_event_handling = "warn"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

            @handles(CounterIncremented)
            def _on_counter_incremented(self, event: CounterIncremented) -> None:
                if self._state is None:
                    self._state = self._get_initial_state()
                self._state = self._state.model_copy(
                    update={"value": self._state.value + event.increment}
                )

        aggregate = WarnAggregate(uuid4())

        class UnknownEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        with caplog.at_level(logging.WARNING):
            aggregate.apply_event(event)

        # Check warning was logged
        assert "No handler registered" in caplog.text
        assert "UnknownEvent" in caplog.text
        assert "WarnAggregate" in caplog.text
        # Event should still be applied (version updated)
        assert aggregate.version == 1

    def test_error_message_includes_available_handlers(self) -> None:
        """AC4: Error message lists handlers that ARE available."""

        class MultiHandlerAggregate(DeclarativeAggregate[CounterState]):
            aggregate_type = "MultiHandlerAggregate"
            unregistered_event_handling = "error"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

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
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        with pytest.raises(UnhandledEventError) as exc_info:
            aggregate.apply_event(event)

        error = exc_info.value
        assert "CounterIncremented" in error.available_handlers
        assert "CounterDecremented" in error.available_handlers
        assert error.event_type == "UnknownEvent"
        assert error.handler_class == "MultiHandlerAggregate"

        # Check error message format
        error_msg = str(error)
        assert "CounterIncremented" in error_msg
        assert "CounterDecremented" in error_msg

    def test_error_message_shows_no_handlers_when_empty(self) -> None:
        """Error message shows 'none' when no handlers are registered."""

        class EmptyAggregate(DeclarativeAggregate[CounterState]):
            aggregate_type = "EmptyAggregate"
            unregistered_event_handling = "error"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

        aggregate = EmptyAggregate(uuid4())

        class UnknownEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = UnknownEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        with pytest.raises(UnhandledEventError) as exc_info:
            aggregate.apply_event(event)

        error = exc_info.value
        assert error.available_handlers == []
        assert "none" in str(error)

    def test_subclass_can_override_handling_mode(self) -> None:
        """Subclasses can override the handling mode."""

        class BaseAggregate(DeclarativeAggregate[CounterState]):
            unregistered_event_handling = "ignore"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

        class StrictSubclass(BaseAggregate):
            unregistered_event_handling = "error"

        assert BaseAggregate.unregistered_event_handling == "ignore"
        assert StrictSubclass.unregistered_event_handling == "error"

    def test_handled_event_is_not_affected_by_mode(self) -> None:
        """Events with handlers work regardless of handling mode."""

        class StrictAggregate(DeclarativeAggregate[CounterState]):
            aggregate_type = "StrictAggregate"
            unregistered_event_handling = "error"

            def _get_initial_state(self) -> CounterState:
                return CounterState(counter_id=self.aggregate_id)

            @handles(CounterIncremented)
            def _on_counter_incremented(self, event: CounterIncremented) -> None:
                if self._state is None:
                    self._state = self._get_initial_state()
                self._state = self._state.model_copy(
                    update={"value": self._state.value + event.increment}
                )

        aggregate = StrictAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )

        # Should work fine - handler exists
        aggregate.apply_event(event)
        assert aggregate.state is not None
        assert aggregate.state.value == 5

    def test_unregistered_event_raises_by_default_without_opt_down(self) -> None:
        """Aggregates that don't opt down raise on an unhandled event -- the
        strict default catches events silently falling through to no handler."""
        # DeclarativeCounterAggregate is defined without explicit mode setting
        aggregate = DeclarativeCounterAggregate(uuid4())

        # Create an event type with no handler
        class FutureEvent(DomainEvent):
            aggregate_type: str = "Counter"

        event = FutureEvent(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
        )

        with pytest.raises(UnhandledEventError):
            aggregate.apply_event(event)
