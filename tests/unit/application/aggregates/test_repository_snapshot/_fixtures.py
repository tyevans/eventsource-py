"""Common test state, events, and aggregate definitions for snapshot tests."""

from __future__ import annotations

from pydantic import BaseModel, Field

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event


class TestState(BaseModel):
    """Simple state for testing."""

    __test__ = False
    value: str = ""
    count: int = 0
    items: list[str] = Field(default_factory=list)


_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class TestEvent(DomainEvent):
    """Simple event for testing."""

    __test__ = False
    aggregate_type: str = "Test"
    value: str


@register_event(registry=_REGISTRY)
class CountEvent(DomainEvent):
    """Event that increments count."""

    aggregate_type: str = "Test"


class TestAggregate(AggregateRoot[TestState]):
    """Test aggregate with snapshot support."""

    __test__ = False
    aggregate_type = "Test"
    schema_version = 1

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TestEvent):
            if self._state is None:
                self._state = TestState()
            self._state = self._state.model_copy(
                update={
                    "value": event.value,
                    "count": self._state.count + 1,
                }
            )
        elif isinstance(event, CountEvent):
            if self._state is None:
                self._state = TestState()
            self._state = self._state.model_copy(update={"count": self._state.count + 1})

    def _get_initial_state(self) -> TestState:
        return TestState()

    def do_something(self, value: str) -> None:
        """Apply a test event."""
        event = TestEvent(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            value=value,
        )
        self.apply_event(event)

    def increment(self) -> None:
        """Apply a count event."""
        event = CountEvent(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


class TestAggregateV2(AggregateRoot[TestState]):
    """Same aggregate with different schema version."""

    __test__ = False
    aggregate_type = "Test"
    schema_version = 2  # Different from TestAggregate

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TestEvent):
            if self._state is None:
                self._state = TestState()
            self._state = self._state.model_copy(
                update={
                    "value": event.value,
                    "count": self._state.count + 1,
                }
            )

    def _get_initial_state(self) -> TestState:
        return TestState()
