"""Shared fixtures and test aggregate definitions for repository tracing tests."""

from __future__ import annotations

from pydantic import BaseModel, Field

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event


class TracingTestState(BaseModel):
    """Simple state for tracing tests."""

    value: str = ""
    count: int = 0
    items: list[str] = Field(default_factory=list)


_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class TracingTestEvent(DomainEvent):
    """Test event for tracing tests."""

    __test__ = False

    aggregate_type: str = "TracingTest"
    value: str


class TracingTestAggregate(AggregateRoot[TracingTestState]):
    """Test aggregate with tracing support."""

    __test__ = False

    aggregate_type = "TracingTest"
    schema_version = 1

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TracingTestEvent):
            if self._state is None:
                self._state = TracingTestState()
            self._state = self._state.model_copy(
                update={
                    "value": event.value,
                    "count": self._state.count + 1,
                }
            )

    def _get_initial_state(self) -> TracingTestState:
        return TracingTestState()

    def do_something(self, value: str) -> None:
        """Apply a test event."""
        event = TracingTestEvent(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            value=value,
        )
        self.apply_event(event)


__all__ = [
    "TracingTestAggregate",
    "TracingTestEvent",
    "TracingTestState",
    "_REGISTRY",
]
