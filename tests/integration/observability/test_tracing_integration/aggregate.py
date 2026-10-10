"""Test aggregate for repository tracing integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from ..conftest import TracingTestEventCreated, TracingTestEventUpdated

if TYPE_CHECKING:
    from eventsource import DomainEvent


class TracingTestAggregateState:
    """State for TracingTestAggregate."""

    def __init__(self) -> None:
        self.name: str = ""
        self.value: int = 0


class TracingTestAggregate:
    """Simple test aggregate for tracing integration tests."""

    aggregate_type = "TracingTestAggregate"
    schema_version = 1

    def __init__(self, aggregate_id: UUID) -> None:
        self.aggregate_id = aggregate_id
        self._version = 0
        self._uncommitted_events: list[DomainEvent] = []
        self._state = TracingTestAggregateState()

    @property
    def version(self) -> int:
        return self._version

    @property
    def uncommitted_events(self) -> list[DomainEvent]:
        return self._uncommitted_events.copy()

    @property
    def has_uncommitted_events(self) -> bool:
        return len(self._uncommitted_events) > 0

    @property
    def state(self) -> TracingTestAggregateState:
        return self._state

    def mark_events_as_committed(self) -> None:
        self._uncommitted_events.clear()

    def apply_event(self, event: DomainEvent, is_new: bool = True) -> None:
        if isinstance(event, TracingTestEventCreated):
            self._state.name = event.name
        elif isinstance(event, TracingTestEventUpdated):
            self._state.value = event.value

        if is_new:
            self._uncommitted_events.append(event)

        self._version += 1

    def load_from_history(self, events: list[DomainEvent]) -> None:
        for event in events:
            self.apply_event(event, is_new=False)

    def create(self, name: str) -> None:
        """Create the aggregate with a name."""
        event = TracingTestEventCreated(
            aggregate_id=self.aggregate_id,
            aggregate_version=self._version + 1,
            name=name,
        )
        self.apply_event(event)

    def update_value(self, value: int) -> None:
        """Update the value."""
        event = TracingTestEventUpdated(
            aggregate_id=self.aggregate_id,
            aggregate_version=self._version + 1,
            value=value,
        )
        self.apply_event(event)
