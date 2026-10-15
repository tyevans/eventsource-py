"""Event-specific test assertions for event sourcing tests."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar
from uuid import UUID

from eventsource.domain.event import DomainEvent

TEvent = TypeVar("TEvent", bound=DomainEvent)


_MISSING = object()


class EventAssertions:
    """Domain-specific assertions for event sourcing tests."""

    def __init__(self, events: Sequence[DomainEvent]) -> None:
        """Initialize assertions with a sequence of events."""
        self._events = list(events)

    @property
    def events(self) -> list[DomainEvent]:
        """Get a copy of the events being asserted against."""
        return self._events.copy()

    def assert_event_published(self, event_type: type[TEvent]) -> TEvent:
        """Assert that an event of the given type was published and return the first match."""
        if matching := self.get_events_of_type(event_type):
            return matching[0]
        found = [type(e).__name__ for e in self._events] or "none"
        raise AssertionError(
            f"Expected {event_type.__name__} to be published, but no events of that type were found. Published event types: {found}"
        )

    def assert_no_event_published(self, event_type: type[DomainEvent]) -> None:
        """Assert that no event of the given type was published."""
        if matching := self.get_events_of_type(event_type):
            raise AssertionError(
                f"Expected no {event_type.__name__} events to be published, but found {len(matching)}"
            )

    def assert_event_count(self, count: int) -> None:
        """Assert the exact number of events published."""
        if len(self._events) != count:
            types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Event count mismatch: expected {count} events, got {len(self._events)}.\nPublished: {types}"
            )

    def assert_event_type_count(self, event_type: type[TEvent], count: int) -> list[TEvent]:
        """Assert the exact number of events of a given type and return matching events."""
        matching = self.get_events_of_type(event_type)
        if len(matching) != count:
            found = [type(e).__name__ for e in self._events] or "none"
            raise AssertionError(
                f"Expected {count} {event_type.__name__} event(s), got {len(matching)}. Published event types: {found}"
            )
        return matching

    def assert_event_sequence(self, event_types: Sequence[type[DomainEvent]]) -> None:
        """Assert that events were published in the given order."""
        actual = [type(e) for e in self._events]
        seq = f"Expected: {[t.__name__ for t in event_types]}\nActual:   {[t.__name__ for t in actual]}"
        if len(actual) != len(event_types):
            raise AssertionError(
                f"Event count mismatch: expected {len(event_types)} events, got {len(actual)}.\n{seq}"
            )
        for i, (act, exp) in enumerate(zip(actual, event_types, strict=True)):
            if act != exp:
                raise AssertionError(
                    f"Event type mismatch at position {i}: expected {exp.__name__}, got {act.__name__}.\nFull sequence:\n{seq}"
                )

    def assert_event_with_fields(self, event_type: type[TEvent], **expected_fields: Any) -> TEvent:
        """Assert an event was published with specific field values and return the match."""
        self.assert_event_published(event_type)
        matching_events = self.get_events_of_type(event_type)
        for event in matching_events:
            if all(
                getattr(event, k, _MISSING) == v and hasattr(event, k)
                for k, v in expected_fields.items()
            ):
                return event

        attr_desc = ", ".join(f"{k}={v!r}" for k, v in expected_fields.items())
        found_desc = "\n".join(
            f"  - {type(e).__name__}({ {k: getattr(e, k, None) for k in expected_fields} })"
            for e in matching_events
        )
        raise AssertionError(
            f"Expected {event_type.__name__}({attr_desc}) to be published.\nFound {len(matching_events)} {event_type.__name__} event(s) but none matched the expected attributes:\n{found_desc}"
        )

    def assert_no_events_published(self) -> None:
        """Assert that no events were published."""
        if self._events:
            found_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Expected no events to be published, but found {len(self._events)}: {found_types}"
            )

    def assert_event_for_aggregate(self, event_type: type[TEvent], aggregate_id: UUID) -> TEvent:
        """Assert an event was published for a specific aggregate and return the match."""
        type_matching = self.get_events_of_type(event_type)
        for e in type_matching:
            if e.aggregate_id == aggregate_id:
                return e
        if type_matching:
            found_agg_ids = [str(e.aggregate_id) for e in type_matching]
            raise AssertionError(
                f"Expected {event_type.__name__} for aggregate {aggregate_id}, but found {event_type.__name__} only for aggregates: {found_agg_ids}"
            )
        found = [type(e).__name__ for e in self._events] or "none"
        raise AssertionError(
            f"Expected {event_type.__name__} for aggregate {aggregate_id}, but no events of that type were found. Published event types: {found}"
        )

    def for_aggregate(self, aggregate_id: UUID) -> EventAssertions:
        """Return a new EventAssertions scoped to events for the given aggregate."""
        return EventAssertions([e for e in self._events if e.aggregate_id == aggregate_id])

    def assert_events_for_aggregate(
        self, aggregate_id: UUID, expected_count: int | None = None
    ) -> list[DomainEvent]:
        """Assert events were published for an aggregate and return matching events."""
        matching = [e for e in self._events if e.aggregate_id == aggregate_id]
        if expected_count is not None and len(matching) != expected_count:
            raise AssertionError(
                f"Expected {expected_count} event(s) for aggregate {aggregate_id}, got {len(matching)}."
            )
        if expected_count is None and not matching:
            raise AssertionError(
                f"Expected events for aggregate {aggregate_id}, but none were found."
            )
        return matching

    def _assert_positional_event(
        self, index: int, label: str, event_type: type[TEvent] | None, fields: dict[str, Any]
    ) -> DomainEvent:
        if not self._events:
            raise AssertionError("Expected at least one event, but no events were recorded.")
        event = self._events[index]
        if event_type is not None and not isinstance(event, event_type):
            raise AssertionError(
                f"Expected {label} event to be {event_type.__name__}, got {type(event).__name__}."
            )
        for k, v in fields.items():
            if (actual := getattr(event, k, None)) != v:
                raise AssertionError(f"Expected {label} event field {k}={v!r}, got {actual!r}.")
        return event

    def assert_first_event(
        self, event_type: type[TEvent] | None = None, **expected_fields: Any
    ) -> DomainEvent:
        """Assert on the first recorded event and return it."""
        return self._assert_positional_event(0, "first", event_type, expected_fields)

    def assert_last_event(
        self, event_type: type[TEvent] | None = None, **expected_fields: Any
    ) -> DomainEvent:
        """Assert on the last recorded event and return it."""
        return self._assert_positional_event(-1, "last", event_type, expected_fields)

    def get_events_of_type(self, event_type: type[TEvent]) -> list[TEvent]:
        """Get all events of a specific type."""
        return [e for e in self._events if isinstance(e, event_type)]

    def __bool__(self) -> bool:
        """Return True so EventAssertions instances are always truthy."""
        return True

    def __len__(self) -> int:
        """Return total number of events."""
        return len(self._events)

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"EventAssertions(events={[type(e).__name__ for e in self._events]})"


__all__ = ["EventAssertions"]
