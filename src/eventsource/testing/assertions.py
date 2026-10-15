"""Event-specific test assertions for event sourcing tests."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar
from uuid import UUID

from eventsource.domain.event import DomainEvent

TEvent = TypeVar("TEvent", bound=DomainEvent)


class _Missing:
    """Sentinel class for detecting missing attributes."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<MISSING>"


_MISSING = _Missing()


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
        matching = [e for e in self._events if isinstance(e, event_type)]
        if not matching:
            found_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Expected {event_type.__name__} to be published, but no events "
                f"of that type were found. Published event types: {found_types or 'none'}"
            )
        return matching[0]

    def assert_no_event_published(self, event_type: type[DomainEvent]) -> None:
        """Assert that no event of the given type was published."""
        matching = [e for e in self._events if isinstance(e, event_type)]
        if matching:
            raise AssertionError(
                f"Expected no {event_type.__name__} events to be published, "
                f"but found {len(matching)}"
            )

    def assert_event_count(self, count: int) -> None:
        """Assert the exact number of events published."""
        actual_count = len(self._events)
        if actual_count != count:
            event_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Event count mismatch: expected {count} events, "
                f"got {actual_count}.\n"
                f"Published: {event_types}"
            )

    def assert_event_type_count(self, event_type: type[TEvent], count: int) -> list[TEvent]:
        """Assert the exact number of events of a given type and return matching events."""
        matching = [e for e in self._events if isinstance(e, event_type)]
        if len(matching) != count:
            found_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Expected {count} {event_type.__name__} event(s), got {len(matching)}. "
                f"Published event types: {found_types or 'none'}"
            )
        return matching

    def assert_event_sequence(self, event_types: Sequence[type[DomainEvent]]) -> None:
        """Assert that events were published in the given order."""
        actual_types = [type(e) for e in self._events]
        if len(actual_types) != len(event_types):
            raise AssertionError(
                f"Event count mismatch: expected {len(event_types)} events, "
                f"got {len(actual_types)}.\n"
                f"Expected: {[t.__name__ for t in event_types]}\n"
                f"Actual:   {[t.__name__ for t in actual_types]}"
            )

        for i, (actual, expected) in enumerate(zip(actual_types, event_types, strict=True)):
            if actual != expected:
                raise AssertionError(
                    f"Event type mismatch at position {i}: "
                    f"expected {expected.__name__}, got {actual.__name__}.\n"
                    f"Full sequence:\n"
                    f"Expected: {[t.__name__ for t in event_types]}\n"
                    f"Actual:   {[t.__name__ for t in actual_types]}"
                )

    def assert_event_with_fields(self, event_type: type[TEvent], **expected_fields: Any) -> TEvent:
        """Assert an event was published with specific field values and return the match."""
        matching_events = [e for e in self._events if isinstance(e, event_type)]
        if not matching_events:
            found_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Expected {event_type.__name__} to be published, but no events "
                f"of that type were found. Published event types: {found_types or 'none'}"
            )

        if not expected_fields:
            return matching_events[0]

        for event in matching_events:
            if all(
                (actual := getattr(event, attr, _MISSING)) is not _MISSING
                and actual == expected_value
                for attr, expected_value in expected_fields.items()
            ):
                return event

        attr_desc = ", ".join(f"{k}={v!r}" for k, v in expected_fields.items())
        found_desc = [
            f"{type(e).__name__}({ {k: getattr(e, k, None) for k in expected_fields} })"
            for e in matching_events
        ]

        raise AssertionError(
            f"Expected {event_type.__name__}({attr_desc}) to be published.\n"
            f"Found {len(matching_events)} {event_type.__name__} event(s) but "
            f"none matched the expected attributes:\n"
            + "\n".join(f"  - {desc}" for desc in found_desc)
        )

    def assert_no_events_published(self) -> None:
        """Assert that no events were published."""
        if self._events:
            found_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Expected no events to be published, but found {len(self._events)}: {found_types}"
            )

    def assert_event_for_aggregate(
        self,
        event_type: type[TEvent],
        aggregate_id: UUID,
    ) -> TEvent:
        """Assert an event was published for a specific aggregate and return the match."""
        matching = [
            e for e in self._events if isinstance(e, event_type) and e.aggregate_id == aggregate_id
        ]
        if not matching:
            type_matching = [e for e in self._events if isinstance(e, event_type)]
            if type_matching:
                found_agg_ids = [str(e.aggregate_id) for e in type_matching]
                raise AssertionError(
                    f"Expected {event_type.__name__} for aggregate {aggregate_id}, "
                    f"but found {event_type.__name__} only for aggregates: {found_agg_ids}"
                )
            found_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Expected {event_type.__name__} for aggregate {aggregate_id}, "
                f"but no events of that type were found. "
                f"Published event types: {found_types or 'none'}"
            )
        return matching[0]

    def get_events_of_type(self, event_type: type[TEvent]) -> list[TEvent]:
        """Get all events of a specific type."""
        return [e for e in self._events if isinstance(e, event_type)]

    def __len__(self) -> int:
        """Return total number of events."""
        return len(self._events)

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        event_types = [type(e).__name__ for e in self._events]
        return f"EventAssertions(events={event_types})"


__all__ = ["EventAssertions"]
