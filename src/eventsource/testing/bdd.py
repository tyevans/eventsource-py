"""
BDD (Behavior-Driven Development) helpers for event sourcing tests.

Provides Given-When-Then style helpers that integrate with InMemoryTestHarness
and AggregateRoot subclasses to make event sourcing tests read like specifications.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion
from eventsource.testing.harness import InMemoryTestHarness
from eventsource.testing.scenario import DeciderScenario


class _Missing:
    """Sentinel class for detecting missing attributes."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<MISSING>"


_MISSING = _Missing()


async def given_events(
    harness: InMemoryTestHarness,
    events: Sequence[DomainEvent] | DomainEvent,
    *,
    expected_version: ExpectedVersion | None = None,
) -> None:
    """
    Set up the test scenario with pre-existing events.

    Establishes the "Given" part of the BDD scenario by grouping events by
    aggregate and appending them in order to the event store.

    Args:
        harness: The InMemoryTestHarness instance
        events: Initial state events (single event or sequence)
        expected_version: Optional stream version expectation (defaults to ExpectedVersion.any_())
    """
    event_list = [events] if isinstance(events, DomainEvent) else list(events)
    if not event_list:
        return

    by_aggregate: dict[tuple[UUID, str], list[DomainEvent]] = {}
    for event in event_list:
        key = (event.aggregate_id, event.aggregate_type)
        by_aggregate.setdefault(key, []).append(event)

    version_constraint = expected_version or ExpectedVersion.any_()
    for (agg_id, agg_type), agg_events in by_aggregate.items():
        await harness.event_store.append(
            StreamId(aggregate_id=agg_id, category=agg_type),
            agg_events,
            version_constraint,
        )


def when_command[TAggregate: AggregateRoot[Any]](
    aggregate: TAggregate,
    command: Callable[[TAggregate], None],
) -> list[DomainEvent]:
    """
    Execute a command on an aggregate and return newly uncommitted events.

    Args:
        aggregate: The aggregate to execute the command on
        command: A callable that takes the aggregate and executes a command

    Returns:
        List of new uncommitted events added during the command execution
    """
    before_count = len(aggregate.uncommitted_events)
    command(aggregate)
    return list(aggregate.uncommitted_events[before_count:])


def then_event_published[TEvent: DomainEvent](
    harness: InMemoryTestHarness,
    event_type: type[TEvent],
    **expected_fields: Any,
) -> TEvent:
    """
    Assert that an event of the given type was published through the event bus.

    Args:
        harness: The InMemoryTestHarness instance
        event_type: The expected event class
        **expected_fields: Optional field values to match

    Returns:
        The matching event for further assertions

    Raises:
        AssertionError: If no matching event was published
    """
    published = harness.published_events
    matching_events: list[DomainEvent] = [
        event for event in published if isinstance(event, event_type)
    ]

    if not matching_events:
        found_types = [type(e).__name__ for e in published]
        raise AssertionError(
            f"Expected {event_type.__name__} to be published, but no events "
            f"of that type were found. Published event types: {found_types or 'none'}"
        )

    if not expected_fields:
        return matching_events[0]  # type: ignore[return-value]

    for event in matching_events:
        if all(
            (actual := getattr(event, attr, _MISSING)) is not _MISSING and actual == expected_value
            for attr, expected_value in expected_fields.items()
        ):
            return event  # type: ignore[return-value]

    attr_desc = ", ".join(f"{k}={v!r}" for k, v in expected_fields.items())
    found_desc = [
        f"{type(e).__name__}({ {k: getattr(e, k, None) for k in expected_fields} })"
        for e in matching_events
    ]

    raise AssertionError(
        f"Expected {event_type.__name__}({attr_desc}) to be published.\n"
        f"Found {len(matching_events)} {event_type.__name__} event(s) but "
        f"none matched the expected attributes:\n" + "\n".join(f"  - {desc}" for desc in found_desc)
    )


def then_no_events_published(
    harness: InMemoryTestHarness,
    event_type: type[DomainEvent] | None = None,
) -> None:
    """
    Assert that no events (or no events of a specific type) were published.

    Args:
        harness: The InMemoryTestHarness instance
        event_type: Optional event type to check; if None, checks that no events were published

    Raises:
        AssertionError: If events were found
    """
    published = harness.published_events
    if event_type is None:
        if published:
            found_types = [type(e).__name__ for e in published]
            raise AssertionError(
                f"Expected no events to be published, but found {len(published)}: {found_types}"
            )
    else:
        matching = [e for e in published if isinstance(e, event_type)]
        if matching:
            raise AssertionError(
                f"Expected no {event_type.__name__} events to be published, "
                f"but found {len(matching)}"
            )


def then_event_sequence(
    harness: InMemoryTestHarness,
    event_types: Sequence[type[DomainEvent]],
) -> list[DomainEvent]:
    """
    Assert that events were published in the given sequence of types.

    Args:
        harness: The InMemoryTestHarness instance
        event_types: The expected sequence of event types

    Returns:
        The list of published events for further assertions

    Raises:
        AssertionError: If the sequence doesn't match (wrong count, types, or order)
    """
    published = harness.published_events
    actual_types = [type(e) for e in published]

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

    return list(published)


def then_event_count(
    harness: InMemoryTestHarness,
    count: int,
) -> None:
    """
    Assert the exact number of events published.

    Args:
        harness: The InMemoryTestHarness instance
        count: The expected number of events

    Raises:
        AssertionError: If the count doesn't match
    """
    actual_count = len(harness.published_events)
    if actual_count != count:
        event_types = [type(e).__name__ for e in harness.published_events]
        raise AssertionError(
            f"Event count mismatch: expected {count} events, "
            f"got {actual_count}.\n"
            f"Published: {event_types}"
        )


__all__ = [
    "given_events",
    "when_command",
    "then_event_published",
    "then_no_events_published",
    "then_event_sequence",
    "then_event_count",
    "DeciderScenario",
]
