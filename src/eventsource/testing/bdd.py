"""
BDD (Behavior-Driven Development) helpers for event sourcing tests.

Provides Given-When-Then style helpers that integrate with InMemoryTestHarness
and AggregateRoot subclasses to make event sourcing tests read like specifications.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion
from eventsource.testing.assertions import EventAssertions
from eventsource.testing.harness import InMemoryTestHarness
from eventsource.testing.scenario import DeciderScenario


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
    await harness.append_events(*event_list, expected_version=expected_version)


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
    return EventAssertions(harness.published_events).assert_event_with_fields(
        event_type, **expected_fields
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
    assertions = EventAssertions(harness.published_events)
    if event_type is None:
        assertions.assert_no_events_published()
    else:
        assertions.assert_no_event_published(event_type)


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
    assertions = EventAssertions(harness.published_events)
    assertions.assert_event_sequence(event_types)
    return assertions.events


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
    EventAssertions(harness.published_events).assert_event_count(count)


__all__ = [
    "given_events",
    "when_command",
    "then_event_published",
    "then_no_events_published",
    "then_event_sequence",
    "then_event_count",
    "DeciderScenario",
]
