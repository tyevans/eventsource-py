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
    """
    event_list = [events] if isinstance(events, DomainEvent) else list(events)
    await harness.append_events(*event_list, expected_version=expected_version)


def when_command[TAggregate: AggregateRoot[Any]](
    aggregate: TAggregate,
    command: Callable[[TAggregate], None],
) -> list[DomainEvent]:
    """Execute a command on an aggregate and return newly uncommitted events."""
    before_count = len(aggregate.uncommitted_events)
    command(aggregate)
    return list(aggregate.uncommitted_events[before_count:])


def then_event_published[TEvent: DomainEvent](
    harness: InMemoryTestHarness,
    event_type: type[TEvent],
    **expected_fields: Any,
) -> TEvent:
    """Assert that an event of ``event_type`` (matching optional ``expected_fields``) was published."""
    return harness.assertions.assert_event_with_fields(event_type, **expected_fields)


def then_no_events_published(
    harness: InMemoryTestHarness,
    event_type: type[DomainEvent] | None = None,
) -> None:
    """Assert that no events (or no events of ``event_type``) were published."""
    if event_type is None:
        harness.assertions.assert_no_events_published()
    else:
        harness.assertions.assert_no_event_published(event_type)


def then_event_sequence(
    harness: InMemoryTestHarness,
    event_types: Sequence[type[DomainEvent]],
) -> list[DomainEvent]:
    """Assert that events were published in the given sequence of types and return them."""
    assertions = harness.assertions
    assertions.assert_event_sequence(event_types)
    return assertions.events


def then_event_count(
    harness: InMemoryTestHarness,
    count: int,
    *,
    event_type: type[DomainEvent] | None = None,
) -> None:
    """Assert the exact number of published events (or published events of ``event_type``)."""
    if event_type is None:
        harness.assertions.assert_event_count(count)
    else:
        harness.assertions.assert_event_type_count(event_type, count)


__all__ = [
    "given_events",
    "when_command",
    "then_event_published",
    "then_no_events_published",
    "then_event_sequence",
    "then_event_count",
    "DeciderScenario",
]
