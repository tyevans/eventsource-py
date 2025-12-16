"""
BDD (Behavior-Driven Development) helpers for event sourcing tests.

This module provides Given-When-Then style helpers that make event sourcing
tests read more like specifications. These helpers integrate with the
InMemoryTestHarness to provide a clean, expressive testing API.

Example:
    >>> from eventsource.testing import (
    ...     InMemoryTestHarness,
    ...     given_events,
    ...     when_command,
    ...     then_event_published,
    ...     then_event_sequence,
    ...     then_event_count,
    ... )
    >>>
    >>> async def test_order_can_be_shipped():
    ...     harness = InMemoryTestHarness()
    ...     order = OrderAggregate(order_id)
    ...
    ...     # Given: An order has been created and paid
    ...     await given_events(harness, [
    ...         OrderCreated(aggregate_id=order_id, customer_id=customer_id),
    ...         PaymentReceived(aggregate_id=order_id, amount=Decimal("99.99")),
    ...     ])
    ...
    ...     # When: We execute the ship command on the aggregate
    ...     new_events = when_command(order, lambda o: o.ship("TRACK123"))
    ...
    ...     # Then: An OrderShipped event should be created
    ...     assert len(new_events) == 1
    ...     assert isinstance(new_events[0], OrderShipped)

Note:
    These helpers are designed to be used with InMemoryTestHarness and
    AggregateRoot subclasses. They work with the testing module's components
    to provide a cohesive testing experience.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, TypeVar
from uuid import UUID

from eventsource.aggregates.base import AggregateRoot
from eventsource.events.base import DomainEvent
from eventsource.testing.harness import InMemoryTestHarness

# Type variables for generic command/event handling
TAggregate = TypeVar("TAggregate", bound=AggregateRoot[Any])
TEvent = TypeVar("TEvent", bound=DomainEvent)


class _Missing:
    """Sentinel class for detecting missing attributes."""

    __slots__ = ()

    def __repr__(self) -> str:
        """Return a string representation."""
        return "<MISSING>"


_MISSING = _Missing()


async def given_events(
    harness: InMemoryTestHarness,
    events: Sequence[DomainEvent],
) -> None:
    """
    Set up the test scenario with pre-existing events.

    This establishes the "Given" part of the BDD scenario by appending
    events to the event store. Events are grouped by aggregate and
    appended in order.

    Args:
        harness: The InMemoryTestHarness instance
        events: The events that represent the initial state

    Example:
        >>> harness = InMemoryTestHarness()
        >>> await given_events(harness, [
        ...     OrderCreated(aggregate_id=order_id, aggregate_type="Order",
        ...                  event_type="OrderCreated", aggregate_version=1,
        ...                  customer_id=customer_id),
        ...     PaymentReceived(aggregate_id=order_id, aggregate_type="Order",
        ...                     event_type="PaymentReceived", aggregate_version=2,
        ...                     amount=Decimal("99.99")),
        ... ])

    Note:
        Events are assumed to be for fresh aggregates (expected_version=0).
        For complex scenarios with multiple aggregates, events are
        automatically grouped by (aggregate_id, aggregate_type).
    """
    if not events:
        return

    # Group events by aggregate (aggregate_id, aggregate_type)
    by_aggregate: dict[tuple[UUID, str], list[DomainEvent]] = {}
    for event in events:
        key = (event.aggregate_id, event.aggregate_type)
        by_aggregate.setdefault(key, []).append(event)

    # Append each aggregate's events to the store
    for (agg_id, agg_type), agg_events in by_aggregate.items():
        await harness.event_store.append_events(
            aggregate_id=agg_id,
            aggregate_type=agg_type,
            events=agg_events,
            expected_version=0,
        )


def when_command(
    aggregate: TAggregate,
    command: Callable[[TAggregate], None],
) -> list[DomainEvent]:
    """
    Execute a command on an aggregate and return resulting events.

    This establishes the "When" part of the BDD scenario by executing
    a command that should produce events. The command is typically
    a lambda calling a method on the aggregate.

    Args:
        aggregate: The aggregate to execute the command on
        command: A callable that takes the aggregate and executes a command

    Returns:
        List of new events created by the command (uncommitted events
        added during the command execution)

    Example:
        >>> order = OrderAggregate(order_id)
        >>> order.load_from_history(existing_events)
        >>> new_events = when_command(order, lambda o: o.ship("TRACK123"))
        >>> assert len(new_events) == 1
        >>> assert isinstance(new_events[0], OrderShipped)

    Note:
        The returned events are the new uncommitted events only,
        not all uncommitted events on the aggregate. This allows
        testing a single command in isolation.
    """
    before_count = len(aggregate.uncommitted_events)
    command(aggregate)
    return list(aggregate.uncommitted_events[before_count:])


def then_event_published(
    harness: InMemoryTestHarness,
    event_type: type[TEvent],
    **expected_fields: Any,
) -> TEvent:
    """
    Assert that an event of the given type was published.

    This establishes the "Then" part of the BDD scenario by asserting
    on the events that were published through the event bus.

    Args:
        harness: The InMemoryTestHarness instance
        event_type: The expected event class
        **expected_fields: Optional field values to match

    Returns:
        The matching event for further assertions

    Example:
        >>> # Basic assertion
        >>> then_event_published(harness, OrderShipped)

        >>> # With field matching
        >>> event = then_event_published(
        ...     harness,
        ...     OrderShipped,
        ...     shipping_method="express",
        ... )
        >>> assert event.tracking_number is not None

    Raises:
        AssertionError: If no matching event was published
    """
    published = harness.published_events

    # Find events of the expected type
    matching_events: list[DomainEvent] = [
        event for event in published if isinstance(event, event_type)
    ]

    if not matching_events:
        # No events of this type found
        found_types = [type(e).__name__ for e in published]
        raise AssertionError(
            f"Expected {event_type.__name__} to be published, but no events "
            f"of that type were found. Published event types: {found_types or 'none'}"
        )

    # If no field constraints, return first match
    if not expected_fields:
        # Type assertion: we know matching_events[0] is TEvent because
        # we filtered by isinstance(event, event_type)
        return matching_events[0]  # type: ignore[return-value]

    # Check attribute matches
    for event in matching_events:
        match = True
        for attr, expected_value in expected_fields.items():
            actual_value = getattr(event, attr, _MISSING)
            if actual_value is _MISSING:
                match = False
                break
            if actual_value != expected_value:
                match = False
                break
        if match:
            return event  # type: ignore[return-value]

    # Build detailed error message
    attr_desc = ", ".join(f"{k}={v!r}" for k, v in expected_fields.items())
    found_desc = []
    for event in matching_events:
        event_attrs = {k: getattr(event, k, None) for k in expected_fields}
        found_desc.append(f"{type(event).__name__}({event_attrs})")

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

    This is useful for testing scenarios where a command should be
    rejected or have no effect.

    Args:
        harness: The InMemoryTestHarness instance
        event_type: If provided, only check for this event type.
                   If None, assert no events at all were published.

    Example:
        >>> # Assert no events published at all
        >>> then_no_events_published(harness)

        >>> # Assert no OrderCancelled events published
        >>> then_no_events_published(harness, OrderCancelled)

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
    Assert that events were published in the given sequence.

    This verifies both the types and order of events published
    through the event bus.

    Args:
        harness: The InMemoryTestHarness instance
        event_types: The expected sequence of event types

    Returns:
        The list of published events (for further assertions)

    Example:
        >>> events = then_event_sequence(harness, [
        ...     PaymentProcessed,
        ...     OrderShipped,
        ...     ShipmentNotificationSent,
        ... ])
        >>> # Events are returned for further assertions
        >>> assert events[1].tracking_number is not None

    Raises:
        AssertionError: If the sequence doesn't match (wrong count,
                       wrong types, or wrong order)
    """
    published = harness.published_events
    actual_types = [type(e) for e in published]

    # Check count first for clearer error message
    if len(actual_types) != len(event_types):
        raise AssertionError(
            f"Event count mismatch: expected {len(event_types)} events, "
            f"got {len(actual_types)}.\n"
            f"Expected: {[t.__name__ for t in event_types]}\n"
            f"Actual:   {[t.__name__ for t in actual_types]}"
        )

    # Check each event type in sequence
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

    Example:
        >>> then_event_count(harness, 3)

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
]
