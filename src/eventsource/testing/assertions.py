"""
Event-specific test assertions for event sourcing tests.

This module provides domain-specific assertion helpers that make test
assertions more readable and provide better error messages.

Example:
    >>> from eventsource.testing import EventAssertions
    >>> from my_app.events import OrderCreated, OrderShipped
    >>>
    >>> assertions = EventAssertions(published_events)
    >>> assertions.assert_event_published(OrderCreated)
    >>> assertions.assert_event_count(2)
    >>> assertions.assert_event_sequence([OrderCreated, OrderShipped])

Note:
    These assertions are designed to provide clear, actionable error messages
    that help quickly identify what went wrong in a failing test.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar
from uuid import UUID

from eventsource.events.base import DomainEvent

# Type variable for generic event assertions
TEvent = TypeVar("TEvent", bound=DomainEvent)


class _Missing:
    """Sentinel class for detecting missing attributes."""

    __slots__ = ()

    def __repr__(self) -> str:
        """Return a string representation."""
        return "<MISSING>"


_MISSING = _Missing()


class EventAssertions:
    """
    Domain-specific assertions for event sourcing tests.

    EventAssertions provides readable, intention-revealing assertion methods
    with clear error messages for common event sourcing test scenarios.

    Example:
        >>> assertions = EventAssertions(harness.published_events)
        >>>
        >>> # Assert a specific event type was published
        >>> assertions.assert_event_published(OrderCreated)
        >>>
        >>> # Assert exact event count
        >>> assertions.assert_event_count(2)
        >>>
        >>> # Assert event sequence
        >>> assertions.assert_event_sequence([OrderCreated, OrderShipped])
        >>>
        >>> # Assert event field values
        >>> assertions.assert_event_with_fields(
        ...     OrderCreated,
        ...     customer_id=expected_customer_id,
        ...     order_total=Decimal("99.99")
        ... )

    Attributes:
        events: The list of events to assert against (read-only)
    """

    def __init__(self, events: Sequence[DomainEvent]) -> None:
        """
        Initialize assertions with a sequence of events.

        Args:
            events: The events to make assertions about. This is typically
                   harness.published_events or aggregate.uncommitted_events.
        """
        self._events = list(events)

    @property
    def events(self) -> list[DomainEvent]:
        """
        Get the events being asserted against.

        Returns:
            Copy of the events list to prevent external modification.
        """
        return self._events.copy()

    def assert_event_published(self, event_type: type[TEvent]) -> TEvent:
        """
        Assert that an event of the given type was published.

        Args:
            event_type: The expected event class

        Returns:
            The first event of the given type (for further assertions)

        Raises:
            AssertionError: If no event of the type was published

        Example:
            >>> event = assertions.assert_event_published(OrderCreated)
            >>> assert event.customer_id == expected_customer_id
        """
        matching = [e for e in self._events if isinstance(e, event_type)]

        if not matching:
            found_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Expected {event_type.__name__} to be published, but no events "
                f"of that type were found. Published event types: {found_types or 'none'}"
            )

        return matching[0]

    def assert_no_event_published(self, event_type: type[DomainEvent]) -> None:
        """
        Assert that no event of the given type was published.

        Args:
            event_type: The event class that should not be present

        Raises:
            AssertionError: If an event of the type was published

        Example:
            >>> # Ensure no cancellation event was published
            >>> assertions.assert_no_event_published(OrderCancelled)
        """
        matching = [e for e in self._events if isinstance(e, event_type)]

        if matching:
            raise AssertionError(
                f"Expected no {event_type.__name__} events to be published, "
                f"but found {len(matching)}"
            )

    def assert_event_count(self, count: int) -> None:
        """
        Assert the exact number of events published.

        Args:
            count: The expected number of events

        Raises:
            AssertionError: If the count doesn't match

        Example:
            >>> assertions.assert_event_count(3)
        """
        actual_count = len(self._events)
        if actual_count != count:
            event_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Event count mismatch: expected {count} events, "
                f"got {actual_count}.\n"
                f"Published: {event_types}"
            )

    def assert_event_sequence(self, event_types: Sequence[type[DomainEvent]]) -> None:
        """
        Assert that events were published in the given order.

        Args:
            event_types: The expected sequence of event types

        Raises:
            AssertionError: If the sequence doesn't match (wrong count,
                           wrong types, or wrong order)

        Example:
            >>> assertions.assert_event_sequence([
            ...     OrderCreated,
            ...     PaymentReceived,
            ...     OrderShipped,
            ... ])
        """
        actual_types = [type(e) for e in self._events]

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

    def assert_event_with_fields(self, event_type: type[TEvent], **expected_fields: Any) -> TEvent:
        """
        Assert an event was published with specific field values.

        Args:
            event_type: The expected event class
            **expected_fields: Field names and expected values

        Returns:
            The matching event

        Raises:
            AssertionError: If no event matches the criteria

        Example:
            >>> event = assertions.assert_event_with_fields(
            ...     OrderCreated,
            ...     customer_id=expected_customer_id,
            ...     order_total=Decimal("99.99"),
            ... )
        """
        matching_events = [e for e in self._events if isinstance(e, event_type)]

        if not matching_events:
            found_types = [type(e).__name__ for e in self._events]
            raise AssertionError(
                f"Expected {event_type.__name__} to be published, but no events "
                f"of that type were found. Published event types: {found_types or 'none'}"
            )

        # If no field constraints, return first match
        if not expected_fields:
            return matching_events[0]

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
                return event

        # Build detailed error message
        attr_desc = ", ".join(f"{k}={v!r}" for k, v in expected_fields.items())
        found_desc = []
        for event in matching_events:
            event_attrs = {k: getattr(event, k, None) for k in expected_fields}
            found_desc.append(f"{type(event).__name__}({event_attrs})")

        raise AssertionError(
            f"Expected {event_type.__name__}({attr_desc}) to be published.\n"
            f"Found {len(matching_events)} {event_type.__name__} event(s) but "
            f"none matched the expected attributes:\n"
            + "\n".join(f"  - {desc}" for desc in found_desc)
        )

    def assert_no_events_published(self) -> None:
        """
        Assert that no events were published.

        Raises:
            AssertionError: If any events were published

        Example:
            >>> # After a no-op command
            >>> assertions.assert_no_events_published()
        """
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
        """
        Assert an event was published for a specific aggregate.

        Args:
            event_type: The expected event class
            aggregate_id: The expected aggregate ID

        Returns:
            The matching event

        Raises:
            AssertionError: If no matching event found

        Example:
            >>> event = assertions.assert_event_for_aggregate(
            ...     OrderShipped, order_id
            ... )
        """
        matching = [
            e for e in self._events if isinstance(e, event_type) and e.aggregate_id == aggregate_id
        ]

        if not matching:
            # Check if any events of the type exist
            type_matching = [e for e in self._events if isinstance(e, event_type)]
            if type_matching:
                found_agg_ids = [str(e.aggregate_id) for e in type_matching]
                raise AssertionError(
                    f"Expected {event_type.__name__} for aggregate {aggregate_id}, "
                    f"but found {event_type.__name__} only for aggregates: {found_agg_ids}"
                )
            else:
                found_types = [type(e).__name__ for e in self._events]
                raise AssertionError(
                    f"Expected {event_type.__name__} for aggregate {aggregate_id}, "
                    f"but no events of that type were found. "
                    f"Published event types: {found_types or 'none'}"
                )

        return matching[0]

    def get_events_of_type(self, event_type: type[TEvent]) -> list[TEvent]:
        """
        Get all events of a specific type.

        Args:
            event_type: The event class to filter by

        Returns:
            List of events matching the type

        Example:
            >>> item_events = assertions.get_events_of_type(ItemAdded)
            >>> assert len(item_events) == 3
        """
        return [e for e in self._events if isinstance(e, event_type)]

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        event_types = [type(e).__name__ for e in self._events]
        return f"EventAssertions(events={event_types})"


__all__ = ["EventAssertions"]
