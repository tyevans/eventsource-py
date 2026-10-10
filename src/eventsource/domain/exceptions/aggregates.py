"""Aggregate-related exceptions for the eventsource package."""

from __future__ import annotations

from uuid import UUID

from eventsource.domain.exceptions.base import EventSourceError


class AggregateNotFoundError(EventSourceError):
    """Raised when an aggregate cannot be found."""

    def __init__(self, aggregate_id: UUID, aggregate_type: str | None = None) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        type_info = f" of type {aggregate_type}" if aggregate_type else ""
        super().__init__(f"Aggregate{type_info} not found: {aggregate_id}")


class AggregateNotCreatedError(EventSourceError):
    """
    Raised when accessing state of an aggregate before creation event.

    This error occurs when:
    1. Aggregate has `requires_creation_event = True`
    2. No events have been applied yet
    3. Code attempts to access `aggregate.state`

    Use `aggregate.state_or_none` or `aggregate.is_created` to safely
    check if the aggregate has been created.

    Attributes:
        aggregate_class: Name of the aggregate class that wasn't created
        suggestion: Optional hint for how to resolve the error
    """

    def __init__(self, aggregate_class: str, suggestion: str | None = None) -> None:
        self.aggregate_class = aggregate_class
        self.suggestion = suggestion

        message = f"{aggregate_class} has not been created. Apply a creation event first."
        if suggestion:
            message += f" Hint: {suggestion}"

        super().__init__(message)


class AggregateTypeMismatchError(EventSourceError):
    """An event class declares a different aggregate_type than its aggregate.

    Emitting `OrderShipped(aggregate_type="Shipment")` from an aggregate
    whose `aggregate_type` is `"Order"` used to be silently restamped to
    `"Order"` -- the declared value was accepted at import, then discarded
    at emit time with no signal. Since `aggregate_type` becomes the stream
    category, the disagreement is invisible in a save/load round-trip and
    only shows up as events missing from a category read.

    Attributes:
        event_class: Name of the event class with the divergent declaration
        event_aggregate_type: What the event class declares
        aggregate_class: Name of the aggregate emitting it
        aggregate_type: What the aggregate declares
    """

    def __init__(
        self,
        event_class: str,
        event_aggregate_type: str,
        aggregate_class: str,
        aggregate_type: str,
    ) -> None:
        self.event_class = event_class
        self.event_aggregate_type = event_aggregate_type
        self.aggregate_class = aggregate_class
        self.aggregate_type = aggregate_type
        super().__init__(
            f"{event_class} declares aggregate_type={event_aggregate_type!r} but is "
            f"emitted from {aggregate_class}, which declares {aggregate_type!r}. "
            f"An event's aggregate_type is its stream category, so the two must "
            f"agree. Drop the declaration from {event_class} (the aggregate stamps "
            f"it) or emit the event from the matching aggregate."
        )


class AggregateIdMismatchError(EventSourceError):
    """An event names a different aggregate_id than the aggregate emitting it.

    `aggregate_id` is the stream key. An event emitted from one aggregate
    while naming another is appended to a stream that disowns it: the
    aggregate it claims never loads it, and the one that emitted it carries
    an event about someone else. Neither side sees the disagreement on a
    save/load round-trip, so it surfaces only as state that silently went
    missing.

    A command that names a target is the usual source -- the target is
    routing information for choosing *which* aggregate to load, not a value
    to copy onto the event. Load the named aggregate and emit from it.

    Attributes:
        event_class: Name of the event class carrying the foreign id
        event_aggregate_id: The id the event names
        aggregate_class: Name of the aggregate emitting it
        aggregate_id: The id of the aggregate emitting it
        command_class: Name of the command being executed, when known
    """

    def __init__(
        self,
        event_class: str,
        event_aggregate_id: UUID,
        aggregate_class: str,
        aggregate_id: UUID,
        command_class: str | None = None,
    ) -> None:
        self.event_class = event_class
        self.event_aggregate_id = event_aggregate_id
        self.aggregate_class = aggregate_class
        self.aggregate_id = aggregate_id
        self.command_class = command_class

        origin = f" while handling {command_class}" if command_class else ""
        super().__init__(
            f"{event_class} names aggregate_id={event_aggregate_id} but is "
            f"emitted from {aggregate_class}({aggregate_id}){origin}. An "
            f"event's aggregate_id is its stream key, so an event emitted "
            f"here cannot belong to another aggregate. Drop the aggregate_id "
            f"(the aggregate stamps it) or load {event_aggregate_id} and emit "
            f"from that aggregate."
        )


class AggregateTypeNotSetError(EventSourceError):
    """
    Raised when a concrete aggregate class is constructed without declaring
    aggregate_type.

    Aggregate identity is not optional: aggregate_type becomes the stream
    category, so a missing value would silently create wrongly-typed
    streams (the old behavior was a silent "Unknown" default).
    """

    def __init__(self, class_name: str) -> None:
        self.class_name = class_name
        super().__init__(
            f"{class_name} does not declare 'aggregate_type'. Every concrete "
            f"aggregate class must set it to its stream category, e.g. "
            f'aggregate_type = "Order".'
        )


__all__ = [
    "AggregateIdMismatchError",
    "AggregateNotCreatedError",
    "AggregateNotFoundError",
    "AggregateTypeMismatchError",
    "AggregateTypeNotSetError",
]
