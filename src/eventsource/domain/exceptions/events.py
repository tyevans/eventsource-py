"""Event-related exceptions for the eventsource package."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.domain.exceptions.base import EventSourceError

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent


class EventNotFoundError(EventSourceError):
    """Raised when an event cannot be found."""

    def __init__(self, event_id: UUID) -> None:
        self.event_id = event_id
        super().__init__(f"Event not found: {event_id}")


class EventStoreError(EventSourceError):
    """Raised when there's an error in the event store."""


class EventBusError(EventSourceError):
    """Raised when there's an error in the event bus."""


class EventVersionError(EventSourceError):
    """Raised when event version validation fails during aggregate event application."""

    def __init__(
        self,
        expected_version: int,
        actual_version: int,
        event_id: UUID,
        aggregate_id: UUID,
    ) -> None:
        self.expected_version = expected_version
        self.actual_version = actual_version
        self.event_id = event_id
        self.aggregate_id = aggregate_id
        super().__init__(
            f"Event version mismatch for aggregate {aggregate_id}: "
            f"expected version {expected_version}, got {actual_version} "
            f"(event_id: {event_id})"
        )


class UnhandledEventError(EventSourceError):
    """Raised when an event has no registered handler and strict mode is enabled."""

    def __init__(
        self,
        event_type: str,
        event_id: UUID,
        handler_class: str,
        available_handlers: list[str],
    ) -> None:
        self.event_type = event_type
        self.event_id = event_id
        self.handler_class = handler_class
        self.available_handlers = available_handlers
        handlers_str = ", ".join(available_handlers) if available_handlers else "none"
        super().__init__(
            f"No handler registered for event type '{event_type}' "
            f"in {handler_class}. "
            f"Available handlers: {handlers_str}. "
            f"Add @handles({event_type}) decorator or set "
            f"unregistered_event_handling='ignore' or 'warn'."
        )


class DuplicateEventError(EventSourceError):
    """An event with this event_id already exists in the store."""


class EventTypeNotFoundError(EventSourceError):
    """Raised when an event type is not found in the registry."""

    def __init__(self, event_type: str, available_types: list[str]) -> None:
        self.event_type = event_type
        self.available_types = available_types
        available = ", ".join(sorted(available_types)) if available_types else "none"
        super().__init__(
            f"Unknown event type: '{event_type}'. "
            f"Available types: {available}. "
            f"Did you forget to register this event type?"
        )


class DuplicateEventTypeError(EventSourceError):
    """Raised when attempting to register a different class with an existing event type name."""

    def __init__(
        self,
        event_type: str,
        existing_class: type[DomainEvent],
        new_class: type[DomainEvent],
    ) -> None:
        self.event_type = event_type
        self.existing_class = existing_class
        self.new_class = new_class
        super().__init__(
            f"Event type '{event_type}' is already registered to {existing_class.__name__}. "
            f"Cannot register {new_class.__name__} with the same type name."
        )


__all__ = [
    "DuplicateEventError",
    "DuplicateEventTypeError",
    "EventBusError",
    "EventNotFoundError",
    "EventStoreError",
    "EventTypeNotFoundError",
    "EventVersionError",
    "UnhandledEventError",
]
