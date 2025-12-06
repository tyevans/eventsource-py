"""Event primitives for the eventsource library."""

from eventsource.events.base import DomainEvent
from eventsource.events.registry import (
    DuplicateEventTypeError,
    EventRegistry,
    EventTypeNotFoundError,
    default_registry,
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
    register_event,
)

__all__ = [
    # Base event class
    "DomainEvent",
    # Registry class
    "EventRegistry",
    # Default registry instance
    "default_registry",
    # Decorator
    "register_event",
    # Convenience functions
    "get_event_class",
    "get_event_class_or_none",
    "is_event_registered",
    "list_registered_events",
    # Exceptions
    "EventTypeNotFoundError",
    "DuplicateEventTypeError",
]
