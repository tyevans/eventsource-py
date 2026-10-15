"""
Event type registry for serialization and deserialization.

Provides thread-safe mechanisms to register and look up event types by name,
supporting decorator-based registration, explicit programmatic registration,
and isolated registries for testing.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, overload

from eventsource.domain.exceptions import DuplicateEventTypeError, EventTypeNotFoundError

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent

logger = logging.getLogger(__name__)


class EventRegistry:
    """
    Thread-safe registry for mapping event type names to event classes.

    Enables deserialization of events from storage by looking up the
    appropriate class based on the event_type string. Can be used via the
    module-level `default_registry` or instantiated for isolated testing.
    """

    def __init__(self) -> None:
        """Initialize an empty event registry."""
        self._registry: dict[str, type[DomainEvent]] = {}
        self._lock = threading.RLock()

    def register[TEvent: "DomainEvent"](
        self,
        event_class: type[TEvent],
        event_type: str | None = None,
    ) -> type[TEvent]:
        """
        Register an event class in the registry.

        Args:
            event_class: The event class to register
            event_type: Optional type name override. Defaults to class event_type_name().

        Returns:
            The registered event class (enables use as decorator)

        Raises:
            DuplicateEventTypeError: If event_type is already registered to a different class
        """
        resolved_type = self._resolve_event_type(event_class, event_type)

        with self._lock:
            if resolved_type in self._registry:
                existing = self._registry[resolved_type]
                if existing is not event_class:
                    raise DuplicateEventTypeError(resolved_type, existing, event_class)
                return event_class

            self._registry[resolved_type] = event_class
            logger.debug(
                "Registered event type '%s' -> %s",
                resolved_type,
                event_class.__name__,
                extra={
                    "event_type": resolved_type,
                    "event_class": event_class.__name__,
                },
            )
            return event_class

    def _resolve_event_type[TEvent: "DomainEvent"](
        self,
        event_class: type[TEvent],
        event_type: str | None,
    ) -> str:
        """Resolve the event type name from explicit parameter or event_type_name()."""
        if event_type is not None:
            return event_type
        return event_class.event_type_name()

    def get(self, event_type: str) -> type[DomainEvent]:
        """
        Get event class by type name.

        Raises:
            EventTypeNotFoundError: If event type is not registered
        """
        with self._lock:
            if event_type not in self._registry:
                raise EventTypeNotFoundError(event_type, list(self._registry.keys()))
            return self._registry[event_type]

    def get_or_none(self, event_type: str) -> type[DomainEvent] | None:
        """Get event class by type name, returning None if not found."""
        with self._lock:
            return self._registry.get(event_type)

    def contains(self, event_type: str) -> bool:
        """Check if an event type is registered."""
        with self._lock:
            return event_type in self._registry

    def list_types(self) -> list[str]:
        """Return sorted list of all registered event type names."""
        with self._lock:
            return sorted(self._registry.keys())

    def list_classes(self) -> list[type[DomainEvent]]:
        """Return registered event classes in sorted order by type name."""
        with self._lock:
            return [self._registry[t] for t in sorted(self._registry.keys())]

    def clear(self) -> None:
        """Clear all registered event types (primarily useful for testing)."""
        with self._lock:
            self._registry.clear()
            logger.debug("Event registry cleared")

    def unregister(self, event_type: str) -> bool:
        """Unregister an event type, returning True if removed or False if not found."""
        with self._lock:
            if event_type in self._registry:
                del self._registry[event_type]
                logger.debug(
                    "Unregistered event type '%s'",
                    event_type,
                    extra={"event_type": event_type},
                )
                return True
            return False

    def __len__(self) -> int:
        """Return the number of registered event types."""
        with self._lock:
            return len(self._registry)

    def __bool__(self) -> bool:
        """Registry is always truthy, even when empty."""
        return True

    def __contains__(self, event_type: str) -> bool:
        """Support 'in' operator for checking registration."""
        return self.contains(event_type)

    def __iter__(self) -> Iterator[str]:
        """Iterate over a snapshot of registered event type names."""
        with self._lock:
            return iter(list(self._registry.keys()))


# Module-level default registry instance
default_registry = EventRegistry()


@overload
def register_event[TEvent: "DomainEvent"](event_class: type[TEvent]) -> type[TEvent]: ...


@overload
def register_event[TEvent: "DomainEvent"](
    event_class: None = None,
    *,
    event_type: str | None = None,
    registry: EventRegistry | None = None,
) -> Callable[[type[TEvent]], type[TEvent]]: ...


def register_event[TEvent: "DomainEvent"](
    event_class: type[TEvent] | None = None,
    *,
    event_type: str | None = None,
    registry: EventRegistry | None = None,
) -> type[TEvent] | Callable[[type[TEvent]], type[TEvent]]:
    """
    Decorator to register an event class in the registry.

    Can be used as `@register_event` or `@register_event(event_type=..., registry=...)`.
    """
    target_registry = registry or default_registry

    def decorator(cls: type[TEvent]) -> type[TEvent]:
        return target_registry.register(cls, event_type)

    if event_class is not None:
        return decorator(event_class)
    return decorator


def get_event_class(event_type: str) -> type[DomainEvent]:
    """Get event class from the default registry, raising EventTypeNotFoundError if missing."""
    return default_registry.get(event_type)


def get_event_class_or_none(event_type: str) -> type[DomainEvent] | None:
    """Get event class from the default registry, returning None if not found."""
    return default_registry.get_or_none(event_type)


def is_event_registered(event_type: str) -> bool:
    """Check if an event type is registered in the default registry."""
    return default_registry.contains(event_type)


def list_registered_events() -> list[str]:
    """Return sorted list of all event types registered in the default registry."""
    return default_registry.list_types()
