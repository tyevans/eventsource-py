"""Event type registry for serialization and deserialization."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Self, overload

from eventsource.domain.exceptions import DuplicateEventTypeError, EventTypeNotFoundError

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent

logger = logging.getLogger(__name__)


class EventRegistry:
    """Thread-safe registry for mapping event type names to event classes."""

    def __init__(self, events: Iterable[type[DomainEvent]] | None = None) -> None:
        """Initialize an event registry, optionally pre-registering event classes."""
        self._registry: dict[str, type[DomainEvent]] = {}
        self._lock = threading.RLock()
        if events is not None:
            self.register_all(*events)

    def register[TEvent: "DomainEvent"](self, event_class: type[TEvent], event_type: str | None = None) -> type[TEvent]:  # fmt: skip
        """Register an event class in the registry (raises DuplicateEventTypeError on conflict)."""
        resolved_type = self._resolve_event_type(event_class, event_type)
        with self._lock:
            if resolved_type in self._registry:
                if (existing := self._registry[resolved_type]) is not event_class:
                    raise DuplicateEventTypeError(resolved_type, existing, event_class)
                return event_class
            self._registry[resolved_type] = event_class
            extra = {"event_type": resolved_type, "event_class": event_class.__name__}
            logger.debug("Registered event type '%s' -> %s", resolved_type, event_class.__name__, extra=extra)  # fmt: skip
            return event_class

    def register_all(self, *event_classes: type[DomainEvent]) -> Self:
        """Register multiple event classes and return self for fluent chaining."""
        for cls in event_classes:
            self.register(cls)
        return self

    def _resolve_event_type[TEvent: "DomainEvent"](self, event_class: type[TEvent], event_type: str | None) -> str:  # fmt: skip
        """Resolve the event type name from explicit parameter or event_type_name()."""
        return event_type if event_type is not None else event_class.event_type_name()

    def get(self, event_type: str) -> type[DomainEvent]:
        """Get event class by type name, raising EventTypeNotFoundError if not registered."""
        with self._lock:
            if event_type not in self._registry:
                raise EventTypeNotFoundError(event_type, list(self._registry.keys()))
            return self._registry[event_type]

    def get_or_none(self, event_type: str) -> type[DomainEvent] | None:
        """Get event class by type name, returning None if not found."""
        with self._lock:
            return self._registry.get(event_type)

    def contains(self, event_type: str | type[DomainEvent]) -> bool:
        """Check if an event type name or event class is registered."""
        event_type = event_type if isinstance(event_type, str) else event_type.event_type_name()
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

    def unregister(self, event_type: str | type[DomainEvent]) -> bool:
        """Unregister an event type name or class, returning True if removed or False if missing."""
        event_type = event_type if isinstance(event_type, str) else event_type.event_type_name()
        with self._lock:
            if event_type in self._registry:
                del self._registry[event_type]
                logger.debug("Unregistered event type '%s'", event_type, extra={"event_type": event_type})  # fmt: skip
                return True
            return False

    def __len__(self) -> int:
        with self._lock:
            return len(self._registry)

    def __bool__(self) -> bool:
        return True

    def __contains__(self, event_type: str | type[DomainEvent]) -> bool:
        return self.contains(event_type)

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(list(self._registry.keys()))


default_registry = EventRegistry()


@overload
def register_event[TEvent: "DomainEvent"](event_class: type[TEvent]) -> type[TEvent]: ...
@overload
def register_event[TEvent: "DomainEvent"](
    event_class: None = None, *, event_type: str | None = None, registry: EventRegistry | None = None
) -> Callable[[type[TEvent]], type[TEvent]]: ...  # fmt: skip


def register_event[TEvent: "DomainEvent"](
    event_class: type[TEvent] | None = None,
    *,
    event_type: str | None = None,
    registry: EventRegistry | None = None,
) -> type[TEvent] | Callable[[type[TEvent]], type[TEvent]]:
    """Decorator to register an event class (`@register_event` or `@register_event(...)`)."""
    target_registry = registry or default_registry

    def decorator(cls: type[TEvent]) -> type[TEvent]:
        return target_registry.register(cls, event_type)

    return decorator(event_class) if event_class is not None else decorator


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
