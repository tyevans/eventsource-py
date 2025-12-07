"""
Event type registry for serialization and deserialization.

This module provides mechanisms to register and look up event types by name,
enabling proper deserialization of events from storage or message queues.

The registry is thread-safe and supports multiple registration patterns:
- Decorator-based registration
- Explicit programmatic registration
- Multiple independent registries for testing isolation

Usage:
    # Option 1: Decorator-based registration
    @register_event
    class OrderCreated(DomainEvent):
        event_type: str = "OrderCreated"
        ...

    # Option 2: Decorator with explicit type name
    @register_event(event_type="order.created")
    class OrderCreated(DomainEvent):
        ...

    # Option 3: Explicit registration
    registry = EventRegistry()
    registry.register(OrderCreated)

    # Option 4: Registration with custom type name
    registry.register(OrderCreated, "order.created")

    # Lookup
    event_class = registry.get("OrderCreated")
    event_class = get_event_class("OrderCreated")  # Using default registry
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, TypeVar, overload

if TYPE_CHECKING:
    from eventsource.events.base import DomainEvent

logger = logging.getLogger(__name__)

# Type variable for event classes
TEvent = TypeVar("TEvent", bound="DomainEvent")


class EventTypeNotFoundError(KeyError):
    """
    Raised when an event type is not found in the registry.

    Provides helpful error messages including a list of available event types.
    """

    def __init__(self, event_type: str, available_types: list[str]) -> None:
        self.event_type = event_type
        self.available_types = available_types
        available = ", ".join(sorted(available_types)) if available_types else "none"
        super().__init__(
            f"Unknown event type: '{event_type}'. "
            f"Available types: {available}. "
            f"Did you forget to register this event type?"
        )


class DuplicateEventTypeError(ValueError):
    """
    Raised when attempting to register a different class with an existing event type name.
    """

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


class EventRegistry:
    """
    Registry for mapping event type names to event classes.

    Enables deserialization of events from storage by looking up
    the appropriate class based on the event_type string.

    This registry can be used as a singleton (via the module-level
    `default_registry`) or instantiated for isolated testing.

    Thread-Safety:
        All operations are thread-safe and use internal locking.

    Attributes:
        _registry: Internal mapping of event type names to classes
        _lock: Threading lock for thread-safe operations

    Example:
        >>> registry = EventRegistry()
        >>> registry.register(OrderCreated)
        >>> event_class = registry.get("OrderCreated")
        >>> event = event_class.model_validate(event_data)
    """

    def __init__(self) -> None:
        """Initialize an empty event registry."""
        self._registry: dict[str, type[DomainEvent]] = {}
        self._lock = threading.RLock()

    def register(
        self,
        event_class: type[TEvent],
        event_type: str | None = None,
    ) -> type[TEvent]:
        """
        Register an event class in the registry.

        Args:
            event_class: The event class to register
            event_type: Optional type name override. If not provided,
                       uses the class's event_type field default or class name.

        Returns:
            The registered event class (enables use as decorator)

        Raises:
            DuplicateEventTypeError: If event_type is already registered to a different class

        Example:
            >>> registry.register(OrderCreated)
            >>> registry.register(OrderShipped, "order.shipped")
        """
        # Determine event type name
        resolved_type = self._resolve_event_type(event_class, event_type)

        with self._lock:
            # Check for duplicates
            if resolved_type in self._registry:
                existing = self._registry[resolved_type]
                if existing is not event_class:
                    raise DuplicateEventTypeError(resolved_type, existing, event_class)
                # Same class, already registered - no-op
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

    def _resolve_event_type(
        self,
        event_class: type[TEvent],
        event_type: str | None,
    ) -> str:
        """
        Resolve the event type name for a class.

        Resolution order:
        1. Explicit parameter
        2. Pydantic field default value (if it's a string)
        3. Class name fallback
        """
        if event_type is not None:
            return event_type

        # Try to get from class field default
        if hasattr(event_class, "model_fields"):
            field_info = event_class.model_fields.get("event_type")
            # Check if there's a default and it's a string (not PydanticUndefined)
            if field_info and isinstance(field_info.default, str):
                return field_info.default

        # Fallback to class name
        return event_class.__name__

    def get(self, event_type: str) -> type[DomainEvent]:
        """
        Get event class by type name.

        Args:
            event_type: The event type name to look up

        Returns:
            The registered event class

        Raises:
            EventTypeNotFoundError: If event type is not registered

        Example:
            >>> event_class = registry.get("OrderCreated")
            >>> event = event_class.model_validate(data)
        """
        with self._lock:
            if event_type not in self._registry:
                raise EventTypeNotFoundError(event_type, list(self._registry.keys()))
            return self._registry[event_type]

    def get_or_none(self, event_type: str) -> type[DomainEvent] | None:
        """
        Get event class by type name, returning None if not found.

        Args:
            event_type: The event type name to look up

        Returns:
            The registered event class, or None if not found
        """
        with self._lock:
            return self._registry.get(event_type)

    def contains(self, event_type: str) -> bool:
        """
        Check if an event type is registered.

        Args:
            event_type: The event type name to check

        Returns:
            True if registered, False otherwise
        """
        with self._lock:
            return event_type in self._registry

    def list_types(self) -> list[str]:
        """
        List all registered event type names.

        Returns:
            Sorted list of registered event type names
        """
        with self._lock:
            return sorted(self._registry.keys())

    def list_classes(self) -> list[type[DomainEvent]]:
        """
        List all registered event classes.

        Returns:
            List of registered event classes (in sorted order by type name)
        """
        with self._lock:
            return [self._registry[t] for t in sorted(self._registry.keys())]

    def clear(self) -> None:
        """
        Clear all registered event types.

        Primarily useful for testing to reset state between tests.
        """
        with self._lock:
            self._registry.clear()
            logger.debug("Event registry cleared")

    def unregister(self, event_type: str) -> bool:
        """
        Unregister an event type.

        Args:
            event_type: The event type name to unregister

        Returns:
            True if the type was registered and removed, False if not found
        """
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
        """Iterate over registered event type names."""
        with self._lock:
            # Return a copy to avoid issues with concurrent modification
            return iter(list(self._registry.keys()))


# Module-level default registry instance
default_registry = EventRegistry()


# Overloads for type checking - decorator with and without parentheses
@overload
def register_event(event_class: type[TEvent]) -> type[TEvent]: ...


@overload
def register_event(
    event_class: None = None,
    *,
    event_type: str | None = None,
    registry: EventRegistry | None = None,
) -> Callable[[type[TEvent]], type[TEvent]]: ...


def register_event(
    event_class: type[TEvent] | None = None,
    *,
    event_type: str | None = None,
    registry: EventRegistry | None = None,
) -> type[TEvent] | Callable[[type[TEvent]], type[TEvent]]:
    """
    Decorator to register an event class in the registry.

    Can be used with or without parentheses:

        @register_event
        class OrderCreated(DomainEvent):
            ...

        @register_event(event_type="order.created")
        class OrderCreated(DomainEvent):
            ...

        @register_event(registry=custom_registry)
        class OrderCreated(DomainEvent):
            ...

    Args:
        event_class: The event class (when used without parentheses)
        event_type: Optional type name override
        registry: Optional registry to use (defaults to module-level registry)

    Returns:
        The registered event class, or a decorator function

    Example:
        >>> @register_event
        ... class OrderCreated(DomainEvent):
        ...     event_type: str = "OrderCreated"
        ...     aggregate_type: str = "Order"
        ...     order_id: UUID
        ...
        >>> @register_event(event_type="order.shipped")
        ... class OrderShipped(DomainEvent):
        ...     event_type: str = "order.shipped"
        ...     aggregate_type: str = "Order"
    """
    target_registry = registry or default_registry

    def decorator(cls: type[TEvent]) -> type[TEvent]:
        return target_registry.register(cls, event_type)

    # Handle both @register_event and @register_event()
    if event_class is not None:
        return decorator(event_class)
    return decorator


def get_event_class(event_type: str) -> type[DomainEvent]:
    """
    Get event class from the default registry.

    Convenience function for the common case of using the module-level registry.

    Args:
        event_type: The event type name to look up

    Returns:
        The registered event class

    Raises:
        EventTypeNotFoundError: If event type is not registered
    """
    return default_registry.get(event_type)


def get_event_class_or_none(event_type: str) -> type[DomainEvent] | None:
    """
    Get event class from the default registry, returning None if not found.

    Convenience function for the common case of using the module-level registry.

    Args:
        event_type: The event type name to look up

    Returns:
        The registered event class, or None if not found
    """
    return default_registry.get_or_none(event_type)


def is_event_registered(event_type: str) -> bool:
    """
    Check if an event type is registered in the default registry.

    Args:
        event_type: The event type name to check

    Returns:
        True if registered, False otherwise
    """
    return default_registry.contains(event_type)


def list_registered_events() -> list[str]:
    """
    List all event types registered in the default registry.

    Returns:
        Sorted list of registered event type names
    """
    return default_registry.list_types()
