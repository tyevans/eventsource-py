"""
Canonical protocol definitions for the eventsource library.

This module contains the authoritative definitions for all protocols
used throughout the library. Import protocols from here for type hints
and protocol implementations.

Protocols:
- EventHandler: Async handler for domain events (primary, async-only)
- SyncEventHandler: Sync handler for domain events
- FlexibleEventHandler: Handler that may be sync or async
- EventSubscriber: Handler that declares event subscriptions (ABC-based)
- FlexibleEventSubscriber: Protocol version of EventSubscriber for flexible typing

Example:
    >>> from eventsource.protocols import EventHandler, EventSubscriber
    >>>
    >>> class MyHandler:
    ...     async def handle(self, event: DomainEvent) -> None:
    ...         await self.process(event)
    >>>
    >>> class MyProjection(EventSubscriber):
    ...     def subscribed_to(self) -> list[type[DomainEvent]]:
    ...         return [OrderCreated, OrderShipped]
    ...
    ...     async def handle(self, event: DomainEvent) -> None:
    ...         if isinstance(event, OrderCreated):
    ...             await self._handle_created(event)
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable
from typing import Protocol, runtime_checkable

from eventsource.events.base import DomainEvent


@runtime_checkable
class EventHandler(Protocol):
    """
    Protocol for async event handlers.

    Event handlers process domain events, typically for:
    - Updating read models (projections)
    - Sending notifications
    - Triggering workflows

    All handlers should be async for consistency. Use SyncEventHandler
    if you need synchronous handling.

    Example:
        >>> class MyHandler:
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         await self.process(event)
    """

    async def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event asynchronously.

        Args:
            event: The domain event to process

        Raises:
            Exception: If handling fails
        """
        ...


@runtime_checkable
class SyncEventHandler(Protocol):
    """
    Protocol for synchronous event handlers.

    Use for handlers that don't require async I/O, such as:
    - In-memory state updates
    - Simple logging
    - Test mocks

    Example:
        >>> class SimpleLogger:
        ...     def handle(self, event: DomainEvent) -> None:
        ...         print(f"Event: {event.event_type}")
    """

    def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event synchronously.

        Args:
            event: The domain event to process
        """
        ...


@runtime_checkable
class FlexibleEventHandler(Protocol):
    """
    Protocol for handlers that may be sync or async.

    Used primarily by EventBus implementations that need to support
    both sync and async handlers. Prefer EventHandler for new code.

    Note: This protocol exists for backward compatibility with
    existing EventBus subscribers.
    """

    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """Handle event, returning Awaitable if async."""
        ...


class EventSubscriber(ABC):
    """
    Abstract base class for event subscribers.

    Subscribers declare which event types they handle and provide
    a handler method. This is the primary interface for projections.

    Unlike EventHandler (Protocol), this is an ABC to allow
    additional methods in subclasses (e.g., reset, get_checkpoint).

    Example:
        >>> class OrderProjection(EventSubscriber):
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated, OrderShipped]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             await self._handle_created(event)
    """

    @abstractmethod
    def subscribed_to(self) -> list[type[DomainEvent]]:
        """
        Return list of event types this subscriber handles.

        Returns:
            List of event type classes
        """
        pass

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event.

        Args:
            event: The event to handle
        """
        pass


@runtime_checkable
class FlexibleEventSubscriber(Protocol):
    """
    Protocol version of EventSubscriber for flexible typing.

    Used when you need to accept both sync and async subscribers
    in type hints. For most cases, use EventSubscriber (ABC) instead.
    """

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return list of event types this subscriber handles."""
        ...

    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """Handle event, returning Awaitable if async."""
        ...


class AsyncEventHandler(ABC):
    """
    Base class for async event handlers.

    Provides a more structured approach than the EventHandler protocol
    for handlers that need additional methods like event filtering.

    This is the canonical location for AsyncEventHandler.

    Example:
        >>> from eventsource.protocols import AsyncEventHandler
        >>>
        >>> class OrderEmailHandler(AsyncEventHandler):
        ...     def event_types(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated, OrderShipped]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             await self.send_confirmation_email(event)
    """

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """
        Handle an event asynchronously.

        Args:
            event: The event to handle

        Raises:
            Exception: If handling fails
        """
        pass

    def can_handle(self, event: DomainEvent) -> bool:
        """
        Check if this handler can handle the given event.

        By default, checks if event type is in the handler's event types.
        Override for custom filtering logic.

        Args:
            event: The event to check

        Returns:
            True if this handler can handle the event
        """
        return type(event) in self.event_types()

    @abstractmethod
    def event_types(self) -> list[type[DomainEvent]]:
        """
        Return list of event types this handler supports.

        Returns:
            List of event classes this handler can process
        """
        pass


__all__ = [
    "EventHandler",
    "SyncEventHandler",
    "FlexibleEventHandler",
    "EventSubscriber",
    "FlexibleEventSubscriber",
    "AsyncEventHandler",
]
