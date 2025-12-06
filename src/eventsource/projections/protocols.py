"""
Event subscriber and handler protocols.

Defines protocols for subscribing to and handling domain events.
These protocols enable a clean separation between the event bus
and the projection system.
"""

from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable

from eventsource.events.base import DomainEvent


@runtime_checkable
class EventHandler(Protocol):
    """
    Protocol for handling a specific event type.

    Event handlers are registered with the event bus and called
    when matching events are published.

    Example:
        >>> class MyHandler:
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         print(f"Handling {event.event_type}")
    """

    async def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event.

        Args:
            event: The event to handle

        Raises:
            Exception: If handling fails (will be logged but not re-raised)
        """
        ...


@runtime_checkable
class SyncEventHandler(Protocol):
    """
    Protocol for synchronous event handlers.

    Useful for projections that don't need async I/O or for
    testing scenarios.
    """

    def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event synchronously.

        Args:
            event: The event to handle

        Raises:
            Exception: If handling fails
        """
        ...


class EventSubscriber(ABC):
    """
    Base class for event subscribers.

    Subscribers register interest in specific event types and
    provide handlers for processing them. This is the main interface
    that projections implement to receive events from the event bus.

    Example:
        >>> class MyProjection(EventSubscriber):
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated, OrderShipped]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             await self._handle_order_created(event)
    """

    @abstractmethod
    def subscribed_to(self) -> list[type[DomainEvent]]:
        """
        Return list of event types this subscriber handles.

        Returns:
            List of event classes this subscriber is interested in
        """
        pass

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event.

        Args:
            event: The event to handle

        Raises:
            Exception: If handling fails
        """
        pass


class AsyncEventHandler(ABC):
    """
    Base class for async event handlers.

    Handlers process events asynchronously and can be used
    for projections, notifications, and other side effects.

    Unlike EventSubscriber, this provides a can_handle() method
    for more flexible event routing.

    Example:
        >>> class NotificationHandler(AsyncEventHandler):
        ...     def event_types(self) -> list[type[DomainEvent]]:
        ...         return [OrderShipped]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         await send_notification(event)
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
        Override for custom logic.

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
