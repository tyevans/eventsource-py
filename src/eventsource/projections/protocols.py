"""
Event subscriber and handler protocols.

Protocol definitions are in eventsource.protocols.
Use `from eventsource.protocols import EventHandler` instead.

This module provides the AsyncEventHandler abstract base class.
"""

from abc import ABC, abstractmethod

from eventsource.events.base import DomainEvent


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


__all__ = [
    "AsyncEventHandler",
]
