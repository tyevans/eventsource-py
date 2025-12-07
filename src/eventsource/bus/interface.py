"""Event bus interface definitions.

This module contains the EventBus abstract base class and related protocols
for publishing and subscribing to domain events.

The event bus decouples event producers from consumers, allowing projections
and other handlers to react to events independently.
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from eventsource.events.base import DomainEvent


@runtime_checkable
class EventHandler(Protocol):
    """
    Protocol for handling domain events.

    Event handlers are registered with the event bus and called
    when matching events are published. Handlers can be sync or async.

    Example:
        >>> class MyHandler:
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         print(f"Handling {event.event_type}")
        ...
        >>> handler = MyHandler()
        >>> event_bus.subscribe(OrderCreated, handler)
    """

    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """
        Handle a domain event.

        Can be synchronous (return None) or asynchronous (return Awaitable).
        Implementations should be idempotent when possible, as events may
        be delivered more than once in some scenarios.

        Args:
            event: The domain event to handle

        Raises:
            Exception: If handling fails (will be logged but not re-raised
                      to allow other handlers to continue)
        """
        ...


@runtime_checkable
class EventSubscriber(Protocol):
    """
    Protocol for event subscribers (e.g., projections).

    Subscribers declare which event types they're interested in and
    provide a handler for processing them. This is useful for projections
    that need to handle multiple event types.

    Example:
        >>> class OrderProjection:
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated, OrderShipped, OrderCancelled]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             self.create_order(event)
        ...         # ...
        ...
        >>> projection = OrderProjection()
        >>> event_bus.subscribe_all(projection)
    """

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """
        Return list of event types this subscriber handles.

        Returns:
            List of event classes this subscriber is interested in
        """
        ...

    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """
        Handle a domain event.

        Can be synchronous (return None) or asynchronous (return Awaitable).

        Args:
            event: The domain event to handle

        Raises:
            Exception: If handling fails
        """
        ...


# Type alias for simple function-based handlers
EventHandlerFunc = Callable[[DomainEvent], Awaitable[None] | None]


class EventBus(ABC):
    """
    Abstract event bus for publishing and subscribing to domain events.

    The event bus decouples event producers from consumers, allowing
    projections and other handlers to react to events independently.

    Implementations must be thread-safe and support both synchronous
    and asynchronous handlers.

    Example:
        >>> event_bus = InMemoryEventBus()
        >>> event_bus.subscribe(OrderCreated, order_handler)
        >>> event_bus.subscribe_all(order_projection)
        >>> await event_bus.publish([OrderCreated(...)])
    """

    @abstractmethod
    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """
        Publish events to all registered subscribers.

        Events are processed in order, and all handlers for each event
        are invoked before moving to the next event.

        Args:
            events: List of events to publish
            background: If True, publish asynchronously without blocking
                       (fire-and-forget). This improves response times but
                       introduces eventual consistency.

        Raises:
            Exception: If publishing fails critically (only in synchronous mode)

        Note:
            Handler errors are caught and logged but don't prevent other
            handlers from executing.
        """
        pass

    @abstractmethod
    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: EventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to a specific event type.

        The handler will be invoked whenever an event of the specified type
        is published. Multiple handlers can be subscribed to the same event type.

        Args:
            event_type: The event class to subscribe to
            handler: Object with handle() method or callable

        Example:
            >>> event_bus.subscribe(OrderCreated, order_handler)
            >>> event_bus.subscribe(OrderCreated, lambda e: print(e))
        """
        pass

    @abstractmethod
    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: EventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from a specific event type.

        Args:
            event_type: The event class to unsubscribe from
            handler: The handler to remove

        Returns:
            True if the handler was found and removed, False otherwise
        """
        pass

    @abstractmethod
    def subscribe_all(self, subscriber: EventSubscriber) -> None:
        """
        Subscribe an EventSubscriber to all its declared event types.

        This is a convenience method that calls subscribe() for each
        event type returned by subscriber.subscribed_to().

        Args:
            subscriber: The subscriber to register

        Example:
            >>> event_bus.subscribe_all(order_projection)
        """
        pass

    @abstractmethod
    def subscribe_to_all_events(
        self,
        handler: EventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to all event types (wildcard subscription).

        The handler will receive every event published to the bus,
        regardless of event type. Useful for audit logging, metrics,
        debugging, or other cross-cutting concerns.

        Args:
            handler: Handler that will receive all events

        Example:
            >>> event_bus.subscribe_to_all_events(audit_logger)
        """
        pass

    @abstractmethod
    def unsubscribe_from_all_events(
        self,
        handler: EventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from the wildcard subscription.

        Args:
            handler: The handler to remove from wildcard subscriptions

        Returns:
            True if the handler was found and removed, False otherwise
        """
        pass


class AsyncEventHandler(ABC):
    """
    Base class for async event handlers.

    Provides a more structured approach than the EventHandler protocol
    for handlers that need additional methods like event filtering.

    Example:
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
    "EventBus",
    "EventHandler",
    "EventSubscriber",
    "EventHandlerFunc",
    "AsyncEventHandler",
]
