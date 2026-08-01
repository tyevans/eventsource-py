"""Event publisher port: contract for pushing persisted events outward."""

from typing import Protocol

from eventsource.events.base import DomainEvent
from eventsource.ports.handlers import EventHandlerFunc, FlexibleEventHandler

__all__ = ["EventPublisher", "SubscribableEventBus"]


class EventPublisher(Protocol):
    """
    Protocol for publishing events to external systems.

    Event publishers enable downstream consumers to react to events
    asynchronously (e.g., sending notifications, updating search indices).

    This protocol defines the contract that event bus implementations
    and other publishing mechanisms should follow.

    Example:
        >>> class NotificationPublisher:
        ...     async def publish(self, events: list[DomainEvent]) -> None:
        ...         for event in events:
        ...             await send_notification(event)
    """

    async def publish(self, events: list[DomainEvent]) -> None:
        """
        Publish events to external systems.

        Args:
            events: Events to publish

        Raises:
            Exception: If publishing fails (implementation-specific)
        """
        ...


class SubscribableEventBus(Protocol):
    """
    Protocol for the narrow slice of ``EventBus`` that live subscription
    runners depend on: registering and removing a handler for a single
    event type.

    Use cases that only need to (un)subscribe handlers should type-hint
    this port instead of the concrete ``eventsource.bus.interface.EventBus``
    ABC -- every ``EventBus`` implementation satisfies it structurally, with
    no explicit subclassing required.

    Example:
        >>> def wire_runner(bus: SubscribableEventBus) -> None:
        ...     bus.subscribe(OrderCreated, handler)
    """

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """
        Subscribe a handler to a specific event type.

        Args:
            event_type: The event class to subscribe to
            handler: Object with handle() method or callable
        """
        ...

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """
        Unsubscribe a handler from a specific event type.

        Args:
            event_type: The event class to unsubscribe from
            handler: The handler to remove

        Returns:
            True if the handler was found and removed, False otherwise
        """
        ...
