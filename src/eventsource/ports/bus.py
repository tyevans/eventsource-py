"""Event publisher port: contract for pushing persisted events outward."""

from typing import Protocol

from eventsource.events.base import DomainEvent

__all__ = ["EventPublisher"]


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
