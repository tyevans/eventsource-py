"""
Subscriber registry for EventSubscriber instances.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import EventSubscriber

logger = logging.getLogger(__name__)


class SubscriberRegistry:
    """
    Registry for EventSubscriber instances.

    Provides a more specific registry for subscribers that implement
    the EventSubscriber protocol, with filtering by event type.

    Example:
        >>> registry = SubscriberRegistry()
        >>> registry.register(order_projection)
        >>> subscribers = registry.get_subscribers_for(OrderCreated)
        >>>
        >>> # Cap fan-out concurrency for a large registry
        >>> registry = SubscriberRegistry(max_concurrency=16)
    """

    def __init__(self, max_concurrency: int | None = None) -> None:
        """
        Initialize the subscriber registry.

        Args:
            max_concurrency: Maximum number of subscribers dispatched
                          concurrently for a single event. `None` (the
                          default) leaves fan-out uncapped. Enforced with one
                          semaphore owned by this instance -- see
                          `ProjectionRegistry.__init__` for why it must not
                          be constructed per call.
        """
        self._subscribers: list[EventSubscriber] = []
        self._semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None

    def register(self, subscriber: EventSubscriber) -> None:
        """
        Register an event subscriber.

        Args:
            subscriber: The subscriber to register
        """
        self._subscribers.append(subscriber)
        logger.debug(
            "Registered subscriber %s for events: %s",
            subscriber.__class__.__name__,
            [et.__name__ for et in subscriber.subscribed_to()],
            extra={
                "subscriber": subscriber.__class__.__name__,
                "event_types": [et.__name__ for et in subscriber.subscribed_to()],
            },
        )

    def unregister(self, subscriber: EventSubscriber) -> bool:
        """
        Unregister a subscriber.

        Args:
            subscriber: The subscriber to unregister

        Returns:
            True if subscriber was found and removed, False otherwise
        """
        try:
            self._subscribers.remove(subscriber)
            return True
        except ValueError:
            return False

    def get_subscribers_for(self, event_type: type[DomainEvent]) -> list[EventSubscriber]:
        """
        Get all subscribers interested in an event type.

        Args:
            event_type: The event type to look up

        Returns:
            List of subscribers that handle this event type
        """
        return [s for s in self._subscribers if event_type in s.subscribed_to()]

    async def _bounded(self, coro: Any) -> Any:
        """Run `coro` under the fan-out semaphore, if one is configured."""
        if self._semaphore is None:
            return await coro
        async with self._semaphore:
            return await coro

    async def dispatch(self, event: DomainEvent) -> None:
        """
        Dispatch an event to all interested subscribers.

        Only dispatches to subscribers that have subscribed to this event type.

        Args:
            event: The event to dispatch
        """
        event_type = type(event)
        subscribers = self.get_subscribers_for(event_type)

        if not subscribers:
            logger.debug(
                "No subscribers for event type %s",
                event_type.__name__,
                extra={"event_type": event_type.__name__},
            )
            return

        tasks = [self._bounded(s.handle(event)) for s in subscribers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                subscriber_name = subscribers[i].__class__.__name__
                logger.error(
                    "Error in subscriber %s while processing %s: %s",
                    subscriber_name,
                    event_type.__name__,
                    result,
                    exc_info=result,
                    extra={
                        "subscriber": subscriber_name,
                        "event_type": event_type.__name__,
                        "event_id": str(event.event_id),
                    },
                )

    async def dispatch_many(self, events: list[DomainEvent]) -> None:
        """
        Dispatch multiple events in order.

        Args:
            events: Events to dispatch
        """
        for event in events:
            await self.dispatch(event)

    @property
    def subscribers(self) -> list[EventSubscriber]:
        """Get list of registered subscribers."""
        return list(self._subscribers)

    def get_subscriber_count(self) -> int:
        """Get number of registered subscribers."""
        return len(self._subscribers)


__all__ = ["SubscriberRegistry"]
