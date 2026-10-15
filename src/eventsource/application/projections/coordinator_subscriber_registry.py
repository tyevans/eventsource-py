"""Subscriber registry for EventSubscriber instances."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator, Sequence
from typing import Any, Self

from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import EventSubscriber

logger = logging.getLogger(__name__)


class SubscriberRegistry:
    """
    Registry for EventSubscriber instances with O(1) event-type lookup.

    Example:
        >>> registry = SubscriberRegistry(subscribers=[order_projection])
        >>> registry.register(shipping_subscriber, audit_subscriber)
        >>> subscribers = registry.get_subscribers_for(OrderCreated)
    """

    def __init__(
        self,
        max_concurrency: int | None = None,
        *,
        subscribers: Sequence[EventSubscriber] | None = None,
    ) -> None:
        """
        Initialize the subscriber registry.

        Args:
            max_concurrency: Maximum number of subscribers dispatched
                concurrently for a single event (`None` leaves fan-out uncapped).
            subscribers: Optional initial sequence of subscribers to register.
        """
        self._subscribers: list[EventSubscriber] = []
        self._by_event_type: dict[type[DomainEvent], list[EventSubscriber]] = {}
        self._semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None
        if subscribers:
            self.register(*subscribers)

    def register(self, *subscribers: EventSubscriber) -> Self:
        """Register one or more event subscribers and return ``self`` for chaining."""
        for subscriber in subscribers:
            self._subscribers.append(subscriber)
            event_types = subscriber.subscribed_to()
            for et in event_types:
                self._by_event_type.setdefault(et, []).append(subscriber)
            event_names = [et.__name__ for et in event_types]
            logger.debug(
                "Registered subscriber %s for events: %s",
                subscriber.__class__.__name__,
                event_names,
                extra={
                    "subscriber": subscriber.__class__.__name__,
                    "event_types": event_names,
                },
            )
        return self

    def unregister(self, subscriber: EventSubscriber) -> bool:
        """Unregister a subscriber. Returns True if found and removed, False otherwise."""
        try:
            self._subscribers.remove(subscriber)
        except ValueError:
            return False
        for subs in self._by_event_type.values():
            if subscriber in subs:
                subs.remove(subscriber)
        return True

    def clear(self) -> None:
        """Remove all registered subscribers and clear the event-type index."""
        self._subscribers.clear()
        self._by_event_type.clear()

    def get_subscribers_for(self, event_type: type[DomainEvent]) -> list[EventSubscriber]:
        """Get all subscribers interested in ``event_type`` via O(1) index lookup."""
        return list(self._by_event_type.get(event_type, ()))

    async def _bounded(self, coro: Any) -> Any:
        """Run `coro` under the fan-out semaphore, if one is configured."""
        if self._semaphore is None:
            return await coro
        async with self._semaphore:
            return await coro

    async def dispatch(self, event: DomainEvent) -> None:
        """Dispatch an event to all interested subscribers."""
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

    async def dispatch_many(self, events: Sequence[DomainEvent]) -> None:
        """Dispatch multiple events in order."""
        for event in events:
            await self.dispatch(event)

    @property
    def subscribers(self) -> list[EventSubscriber]:
        """Get list of registered subscribers."""
        return list(self._subscribers)

    def get_subscriber_count(self) -> int:
        """Get number of registered subscribers."""
        return len(self._subscribers)

    def __len__(self) -> int:
        """Return number of registered subscribers."""
        return len(self._subscribers)

    def __bool__(self) -> bool:
        """Empty registries remain truthy."""
        return True

    def __contains__(self, subscriber: EventSubscriber) -> bool:
        """Return True if subscriber is registered."""
        return subscriber in self._subscribers

    def __iter__(self) -> Iterator[EventSubscriber]:
        """Iterate over a snapshot of registered subscribers."""
        return iter(list(self._subscribers))


__all__ = ["SubscriberRegistry"]
