"""Event bus decorator that records what was published, for tests.

InMemoryEventBus used to carry this itself, in an unbounded list that never
got trimmed -- a test affordance living in production code, and a memory leak
proportional to total events published in any long-lived process.
"""

from __future__ import annotations

import threading
from collections import deque

from eventsource.bus.interface import EventBus, EventHandlerFunc
from eventsource.events.base import DomainEvent
from eventsource.protocols import FlexibleEventHandler, FlexibleEventSubscriber


class RecordingEventBus(EventBus):
    """Wraps any EventBus and records the events published through it.

    Args:
        wrapped: The bus to delegate to.
        max_events: Maximum events to retain; oldest are dropped past this.
            None retains everything -- only safe for short-lived tests.

    Example:
        >>> bus = RecordingEventBus(InMemoryEventBus())
        >>> await bus.publish([OrderCreated(...)])
        >>> assert len(bus.published_events) == 1
    """

    def __init__(self, wrapped: EventBus, *, max_events: int | None = 10_000) -> None:
        self._wrapped = wrapped
        self._lock = threading.RLock()
        self._published: deque[DomainEvent] = deque(maxlen=max_events)

    @property
    def wrapped(self) -> EventBus:
        """The underlying bus."""
        return self._wrapped

    @property
    def published_events(self) -> list[DomainEvent]:
        """A copy of the recorded events, in publication order."""
        with self._lock:
            return list(self._published)

    def clear_published_events(self) -> None:
        """Discard the recorded events."""
        with self._lock:
            self._published.clear()

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        with self._lock:
            self._published.extend(events)
        await self._wrapped.publish(events, background=background)

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        self._wrapped.subscribe(event_type, handler)

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        return self._wrapped.unsubscribe(event_type, handler)

    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None:
        self._wrapped.subscribe_all(subscriber)

    def subscribe_to_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        self._wrapped.subscribe_to_all_events(handler)

    def unsubscribe_from_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        return self._wrapped.unsubscribe_from_all_events(handler)


__all__ = ["RecordingEventBus"]
