"""Event bus decorator that records what was published, for tests.

InMemoryEventBus used to carry this itself, in an unbounded list that never
got trimmed -- a test affordance living in production code, and a memory leak
proportional to total events published in any long-lived process.
"""

from __future__ import annotations

import threading
from collections import deque

from eventsource.domain.event import DomainEvent
from eventsource.ports.bus import EventBus, EventHandlerFunc
from eventsource.ports.handlers import FlexibleEventHandler, FlexibleEventSubscriber


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
        """Record then delegate.

        Events are appended to ``published_events`` before delegating to the
        wrapped bus, so a publish that later fails (raises, or -- in
        ``background=True`` mode -- fails asynchronously) still shows up in
        ``published_events``. This is deliberate: tests observing "what was
        published" should see it even when delivery itself failed.
        """
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

    def clear_subscribers(self) -> None:
        """Delegate to the wrapped bus. Requires a bus providing this method."""
        self._wrapped.clear_subscribers()  # type: ignore[attr-defined]

    def get_subscriber_count(self, event_type: type[DomainEvent] | None = None) -> int:
        """Delegate to the wrapped bus. Requires a bus providing this method."""
        return self._wrapped.get_subscriber_count(event_type)  # type: ignore[attr-defined, no-any-return]

    def get_wildcard_subscriber_count(self) -> int:
        """Delegate to the wrapped bus. Requires a bus providing this method."""
        return self._wrapped.get_wildcard_subscriber_count()  # type: ignore[attr-defined, no-any-return]


__all__ = ["RecordingEventBus"]
