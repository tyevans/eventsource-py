"""Shared subscription registry for event bus implementations.

Every EventBus backend needs the same thing: a thread-safe mapping of event
type to handlers, plus a wildcard list. Before this module existed, all four
backends implemented it independently and inconsistently -- InMemory locked,
Redis did not, and Kafka keyed by event type *name* rather than by the class.
"""

from __future__ import annotations

import threading
from typing import Any

from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import FlexibleEventSubscriber


class SubscriptionRegistry:
    """Thread-safe registry of event handlers, keyed by event class.

    Handlers are stored as immutable tuples and the combined
    ``specific + wildcard`` tuple for each event type is cached, so dispatch
    performs no allocation per event. Any mutation drops the cache.

    Thread Safety:
        All public methods are safe to call from any thread. Subscription
        changes are rare relative to dispatch, so a single ``RLock`` around
        mutations and reads is not a meaningful cost.

    Example:
        >>> registry = SubscriptionRegistry()
        >>> registry.add(OrderCreated, my_handler)
        >>> for adapter in registry.handlers_for(OrderCreated):
        ...     await adapter.handle(event)
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._specific: dict[type[DomainEvent], tuple[HandlerAdapter, ...]] = {}
        self._wildcard: tuple[HandlerAdapter, ...] = ()
        self._combined: dict[type[DomainEvent], tuple[HandlerAdapter, ...]] = {}

    def _invalidate(self) -> None:
        """Drop the combined-tuple cache. Caller must hold the lock."""
        self._combined = {}

    def add(self, event_type: type[DomainEvent], handler: Any) -> HandlerAdapter:
        """Register a handler for a specific event type.

        Registering the same handler twice is allowed; it will be invoked
        twice, and each ``remove`` strips one registration.

        Args:
            event_type: The event class to subscribe to.
            handler: Object with a ``handle()`` method, or a callable.

        Returns:
            The HandlerAdapter wrapping the handler, for logging by callers.
        """
        adapter = HandlerAdapter(handler)
        with self._lock:
            self._specific[event_type] = self._specific.get(event_type, ()) + (adapter,)
            self._invalidate()
        return adapter

    def remove(self, event_type: type[DomainEvent], handler: Any) -> bool:
        """Remove one registration of a handler for an event type.

        Compares against the raw handler via ``HandlerAdapter.__eq__``, so no
        throwaway adapter is constructed.

        Returns:
            True if a registration was found and removed, False otherwise.
        """
        with self._lock:
            adapters = self._specific.get(event_type, ())
            for i, adapter in enumerate(adapters):
                if adapter == handler:
                    remaining = adapters[:i] + adapters[i + 1 :]
                    if remaining:
                        self._specific[event_type] = remaining
                    else:
                        del self._specific[event_type]
                    self._invalidate()
                    return True
        return False

    def add_wildcard(self, handler: Any) -> HandlerAdapter:
        """Register a handler that receives every published event."""
        adapter = HandlerAdapter(handler)
        with self._lock:
            self._wildcard = self._wildcard + (adapter,)
            self._invalidate()
        return adapter

    def remove_wildcard(self, handler: Any) -> bool:
        """Remove one wildcard registration.

        Returns:
            True if a registration was found and removed, False otherwise.
        """
        with self._lock:
            for i, adapter in enumerate(self._wildcard):
                if adapter == handler:
                    self._wildcard = self._wildcard[:i] + self._wildcard[i + 1 :]
                    self._invalidate()
                    return True
        return False

    def add_subscriber(self, subscriber: FlexibleEventSubscriber) -> None:
        """Register a subscriber for every type it declares via subscribed_to()."""
        for event_type in subscriber.subscribed_to():
            self.add(event_type, subscriber)

    def handlers_for(self, event_type: type[DomainEvent]) -> tuple[HandlerAdapter, ...]:
        """Get all handlers for an event type: specific first, then wildcard.

        The returned tuple is cached and shared. Callers must treat it as
        immutable -- it is the same object across calls until a mutation.
        """
        with self._lock:
            cached = self._combined.get(event_type)
            if cached is not None:
                return cached
            combined = self._specific.get(event_type, ()) + self._wildcard
            self._combined[event_type] = combined
            return combined

    def clear(self) -> None:
        """Remove every registration, specific and wildcard."""
        with self._lock:
            self._specific = {}
            self._wildcard = ()
            self._invalidate()

    def count(self, event_type: type[DomainEvent] | None = None) -> int:
        """Count type-specific registrations. Excludes wildcard handlers.

        Args:
            event_type: If given, count only this type. Otherwise count all.
        """
        with self._lock:
            if event_type is None:
                return sum(len(adapters) for adapters in self._specific.values())
            return len(self._specific.get(event_type, ()))

    def wildcard_count(self) -> int:
        """Count wildcard registrations."""
        with self._lock:
            return len(self._wildcard)


__all__ = ["SubscriptionRegistry"]
