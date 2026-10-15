"""Canonical protocol definitions for event handlers and subscribers."""

from abc import ABC, abstractmethod
from collections.abc import Awaitable
from typing import Protocol, runtime_checkable

from eventsource.domain.event import DomainEvent


@runtime_checkable
class EventHandler(Protocol):
    """Protocol for async event handlers processing domain events."""

    async def handle(self, event: DomainEvent) -> None:
        """Handle a domain event asynchronously."""
        ...


@runtime_checkable
class SyncEventHandler(Protocol):
    """Protocol for synchronous event handlers that do not require async I/O."""

    def handle(self, event: DomainEvent) -> None:
        """Handle a domain event synchronously."""
        ...


@runtime_checkable
class FlexibleEventHandler(Protocol):
    """Protocol for handlers that may be sync or async."""

    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """Handle event, returning Awaitable if async."""
        ...


class EventSubscriber(ABC):
    """Abstract base class for event subscribers declaring handled event types."""

    @abstractmethod
    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return list of event types this subscriber handles."""
        pass

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """Handle a domain event."""
        pass


@runtime_checkable
class FlexibleEventSubscriber(Protocol):
    """Protocol version of EventSubscriber for flexible sync/async typing."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return list of event types this subscriber handles."""
        ...

    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """Handle event, returning Awaitable if async."""
        ...


class AsyncEventHandler(ABC):
    """Base class for async event handlers with event type filtering."""

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """Handle an event asynchronously."""
        pass

    def can_handle(self, event: DomainEvent) -> bool:
        """Return True if this handler supports the given event's type."""
        return type(event) in self.event_types()

    @abstractmethod
    def event_types(self) -> list[type[DomainEvent]]:
        """Return list of event types this handler supports."""
        pass


__all__ = [
    "EventHandler",
    "SyncEventHandler",
    "FlexibleEventHandler",
    "EventSubscriber",
    "FlexibleEventSubscriber",
    "AsyncEventHandler",
]
