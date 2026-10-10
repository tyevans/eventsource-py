"""
Base abstract classes and protocol definitions for projections and event handlers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from uuid import UUID

from eventsource.domain.event import DomainEvent

# Type alias for unregistered event handling mode
UnregisteredEventHandling = str  # "ignore" | "warn" | "error"

# Tenant filter can be:
# - Static UUID: Always filter by this tenant
# - Callable: Dynamic filter, called per-event (e.g., get_current_tenant)
# - None: No filtering, process all events
type TenantFilter = UUID | Callable[[], UUID | None] | None


class Projection(ABC):
    """
    Base class for projections.

    Projections consume domain events and build read models
    optimized for specific query patterns. They provide the
    query side in CQRS architecture.

    Subclasses must implement:
    - handle(): Process a single event
    - reset(): Clear all read model data

    Example:
        >>> class OrderSummaryProjection(Projection):
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             await self._create_summary(event)
        ...
        ...     async def reset(self) -> None:
        ...         await self._clear_all_summaries()
    """

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event.

        Args:
            event: The domain event to process
        """
        pass

    @abstractmethod
    async def reset(self) -> None:
        """
        Reset the projection (clear all read model data).

        Useful for rebuilding projections from scratch.
        """
        pass


class SyncProjection(ABC):
    """
    Synchronous base class for projections.

    Useful for projections that don't require async I/O,
    or for testing scenarios.
    """

    @abstractmethod
    def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event synchronously.

        Args:
            event: The domain event to process
        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """
        Reset the projection (clear all read model data).
        """
        pass


class EventHandlerBase(ABC):
    """
    Base class for event handlers.

    Event handlers react to specific event types and perform actions
    (update read models, send notifications, trigger workflows, etc.)

    Unlike projections, handlers are focused on individual event types
    and provide explicit can_handle() checking.

    Example:
        >>> class OrderNotificationHandler(EventHandlerBase):
        ...     def can_handle(self, event: DomainEvent) -> bool:
        ...         return isinstance(event, (OrderShipped, OrderDelivered))
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         await send_notification(event)
    """

    @abstractmethod
    def can_handle(self, event: DomainEvent) -> bool:
        """
        Check if this handler can process the given event.

        Args:
            event: The event to check

        Returns:
            True if this handler can process the event
        """
        pass

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """
        Handle the event.

        Args:
            event: The event to process
        """
        pass


__all__ = [
    "EventHandlerBase",
    "Projection",
    "SyncProjection",
    "TenantFilter",
    "UnregisteredEventHandling",
]
