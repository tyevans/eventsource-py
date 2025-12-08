"""
Event subscriber and handler protocols.

Note: Protocol definitions have been moved to eventsource.protocols.
Imports from this module still work but emit deprecation warnings.
Use `from eventsource.protocols import EventHandler` instead.

Defines protocols for subscribing to and handling domain events.
These protocols enable a clean separation between the event bus
and the projection system.
"""

import warnings
from abc import ABC, abstractmethod

from eventsource.events.base import DomainEvent

# Import canonical protocols for re-export
from eventsource.protocols import (
    EventHandler as _EventHandler,
)
from eventsource.protocols import (
    EventSubscriber as _EventSubscriber,
)
from eventsource.protocols import (
    SyncEventHandler as _SyncEventHandler,
)


def __getattr__(name: str) -> type:
    """
    Handle deprecated imports with warnings.

    This enables deprecation warnings when importing protocols
    from this module.
    """
    if name == "EventHandler":
        warnings.warn(
            "Importing 'EventHandler' from eventsource.projections.protocols is deprecated. "
            "Use 'from eventsource.protocols import EventHandler' instead. "
            "This import will be removed in version 0.3.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _EventHandler
    elif name == "SyncEventHandler":
        warnings.warn(
            "Importing 'SyncEventHandler' from eventsource.projections.protocols is deprecated. "
            "Use 'from eventsource.protocols import SyncEventHandler' instead. "
            "This import will be removed in version 0.3.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _SyncEventHandler
    elif name == "EventSubscriber":
        warnings.warn(
            "Importing 'EventSubscriber' from eventsource.projections.protocols is deprecated. "
            "Use 'from eventsource.protocols import EventSubscriber' instead. "
            "This import will be removed in version 0.3.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _EventSubscriber
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# For internal use without deprecation warnings
# These are re-exported for backward compatibility in internal code
EventHandler = _EventHandler
SyncEventHandler = _SyncEventHandler
EventSubscriber = _EventSubscriber


class AsyncEventHandler(ABC):
    """
    Base class for async event handlers.

    Handlers process events asynchronously and can be used
    for projections, notifications, and other side effects.

    Unlike EventSubscriber, this provides a can_handle() method
    for more flexible event routing.

    Example:
        >>> class NotificationHandler(AsyncEventHandler):
        ...     def event_types(self) -> list[type[DomainEvent]]:
        ...         return [OrderShipped]
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         await send_notification(event)
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
        Override for custom logic.

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
    "EventHandler",
    "SyncEventHandler",
    "EventSubscriber",
    "AsyncEventHandler",
]
