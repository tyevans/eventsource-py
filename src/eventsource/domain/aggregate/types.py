"""Type aliases for aggregates."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from eventsource.domain.event import DomainEvent

# Type alias for unregistered event handling mode
UnregisteredEventHandling = str  # "ignore" | "warn" | "error"

# Type alias for event handler functions
EventHandler = Callable[[DomainEvent], None]

# Type variable for event types (used by create_event)
TEvent = TypeVar("TEvent", bound=DomainEvent)

__all__ = [
    "EventHandler",
    "TEvent",
    "UnregisteredEventHandling",
]
