"""
Handler infrastructure for event sourcing.

This module provides utilities for working with event handlers:
- HandlerAdapter: Normalizes sync/async handlers to consistent async interface
- HandlerRegistry: Discovers and routes handlers for declarative projections
- HandlerInfo: Metadata about registered handlers
- handles: Decorator for marking event handler methods
- get_handled_event_type: Get event type from decorated handler
- is_event_handler: Check if function is an event handler

Example:
    >>> from eventsource.handlers import handles, HandlerAdapter, HandlerRegistry
    >>>
    >>> @handles(OrderCreated)
    >>> async def my_handler(self, conn, event): pass
    >>>
    >>> adapter = HandlerAdapter(my_sync_handler)
    >>> await adapter.handle(event)
"""

from eventsource.handlers.adapter import HandlerAdapter, get_handler_name
from eventsource.handlers.decorators import (
    get_handled_event_type,
    handles,
    is_event_handler,
)
from eventsource.handlers.registry import (
    HandlerInfo,
    HandlerRegistry,
    HandlerSignatureError,
)

__all__ = [
    "HandlerAdapter",
    "HandlerInfo",
    "HandlerRegistry",
    "HandlerSignatureError",
    "get_handler_name",
    "handles",
    "get_handled_event_type",
    "is_event_handler",
]
