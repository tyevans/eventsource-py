"""
Handler infrastructure for event sourcing.

This module provides utilities for working with event handlers:
- HandlerAdapter: Normalizes sync/async handlers to consistent async interface
- HandlerRegistry: Discovers and routes handlers for declarative projections
- HandlerInfo: Metadata about registered handlers

Example:
    >>> from eventsource.handlers import HandlerAdapter, HandlerRegistry
    >>>
    >>> # Normalize handlers for event bus
    >>> adapter = HandlerAdapter(my_sync_handler)
    >>> await adapter.handle(event)  # Works with both sync and async handlers
    >>>
    >>> # Discover and route handlers for projections
    >>> registry = HandlerRegistry(my_projection)
    >>> await registry.dispatch(event)
"""

# HandlerAdapter is safe to import directly - it has no circular dependencies
from eventsource.handlers.adapter import HandlerAdapter, get_handler_name


def __getattr__(name: str) -> object:
    """Lazy import for HandlerRegistry and HandlerInfo to avoid circular imports.

    The registry module imports from projections.decorators which imports from
    projections.base which imports HandlerRegistry, creating a circular dependency.
    Using lazy imports breaks this cycle.
    """
    if name == "HandlerRegistry":
        from eventsource.handlers.registry import HandlerRegistry

        return HandlerRegistry
    elif name == "HandlerInfo":
        from eventsource.handlers.registry import HandlerInfo

        return HandlerInfo
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "HandlerAdapter",
    "HandlerInfo",
    "HandlerRegistry",
    "get_handler_name",
]
