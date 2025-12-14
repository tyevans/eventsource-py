"""
Handler adapter for normalizing event handlers.

This module provides utilities for normalizing different handler types
(sync/async, methods/callables) to a consistent async interface.

This addresses the Interface Segregation Principle violation where
handlers with different signatures had to be handled with runtime
type checking throughout the codebase.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from eventsource.events.base import DomainEvent

# Import canonical protocol definitions
from eventsource.protocols import AsyncEventHandler, SyncEventHandler

logger = logging.getLogger(__name__)

T = TypeVar("T")


# Type for async handler function
AsyncHandlerFunc = Callable[[DomainEvent], Awaitable[None]]


# Note: AsyncEventHandler and SyncEventHandler are now imported from
# eventsource.protocols. The imports above re-export them for backward
# compatibility with code that imports from this module.


def get_handler_name(handler: Any) -> str:
    """
    Get a descriptive name for a handler for logging and debugging.

    Args:
        handler: Any handler object (class instance, function, lambda)

    Returns:
        String name for the handler
    """
    if hasattr(handler, "__class__") and handler.__class__.__name__ != "function":
        return str(handler.__class__.__name__)
    elif hasattr(handler, "__name__"):
        return str(handler.__name__)
    else:
        return repr(handler)


class HandlerAdapter:
    """
    Adapter that normalizes event handlers to a consistent async interface.

    This adapter accepts handlers in various forms:
    - Objects with async handle() method
    - Objects with sync handle() method
    - Async callable functions
    - Sync callable functions

    All handlers are normalized to an async interface, allowing uniform
    handling throughout the codebase without runtime type checking.

    This addresses ISP violations by providing a single interface regardless
    of the original handler type.

    Example:
        >>> # Works with async handlers
        >>> class MyAsyncHandler:
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         await process(event)
        >>> adapter = HandlerAdapter(MyAsyncHandler())
        >>> await adapter.handle(event)

        >>> # Works with sync handlers
        >>> def sync_handler(event: DomainEvent) -> None:
        ...     print(event)
        >>> adapter = HandlerAdapter(sync_handler)
        >>> await adapter.handle(event)

    Attributes:
        original: The original unwrapped handler
        name: Descriptive name for logging
    """

    def __init__(self, handler: Any) -> None:
        """
        Initialize the adapter with a handler.

        Args:
            handler: Object with handle() method or callable

        Raises:
            TypeError: If handler doesn't have handle() method and isn't callable
        """
        self._original = handler
        self._name = get_handler_name(handler)
        self._async_handler = self._normalize(handler)

    def _normalize(self, handler: Any) -> AsyncHandlerFunc:
        """
        Normalize a handler to an async callable.

        Args:
            handler: Object with handle() method or callable

        Returns:
            Async function that handles events

        Raises:
            TypeError: If handler is not valid
        """
        # If it's an object with a handle method
        if hasattr(handler, "handle"):
            handle_method = handler.handle
            if asyncio.iscoroutinefunction(handle_method):
                # Already async - return the bound method directly
                # Cast needed because mypy can't infer the bound method type
                return handle_method  # type: ignore[no-any-return]
            else:
                # Wrap sync method in async wrapper
                def make_async_wrapper(method: Callable[[DomainEvent], Any]) -> AsyncHandlerFunc:
                    async def async_wrapper(event: DomainEvent) -> None:
                        result = method(event)
                        # Handle case where sync method unexpectedly returns a coroutine
                        if asyncio.iscoroutine(result):
                            await result

                    return async_wrapper

                return make_async_wrapper(handle_method)

        # It's a callable (function or lambda)
        elif callable(handler):
            if asyncio.iscoroutinefunction(handler):
                # Cast needed because mypy can't narrow the callable type
                return handler  # type: ignore[no-any-return]
            else:
                # Wrap sync callable in async wrapper
                callable_handler = handler

                async def async_callable_wrapper(event: DomainEvent) -> None:
                    result = callable_handler(event)
                    if asyncio.iscoroutine(result):
                        await result

                return async_callable_wrapper
        else:
            raise TypeError(
                f"Handler must have a handle() method or be callable, got {type(handler)}"
            )

    @property
    def original(self) -> Any:
        """Get the original unwrapped handler."""
        return self._original

    @property
    def name(self) -> str:
        """Get the handler's descriptive name."""
        return self._name

    async def handle(self, event: DomainEvent) -> None:
        """
        Handle an event using the normalized async handler.

        Args:
            event: The domain event to handle
        """
        await self._async_handler(event)

    def __eq__(self, other: object) -> bool:
        """Check equality based on original handler identity."""
        if isinstance(other, HandlerAdapter):
            return self._original is other._original
        return self._original is other

    def __hash__(self) -> int:
        """Hash based on original handler identity."""
        return id(self._original)

    def __repr__(self) -> str:
        return f"HandlerAdapter({self._name})"


__all__ = [
    "HandlerAdapter",
    "AsyncHandlerFunc",
    "AsyncEventHandler",
    "SyncEventHandler",
    "get_handler_name",
]
