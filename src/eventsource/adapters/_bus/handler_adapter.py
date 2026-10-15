"""Handler adapter for normalizing sync/async event handlers to a consistent async interface."""

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from eventsource.domain.event import DomainEvent

logger = logging.getLogger(__name__)

AsyncHandlerFunc = Callable[[DomainEvent], Awaitable[None]]


def get_handler_name(handler: Any) -> str:
    """Get a descriptive name for a handler for logging and debugging."""
    if hasattr(handler, "__class__") and handler.__class__.__name__ != "function":
        return str(handler.__class__.__name__)
    if hasattr(handler, "__name__"):
        return str(handler.__name__)
    return repr(handler)


class HandlerAdapter:
    """Adapter that normalizes sync/async event handlers (methods or callables) to async."""

    def __init__(self, handler: Any) -> None:
        """Initialize the adapter with an object having handle() or a callable."""
        self._original = handler
        self._name = get_handler_name(handler)
        self._async_handler = self._normalize(handler)

    def _normalize(self, handler: Any) -> AsyncHandlerFunc:
        """Normalize a handler to an async callable."""
        if hasattr(handler, "handle"):
            handle_method = handler.handle
            if inspect.iscoroutinefunction(handle_method):
                return handle_method  # type: ignore[no-any-return]

            def make_async_wrapper(method: Callable[[DomainEvent], Any]) -> AsyncHandlerFunc:
                async def async_wrapper(event: DomainEvent) -> None:
                    result = method(event)
                    if asyncio.iscoroutine(result):
                        await result

                return async_wrapper

            return make_async_wrapper(handle_method)

        if callable(handler):
            if inspect.iscoroutinefunction(handler):
                return handler  # type: ignore[no-any-return]
            callable_handler = handler

            async def async_callable_wrapper(event: DomainEvent) -> None:
                result = callable_handler(event)
                if asyncio.iscoroutine(result):
                    await result

            return async_callable_wrapper

        raise TypeError(f"Handler must have a handle() method or be callable, got {type(handler)}")

    @property
    def original(self) -> Any:
        """Get the original unwrapped handler."""
        return self._original

    @property
    def name(self) -> str:
        """Get the handler's descriptive name."""
        return self._name

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event using the normalized async handler."""
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
    "get_handler_name",
]
