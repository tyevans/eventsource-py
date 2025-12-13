"""
Event handler decorators.

This module contains the @handles decorator for declarative event handling,
enabling automatic event routing based on decorated handler methods.

The @handles decorator is the canonical location for marking methods as event
handlers, and works with both:
- DeclarativeAggregate: For sync handlers that apply events to aggregate state
- DeclarativeProjection: For async handlers that build read models

Example:
    >>> from eventsource.handlers import handles
    >>> # or: from eventsource import handles
"""

from collections.abc import Callable
from typing import Any, TypeVar

from eventsource.events.base import DomainEvent

# Type variable for handler functions - preserves the exact type of the decorated function
F = TypeVar("F", bound=Callable[..., Any])


def handles(event_type: type[DomainEvent]) -> Callable[[F], F]:
    """
    Decorator to mark a method as an event handler for a specific event type.

    This is the canonical decorator for event handling in the eventsource library.
    It works with both DeclarativeAggregate (for aggregates) and DeclarativeProjection
    (for projections), enabling automatic event routing based on event type.

    The decorator attaches the event type to the function, which is then discovered
    during class initialization by both aggregate and projection base classes.

    Args:
        event_type: The DomainEvent subclass this handler processes

    Returns:
        A decorator function that marks the handler and preserves the original function

    Handler Signatures:
        Valid handler signatures are:

        For aggregates (sync):
            def handler(self, event: EventType) -> None

        For projections (async with context):
            async def handler(self, context, event: EventType) -> None

        For standalone handlers (async):
            async def handler(self, event: EventType) -> None

        Invalid signatures will raise HandlerSignatureError at class
        initialization time with examples of valid signatures.

    Raises:
        HandlerSignatureError: If handler signature is invalid (at class init).
            This occurs when the handler has the wrong number of parameters.
            The error message includes expected signature patterns and hints.

    Example (Aggregate):
        >>> from eventsource.handlers import handles
        >>> from eventsource.aggregates import DeclarativeAggregate
        >>>
        >>> class OrderAggregate(DeclarativeAggregate[OrderState]):
        ...     @handles(OrderCreated)
        ...     def _on_order_created(self, event: OrderCreated) -> None:
        ...         self._state = OrderState(
        ...             order_id=self.aggregate_id,
        ...             status="created",
        ...         )

    Example (Projection):
        >>> from eventsource.handlers import handles
        >>> from eventsource.projections import DeclarativeProjection
        >>>
        >>> class OrderProjection(DeclarativeProjection):
        ...     @handles(OrderCreated)
        ...     async def _handle_order_created(self, conn, event: OrderCreated) -> None:
        ...         await conn.execute(...)

    Notes:
        - For aggregates: Handler signature is (self, event: EventType) -> None (sync)
        - For projections: Handler signature is (self, conn, event: EventType) -> None (async)
        - The event type in the @handles decorator should match the event parameter type
        - Base classes will validate signatures and discover handlers at initialization
    """

    def decorator(func: F) -> F:
        # Attach the event type to the function for later discovery
        func._handles_event_type = event_type  # type: ignore[attr-defined]
        return func

    return decorator


def get_handled_event_type(func: Callable[..., Any]) -> type[DomainEvent] | None:
    """
    Get the event type handled by a decorated function.

    This is a utility function to inspect handler methods and determine
    what event type they handle.

    Args:
        func: A function potentially decorated with @handles

    Returns:
        The event type if decorated with @handles, None otherwise

    Example:
        >>> @handles(OrderCreated)
        ... async def my_handler(self, conn, event): pass
        >>> get_handled_event_type(my_handler)
        <class 'OrderCreated'>
    """
    return getattr(func, "_handles_event_type", None)


def is_event_handler(func: Callable[..., Any]) -> bool:
    """
    Check if a function is decorated as an event handler.

    Args:
        func: A function to check

    Returns:
        True if the function is decorated with @handles, False otherwise

    Example:
        >>> @handles(OrderCreated)
        ... async def my_handler(self, conn, event): pass
        >>> is_event_handler(my_handler)
        True
        >>> def regular_function(): pass
        >>> is_event_handler(regular_function)
        False
    """
    return hasattr(func, "_handles_event_type")


__all__ = [
    "handles",
    "get_handled_event_type",
    "is_event_handler",
]
