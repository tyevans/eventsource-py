"""
Projection decorators.

This module contains the @handles decorator for declarative projections,
enabling automatic event routing based on decorated handler methods.
"""

from collections.abc import Callable
from typing import Any, TypeVar

from eventsource.events.base import DomainEvent

# Type variable for handler functions
F = TypeVar("F", bound=Callable[..., Any])


def handles(event_type: type[DomainEvent]) -> Callable[[F], F]:
    """
    Decorator to mark a method as an event handler for a specific event type.

    Used by DeclarativeProjection to automatically route events to handler methods.
    The decorator attaches the event type to the function, which is then discovered
    during projection initialization.

    Args:
        event_type: The event type this handler processes

    Returns:
        A decorator function that marks the handler

    Example:
        >>> from eventsource.projections.decorators import handles
        >>> from eventsource.projections.base import DeclarativeProjection
        >>>
        >>> class MyProjection(DeclarativeProjection):
        ...     @handles(OrderCreated)
        ...     async def _handle_order_created(self, conn, event: OrderCreated) -> None:
        ...         # Handle the event
        ...         await conn.execute(...)
        ...
        ...     @handles(OrderShipped)
        ...     async def _handle_order_shipped(self, conn, event: OrderShipped) -> None:
        ...         # Handle shipping event
        ...         pass

    Notes:
        - Handler methods must be async functions
        - Handler signature must be: (self, conn, event: EventType) -> None
        - The event type in the @handles decorator should match the event parameter type annotation
        - DeclarativeProjection will validate signatures at initialization time
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
