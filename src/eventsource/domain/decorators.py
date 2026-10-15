"""Declarative `@handles` event handler decorator and discovery utilities."""

from collections.abc import Callable
from typing import Any, TypeVar

from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import DuplicateHandlerError

F = TypeVar("F", bound=Callable[..., Any])


def handles(event_type: type[DomainEvent]) -> Callable[[F], F]:
    """Mark a method as an event handler for `event_type` on aggregates or projections."""

    def decorator(func: F) -> F:
        func._handles_event_type = event_type  # type: ignore[attr-defined]
        return func

    return decorator


def get_handled_event_type(func: Callable[..., Any]) -> type[DomainEvent] | None:
    """Return the `DomainEvent` subclass attached by `@handles`, or `None`."""
    return getattr(func, "_handles_event_type", None)


def is_event_handler(func: Callable[..., Any]) -> bool:
    """Return True if `func` is decorated with `@handles`."""
    return hasattr(func, "_handles_event_type")


def discover_handlers(owner: type) -> dict[type[DomainEvent], str]:
    """Scan `owner` for `@handles`-decorated methods; raise `DuplicateHandlerError` on collision."""
    handlers: dict[type[DomainEvent], str] = {}
    for name in dir(owner):
        try:
            attr = getattr(owner, name)
        except AttributeError:
            continue
        event_type = get_handled_event_type(attr)
        if event_type is None or not isinstance(event_type, type):
            continue
        existing = handlers.get(event_type)
        if existing is not None and existing != name:
            raise DuplicateHandlerError(
                owner_name=owner.__name__,
                event_type=event_type,
                first_handler=existing,
                second_handler=name,
            )
        handlers[event_type] = name
    return handlers


__all__ = [
    "handles",
    "get_handled_event_type",
    "is_event_handler",
    "discover_handlers",
]
