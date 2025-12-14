"""
OpenTelemetry tracing utilities for eventsource.

This module provides reusable tracing utilities that reduce boilerplate
and ensure consistent observability across all eventsource components.

Example:
    >>> from eventsource.observability import traced, Tracer, create_tracer, OTEL_AVAILABLE
    >>>
    >>> class MyStore:
    ...     def __init__(self, enable_tracing: bool = True):
    ...         self._tracer = create_tracer(__name__, enable_tracing)
    ...         self._enable_tracing = self._tracer.enabled
    ...
    ...     @traced("my_store.operation")
    ...     async def operation(self, item_id: str) -> None:
    ...         # Implementation
    ...         pass
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

# Optional OpenTelemetry import - single source of truth
try:
    from opentelemetry import trace

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    trace = None  # type: ignore[assignment]


def get_tracer(name: str) -> Tracer | None:
    """
    Get an OpenTelemetry tracer if available.

    This is a convenience function that handles the OTEL_AVAILABLE check
    and returns None if OpenTelemetry is not installed.

    Args:
        name: The name for the tracer (typically __name__ of the module)

    Returns:
        OpenTelemetry Tracer if available, None otherwise

    Example:
        >>> tracer = get_tracer(__name__)
        >>> if tracer:
        ...     with tracer.start_as_current_span("operation"):
        ...         # traced operation
        ...         pass
    """
    if OTEL_AVAILABLE and trace is not None:
        return trace.get_tracer(name)
    return None


def should_trace(enable_tracing: bool) -> bool:
    """
    Determine if tracing should be active.

    Combines the component's enable_tracing setting with global OTEL availability.

    Args:
        enable_tracing: Component-level tracing configuration

    Returns:
        True if both tracing is enabled and OpenTelemetry is available

    Example:
        >>> if should_trace(self._enable_tracing):
        ...     # do tracing
        ...     pass
    """
    return enable_tracing and OTEL_AVAILABLE


# Type variables for decorator
P = ParamSpec("P")
R = TypeVar("R")


def traced(
    name: str,
    attributes: dict[str, Any] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator to add OpenTelemetry tracing to a method.

    The decorated method's class must have `_tracer` and `_enable_tracing`
    attributes (typically provided by TracingMixin or manual initialization).
    If tracing is disabled or tracer is None, the decorator is a no-op.

    Args:
        name: Span name (e.g., "event_store.append_events")
        attributes: Static attributes to include in span (optional)

    Returns:
        Decorated function with tracing support

    Example:
        >>> class MyStore:
        ...     _tracer: Tracer | None = None
        ...     _enable_tracing: bool = True
        ...
        ...     @traced("my_store.operation")
        ...     async def operation(self, item_id: str) -> None:
        ...         # Method body - automatically traced
        ...         pass
        ...
        ...     @traced("my_store.query", attributes={"db.system": "sqlite"})
        ...     async def query(self, query: str) -> list:
        ...         # Method body with static attributes
        ...         pass

    Note:
        For dynamic attributes that depend on method arguments, use the
        TracingMixin._create_span_context() method instead.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        async def async_wrapper(self: Any, *args: P.args, **kwargs: P.kwargs) -> R:
            tracer = getattr(self, "_tracer", None)
            enable_tracing = getattr(self, "_enable_tracing", False)

            if not enable_tracing or tracer is None:
                return await func(self, *args, **kwargs)  # type: ignore[misc, no-any-return]

            with tracer.start_as_current_span(name, attributes=attributes or {}):
                return await func(self, *args, **kwargs)  # type: ignore[misc, no-any-return]

        @functools.wraps(func)
        def sync_wrapper(self: Any, *args: P.args, **kwargs: P.kwargs) -> R:
            tracer = getattr(self, "_tracer", None)
            enable_tracing = getattr(self, "_enable_tracing", False)

            if not enable_tracing or tracer is None:
                return func(self, *args, **kwargs)

            with tracer.start_as_current_span(name, attributes=attributes or {}):
                return func(self, *args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper  # type: ignore[return-value]
        return sync_wrapper  # type: ignore[return-value]

    return decorator


__all__ = [
    "OTEL_AVAILABLE",
    "get_tracer",
    "should_trace",
    "traced",
]
