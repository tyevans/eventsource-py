"""
OpenTelemetry tracing utilities for eventsource.

This module provides reusable tracing utilities that reduce boilerplate
and ensure consistent observability across all eventsource components.

Example:
    >>> from eventsource.observability import traced, TracingMixin, OTEL_AVAILABLE
    >>>
    >>> class MyStore(TracingMixin):
    ...     def __init__(self, enable_tracing: bool = True):
    ...         self._init_tracing(__name__, enable_tracing)
    ...
    ...     @traced("my_store.operation")
    ...     async def operation(self, item_id: str) -> None:
    ...         # Implementation
    ...         pass
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

if TYPE_CHECKING:
    from opentelemetry.trace import Span, Tracer

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


class TracingMixin:
    """
    Mixin class providing OpenTelemetry tracing utilities.

    Classes using this mixin gain standardized tracing support with
    minimal boilerplate. The mixin provides:

    - `_tracer`: OpenTelemetry tracer instance (or None)
    - `_enable_tracing`: Boolean flag for tracing state
    - `_init_tracing()`: Initialize tracing attributes
    - `_create_span_context()`: Create conditional span context manager

    Example:
        >>> class MyStore(TracingMixin):
        ...     def __init__(self, enable_tracing: bool = True):
        ...         self._init_tracing(__name__, enable_tracing)
        ...
        ...     async def save(self, item_id: str, data: dict) -> None:
        ...         with self._create_span_context(
        ...             "my_store.save",
        ...             {"item.id": item_id, "data.size": len(data)},
        ...         ):
        ...             # Implementation with dynamic attributes
        ...             await self._do_save(item_id, data)

    Note:
        For methods with only static attributes, prefer using the
        @traced decorator which is more concise.
    """

    _tracer: Tracer | None
    _enable_tracing: bool

    def _init_tracing(
        self,
        tracer_name: str,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize tracing attributes.

        Call this in your __init__ to set up tracing support.

        Args:
            tracer_name: Name for the tracer (typically __name__)
            enable_tracing: Whether to enable tracing (default True)

        Example:
            >>> def __init__(self, enable_tracing: bool = True):
            ...     self._init_tracing(__name__, enable_tracing)
        """
        self._enable_tracing = enable_tracing and OTEL_AVAILABLE
        if self._enable_tracing and trace is not None:
            self._tracer = trace.get_tracer(tracer_name)
        else:
            self._tracer = None

    def _create_span_context(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> AbstractContextManager[Span | None]:
        """
        Create a span context that handles None tracer gracefully.

        This method returns a context manager that:
        - Creates a span if tracing is enabled and tracer is available
        - Returns a nullcontext if tracing is disabled
        - Handles dynamic attributes computed at runtime

        Args:
            name: Span name (e.g., "event_store.append_events")
            attributes: Dynamic attributes to include in span

        Returns:
            Context manager that yields Span or None

        Example:
            >>> async def save(self, item_id: str) -> None:
            ...     with self._create_span_context(
            ...         "store.save",
            ...         {"item.id": item_id},
            ...     ) as span:
            ...         result = await self._do_save(item_id)
            ...         if span:
            ...             span.set_attribute("result.success", True)
            ...         return result
        """
        if not self._enable_tracing or self._tracer is None:
            return contextlib.nullcontext()
        return self._tracer.start_as_current_span(
            name,
            attributes=attributes or {},
        )

    @property
    def tracing_enabled(self) -> bool:
        """
        Check if tracing is currently enabled.

        Returns:
            True if tracing is enabled and tracer is available

        Example:
            >>> if store.tracing_enabled:
            ...     print("Tracing is active")
        """
        return self._enable_tracing and self._tracer is not None


__all__ = [
    "OTEL_AVAILABLE",
    "get_tracer",
    "should_trace",
    "traced",
    "TracingMixin",
]
