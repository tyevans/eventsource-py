"""OpenTelemetry tracing utilities for eventsource."""

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
    """Get an OpenTelemetry tracer if available, or None if OpenTelemetry is not installed."""
    if OTEL_AVAILABLE and trace is not None:
        return trace.get_tracer(name)
    return None


def should_trace(enable_tracing: bool) -> bool:
    """Determine if tracing should be active (enabled and OpenTelemetry available)."""
    return enable_tracing and OTEL_AVAILABLE


P = ParamSpec("P")
R = TypeVar("R")


def traced(
    name: str,
    attributes: dict[str, Any] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to add OpenTelemetry tracing to a method on classes with _tracer and _enable_tracing."""

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
