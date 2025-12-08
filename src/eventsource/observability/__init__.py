"""
Observability utilities for eventsource.

This module provides OpenTelemetry integration and tracing utilities
that can be used across all eventsource components.

The utilities in this module are designed to:
- Reduce tracing boilerplate code
- Ensure consistent span naming and attributes
- Handle the case where OpenTelemetry is not installed

Example:
    >>> from eventsource.observability import OTEL_AVAILABLE, get_tracer, traced
    >>>
    >>> # Check if tracing is available
    >>> if OTEL_AVAILABLE:
    ...     tracer = get_tracer(__name__)
    ...     with tracer.start_as_current_span("operation"):
    ...         # traced operation
    ...         pass
    >>>
    >>> # Using the decorator
    >>> class MyStore(TracingMixin):
    ...     def __init__(self):
    ...         self._init_tracing(__name__)
    ...
    ...     @traced("my_store.save")
    ...     async def save(self, item_id: str) -> None:
    ...         pass

Note:
    OpenTelemetry is an optional dependency. All utilities in this module
    gracefully handle the case where OpenTelemetry is not installed.
"""

from eventsource.observability.tracing import (
    OTEL_AVAILABLE,
    TracingMixin,
    get_tracer,
    should_trace,
    traced,
)

__all__ = [
    "OTEL_AVAILABLE",
    "TracingMixin",
    "get_tracer",
    "should_trace",
    "traced",
]
