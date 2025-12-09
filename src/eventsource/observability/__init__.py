"""
Observability utilities for eventsource.

This module provides tracing, metrics (future), and standard attribute
definitions for consistent observability across all eventsource components.

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

from eventsource.observability.attributes import (
    ATTR_ACTOR_ID,
    # Aggregate
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_DB_NAME,
    ATTR_DB_OPERATION,
    # Database (OTEL semantic)
    ATTR_DB_SYSTEM,
    ATTR_ERROR_TYPE,
    ATTR_EVENT_COUNT,
    # Event
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_EXPECTED_VERSION,
    ATTR_FROM_VERSION,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_OPERATION,
    # Messaging (OTEL semantic)
    ATTR_MESSAGING_SYSTEM,
    ATTR_POSITION,
    # Component-specific
    ATTR_PROJECTION_NAME,
    # Error/Retry
    ATTR_RETRY_COUNT,
    ATTR_STREAM_ID,
    # Tenant/Actor
    ATTR_TENANT_ID,
    # Version
    ATTR_VERSION,
)
from eventsource.observability.tracing import (
    OTEL_AVAILABLE,
    TracingMixin,
    get_tracer,
    should_trace,
    traced,
)

__all__ = [
    # Tracing
    "OTEL_AVAILABLE",
    "TracingMixin",
    "get_tracer",
    "should_trace",
    "traced",
    # Attributes - Aggregate
    "ATTR_AGGREGATE_ID",
    "ATTR_AGGREGATE_TYPE",
    # Attributes - Event
    "ATTR_EVENT_ID",
    "ATTR_EVENT_TYPE",
    "ATTR_EVENT_COUNT",
    # Attributes - Version
    "ATTR_VERSION",
    "ATTR_EXPECTED_VERSION",
    "ATTR_FROM_VERSION",
    # Attributes - Tenant/Actor
    "ATTR_TENANT_ID",
    "ATTR_ACTOR_ID",
    # Attributes - Component-specific
    "ATTR_PROJECTION_NAME",
    "ATTR_HANDLER_NAME",
    "ATTR_HANDLER_COUNT",
    "ATTR_STREAM_ID",
    "ATTR_POSITION",
    # Attributes - Database
    "ATTR_DB_SYSTEM",
    "ATTR_DB_NAME",
    "ATTR_DB_OPERATION",
    # Attributes - Messaging
    "ATTR_MESSAGING_SYSTEM",
    "ATTR_MESSAGING_DESTINATION",
    "ATTR_MESSAGING_OPERATION",
    # Attributes - Error/Retry
    "ATTR_RETRY_COUNT",
    "ATTR_ERROR_TYPE",
]
