"""
Observability utilities for eventsource.

This module provides tracing, metrics (future), and standard attribute
definitions for consistent observability across all eventsource components.

The utilities in this module are designed to:
- Reduce tracing boilerplate code
- Ensure consistent span naming and attributes
- Handle the case where OpenTelemetry is not installed

Example:
    >>> from eventsource.observability import OTEL_AVAILABLE, get_tracer, traced, create_tracer
    >>>
    >>> # Check if tracing is available
    >>> if OTEL_AVAILABLE:
    ...     tracer = get_tracer(__name__)
    ...     with tracer.start_as_current_span("operation"):
    ...         # traced operation
    ...         pass
    >>>
    >>> # Using composition-based Tracer
    >>> class MyStore:
    ...     def __init__(self, enable_tracing: bool = True):
    ...         self._tracer = create_tracer(__name__, enable_tracing)
    ...         self._enable_tracing = self._tracer.enabled
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
    ATTR_BATCH_SIZE,
    ATTR_BUFFER_SIZE,
    ATTR_DB_NAME,
    ATTR_DB_OPERATION,
    # Database (OTEL semantic)
    ATTR_DB_SYSTEM,
    ATTR_ERROR_TYPE,
    ATTR_EVENT_COUNT,
    # Event
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_EVENTS_PROCESSED,
    ATTR_EVENTS_SKIPPED,
    ATTR_EXPECTED_VERSION,
    ATTR_FROM_POSITION,
    ATTR_FROM_VERSION,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_HANDLER_SUCCESS,
    # Lock
    ATTR_LOCK_ACQUIRED,
    ATTR_LOCK_ID,
    ATTR_LOCK_KEY,
    ATTR_LOCK_TIMEOUT,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_OPERATION,
    # Messaging (OTEL semantic)
    ATTR_MESSAGING_SYSTEM,
    # Migration
    ATTR_MIGRATION_EVENTS_COPIED,
    ATTR_MIGRATION_EVENTS_TOTAL,
    ATTR_MIGRATION_ID,
    ATTR_MIGRATION_PHASE,
    ATTR_MIGRATION_PROGRESS_PERCENT,
    ATTR_MIGRATION_SOURCE_STORE,
    ATTR_MIGRATION_SYNC_LAG_EVENTS,
    ATTR_MIGRATION_SYNC_LAG_MS,
    ATTR_MIGRATION_TARGET_STORE,
    ATTR_MIGRATION_TENANT_ID,
    ATTR_POSITION,
    # Component-specific
    ATTR_PROJECTION_NAME,
    # Error/Retry
    ATTR_RETRY_COUNT,
    ATTR_STREAM_ID,
    # Subscription
    ATTR_SUBSCRIPTION_NAME,
    ATTR_SUBSCRIPTION_PHASE,
    ATTR_SUBSCRIPTION_STATE,
    # Tenant/Actor
    ATTR_TENANT_ID,
    ATTR_TO_POSITION,
    # Version
    ATTR_VERSION,
    ATTR_WATERMARK,
)
from eventsource.observability.tracer import (
    MockTracer,
    NullTracer,
    OpenTelemetryTracer,
    SpanKindEnum,
    Tracer,
    create_tracer,
)
from eventsource.observability.tracing import (
    OTEL_AVAILABLE,
    get_tracer,
    should_trace,
    traced,
)

__all__ = [
    # Tracing utilities
    "OTEL_AVAILABLE",
    "get_tracer",
    "should_trace",
    "traced",
    # Tracer (composition-based API)
    "Tracer",
    "NullTracer",
    "OpenTelemetryTracer",
    "MockTracer",
    "SpanKindEnum",
    "create_tracer",
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
    "ATTR_HANDLER_SUCCESS",
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
    # Attributes - Subscription
    "ATTR_SUBSCRIPTION_NAME",
    "ATTR_SUBSCRIPTION_STATE",
    "ATTR_SUBSCRIPTION_PHASE",
    "ATTR_FROM_POSITION",
    "ATTR_TO_POSITION",
    "ATTR_BATCH_SIZE",
    "ATTR_BUFFER_SIZE",
    "ATTR_EVENTS_PROCESSED",
    "ATTR_EVENTS_SKIPPED",
    "ATTR_WATERMARK",
    # Attributes - Error/Retry
    "ATTR_RETRY_COUNT",
    "ATTR_ERROR_TYPE",
    # Attributes - Migration
    "ATTR_MIGRATION_ID",
    "ATTR_MIGRATION_PHASE",
    "ATTR_MIGRATION_TENANT_ID",
    "ATTR_MIGRATION_SOURCE_STORE",
    "ATTR_MIGRATION_TARGET_STORE",
    "ATTR_MIGRATION_EVENTS_COPIED",
    "ATTR_MIGRATION_EVENTS_TOTAL",
    "ATTR_MIGRATION_PROGRESS_PERCENT",
    "ATTR_MIGRATION_SYNC_LAG_EVENTS",
    "ATTR_MIGRATION_SYNC_LAG_MS",
    # Attributes - Lock
    "ATTR_LOCK_KEY",
    "ATTR_LOCK_ID",
    "ATTR_LOCK_TIMEOUT",
    "ATTR_LOCK_ACQUIRED",
]
