"""
Standard span and metric attributes for eventsource.

This module defines attribute constants used across all eventsource components
for consistent span naming and metrics labeling. These follow OpenTelemetry
semantic conventions where applicable.

Example:
    >>> from eventsource.observability.attributes import (
    ...     ATTR_AGGREGATE_ID,
    ...     ATTR_EVENT_TYPE,
    ... )
    >>>
    >>> with tracer.start_as_current_span(
    ...     "eventsource.event_store.append_events",
    ...     attributes={
    ...         ATTR_AGGREGATE_ID: str(aggregate_id),
    ...         ATTR_EVENT_TYPE: event.__class__.__name__,
    ...     },
    ... ):
    ...     pass
"""

# =============================================================================
# Aggregate Attributes
# =============================================================================

ATTR_AGGREGATE_ID = "eventsource.aggregate.id"
"""Unique identifier for the aggregate instance (UUID string)."""

ATTR_AGGREGATE_TYPE = "eventsource.aggregate.type"
"""Type name of the aggregate (e.g., 'Order', 'User')."""

# =============================================================================
# Event Attributes
# =============================================================================

ATTR_EVENT_ID = "eventsource.event.id"
"""Unique identifier for the event (UUID string)."""

ATTR_EVENT_TYPE = "eventsource.event.type"
"""Type name of the event (e.g., 'OrderCreated', 'UserRegistered')."""

ATTR_EVENT_COUNT = "eventsource.event.count"
"""Number of events in an operation (integer)."""

# =============================================================================
# Version Attributes
# =============================================================================

ATTR_VERSION = "eventsource.version"
"""Current version of an aggregate or stream (integer)."""

ATTR_EXPECTED_VERSION = "eventsource.expected_version"
"""Expected version for optimistic concurrency (integer)."""

ATTR_FROM_VERSION = "eventsource.from_version"
"""Starting version for event retrieval (integer)."""

# =============================================================================
# Tenant and Actor Attributes
# =============================================================================

ATTR_TENANT_ID = "eventsource.tenant.id"
"""Tenant identifier for multi-tenant systems (string)."""

ATTR_ACTOR_ID = "eventsource.actor.id"
"""Actor/user identifier who initiated the action (string)."""

# =============================================================================
# Component-Specific Attributes
# =============================================================================

ATTR_PROJECTION_NAME = "eventsource.projection.name"
"""Name of the projection processing events (string)."""

ATTR_HANDLER_NAME = "eventsource.handler.name"
"""Name of the event handler being invoked (string)."""

ATTR_HANDLER_COUNT = "eventsource.handler.count"
"""Number of handlers registered for an event type (integer)."""

ATTR_HANDLER_SUCCESS = "eventsource.handler.success"
"""Whether the handler executed successfully (boolean)."""

ATTR_STREAM_ID = "eventsource.stream.id"
"""Identifier for an event stream (string)."""

ATTR_POSITION = "eventsource.position"
"""Position in the event stream (integer)."""

# =============================================================================
# Database Attributes (OpenTelemetry Semantic Conventions)
# =============================================================================

ATTR_DB_SYSTEM = "db.system"
"""Database system identifier (e.g., 'sqlite', 'postgresql')."""

ATTR_DB_NAME = "db.name"
"""Database name being accessed."""

ATTR_DB_OPERATION = "db.operation"
"""Database operation type (e.g., 'INSERT', 'SELECT')."""

# =============================================================================
# Messaging Attributes (OpenTelemetry Semantic Conventions)
# =============================================================================

ATTR_MESSAGING_SYSTEM = "messaging.system"
"""Messaging system identifier (e.g., 'rabbitmq', 'redis')."""

ATTR_MESSAGING_DESTINATION = "messaging.destination"
"""Destination queue or topic name."""

ATTR_MESSAGING_OPERATION = "messaging.operation"
"""Messaging operation type (e.g., 'publish', 'receive')."""

# =============================================================================
# Error and Retry Attributes
# =============================================================================

ATTR_RETRY_COUNT = "eventsource.retry.count"
"""Number of retry attempts for an operation (integer)."""

ATTR_ERROR_TYPE = "eventsource.error.type"
"""Type of error encountered (exception class name)."""

# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Aggregate
    "ATTR_AGGREGATE_ID",
    "ATTR_AGGREGATE_TYPE",
    # Event
    "ATTR_EVENT_ID",
    "ATTR_EVENT_TYPE",
    "ATTR_EVENT_COUNT",
    # Version
    "ATTR_VERSION",
    "ATTR_EXPECTED_VERSION",
    "ATTR_FROM_VERSION",
    # Tenant/Actor
    "ATTR_TENANT_ID",
    "ATTR_ACTOR_ID",
    # Component-specific
    "ATTR_PROJECTION_NAME",
    "ATTR_HANDLER_NAME",
    "ATTR_HANDLER_COUNT",
    "ATTR_HANDLER_SUCCESS",
    "ATTR_STREAM_ID",
    "ATTR_POSITION",
    # Database (OTEL semantic)
    "ATTR_DB_SYSTEM",
    "ATTR_DB_NAME",
    "ATTR_DB_OPERATION",
    # Messaging (OTEL semantic)
    "ATTR_MESSAGING_SYSTEM",
    "ATTR_MESSAGING_DESTINATION",
    "ATTR_MESSAGING_OPERATION",
    # Error/Retry
    "ATTR_RETRY_COUNT",
    "ATTR_ERROR_TYPE",
]
