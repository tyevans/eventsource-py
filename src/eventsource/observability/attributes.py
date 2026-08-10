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
    ...     "eventsource.snapshot.save",
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

# =============================================================================
# Tenant Attributes
# =============================================================================

ATTR_TENANT_ID = "eventsource.tenant.id"
"""Tenant identifier for multi-tenant systems (string)."""

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

ATTR_POSITION = "eventsource.position"
"""Position in the event stream (integer)."""

# =============================================================================
# Database Attributes (OpenTelemetry Semantic Conventions)
# =============================================================================

ATTR_DB_SYSTEM = "db.system"
"""Database system identifier (e.g., 'sqlite', 'postgresql')."""

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
# Subscription Attributes
# =============================================================================

ATTR_SUBSCRIPTION_NAME = "eventsource.subscription.name"
"""Name of the subscription being processed (string)."""

ATTR_SUBSCRIPTION_PHASE = "eventsource.subscription.phase"
"""Current phase of subscription transition (e.g., 'initial_catchup', 'live')."""

ATTR_FROM_POSITION = "eventsource.from_position"
"""Starting position for event reading (integer)."""

ATTR_TO_POSITION = "eventsource.to_position"
"""Target/ending position for event reading (integer)."""

ATTR_BATCH_SIZE = "eventsource.batch.size"
"""Number of events in a batch (integer)."""

ATTR_BUFFER_SIZE = "eventsource.buffer.size"
"""Current buffer size (integer)."""

ATTR_EVENTS_PROCESSED = "eventsource.events.processed"
"""Number of events processed (integer)."""

ATTR_WATERMARK = "eventsource.watermark"
"""Watermark position for catch-up to live transition (integer)."""

# =============================================================================
# Error and Retry Attributes
# =============================================================================

ATTR_RETRY_COUNT = "eventsource.retry.count"
"""Number of retry attempts for an operation (integer)."""

ATTR_ERROR_TYPE = "eventsource.error.type"
"""Type of error encountered (exception class name)."""

# =============================================================================
# ReadModel Attributes
# =============================================================================

ATTR_READMODEL_TYPE = "eventsource.readmodel.type"
"""Type name of the read model class (e.g., 'OrderSummary')."""

ATTR_READMODEL_ID = "eventsource.readmodel.id"
"""Unique identifier for the read model instance (UUID string)."""

ATTR_QUERY_FILTER_COUNT = "eventsource.query.filter_count"
"""Number of filters in a query (integer)."""

ATTR_QUERY_LIMIT = "eventsource.query.limit"
"""Limit value for a query (-1 if no limit) (integer)."""

# =============================================================================
# Migration Attributes
# =============================================================================

ATTR_MIGRATION_ID = "eventsource.migration.id"
"""Unique identifier for the migration (UUID string)."""

ATTR_MIGRATION_PHASE = "eventsource.migration.phase"
"""Current migration phase (e.g., 'pending', 'bulk_copy', 'dual_write')."""

ATTR_MIGRATION_TENANT_ID = "eventsource.migration.tenant_id"
"""Tenant identifier being migrated (UUID string)."""

ATTR_MIGRATION_SOURCE_STORE = "eventsource.migration.source_store"
"""Source event store identifier."""

ATTR_MIGRATION_TARGET_STORE = "eventsource.migration.target_store"
"""Target event store identifier."""

ATTR_MIGRATION_SYNC_LAG_EVENTS = "eventsource.migration.sync_lag_events"
"""Current sync lag in number of events (integer)."""

# =============================================================================
# Lock Attributes
# =============================================================================

ATTR_LOCK_KEY = "eventsource.lock.key"
"""Lock key identifier (string)."""

ATTR_LOCK_ID = "eventsource.lock.id"
"""Numeric lock identifier derived from the lock key (integer)."""

ATTR_LOCK_TIMEOUT = "eventsource.lock.timeout"
"""Lock acquisition timeout in seconds, or -1 when unbounded (float)."""

ATTR_LOCK_ACQUIRED = "eventsource.lock.acquired"
"""Whether lock was acquired (boolean)."""

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
    # Tenant/Actor
    "ATTR_TENANT_ID",
    # Component-specific
    "ATTR_PROJECTION_NAME",
    "ATTR_HANDLER_NAME",
    "ATTR_HANDLER_COUNT",
    "ATTR_HANDLER_SUCCESS",
    "ATTR_POSITION",
    # Database (OTEL semantic)
    "ATTR_DB_SYSTEM",
    "ATTR_DB_OPERATION",
    # Messaging (OTEL semantic)
    "ATTR_MESSAGING_SYSTEM",
    "ATTR_MESSAGING_DESTINATION",
    "ATTR_MESSAGING_OPERATION",
    # Subscription
    "ATTR_SUBSCRIPTION_NAME",
    "ATTR_SUBSCRIPTION_PHASE",
    "ATTR_FROM_POSITION",
    "ATTR_TO_POSITION",
    "ATTR_BATCH_SIZE",
    "ATTR_BUFFER_SIZE",
    "ATTR_EVENTS_PROCESSED",
    "ATTR_WATERMARK",
    # Error/Retry
    "ATTR_RETRY_COUNT",
    "ATTR_ERROR_TYPE",
    # ReadModel
    "ATTR_READMODEL_TYPE",
    "ATTR_READMODEL_ID",
    "ATTR_QUERY_FILTER_COUNT",
    "ATTR_QUERY_LIMIT",
    # Migration
    "ATTR_MIGRATION_ID",
    "ATTR_MIGRATION_PHASE",
    "ATTR_MIGRATION_TENANT_ID",
    "ATTR_MIGRATION_SOURCE_STORE",
    "ATTR_MIGRATION_TARGET_STORE",
    "ATTR_MIGRATION_SYNC_LAG_EVENTS",
    # Lock
    "ATTR_LOCK_KEY",
    "ATTR_LOCK_ID",
    "ATTR_LOCK_TIMEOUT",
    "ATTR_LOCK_ACQUIRED",
]
