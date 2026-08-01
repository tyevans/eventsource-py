"""
eventsource - Production-ready event sourcing library for Python.

This library provides:
- Event store ports with PostgreSQL, SQLite, and in-memory adapters
- Domain Event base class with Pydantic models
- Aggregate pattern with optimistic locking
- Projection system with checkpoint tracking and DLQ
- Event Bus with In-Memory and Redis Streams backends
- Transactional Outbox pattern
"""

import contextlib
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("eventsource-py")
except PackageNotFoundError:
    # Package not installed (running from source without install)
    __version__ = "0.0.0.dev0"

# Multi-tenancy support (DX-010)
# Exceptions - available immediately
# Aggregates (Task 07, Task 08)
# Core-rings surface: domain value objects, boundary ports, and the adapters.
# Shared async engine factory
# Event bus (Task 10)
from eventsource.adapters._bus.base import BaseEventBus
from eventsource.adapters._bus.registry import SubscriptionRegistry
from eventsource.adapters._bus.retry import RetryPolicy
from eventsource.adapters._sql.engine import create_async_engine
from eventsource.adapters._sql.positions import IntPositionCodec

# Kafka Event bus
from eventsource.adapters.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)

# Repository infrastructure (Task 12)
from eventsource.adapters.memory import (
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
    InMemoryEventStore,
    InMemoryOutboxRepository,
)
from eventsource.adapters.memory.bus import InMemoryEventBus

# Snapshots
from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.postgresql import (
    ASYNCPG_AVAILABLE,
    PostgreSQLEventStore,
    PostgreSQLOutboxRepository,
)

# RabbitMQ Event bus
from eventsource.adapters.rabbitmq import (
    RABBITMQ_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
)

# Redis Event bus (Task 11)
from eventsource.adapters.redis import (
    REDIS_AVAILABLE,
    RedisEventBus,
    RedisEventBusConfig,
    RedisEventBusStats,
    RedisNotAvailableError,
)

# Serialization utilities
from eventsource.adapters.serialization import EventSourceJSONEncoder
from eventsource.adapters.sql import SQLCheckpointRepository, SQLDLQRepository

# Projections (Task 09)
from eventsource.adapters.sql.projection import DatabaseProjection

# ReadModel Projections (Phase 3)
from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.adapters.sqlite import (
    AIOSQLITE_AVAILABLE,
    SQLITE_AVAILABLE,
    SQLiteEventStore,
)

# Sync adapters (DX-005)
from eventsource.adapters.sync import SyncEventStoreAdapter
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    Projection,
)
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.command import DomainCommand
from eventsource.domain.decider import DeciderAggregate

# Core event primitives (Task 02)
from eventsource.domain.event import DomainEvent

# Event registry (Task 03)
from eventsource.domain.event_registry import (
    EventRegistry,
    default_registry,
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
    register_event,
)
from eventsource.domain.exceptions import (
    AggregateNotCreatedError,
    AggregateNotFoundError,
    CommandRejectedError,
    DuplicateEventError,
    DuplicateEventTypeError,
    EventNotFoundError,
    EventSourceError,
    EventTypeNotFoundError,
    EventVersionError,
    HandlerDispatchError,
    OptimisticLockError,
    PositionDecodeError,
    PositionForeignError,
    ProjectionError,
    SnapshotDeserializationError,
    SnapshotError,
    SnapshotNotFoundError,
    SnapshotSchemaVersionError,
)

# Decorators - canonical location for @handles (TD-006)
from eventsource.handlers import handles
from eventsource.multitenancy import (
    TenantContextNotSetError,
    TenantContextResetError,
    TenantContextToken,
    TenantDomainEvent,
    TenantMismatchError,
    clear_tenant_context,
    get_current_tenant,
    get_required_tenant,
    reset_tenant_context,
    set_current_tenant,
    tenant_context,
    tenant_scope,
    tenant_scope_sync,
)
from eventsource.ports import (
    AggregateStore,
    AppendResult,
    CategoryQuery,
    CategoryReadOptions,
    EventAppender,
    EventBus,
    EventEnvelope,
    EventHandlerFunc,
    EventLookup,
    EventPublisher,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    GlobalEventFeed,
    OutboxEntry,
    OutboxRepository,
    OutboxStats,
    Position,
    ReadDirection,
    StreamReader,
    StreamReadOptions,
    collect,
    outbox_event_data,
)
from eventsource.ports.checkpoints import CheckpointData, CheckpointRepository, LagMetrics
from eventsource.ports.dlq import (
    DLQEntry,
    DLQRepository,
    DLQStats,
    ProjectionFailureCount,
)

# Protocols - canonical location (TD-007)
from eventsource.ports.handlers import (
    AsyncEventHandler,
    EventHandler,
    EventSubscriber,
    FlexibleEventHandler,
    FlexibleEventSubscriber,
    SyncEventHandler,
)
from eventsource.ports.snapshots import Snapshot, SnapshotStore

# SQLite outbox repository (optional - requires aiosqlite at import time,
# unlike the SQLite store adapter, which imports cleanly without it).
with contextlib.suppress(ImportError):
    from eventsource.adapters.sqlite import SQLiteOutboxRepository  # noqa: F401

# Types - available immediately
from eventsource.domain.types import (
    AggregateId,
    CausationId,
    CorrelationId,
    EventId,
    TenantId,
    TState,
)
from eventsource.testing.recording import RecordingEventBus

__all__ = [
    # Version
    "__version__",
    # Types
    "TState",
    "AggregateId",
    "EventId",
    "TenantId",
    "CorrelationId",
    "CausationId",
    # Engine factory
    "create_async_engine",
    # Events (Task 02)
    "DomainEvent",
    # Commands (Task 05 - decider feature)
    "DomainCommand",
    # Event Registry (Task 03)
    "EventRegistry",
    "default_registry",
    "register_event",
    "get_event_class",
    "get_event_class_or_none",
    "is_event_registered",
    "list_registered_events",
    "EventTypeNotFoundError",
    "DuplicateEventTypeError",
    # Store adapters
    "InMemoryEventStore",
    "PostgreSQLEventStore",
    "ASYNCPG_AVAILABLE",
    # Aggregates (Task 07, Task 08)
    "AggregateRoot",
    "AggregateRepository",
    "DeclarativeAggregate",
    "DeciderAggregate",
    "handles",
    # Event Bus (Task 10)
    "EventBus",
    "BaseEventBus",
    "EventHandlerFunc",
    "AsyncEventHandler",
    "InMemoryEventBus",
    "SubscriptionRegistry",
    "RetryPolicy",
    "RecordingEventBus",
    # Protocols (TD-007)
    "EventHandler",
    "SyncEventHandler",
    "FlexibleEventHandler",
    "EventSubscriber",
    "FlexibleEventSubscriber",
    # Redis Event Bus (Task 11)
    "RedisEventBus",
    "RedisEventBusConfig",
    "RedisEventBusStats",
    "RedisNotAvailableError",
    "REDIS_AVAILABLE",
    # RabbitMQ Event Bus
    "RabbitMQEventBus",
    "RabbitMQEventBusConfig",
    "RabbitMQEventBusStats",
    "RabbitMQNotAvailableError",
    "RABBITMQ_AVAILABLE",
    # Kafka Event Bus
    "KafkaEventBus",
    "KafkaEventBusConfig",
    "KafkaEventBusStats",
    "KafkaNotAvailableError",
    "KAFKA_AVAILABLE",
    # Exceptions
    "AggregateNotCreatedError",
    "AggregateNotFoundError",
    "CommandRejectedError",
    "EventNotFoundError",
    "EventSourceError",
    "EventVersionError",
    "HandlerDispatchError",
    "OptimisticLockError",
    "ProjectionError",
    # Repository infrastructure (Task 12)
    "CheckpointRepository",
    "SQLCheckpointRepository",
    "InMemoryCheckpointRepository",
    "CheckpointData",
    "LagMetrics",
    "DLQRepository",
    "SQLDLQRepository",
    "InMemoryDLQRepository",
    "DLQEntry",
    "DLQStats",
    "ProjectionFailureCount",
    "OutboxRepository",
    "PostgreSQLOutboxRepository",
    "InMemoryOutboxRepository",
    "OutboxEntry",
    "OutboxStats",
    "outbox_event_data",
    "EventSourceJSONEncoder",
    # Projections (Task 09)
    "Projection",
    "CheckpointTrackingProjection",
    "DeclarativeProjection",
    "DatabaseProjection",
    # ReadModel Projections (Phase 3)
    "ReadModelProjection",
    # Snapshots
    "Snapshot",
    "SnapshotStore",
    "InMemorySnapshotStore",
    # Snapshot exceptions
    "SnapshotError",
    "SnapshotDeserializationError",
    "SnapshotSchemaVersionError",
    "SnapshotNotFoundError",
    # Sync adapters (DX-005)
    "SyncEventStoreAdapter",
    # Multi-tenancy (DX-010)
    "tenant_context",
    "TenantContextToken",
    "get_current_tenant",
    "get_required_tenant",
    "set_current_tenant",
    "reset_tenant_context",
    "clear_tenant_context",
    "tenant_scope",
    "tenant_scope_sync",
    "TenantDomainEvent",
    "TenantContextNotSetError",
    "TenantContextResetError",
    "TenantMismatchError",
    # Core-rings surface (Task 14): domain value objects
    "StreamId",
    # Core-rings surface: boundary ports
    "Position",
    "EventEnvelope",
    "EventPublisher",
    "AppendResult",
    "ExpectedVersion",
    "ReadDirection",
    "StreamReadOptions",
    "FeedReadOptions",
    "CategoryReadOptions",
    "AggregateStore",
    "EventAppender",
    "StreamReader",
    "EventLookup",
    "GlobalEventFeed",
    "CategoryQuery",
    "FullEventStore",
    "collect",
    # Core-rings surface: adapters
    "IntPositionCodec",
    # Core-rings surface: exceptions
    "DuplicateEventError",
    "PositionDecodeError",
    "PositionForeignError",
    # Optional-driver availability flags
    "AIOSQLITE_AVAILABLE",
    "SQLITE_AVAILABLE",
    "SQLiteEventStore",
]

# The SQLite outbox repository needs aiosqlite at import time; the store
# adapter does not (its constructor raises with an install hint instead).
if SQLITE_AVAILABLE:
    __all__.append("SQLiteOutboxRepository")
