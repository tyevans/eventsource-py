"""
eventsource - Production-ready event sourcing library for Python.

This library provides:
- Event Store with PostgreSQL and In-Memory backends
- Domain Event base class with Pydantic models
- Aggregate pattern with optimistic locking
- Projection system with checkpoint tracking and DLQ
- Event Bus with In-Memory and Redis Streams backends
- Transactional Outbox pattern
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("eventsource-py")
except PackageNotFoundError:
    # Package not installed (running from source without install)
    __version__ = "0.0.0.dev0"

# Multi-tenancy support (DX-010)
# Exceptions - available immediately
# Aggregates (Task 07, Task 08)
# Core-rings surface (Task 14): domain value objects, boundary ports, the
# memory adapter, and the legacy-surface compatibility wrapper.
#
# Several new names collide with existing top-level exports of the same
# name but a DIFFERENT class -- these are intentionally NOT rebound at top
# level (doing so would silently change what the existing export name
# means, which the "never remove or change an existing export" rule
# forbids). They remain available path-only from `eventsource.ports`:
#   - `ExpectedVersion` (new VO) vs. `stores.interface.ExpectedVersion`
#   - `ReadDirection` (new enum) vs. `stores.interface.ReadDirection`
#   - `AppendResult` (new VO) vs. `stores.interface.AppendResult`
# `SQLiteEventStore`/`PostgreSQLEventStore` (adapter classes) collide with
# the legacy store classes the same way; use
# `eventsource.adapters.sqlite.SQLiteEventStore` /
# `eventsource.adapters.postgresql.PostgreSQLEventStore` path-only.
from eventsource.adapters._sql.positions import IntPositionCodec

# Repository infrastructure (Task 12)
from eventsource.adapters.memory import (
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
    MemoryEventStore,
)

# Snapshots
from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.sql import SQLCheckpointRepository, SQLDLQRepository

# Projections (Task 09)
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    Projection,
)

# Event bus (Task 10)
from eventsource.bus.base import BaseEventBus
from eventsource.bus.interface import (
    EventBus,
    EventHandlerFunc,
)

# Kafka Event bus
from eventsource.bus.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)
from eventsource.bus.memory import InMemoryEventBus

# RabbitMQ Event bus
from eventsource.bus.rabbitmq import (
    RABBITMQ_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
)

# Redis Event bus (Task 11)
from eventsource.bus.redis import (
    REDIS_AVAILABLE,
    RedisEventBus,
    RedisEventBusConfig,
    RedisEventBusStats,
    RedisNotAvailableError,
)
from eventsource.bus.registry import SubscriptionRegistry
from eventsource.bus.retry import RetryPolicy

# Commands (Task 05 - decider feature)
from eventsource.commands import DomainCommand
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.decider import DeciderAggregate

# Shared async engine factory
from eventsource.engine import create_async_engine

# Core event primitives (Task 02)
from eventsource.events.base import DomainEvent

# Event registry (Task 03)
from eventsource.events.registry import (
    DuplicateEventTypeError,
    EventRegistry,
    EventTypeNotFoundError,
    default_registry,
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
    register_event,
)
from eventsource.exceptions import (
    AggregateNotCreatedError,
    AggregateNotFoundError,
    CommandRejectedError,
    DuplicateEventError,
    EventNotFoundError,
    EventSourceError,
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
    CategoryQuery,
    CategoryReadOptions,
    EventAppender,
    EventEnvelope,
    EventLookup,
    FeedReadOptions,
    FullEventStore,
    GlobalEventFeed,
    Position,
    StreamReader,
    StreamReadOptions,
    collect,
)
from eventsource.ports.checkpoints import CheckpointData, CheckpointRepository, LagMetrics
from eventsource.ports.dlq import (
    DLQEntry,
    DLQRepository,
    DLQStats,
    ProjectionFailureCount,
)
from eventsource.ports.snapshots import Snapshot, SnapshotStore

# Protocols - canonical location (TD-007)
from eventsource.protocols import (
    AsyncEventHandler,
    EventHandler,
    EventSubscriber,
    FlexibleEventHandler,
    FlexibleEventSubscriber,
    SyncEventHandler,
)

# ReadModel Projections (Phase 3)
from eventsource.readmodels import ReadModelProjection
from eventsource.repositories import (
    InMemoryOutboxRepository,
    OutboxEntry,
    OutboxRepository,
    OutboxStats,
    PostgreSQLOutboxRepository,
)

# Serialization utilities
from eventsource.serialization import EventSourceJSONEncoder

# Event store implementations (Task 05, Task 06)
from eventsource.stores.in_memory import InMemoryEventStore

# Event store interface and data structures (Task 04)
from eventsource.stores.interface import (
    AppendResult,
    EventPublisher,
    EventStore,
    EventStream,
    ExpectedVersion,
    ReadDirection,
    ReadOptions,
    StoredEvent,
)
from eventsource.stores.legacy import LegacyStoreAdapter
from eventsource.stores.postgresql import PostgreSQLEventStore

# Sync adapters (DX-005)
from eventsource.sync import SyncEventStoreAdapter

# SQLite Event Store and Repositories (optional - requires aiosqlite)
try:
    from eventsource.repositories.outbox import SQLiteOutboxRepository  # noqa: F401
    from eventsource.stores.sqlite import SQLiteEventStore  # noqa: F401

    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

# Types - available immediately
from eventsource.testing.recording import RecordingEventBus
from eventsource.types import (
    AggregateId,
    CausationId,
    CorrelationId,
    EventId,
    TenantId,
    TState,
)

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
    # Event Store Interface and Data Structures (Task 04)
    "EventStore",
    "EventPublisher",
    "EventStream",
    "AppendResult",
    "StoredEvent",
    "ReadOptions",
    "ReadDirection",
    "ExpectedVersion",
    # Event Store Implementations (Task 05, Task 06)
    "InMemoryEventStore",
    "PostgreSQLEventStore",
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
    "MemoryEventStore",
    "IntPositionCodec",
    # Core-rings surface: legacy-surface compatibility wrapper
    "LegacyStoreAdapter",
    # Core-rings surface: exceptions
    "DuplicateEventError",
    "PositionDecodeError",
    "PositionForeignError",
]

# Conditionally add SQLite exports when aiosqlite is available
if SQLITE_AVAILABLE:
    __all__.extend(
        [
            "SQLITE_AVAILABLE",
            "SQLiteEventStore",
            "SQLiteOutboxRepository",
        ]
    )
