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

__version__ = "0.1.0"

# Exceptions - available immediately
# Aggregates (Task 07, Task 08)
from eventsource.aggregates.base import AggregateRoot, DeclarativeAggregate
from eventsource.aggregates.repository import AggregateRepository

# Event bus (Task 10)
from eventsource.bus.interface import (
    AsyncEventHandler,
    EventBus,
    EventHandlerFunc,
)
from eventsource.bus.memory import InMemoryEventBus

# Redis Event bus (Task 11)
from eventsource.bus.redis import (
    REDIS_AVAILABLE,
    RedisEventBus,
    RedisEventBusConfig,
    RedisEventBusStats,
    RedisNotAvailableError,
)

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
    AggregateNotFoundError,
    EventNotFoundError,
    EventSourceError,
    EventVersionError,
    OptimisticLockError,
    ProjectionError,
)

# Projections (Task 09)
from eventsource.projections.base import (
    CheckpointTrackingProjection,
    DatabaseProjection,
    DeclarativeProjection,
    Projection,
)

# Decorators - canonical location for @handles (TD-006)
from eventsource.projections.decorators import handles

# Protocols - canonical location (TD-007)
from eventsource.protocols import (
    EventHandler,
    EventSubscriber,
    FlexibleEventHandler,
    FlexibleEventSubscriber,
    SyncEventHandler,
)

# Repository infrastructure (Task 12)
from eventsource.repositories import (
    CheckpointData,
    CheckpointRepository,
    DLQEntry,
    DLQRepository,
    DLQStats,
    EventSourceJSONEncoder,
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
    InMemoryOutboxRepository,
    LagMetrics,
    OutboxEntry,
    OutboxRepository,
    OutboxStats,
    PostgreSQLCheckpointRepository,
    PostgreSQLDLQRepository,
    PostgreSQLOutboxRepository,
    ProjectionFailureCount,
)

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
from eventsource.stores.postgresql import PostgreSQLEventStore

# Types - available immediately
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
    # Events (Task 02)
    "DomainEvent",
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
    "handles",
    # Event Bus (Task 10)
    "EventBus",
    "EventHandlerFunc",
    "AsyncEventHandler",
    "InMemoryEventBus",
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
    # Exceptions
    "AggregateNotFoundError",
    "EventNotFoundError",
    "EventSourceError",
    "EventVersionError",
    "OptimisticLockError",
    "ProjectionError",
    # Repository infrastructure (Task 12)
    "CheckpointRepository",
    "PostgreSQLCheckpointRepository",
    "InMemoryCheckpointRepository",
    "CheckpointData",
    "LagMetrics",
    "DLQRepository",
    "PostgreSQLDLQRepository",
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
]
