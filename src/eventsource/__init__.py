"""
eventsource - Production-ready event sourcing library for Python.

This library provides:
- Event store ports with PostgreSQL, SQLite, and in-memory adapters
- Domain Event base class with Pydantic models
- Aggregate pattern with optimistic locking
- Projection system with checkpoint tracking and DLQ
- Event Bus with In-Memory and Redis Streams backends
- Transactional Outbox pattern

This module is a PEP 562 lazy front door: `import eventsource` does not
import any adapter or driver (sqlalchemy, asyncpg, aiosqlite, redis,
aiokafka, aio-pika). Each public name is imported on first attribute
access via `__getattr__` and then cached on the module, so repeated
access after the first is a plain attribute lookup. `__version__` is the
only name computed eagerly -- it is pure stdlib (`importlib.metadata`)
and cheap enough to not warrant deferral.
"""

from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING

try:
    __version__ = version("eventsource-py")
except PackageNotFoundError:
    # Package not installed (running from source without install)
    __version__ = "0.0.0.dev0"

if TYPE_CHECKING:
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
        SQLiteOutboxRepository,  # noqa: F401 -- conditionally exported, see __all__ below
    )

    # Sync adapters (DX-005)
    from eventsource.adapters.sync import SyncEventStoreAdapter
    from eventsource.application.aggregates.repository import AggregateRepository
    from eventsource.application.projections.base import (
        CheckpointTrackingProjection,
        DeclarativeProjection,
        Projection,
    )
    from eventsource.application.projections.store import (
        ProjectionOptions,
        StoreProjection,
    )
    from eventsource.domain import StreamId
    from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
    from eventsource.domain.command import DomainCommand
    from eventsource.domain.decider import DeciderAggregate

    # Decorators - canonical location for @handles (TD-006)
    from eventsource.domain.decorators import handles

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
        AggregateTypeMismatchError,
        AggregateTypeNotSetError,
        CommandRejectedError,
        DuplicateEventError,
        DuplicateEventTypeError,
        EventNotFoundError,
        EventSourceError,
        EventTypeNotFoundError,
        EventVersionError,
        HandlerDispatchError,
        OptimisticLockError,
        ProjectionError,
        SnapshotDeserializationError,
        SnapshotError,
        SnapshotNotFoundError,
        SnapshotSchemaVersionError,
        TenantContextNotSetError,
        TenantContextResetError,
        TenantMismatchError,
    )
    from eventsource.domain.tenant_context import (
        TenantContextToken,
        clear_tenant_context,
        get_current_tenant,
        get_required_tenant,
        reset_tenant_context,
        set_current_tenant,
        tenant_context,
        tenant_scope,
        tenant_scope_sync,
    )
    from eventsource.domain.tenant_events import TenantDomainEvent

    # Types - available immediately
    from eventsource.domain.types import (
        AggregateId,
        CausationId,
        CorrelationId,
        EventId,
        TenantId,
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
    from eventsource.ports.exceptions import PositionDecodeError, PositionForeignError

    # Protocols - canonical location (TD-007)
    from eventsource.ports.handlers import (
        AsyncEventHandler,
        EventHandler,
        EventSubscriber,
        FlexibleEventHandler,
        FlexibleEventSubscriber,
        SyncEventHandler,
    )
    from eventsource.ports.readmodels import ReadModel
    from eventsource.ports.snapshots import Snapshot, SnapshotStore
    from eventsource.testing.recording import RecordingEventBus

__all__ = [
    # Version
    "__version__",
    # Types
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
    "StoreProjection",
    "ProjectionOptions",
    "DatabaseProjection",
    # ReadModel Projections (Phase 3)
    "ReadModelProjection",
    "ReadModel",
    "AggregateTypeNotSetError",
    "AggregateTypeMismatchError",
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
#
# Ask the import system whether aiosqlite is installed rather than importing
# it. `import aiosqlite` here *executed* the driver on every `import
# eventsource` -- exactly the eager optional-driver import ADR 0035 exists to
# prevent, and the reason the front door pulled in 177 modules. `find_spec`
# answers the same question without running any of it.
def _module_installed(name: str) -> bool:
    from importlib.util import find_spec

    try:
        return find_spec(name) is not None
    except (ImportError, ValueError):  # pragma: no cover - malformed install
        return False


_AIOSQLITE_AVAILABLE_FOR_ALL = _module_installed("aiosqlite")

if _AIOSQLITE_AVAILABLE_FOR_ALL:
    __all__.append("SQLiteOutboxRepository")

# name -> module path providing it. Built mechanically from the
# TYPE_CHECKING import block above; every name in __all__ (other than
# __version__, computed eagerly) must appear here exactly once.
_LAZY: dict[str, str] = {
    "AggregateId": "eventsource.domain.types",
    "EventId": "eventsource.domain.types",
    "TenantId": "eventsource.domain.types",
    "CorrelationId": "eventsource.domain.types",
    "CausationId": "eventsource.domain.types",
    "create_async_engine": "eventsource.adapters._sql.engine",
    "DomainEvent": "eventsource.domain.event",
    "DomainCommand": "eventsource.domain.command",
    "EventRegistry": "eventsource.domain.event_registry",
    "default_registry": "eventsource.domain.event_registry",
    "register_event": "eventsource.domain.event_registry",
    "get_event_class": "eventsource.domain.event_registry",
    "get_event_class_or_none": "eventsource.domain.event_registry",
    "is_event_registered": "eventsource.domain.event_registry",
    "list_registered_events": "eventsource.domain.event_registry",
    "EventTypeNotFoundError": "eventsource.domain.exceptions",
    "DuplicateEventTypeError": "eventsource.domain.exceptions",
    "InMemoryEventStore": "eventsource.adapters.memory",
    "PostgreSQLEventStore": "eventsource.adapters.postgresql",
    "ASYNCPG_AVAILABLE": "eventsource.adapters.postgresql",
    "AggregateRoot": "eventsource.domain.aggregate",
    "AggregateRepository": "eventsource.application.aggregates.repository",
    "DeclarativeAggregate": "eventsource.domain.aggregate",
    "DeciderAggregate": "eventsource.domain.decider",
    "handles": "eventsource.domain.decorators",
    "EventBus": "eventsource.ports",
    "BaseEventBus": "eventsource.adapters._bus.base",
    "EventHandlerFunc": "eventsource.ports",
    "AsyncEventHandler": "eventsource.ports.handlers",
    "InMemoryEventBus": "eventsource.adapters.memory.bus",
    "SubscriptionRegistry": "eventsource.adapters._bus.registry",
    "RetryPolicy": "eventsource.adapters._bus.retry",
    "RecordingEventBus": "eventsource.testing.recording",
    "EventHandler": "eventsource.ports.handlers",
    "SyncEventHandler": "eventsource.ports.handlers",
    "FlexibleEventHandler": "eventsource.ports.handlers",
    "EventSubscriber": "eventsource.ports.handlers",
    "FlexibleEventSubscriber": "eventsource.ports.handlers",
    "RedisEventBus": "eventsource.adapters.redis",
    "RedisEventBusConfig": "eventsource.adapters.redis",
    "RedisEventBusStats": "eventsource.adapters.redis",
    "RedisNotAvailableError": "eventsource.adapters.redis",
    "REDIS_AVAILABLE": "eventsource.adapters.redis",
    "RabbitMQEventBus": "eventsource.adapters.rabbitmq",
    "RabbitMQEventBusConfig": "eventsource.adapters.rabbitmq",
    "RabbitMQEventBusStats": "eventsource.adapters.rabbitmq",
    "RabbitMQNotAvailableError": "eventsource.adapters.rabbitmq",
    "RABBITMQ_AVAILABLE": "eventsource.adapters.rabbitmq",
    "KafkaEventBus": "eventsource.adapters.kafka",
    "KafkaEventBusConfig": "eventsource.adapters.kafka",
    "KafkaEventBusStats": "eventsource.adapters.kafka",
    "KafkaNotAvailableError": "eventsource.adapters.kafka",
    "KAFKA_AVAILABLE": "eventsource.adapters.kafka",
    "AggregateNotCreatedError": "eventsource.domain.exceptions",
    "AggregateNotFoundError": "eventsource.domain.exceptions",
    "CommandRejectedError": "eventsource.domain.exceptions",
    "EventNotFoundError": "eventsource.domain.exceptions",
    "EventSourceError": "eventsource.domain.exceptions",
    "EventVersionError": "eventsource.domain.exceptions",
    "HandlerDispatchError": "eventsource.domain.exceptions",
    "OptimisticLockError": "eventsource.domain.exceptions",
    "ProjectionError": "eventsource.domain.exceptions",
    "CheckpointRepository": "eventsource.ports.checkpoints",
    "SQLCheckpointRepository": "eventsource.adapters.sql",
    "InMemoryCheckpointRepository": "eventsource.adapters.memory",
    "CheckpointData": "eventsource.ports.checkpoints",
    "LagMetrics": "eventsource.ports.checkpoints",
    "DLQRepository": "eventsource.ports.dlq",
    "SQLDLQRepository": "eventsource.adapters.sql",
    "InMemoryDLQRepository": "eventsource.adapters.memory",
    "DLQEntry": "eventsource.ports.dlq",
    "DLQStats": "eventsource.ports.dlq",
    "ProjectionFailureCount": "eventsource.ports.dlq",
    "OutboxRepository": "eventsource.ports",
    "PostgreSQLOutboxRepository": "eventsource.adapters.postgresql",
    "InMemoryOutboxRepository": "eventsource.adapters.memory",
    "OutboxEntry": "eventsource.ports",
    "OutboxStats": "eventsource.ports",
    "outbox_event_data": "eventsource.ports",
    "EventSourceJSONEncoder": "eventsource.adapters.serialization",
    "Projection": "eventsource.application.projections.base",
    "CheckpointTrackingProjection": "eventsource.application.projections.base",
    "DeclarativeProjection": "eventsource.application.projections.base",
    "StoreProjection": "eventsource.application.projections.store",
    "ProjectionOptions": "eventsource.application.projections.store",
    "DatabaseProjection": "eventsource.adapters.sql.projection",
    "ReadModelProjection": "eventsource.adapters.sql.readmodel_projection",
    "ReadModel": "eventsource.ports.readmodels",
    "AggregateTypeNotSetError": "eventsource.domain.exceptions",
    "AggregateTypeMismatchError": "eventsource.domain.exceptions",
    "Snapshot": "eventsource.ports.snapshots",
    "SnapshotStore": "eventsource.ports.snapshots",
    "InMemorySnapshotStore": "eventsource.adapters.memory.snapshots",
    "SnapshotError": "eventsource.domain.exceptions",
    "SnapshotDeserializationError": "eventsource.domain.exceptions",
    "SnapshotSchemaVersionError": "eventsource.domain.exceptions",
    "SnapshotNotFoundError": "eventsource.domain.exceptions",
    "SyncEventStoreAdapter": "eventsource.adapters.sync",
    "tenant_context": "eventsource.domain.tenant_context",
    "TenantContextToken": "eventsource.domain.tenant_context",
    "get_current_tenant": "eventsource.domain.tenant_context",
    "get_required_tenant": "eventsource.domain.tenant_context",
    "set_current_tenant": "eventsource.domain.tenant_context",
    "reset_tenant_context": "eventsource.domain.tenant_context",
    "clear_tenant_context": "eventsource.domain.tenant_context",
    "tenant_scope": "eventsource.domain.tenant_context",
    "tenant_scope_sync": "eventsource.domain.tenant_context",
    "TenantDomainEvent": "eventsource.domain.tenant_events",
    "TenantContextNotSetError": "eventsource.domain.exceptions",
    "TenantContextResetError": "eventsource.domain.exceptions",
    "TenantMismatchError": "eventsource.domain.exceptions",
    "StreamId": "eventsource.domain",
    "Position": "eventsource.ports",
    "EventEnvelope": "eventsource.ports",
    "EventPublisher": "eventsource.ports",
    "AppendResult": "eventsource.ports",
    "ExpectedVersion": "eventsource.ports",
    "ReadDirection": "eventsource.ports",
    "StreamReadOptions": "eventsource.ports",
    "FeedReadOptions": "eventsource.ports",
    "CategoryReadOptions": "eventsource.ports",
    "AggregateStore": "eventsource.ports",
    "EventAppender": "eventsource.ports",
    "StreamReader": "eventsource.ports",
    "EventLookup": "eventsource.ports",
    "GlobalEventFeed": "eventsource.ports",
    "CategoryQuery": "eventsource.ports",
    "FullEventStore": "eventsource.ports",
    "collect": "eventsource.ports",
    "IntPositionCodec": "eventsource.adapters._sql.positions",
    "DuplicateEventError": "eventsource.domain.exceptions",
    "PositionDecodeError": "eventsource.ports.exceptions",
    "PositionForeignError": "eventsource.ports.exceptions",
    "AIOSQLITE_AVAILABLE": "eventsource.adapters.sqlite",
    "SQLITE_AVAILABLE": "eventsource.adapters.sqlite",
    "SQLiteEventStore": "eventsource.adapters.sqlite",
    "SQLiteOutboxRepository": "eventsource.adapters.sqlite",
}


def __getattr__(name: str) -> object:
    if name == "SQLiteOutboxRepository" and not _AIOSQLITE_AVAILABLE_FOR_ALL:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_path = _LAZY.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    module = importlib.import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals().keys()))
