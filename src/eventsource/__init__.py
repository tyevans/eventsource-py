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
    from eventsource.adapters._bus.base import BaseEventBus
    from eventsource.adapters._bus.registry import SubscriptionRegistry
    from eventsource.adapters._bus.retry import RetryPolicy
    from eventsource.adapters._sql.engine import create_async_engine
    from eventsource.adapters._sql.positions import IntPositionCodec
    from eventsource.adapters.kafka import (
        KAFKA_AVAILABLE,
        KafkaEventBus,
        KafkaEventBusConfig,
        KafkaEventBusStats,
        KafkaNotAvailableError,
    )
    from eventsource.adapters.memory import (
        InMemoryCheckpointRepository,
        InMemoryDLQRepository,
        InMemoryEventStore,
        InMemoryOutboxRepository,
    )
    from eventsource.adapters.memory.bus import InMemoryEventBus
    from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
    from eventsource.adapters.postgresql import (
        ASYNCPG_AVAILABLE,
        PostgreSQLEventStore,
        PostgreSQLOutboxRepository,
    )
    from eventsource.adapters.rabbitmq import (
        RABBITMQ_AVAILABLE,
        RabbitMQEventBus,
        RabbitMQEventBusConfig,
        RabbitMQEventBusStats,
        RabbitMQNotAvailableError,
    )
    from eventsource.adapters.redis import (
        REDIS_AVAILABLE,
        RedisEventBus,
        RedisEventBusConfig,
        RedisEventBusStats,
        RedisNotAvailableError,
    )
    from eventsource.adapters.sql import SQLCheckpointRepository, SQLDLQRepository
    from eventsource.adapters.sql.projection import DatabaseProjection
    from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
    from eventsource.adapters.sqlite import (
        AIOSQLITE_AVAILABLE,
        SQLiteEventStore,
        SQLiteOutboxRepository,  # noqa: F401
    )
    from eventsource.adapters.sync import SyncEventStoreAdapter
    from eventsource.application.aggregates.repository import AggregateRepository
    from eventsource.application.projections.base import (
        CheckpointTrackingProjection,
        DeclarativeProjection,
        Projection,
    )
    from eventsource.application.projections.replay import (
        ReplayFailedError,
        ReplayFailure,
        ReplayReport,
        replay,
    )
    from eventsource.application.projections.store import (
        ProjectionOptions,
        StoreProjection,
    )
    from eventsource.domain import StreamId
    from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
    from eventsource.domain.command import DomainCommand
    from eventsource.domain.decider import DeciderAggregate
    from eventsource.domain.decorators import handles
    from eventsource.domain.event import DomainEvent
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
        AggregateIdMismatchError,
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
    "__version__",
    "AggregateId",
    "EventId",
    "TenantId",
    "CorrelationId",
    "CausationId",
    "create_async_engine",
    "DomainEvent",
    "DomainCommand",
    "EventRegistry",
    "default_registry",
    "register_event",
    "get_event_class",
    "get_event_class_or_none",
    "is_event_registered",
    "list_registered_events",
    "EventTypeNotFoundError",
    "DuplicateEventTypeError",
    "InMemoryEventStore",
    "PostgreSQLEventStore",
    "ASYNCPG_AVAILABLE",
    "AggregateRoot",
    "AggregateRepository",
    "DeclarativeAggregate",
    "DeciderAggregate",
    "handles",
    "EventBus",
    "BaseEventBus",
    "EventHandlerFunc",
    "AsyncEventHandler",
    "InMemoryEventBus",
    "SubscriptionRegistry",
    "RetryPolicy",
    "RecordingEventBus",
    "EventHandler",
    "SyncEventHandler",
    "FlexibleEventHandler",
    "EventSubscriber",
    "FlexibleEventSubscriber",
    "RedisEventBus",
    "RedisEventBusConfig",
    "RedisEventBusStats",
    "RedisNotAvailableError",
    "REDIS_AVAILABLE",
    "RabbitMQEventBus",
    "RabbitMQEventBusConfig",
    "RabbitMQEventBusStats",
    "RabbitMQNotAvailableError",
    "RABBITMQ_AVAILABLE",
    "KafkaEventBus",
    "KafkaEventBusConfig",
    "KafkaEventBusStats",
    "KafkaNotAvailableError",
    "KAFKA_AVAILABLE",
    "AggregateNotCreatedError",
    "AggregateNotFoundError",
    "CommandRejectedError",
    "EventNotFoundError",
    "EventSourceError",
    "EventVersionError",
    "HandlerDispatchError",
    "OptimisticLockError",
    "ProjectionError",
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
    "Projection",
    "CheckpointTrackingProjection",
    "DeclarativeProjection",
    "StoreProjection",
    "ProjectionOptions",
    "DatabaseProjection",
    "replay",
    "ReplayFailure",
    "ReplayReport",
    "ReplayFailedError",
    "ReadModelProjection",
    "ReadModel",
    "AggregateTypeNotSetError",
    "AggregateTypeMismatchError",
    "AggregateIdMismatchError",
    "Snapshot",
    "SnapshotStore",
    "InMemorySnapshotStore",
    "SnapshotError",
    "SnapshotDeserializationError",
    "SnapshotSchemaVersionError",
    "SnapshotNotFoundError",
    "SyncEventStoreAdapter",
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
    "StreamId",
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
    "IntPositionCodec",
    "DuplicateEventError",
    "PositionDecodeError",
    "PositionForeignError",
    "AIOSQLITE_AVAILABLE",
    "SQLiteEventStore",
]

from eventsource._lazy import (
    AIOSQLITE_AVAILABLE_FOR_ALL as _AIOSQLITE_AVAILABLE_FOR_ALL,
)
from eventsource._lazy import (
    LAZY_EXPORTS as _LAZY,
)

if _AIOSQLITE_AVAILABLE_FOR_ALL:
    __all__.append("SQLiteOutboxRepository")


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
