"""Lazy export mapping and module availability detection for PEP 562 front door."""

from __future__ import annotations


def module_installed(name: str) -> bool:
    """Check if a module is installed without importing it."""
    from importlib.util import find_spec

    try:
        return find_spec(name) is not None
    except (ImportError, ValueError):  # pragma: no cover - malformed install
        return False


AIOSQLITE_AVAILABLE_FOR_ALL = module_installed("aiosqlite")
_GROUPS: dict[str, str] = {
    "eventsource.domain.types": "AggregateId EventId TenantId CorrelationId CausationId",
    "eventsource.domain.event_registry": (
        "EventRegistry default_registry register_event get_event_class "
        "get_event_class_or_none is_event_registered list_registered_events"
    ),
    "eventsource.domain.exceptions": (
        "EventTypeNotFoundError DuplicateEventTypeError AggregateNotCreatedError "
        "AggregateNotFoundError CommandRejectedError EventNotFoundError EventSourceError "
        "EventVersionError HandlerDispatchError OptimisticLockError ProjectionError "
        "AggregateTypeNotSetError AggregateTypeMismatchError AggregateIdMismatchError "
        "SnapshotError SnapshotDeserializationError SnapshotSchemaVersionError "
        "SnapshotNotFoundError TenantContextNotSetError TenantContextResetError "
        "TenantMismatchError DuplicateEventError"
    ),
    "eventsource.ports": (
        "EventBus EventHandlerFunc OutboxRepository OutboxEntry OutboxStats "
        "outbox_event_data Position EventEnvelope EventPublisher AppendResult "
        "ExpectedVersion ReadDirection StreamReadOptions FeedReadOptions "
        "CategoryReadOptions AggregateStore EventAppender StreamReader EventLookup "
        "GlobalEventFeed CategoryQuery FullEventStore collect"
    ),
    "eventsource.ports.handlers": (
        "AsyncEventHandler EventHandler SyncEventHandler FlexibleEventHandler "
        "EventSubscriber FlexibleEventSubscriber"
    ),
    "eventsource.adapters.redis": (
        "RedisEventBus RedisEventBusConfig RedisEventBusStats "
        "RedisNotAvailableError REDIS_AVAILABLE"
    ),
    "eventsource.adapters.rabbitmq": (
        "RabbitMQEventBus RabbitMQEventBusConfig RabbitMQEventBusStats "
        "RabbitMQNotAvailableError RABBITMQ_AVAILABLE"
    ),
    "eventsource.adapters.kafka": (
        "KafkaEventBus KafkaEventBusConfig KafkaEventBusStats "
        "KafkaNotAvailableError KAFKA_AVAILABLE"
    ),
    "eventsource.domain.tenant_context": (
        "tenant_context TenantContextToken get_current_tenant get_required_tenant "
        "set_current_tenant reset_tenant_context clear_tenant_context "
        "tenant_scope tenant_scope_sync"
    ),
}
LAZY_EXPORTS: dict[str, str] = {
    **{name: mod for mod, names in _GROUPS.items() for name in names.split()},
    "create_async_engine": "eventsource.adapters._sql.engine",
    "DomainEvent": "eventsource.domain.event",
    "DomainCommand": "eventsource.domain.command",
    "InMemoryEventStore": "eventsource.adapters.memory",
    "PostgreSQLEventStore": "eventsource.adapters.postgresql",
    "ASYNCPG_AVAILABLE": "eventsource.adapters.postgresql",
    "AggregateRoot": "eventsource.domain.aggregate",
    "AggregateRepository": "eventsource.application.aggregates.repository",
    "DeclarativeAggregate": "eventsource.domain.aggregate",
    "DeciderAggregate": "eventsource.domain.decider",
    "handles": "eventsource.domain.decorators",
    "BaseEventBus": "eventsource.adapters._bus.base",
    "InMemoryEventBus": "eventsource.adapters.memory.bus",
    "SubscriptionRegistry": "eventsource.adapters._bus.registry",
    "RetryPolicy": "eventsource.adapters._bus.retry",
    "RecordingEventBus": "eventsource.testing.recording",
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
    "PostgreSQLOutboxRepository": "eventsource.adapters.postgresql",
    "InMemoryOutboxRepository": "eventsource.adapters.memory",
    "Projection": "eventsource.application.projections.base",
    "CheckpointTrackingProjection": "eventsource.application.projections.base",
    "DeclarativeProjection": "eventsource.application.projections.base",
    "replay": "eventsource.application.projections.replay",
    "ReplayFailure": "eventsource.application.projections.replay",
    "ReplayReport": "eventsource.application.projections.replay",
    "ReplayFailedError": "eventsource.application.projections.replay",
    "StoreProjection": "eventsource.application.projections.store",
    "ProjectionOptions": "eventsource.application.projections.store",
    "DatabaseProjection": "eventsource.adapters.sql.projection",
    "ReadModelProjection": "eventsource.adapters.sql.readmodel_projection",
    "ReadModel": "eventsource.ports.readmodels",
    "Snapshot": "eventsource.ports.snapshots",
    "SnapshotStore": "eventsource.ports.snapshots",
    "InMemorySnapshotStore": "eventsource.adapters.memory.snapshots",
    "SyncEventStoreAdapter": "eventsource.adapters.sync",
    "TenantDomainEvent": "eventsource.domain.tenant_events",
    "StreamId": "eventsource.domain",
    "IntPositionCodec": "eventsource.adapters._sql.positions",
    "PositionDecodeError": "eventsource.ports.exceptions",
    "PositionForeignError": "eventsource.ports.exceptions",
    "AIOSQLITE_AVAILABLE": "eventsource.adapters.sqlite",
    "SQLiteEventStore": "eventsource.adapters.sqlite",
    "SQLiteOutboxRepository": "eventsource.adapters.sqlite",
}
