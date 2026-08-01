"""Entities ring. Pure: stdlib + pydantic only.

TRANSITION: DomainEvent/EventRegistry still live in eventsource.events and
count as this ring until sub-project 3 moves them.
"""

from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.command import DomainCommand
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.exceptions import (
    AggregateNotCreatedError,
    AggregateNotFoundError,
    CheckpointError,
    CommandRejectedError,
    DuplicateEventError,
    EventBusError,
    EventNotFoundError,
    EventSourceError,
    EventStoreError,
    EventVersionError,
    HandlerDispatchError,
    LockAcquisitionError,
    LockNotHeldError,
    OptimisticLockError,
    PositionDecodeError,
    PositionForeignError,
    ProjectionError,
    SerializationError,
    SnapshotDeserializationError,
    SnapshotError,
    SnapshotNotFoundError,
    SnapshotSchemaVersionError,
    UnhandledEventError,
)
from eventsource.domain.stream_id import CATEGORY_PATTERN, StreamId
from eventsource.domain.types import (
    AggregateId,
    CausationId,
    CorrelationId,
    EventId,
    GlobalPosition,
    StreamPosition,
    TenantId,
    TState,
    Version,
)

__all__ = [
    "CATEGORY_PATTERN",
    "AggregateId",
    "AggregateNotCreatedError",
    "AggregateNotFoundError",
    "AggregateRoot",
    "CausationId",
    "CheckpointError",
    "CommandRejectedError",
    "CorrelationId",
    "DeciderAggregate",
    "DeclarativeAggregate",
    "DomainCommand",
    "DuplicateEventError",
    "EventBusError",
    "EventId",
    "EventNotFoundError",
    "EventSourceError",
    "EventStoreError",
    "EventVersionError",
    "GlobalPosition",
    "HandlerDispatchError",
    "LockAcquisitionError",
    "LockNotHeldError",
    "OptimisticLockError",
    "PositionDecodeError",
    "PositionForeignError",
    "ProjectionError",
    "SerializationError",
    "SnapshotDeserializationError",
    "SnapshotError",
    "SnapshotNotFoundError",
    "SnapshotSchemaVersionError",
    "StreamId",
    "StreamPosition",
    "TState",
    "TenantId",
    "UnhandledEventError",
    "Version",
]
