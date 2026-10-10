"""Library exceptions for the eventsource package."""

from __future__ import annotations

from eventsource.domain.exceptions.aggregates import (
    AggregateIdMismatchError,
    AggregateNotCreatedError,
    AggregateNotFoundError,
    AggregateTypeMismatchError,
    AggregateTypeNotSetError,
)
from eventsource.domain.exceptions.base import (
    CommandRejectedError,
    EventSourceError,
    OptimisticLockError,
    ProjectionError,
    SerializationError,
)
from eventsource.domain.exceptions.events import (
    DuplicateEventError,
    DuplicateEventTypeError,
    EventBusError,
    EventNotFoundError,
    EventStoreError,
    EventTypeNotFoundError,
    EventVersionError,
    UnhandledEventError,
)
from eventsource.domain.exceptions.handlers import (
    DuplicateHandlerError,
    HandlerDispatchError,
    HandlerSignatureError,
)
from eventsource.domain.exceptions.snapshots import (
    SnapshotDeserializationError,
    SnapshotError,
    SnapshotNotFoundError,
    SnapshotSchemaVersionError,
)
from eventsource.domain.exceptions.tenant import (
    TenantContextNotSetError,
    TenantContextResetError,
    TenantMismatchError,
)

__all__ = [
    "AggregateIdMismatchError",
    "AggregateNotCreatedError",
    "AggregateNotFoundError",
    "AggregateTypeMismatchError",
    "AggregateTypeNotSetError",
    "CommandRejectedError",
    "DuplicateEventError",
    "DuplicateEventTypeError",
    "DuplicateHandlerError",
    "EventBusError",
    "EventNotFoundError",
    "EventSourceError",
    "EventStoreError",
    "EventTypeNotFoundError",
    "EventVersionError",
    "HandlerDispatchError",
    "HandlerSignatureError",
    "OptimisticLockError",
    "ProjectionError",
    "SerializationError",
    "SnapshotDeserializationError",
    "SnapshotError",
    "SnapshotNotFoundError",
    "SnapshotSchemaVersionError",
    "TenantContextNotSetError",
    "TenantContextResetError",
    "TenantMismatchError",
    "UnhandledEventError",
]
