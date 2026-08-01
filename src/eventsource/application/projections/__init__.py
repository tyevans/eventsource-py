"""Projection use cases: base classes, checkpoint/DLQ operations, coordination.

DatabaseProjection is not here -- it takes an `async_sessionmaker` in its
constructor, which makes it an adapter (`eventsource.adapters.sql.projection`).
"""

from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    EventHandlerBase,
    Projection,
    SyncProjection,
    TenantFilter,
)
from eventsource.application.projections.checkpoints import (
    lag_metrics_dict,
    read_checkpoint,
    record_checkpoint,
    reset_checkpoint,
)
from eventsource.application.projections.coordinator import (
    ProjectionCoordinator,
    ProjectionRegistry,
    SubscriberRegistry,
)
from eventsource.application.projections.dlq import read_failed_events, send_to_dlq
from eventsource.domain.decorators import (
    get_handled_event_type,
    handles,
    is_event_handler,
)
from eventsource.ports.handlers import (
    EventHandler,
    EventSubscriber,
    SyncEventHandler,
)

__all__ = [
    "CheckpointTrackingProjection",
    "DeclarativeProjection",
    "EventHandler",
    "EventHandlerBase",
    "EventSubscriber",
    "Projection",
    "ProjectionCoordinator",
    "ProjectionRegistry",
    "SubscriberRegistry",
    "SyncEventHandler",
    "SyncProjection",
    "TenantFilter",
    "get_handled_event_type",
    "handles",
    "is_event_handler",
    "lag_metrics_dict",
    "read_checkpoint",
    "read_failed_events",
    "record_checkpoint",
    "reset_checkpoint",
    "send_to_dlq",
]
