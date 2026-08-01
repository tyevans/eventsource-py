"""Boundary ports (Clean Architecture output ports). Depends on domain only."""

from eventsource.ports.bus import EventPublisher, SubscribableEventBus
from eventsource.ports.checkpoints import (
    CheckpointData,
    CheckpointRepository,
    LagMetrics,
    ProjectionCheckpoints,
    SubscriptionPositions,
)
from eventsource.ports.coordination import (
    LeaderChangeCallback,
    LeaderElector,
    LeaderElectorWithLease,
)
from eventsource.ports.dlq import (
    DLQEntry,
    DLQRepository,
    DLQStats,
    ProjectionFailureCount,
)
from eventsource.ports.envelopes import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    FeedReadOptions,
    ReadDirection,
    StreamReadOptions,
)
from eventsource.ports.handlers import (
    AsyncEventHandler,
    EventHandler,
    EventHandlerFunc,
    EventSubscriber,
    FlexibleEventHandler,
    FlexibleEventSubscriber,
    SyncEventHandler,
)
from eventsource.ports.locks import (
    DistributedLock,
    LockInfo,
    LockManager,
    LockRegistry,
    migration_lock_key,
)
from eventsource.ports.outbox import (
    OutboxEntry,
    OutboxRepository,
    OutboxStats,
    outbox_event_data,
)
from eventsource.ports.positions import ExpectedVersion, Position
from eventsource.ports.snapshots import Snapshot, SnapshotStore
from eventsource.ports.store import (
    AggregateStore,
    CategoryQuery,
    EventAppender,
    EventLookup,
    FullEventStore,
    GlobalEventFeed,
    StreamReader,
    collect,
)
from eventsource.ports.subscribers import (
    BatchSubscriber,
    Subscriber,
    SyncSubscriber,
    get_subscribed_event_types,
    supports_batch_handling,
)

__all__ = [
    # Positions and versions
    "ExpectedVersion",
    "Position",
    # Envelopes and read options
    "EventEnvelope",
    "AppendResult",
    "ReadDirection",
    "StreamReadOptions",
    "FeedReadOptions",
    "CategoryReadOptions",
    # Store ports
    "AggregateStore",
    "EventAppender",
    "StreamReader",
    "EventLookup",
    "GlobalEventFeed",
    "CategoryQuery",
    "FullEventStore",
    "collect",
    # Snapshot port (TRANSITION re-home)
    "Snapshot",
    "SnapshotStore",
    # Bus port (TRANSITION re-home)
    "EventPublisher",
    "SubscribableEventBus",
    # Outbox port
    "OutboxEntry",
    "OutboxRepository",
    "OutboxStats",
    "outbox_event_data",
    # Checkpoint / DLQ ports
    "CheckpointData",
    "CheckpointRepository",
    "LagMetrics",
    "ProjectionCheckpoints",
    "SubscriptionPositions",
    "DLQEntry",
    "DLQRepository",
    "DLQStats",
    "ProjectionFailureCount",
    # Lock port
    "DistributedLock",
    "LockInfo",
    "LockManager",
    "LockRegistry",
    "migration_lock_key",
    # Handler protocols / ABCs
    "EventHandler",
    "SyncEventHandler",
    "FlexibleEventHandler",
    "EventSubscriber",
    "FlexibleEventSubscriber",
    "AsyncEventHandler",
    "EventHandlerFunc",
    # Subscriber protocols / utilities
    "Subscriber",
    "SyncSubscriber",
    "BatchSubscriber",
    "supports_batch_handling",
    "get_subscribed_event_types",
    # Leader election port
    "LeaderElector",
    "LeaderElectorWithLease",
    "LeaderChangeCallback",
]
