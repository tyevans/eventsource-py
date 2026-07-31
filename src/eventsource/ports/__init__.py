"""Boundary ports (Clean Architecture output ports). Depends on domain only."""

from eventsource.ports.bus import EventPublisher
from eventsource.ports.checkpoints import (
    CheckpointData,
    CheckpointRepository,
    LagMetrics,
    ProjectionCheckpoints,
    SubscriptionPositions,
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
from eventsource.ports.positions import ExpectedVersion, Position
from eventsource.ports.snapshots import Snapshot, SnapshotStore
from eventsource.ports.store import (
    CategoryQuery,
    EventAppender,
    EventLookup,
    FullEventStore,
    GlobalEventFeed,
    StreamReader,
    collect,
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
]
