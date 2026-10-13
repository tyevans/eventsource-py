"""Port conformance suites (Clean Architecture ring: testing helpers).

Each suite is an ABC test mixin that verifies a backend adapter conforms
to the corresponding port contract. Suites are sqlalchemy-free: they
import only from `eventsource.ports`, `eventsource.domain`,
`eventsource.domain.exceptions`, and pytest/stdlib.

Suites: `AppenderConformance`, `StreamReaderConformance`,
`EventLookupConformance`, `GlobalFeedConformance`, `CategoryQueryConformance`,
`EventBusConformanceSuite`, `SnapshotConformance`,
`SnapshotTypeInvalidationConformance`, `SnapshotStoreConformance`,
`SnapshotDeserializationConformance`, `ProjectionCheckpointsConformance`,
`SubscriptionPositionsConformance`, `CheckpointRepositoryConformance`,
`DLQRepositoryConformance`, `DistributedLockConformance`, `OutboxRepositoryConformance`,
`ReadModelRepositoryConformance`, `SupportsCloseConformance`,
`LeaderElectorConformance`.

All port conformance suites live cohesively in `eventsource.testing.conformance_ports`.
`eventsource.testing.conformance` is retained as a thin compatibility re-export
for `EventBusConformanceSuite`.
"""

from eventsource.testing.conformance_ports.appender import AppenderConformance
from eventsource.testing.conformance_ports.bus import EventBusConformanceSuite
from eventsource.testing.conformance_ports.category import CategoryQueryConformance
from eventsource.testing.conformance_ports.checkpoints import (
    CheckpointRepositoryConformance,
    ProjectionCheckpointsConformance,
    SubscriptionPositionsConformance,
)
from eventsource.testing.conformance_ports.coordination import LeaderElectorConformance
from eventsource.testing.conformance_ports.dlq import DLQRepositoryConformance
from eventsource.testing.conformance_ports.event_lookup import EventLookupConformance
from eventsource.testing.conformance_ports.feed import GlobalFeedConformance
from eventsource.testing.conformance_ports.lifecycle import (
    CallerOwnedResourceCase,
    SupportsCloseConformance,
)
from eventsource.testing.conformance_ports.locks import DistributedLockConformance
from eventsource.testing.conformance_ports.outbox import OutboxRepositoryConformance
from eventsource.testing.conformance_ports.readmodels import ReadModelRepositoryConformance
from eventsource.testing.conformance_ports.snapshots import (
    SnapshotConformance,
    SnapshotDeserializationConformance,
    SnapshotStoreConformance,
    SnapshotTypeInvalidationConformance,
)
from eventsource.testing.conformance_ports.stream_reader import StreamReaderConformance

__all__ = [
    "AppenderConformance",
    "StreamReaderConformance",
    "EventLookupConformance",
    "GlobalFeedConformance",
    "CategoryQueryConformance",
    "EventBusConformanceSuite",
    "SnapshotConformance",
    "SnapshotTypeInvalidationConformance",
    "SnapshotStoreConformance",
    "SnapshotDeserializationConformance",
    "ProjectionCheckpointsConformance",
    "SubscriptionPositionsConformance",
    "CheckpointRepositoryConformance",
    "DLQRepositoryConformance",
    "DistributedLockConformance",
    "OutboxRepositoryConformance",
    "ReadModelRepositoryConformance",
    "SupportsCloseConformance",
    "CallerOwnedResourceCase",
    "LeaderElectorConformance",
]
