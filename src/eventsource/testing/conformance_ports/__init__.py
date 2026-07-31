"""Port conformance suites (Clean Architecture ring: testing helpers).

Each suite is an ABC test mixin with an abstract `store` pytest fixture.
Backend adapters subclass a suite and provide `store`, yielding a fresh
adapter instance, to verify conformance to the corresponding port
contract. Suites are sqlalchemy-free: they import only from
`eventsource.ports`, `eventsource.domain`, `eventsource.events`,
`eventsource.exceptions`, and pytest/stdlib.

This package sits beside the legacy `eventsource.testing.conformance`
module and will replace it once all adapters migrate to the port suites.
"""

from eventsource.testing.conformance_ports.appender import AppenderConformance
from eventsource.testing.conformance_ports.category import CategoryQueryConformance
from eventsource.testing.conformance_ports.checkpoints import (
    CheckpointRepositoryConformance,
    ProjectionCheckpointsConformance,
    SubscriptionPositionsConformance,
)
from eventsource.testing.conformance_ports.dlq import DLQRepositoryConformance
from eventsource.testing.conformance_ports.event_lookup import EventLookupConformance
from eventsource.testing.conformance_ports.feed import GlobalFeedConformance
from eventsource.testing.conformance_ports.snapshots import SnapshotConformance
from eventsource.testing.conformance_ports.stream_reader import StreamReaderConformance

__all__ = [
    "AppenderConformance",
    "StreamReaderConformance",
    "EventLookupConformance",
    "GlobalFeedConformance",
    "CategoryQueryConformance",
    "SnapshotConformance",
    "ProjectionCheckpointsConformance",
    "SubscriptionPositionsConformance",
    "CheckpointRepositoryConformance",
    "DLQRepositoryConformance",
]
