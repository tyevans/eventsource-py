"""Envelope and read-option value objects for the store ports."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.events import DomainEvent
from eventsource.ports.positions import Position


class ReadDirection(Enum):
    """Direction for reading events from a stream or feed.

    Defines the order in which events are retrieved: forward (oldest to newest)
    or backward (newest to oldest).
    """

    FORWARD = "forward"
    BACKWARD = "backward"


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """Container for a domain event with its storage metadata.

    Carries a domain event together with the stream identity, version number,
    and optional global-feed position where the event was stored.

    Attributes:
        event: The domain event instance
        stream_id: Identity of the stream (aggregate_id + category)
        stream_version: Position in the stream (1-based event count)
        position: Opaque ordered global-feed position (None for feedless stores)
        stored_at: When the event was persisted (UTC)
    """

    event: DomainEvent
    stream_id: StreamId
    stream_version: int
    position: Position | None
    stored_at: datetime


@dataclass(frozen=True, slots=True)
class AppendResult:
    """Result of appending events to a stream.

    Carries the stream identity, the new version number after the append,
    and the global-feed position of the first appended event (None for
    feedless stores).

    Attributes:
        stream: Identity of the stream
        new_version: New version number of the stream after append
        position: Opaque global-feed position of the append (None for feedless stores)
    """

    stream: StreamId
    new_version: int
    position: Position | None


@dataclass(frozen=True, slots=True)
class StreamReadOptions:
    """Options for reading events from a specific stream.

    Configures the direction (forward or backward), version range, and limit
    for stream reads.

    Attributes:
        direction: Read direction (FORWARD or BACKWARD)
        from_version: Starting version (inclusive); None for stream start
        to_version: Ending version (inclusive); None for stream end
        limit: Maximum number of events to return; None for no limit
    """

    direction: ReadDirection = ReadDirection.FORWARD
    from_version: int | None = None
    to_version: int | None = None
    limit: int | None = None


@dataclass(frozen=True, slots=True)
class FeedReadOptions:
    """Options for reading from the global event feed.

    Configures tenant filtering and result limit for feed reads.

    Attributes:
        tenant_id: Tenant to filter by; None for all tenants
        limit: Maximum number of events to return; None for no limit
    """

    tenant_id: UUID | None = None
    limit: int | None = None


@dataclass(frozen=True, slots=True)
class CategoryReadOptions:
    """Options for reading events from a category (all aggregates of a type).

    Configures tenant filtering, timestamp filtering, and result limit
    for category reads.

    Attributes:
        tenant_id: Tenant to filter by; None for all tenants
        from_timestamp: Minimum storage time (inclusive), i.e. `EventEnvelope.stored_at`
            -- when the row was written, NOT the event's own `occurred_at`; None for no minimum
        limit: Maximum number of events to return; None for no limit
    """

    tenant_id: UUID | None = None
    from_timestamp: datetime | None = None
    limit: int | None = None
