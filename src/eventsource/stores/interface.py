"""
Event store interface and core data structures.

The event store is the source of truth in event sourcing architecture.
It persists and retrieves domain events for aggregate reconstruction.

This module provides:
- EventStream: A container for events belonging to an aggregate
- AppendResult: Result of appending events to the store
- StoredEvent: Wrapper for persisted events with position metadata
- ReadOptions: Configuration for reading events
- EventStore: Abstract base class for event store implementations
- EventPublisher: Protocol for publishing events to external systems
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Protocol
from uuid import UUID

from eventsource.events.base import DomainEvent


class ReadDirection(Enum):
    """Direction for reading events from a stream."""

    FORWARD = "forward"
    BACKWARD = "backward"


@dataclass(frozen=True)
class StoredEvent:
    """
    Wrapper for a persisted event with stream and global position metadata.

    StoredEvent adds persistence metadata to a DomainEvent, including:
    - Stream position: The position within the specific aggregate's stream
    - Global position: The position across all events in the store
    - Timestamp: When the event was stored

    This is useful for:
    - Tracking event ordering in projections
    - Implementing catch-up subscriptions
    - Debugging and auditing

    Attributes:
        event: The underlying domain event
        stream_id: Identifier for the event stream (typically aggregate_id:aggregate_type)
        stream_position: Position within the aggregate's stream (1-based)
        global_position: Position across all events in the store (1-based)
        stored_at: When the event was persisted to the store

    Example:
        >>> stored = StoredEvent(
        ...     event=order_created_event,
        ...     stream_id="order:Order",
        ...     stream_position=1,
        ...     global_position=1000,
        ...     stored_at=datetime.now(UTC),
        ... )
        >>> print(f"Event at position {stored.stream_position}")
    """

    event: DomainEvent
    stream_id: str
    stream_position: int
    global_position: int
    stored_at: datetime

    @property
    def event_id(self) -> UUID:
        """Get the event ID from the underlying event."""
        return self.event.event_id

    @property
    def event_type(self) -> str:
        """Get the event type from the underlying event."""
        return self.event.event_type

    @property
    def aggregate_id(self) -> UUID:
        """Get the aggregate ID from the underlying event."""
        return self.event.aggregate_id

    @property
    def aggregate_type(self) -> str:
        """Get the aggregate type from the underlying event."""
        return self.event.aggregate_type

    def __str__(self) -> str:
        """String representation of stored event."""
        return (
            f"StoredEvent({self.event_type}, "
            f"stream_pos={self.stream_position}, "
            f"global_pos={self.global_position})"
        )


@dataclass(frozen=True)
class ReadOptions:
    """
    Options for reading events from the event store.

    This dataclass configures how events are retrieved, including:
    - Direction (forward/backward)
    - Starting position
    - Maximum number of events to retrieve
    - Tenant filtering for multi-tenant systems

    Attributes:
        direction: Whether to read forward or backward (default: forward)
        from_position: Starting position (0 for beginning, -1 for end)
        limit: Maximum number of events to retrieve (None for no limit)
        from_timestamp: Only get events after this timestamp
        to_timestamp: Only get events before this timestamp
        tenant_id: Filter events by tenant ID (None for all tenants).
            When provided, only events belonging to the specified tenant
            are returned. Useful for tenant-specific migrations and queries.

    Example:
        >>> # Read first 100 events forward
        >>> options = ReadOptions(limit=100)
        >>>
        >>> # Read last 10 events backward
        >>> options = ReadOptions(
        ...     direction=ReadDirection.BACKWARD,
        ...     limit=10,
        ... )
        >>>
        >>> # Read events from position 50
        >>> options = ReadOptions(from_position=50, limit=100)
        >>>
        >>> # Read all events for a specific tenant
        >>> options = ReadOptions(tenant_id=my_tenant_uuid)
    """

    direction: ReadDirection = ReadDirection.FORWARD
    from_position: int = 0
    limit: int | None = None
    from_timestamp: datetime | None = None
    to_timestamp: datetime | None = None
    tenant_id: UUID | None = None

    def __post_init__(self) -> None:
        """Validate options."""
        if self.from_position < 0 and self.from_position != -1:
            raise ValueError("from_position must be >= 0 or -1 (for end)")
        if self.limit is not None and self.limit < 0:
            raise ValueError("limit must be >= 0")


@dataclass(frozen=True)
class EventStream:
    """
    Represents a stream of events for a single aggregate.

    An event stream contains all events that have been applied to an aggregate,
    in chronological order. The version represents the current version of the
    aggregate after all events have been applied.

    Attributes:
        aggregate_id: Unique identifier of the aggregate
        aggregate_type: Type name of the aggregate (e.g., 'Order')
        events: List of events in chronological order (oldest first)
        version: Current version of the aggregate (number of events applied)

    Example:
        >>> stream = await event_store.get_events(order_id, "Order")
        >>> print(f"Order has {stream.version} events")
        >>> for event in stream.events:
        ...     aggregate.apply_event(event, is_new=False)
    """

    aggregate_id: UUID
    aggregate_type: str
    events: list[DomainEvent] = field(default_factory=list)
    version: int = 0

    def __post_init__(self) -> None:
        """Validate stream consistency."""
        # Ensure events list is not None
        if self.events is None:
            object.__setattr__(self, "events", [])

    @property
    def is_empty(self) -> bool:
        """Check if the stream has no events."""
        return len(self.events) == 0

    @property
    def latest_event(self) -> DomainEvent | None:
        """Get the most recent event, or None if stream is empty."""
        return self.events[-1] if self.events else None

    @classmethod
    def empty(cls, aggregate_id: UUID, aggregate_type: str) -> "EventStream":
        """
        Create an empty event stream.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate

        Returns:
            Empty EventStream with version 0
        """
        return cls(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=[],
            version=0,
        )


@dataclass(frozen=True)
class AppendResult:
    """
    Result of appending events to the event store.

    Returned by EventStore.append_events() to indicate the outcome
    of the append operation.

    Attributes:
        success: Whether the append was successful
        new_version: The version after appending (aggregate version)
        global_position: The global position of the last appended event
        conflict: Whether there was a version conflict (optimistic lock)

    Example:
        >>> result = await event_store.append_events(...)
        >>> if result.success:
        ...     print(f"Events appended, new version: {result.new_version}")
        >>> elif result.conflict:
        ...     print("Concurrent modification detected, retry needed")
    """

    success: bool
    new_version: int
    global_position: int = 0
    conflict: bool = False

    @classmethod
    def successful(cls, new_version: int, global_position: int = 0) -> "AppendResult":
        """
        Create a successful append result.

        Args:
            new_version: The new aggregate version after appending
            global_position: The global position of the last appended event

        Returns:
            AppendResult indicating success
        """
        return cls(
            success=True,
            new_version=new_version,
            global_position=global_position,
            conflict=False,
        )

    @classmethod
    def conflicted(cls, current_version: int) -> "AppendResult":
        """
        Create a conflict result for optimistic locking failures.

        Args:
            current_version: The actual current version of the aggregate

        Returns:
            AppendResult indicating a version conflict
        """
        return cls(
            success=False,
            new_version=current_version,
            global_position=0,
            conflict=True,
        )


class ExpectedVersion:
    """
    Constants for expected version in append operations.

    These special values provide semantic meaning for common scenarios:
    - ANY: Don't check version (disable optimistic locking)
    - NO_STREAM: Expect the stream to not exist (for creating new aggregates)
    - STREAM_EXISTS: Expect the stream to exist (for updating existing aggregates)
    """

    ANY: int = -1
    NO_STREAM: int = 0
    STREAM_EXISTS: int = -2


class EventStore(ABC):
    """
    Abstract base class for event stores.

    Event stores persist and retrieve domain events, forming the
    source of truth for the entire system. All state is derived
    from the sequence of events stored here.

    Implementations must handle:
    - Atomic event appending with optimistic locking
    - Event retrieval by aggregate ID
    - Event retrieval by type for projections
    - Idempotency checks via event_exists

    Concrete implementations:
    - InMemoryEventStore: For testing and development
    - PostgreSQLEventStore: For production use

    Example:
        >>> event_store = PostgreSQLEventStore(engine)
        >>> result = await event_store.append_events(
        ...     aggregate_id=order_id,
        ...     aggregate_type="Order",
        ...     events=[order_created_event],
        ...     expected_version=0,
        ... )
        >>> if result.success:
        ...     print("Events persisted!")
    """

    @abstractmethod
    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        """
        Append events to an aggregate's event stream.

        This is the primary write operation for event sourcing. Events are
        appended atomically with optimistic locking to prevent concurrent
        modifications.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate (e.g., 'Order', 'User')
            events: Events to append (must not be empty)
            expected_version: Expected current version (for optimistic locking).
                            Use 0 for new aggregates, or use ExpectedVersion constants.

        Returns:
            AppendResult with success status and new version

        Raises:
            OptimisticLockError: If expected_version doesn't match current version
            ValueError: If events list is empty

        Example:
            >>> # For new aggregate (version 0)
            >>> result = await store.append_events(
            ...     aggregate_id=uuid4(),
            ...     aggregate_type="Order",
            ...     events=[OrderCreated(...)],
            ...     expected_version=0,
            ... )
            >>>
            >>> # For existing aggregate
            >>> result = await store.append_events(
            ...     aggregate_id=existing_id,
            ...     aggregate_type="Order",
            ...     events=[OrderShipped(...)],
            ...     expected_version=5,  # Current version is 5
            ... )
        """
        pass

    @abstractmethod
    async def get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
    ) -> EventStream:
        """
        Get all events for an aggregate.

        Retrieves the complete event history for an aggregate, which is used
        to reconstruct the aggregate's current state.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate (optional, filters by type if provided)
            from_version: Start from this version (default: 0 = all events)
            from_timestamp: Only get events after this timestamp.
                          For partitioned stores, enables partition pruning.
            to_timestamp: Only get events before this timestamp.
                        For partitioned stores, enables partition pruning.

        Returns:
            EventStream containing the aggregate's events in chronological order

        Note:
            - For aggregate reconstruction, omit timestamp filters to get full history
            - Timestamp filters are useful for:
              - Querying recent events only
              - Partition pruning in large-scale deployments
              - Point-in-time state reconstruction

        Example:
            >>> stream = await store.get_events(order_id, "Order")
            >>> aggregate.load_from_history(stream.events)
        """
        pass

    @abstractmethod
    async def get_events_by_type(
        self,
        aggregate_type: str,
        tenant_id: UUID | None = None,
        from_timestamp: datetime | None = None,
    ) -> list[DomainEvent]:
        """
        Get all events for a specific aggregate type.

        Useful for building read models and projections that need to process
        all events of a certain type.

        Args:
            aggregate_type: Type of aggregate (e.g., 'Order')
            tenant_id: Filter by tenant (optional, for multi-tenant systems)
            from_timestamp: Only get events after this datetime (optional)

        Returns:
            List of events in chronological order

        Note:
            This method can return large result sets. For production use,
            consider implementing pagination or using projections with
            checkpoint tracking.

        Example:
            >>> from datetime import datetime, UTC, timedelta
            >>> one_hour_ago = datetime.now(UTC) - timedelta(hours=1)
            >>> order_events = await store.get_events_by_type(
            ...     "Order",
            ...     tenant_id=my_tenant_id,
            ...     from_timestamp=one_hour_ago,
            ... )
        """
        pass

    @abstractmethod
    async def event_exists(self, event_id: UUID) -> bool:
        """
        Check if an event with the given ID exists.

        Useful for idempotency checks to prevent duplicate event processing.

        Args:
            event_id: ID of the event to check

        Returns:
            True if event exists, False otherwise

        Example:
            >>> if await store.event_exists(event.event_id):
            ...     print("Event already processed, skipping")
            ...     return
        """
        pass

    async def get_stream_version(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> int:
        """
        Get the current version of an aggregate.

        Default implementation fetches events and returns the version.
        Implementations may override for efficiency.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate

        Returns:
            Current version (0 if aggregate doesn't exist)
        """
        stream = await self.get_events(aggregate_id, aggregate_type)
        return stream.version

    async def read_stream(
        self,
        stream_id: str,
        options: ReadOptions | None = None,
    ) -> AsyncIterator[StoredEvent]:
        """
        Read events from a specific stream.

        This method provides an async iterator over stored events,
        which is memory-efficient for large streams.

        Args:
            stream_id: The stream identifier (format: "aggregate_id:aggregate_type")
            options: Options for reading (direction, limit, etc.)

        Yields:
            StoredEvent instances with position metadata

        Example:
            >>> async for stored_event in store.read_stream("order-123:Order"):
            ...     print(f"Event at position {stored_event.stream_position}")

        Note:
            Default implementation parses stream_id and uses get_events().
            Implementations may override for more efficient streaming.
        """
        if options is None:
            options = ReadOptions()

        # Parse stream_id (format: "aggregate_id:aggregate_type")
        parts = stream_id.rsplit(":", 1)
        if len(parts) != 2:
            # If no colon, treat entire string as aggregate_id
            aggregate_id = UUID(stream_id)
            aggregate_type = None
        else:
            aggregate_id = UUID(parts[0])
            aggregate_type = parts[1]

        # Get events using the existing method
        stream = await self.get_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            from_version=options.from_position,
            from_timestamp=options.from_timestamp,
            to_timestamp=options.to_timestamp,
        )

        events = stream.events
        if options.direction == ReadDirection.BACKWARD:
            events = list(reversed(events))

        if options.limit is not None:
            events = events[: options.limit]

        # Yield stored events with position metadata
        for i, event in enumerate(events):
            position = (
                options.from_position + i + 1
                if options.direction == ReadDirection.FORWARD
                else stream.version - i
            )
            yield StoredEvent(
                event=event,
                stream_id=stream_id,
                stream_position=position,
                global_position=0,  # Not available in default implementation
                stored_at=event.occurred_at,
            )

    async def read_all(
        self,
        options: ReadOptions | None = None,
    ) -> AsyncIterator[StoredEvent]:
        """
        Read all events across all streams.

        This method provides an async iterator over all stored events
        in global position order. Useful for building projections that
        need to process all events.

        Args:
            options: Options for reading (direction, limit, tenant_id, etc.)

        Yields:
            StoredEvent instances with global position metadata

        Note:
            This is an optional operation. Implementations may raise
            NotImplementedError if global ordering is not supported.

            When options.tenant_id is provided, only events for that tenant
            are returned. This is useful for tenant-specific migrations
            and multi-tenant event streaming.

        Example:
            >>> async for stored_event in store.read_all():
            ...     projection.handle(stored_event.event)
            >>>
            >>> # Read all events for a specific tenant
            >>> options = ReadOptions(tenant_id=my_tenant_uuid)
            >>> async for stored_event in store.read_all(options):
            ...     migrate_event(stored_event.event)
        """
        raise NotImplementedError(
            "read_all() is not implemented by default. "
            "Concrete implementations may provide this functionality."
        )
        # Make this an async generator by using yield after raise
        # This ensures the function signature is correct as AsyncIterator
        if False:  # pragma: no cover
            yield

    @abstractmethod
    async def get_global_position(self) -> int:
        """
        Get the current maximum global position in the event store.

        This is the position of the most recently stored event across all streams.
        Used by SubscriptionManager to determine catch-up completion point.

        Returns:
            The maximum global position, or 0 if the store is empty.

        Example:
            >>> max_pos = await event_store.get_global_position()
            >>> print(f"Event store has events up to position {max_pos}")
        """
        ...


class EventPublisher(Protocol):
    """
    Protocol for publishing events to external systems.

    Event publishers enable downstream consumers to react to events
    asynchronously (e.g., sending notifications, updating search indices).

    This protocol defines the contract that event bus implementations
    and other publishing mechanisms should follow.

    Example:
        >>> class NotificationPublisher:
        ...     async def publish(self, events: list[DomainEvent]) -> None:
        ...         for event in events:
        ...             await send_notification(event)
    """

    async def publish(self, events: list[DomainEvent]) -> None:
        """
        Publish events to external systems.

        Args:
            events: Events to publish

        Raises:
            Exception: If publishing fails (implementation-specific)
        """
        ...
