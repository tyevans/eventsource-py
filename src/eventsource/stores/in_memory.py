"""
In-memory event store implementation.

Useful for testing and development. Not suitable for production
as all events are lost when the process terminates.
"""

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from datetime import datetime
from uuid import UUID

from eventsource.events.base import DomainEvent
from eventsource.exceptions import OptimisticLockError
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EXPECTED_VERSION,
    ATTR_FROM_VERSION,
    ATTR_POSITION,
    ATTR_STREAM_ID,
    Tracer,
    create_tracer,
)
from eventsource.stores._compat import validate_timestamp
from eventsource.stores.interface import (
    AppendResult,
    EventStore,
    EventStream,
    ExpectedVersion,
    ReadDirection,
    ReadOptions,
    StoredEvent,
)


class InMemoryEventStore(EventStore):
    """
    In-memory implementation of the event store.

    Stores events in memory using dictionaries. All events are lost
    when the process terminates. Suitable for:

    - Unit testing
    - Development environments
    - Prototyping
    - Single-process applications with ephemeral state

    NOT suitable for:
    - Production deployments requiring persistence
    - Distributed systems
    - High-volume event storage

    Thread-safety:
        Uses a lock for thread-safe operations. Safe for concurrent
        async operations within a single process. For high-concurrency
        production use, prefer PostgreSQLEventStore.

    Features:
        - OpenTelemetry tracing support via Tracer composition

    Example:
        >>> store = InMemoryEventStore()
        >>> result = await store.append_events(
        ...     aggregate_id=order_id,
        ...     aggregate_type="Order",
        ...     events=[OrderCreated(...)],
        ...     expected_version=0,
        ... )
        >>> assert result.success

    Attributes:
        _events: Dictionary mapping aggregate_id to list of events
        _aggregate_types: Dictionary mapping aggregate_id to aggregate type
        _event_ids: Set of all event IDs for idempotency checks
        _global_events: List of all events in global order
        _global_position: Counter for global position
    """

    def __init__(
        self,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize an empty in-memory event store.

        Args:
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces (default: True).
                          Ignored if tracer is explicitly provided.
        """
        # Initialize tracing via composition (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        # Store events by aggregate_id
        self._events: dict[UUID, list[DomainEvent]] = defaultdict(list)
        # Store aggregate types by aggregate_id
        self._aggregate_types: dict[UUID, str] = {}
        # Index events by event_id for idempotency checks
        self._event_ids: set[UUID] = set()
        # Global events list for read_all support
        self._global_events: list[tuple[DomainEvent, int, str]] = []  # (event, position, stream_id)
        # Global position counter
        self._global_position: int = 0
        # Lock for async concurrency safety
        self._lock: asyncio.Lock = asyncio.Lock()

    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        """
        Append events to an aggregate's event stream.

        Implements optimistic locking by checking the expected version
        against the current version before appending. Supports special
        ExpectedVersion constants:
        - ExpectedVersion.ANY (-1): Skip version check
        - ExpectedVersion.NO_STREAM (0): Expect stream to not exist
        - ExpectedVersion.STREAM_EXISTS (-2): Expect stream to exist

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate (e.g., 'Order')
            events: Events to append
            expected_version: Expected current version (0 for new aggregates)

        Returns:
            AppendResult with success status and new version

        Raises:
            OptimisticLockError: If expected version doesn't match current version

        Example:
            >>> # Create new aggregate
            >>> result = await store.append_events(
            ...     aggregate_id=uuid4(),
            ...     aggregate_type="Order",
            ...     events=[order_created],
            ...     expected_version=0,
            ... )
            >>> assert result.success
            >>> assert result.new_version == 1
        """
        if not events:
            return AppendResult.successful(expected_version)

        with self._tracer.span(
            "inmemory_event_store.append_events",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
                ATTR_EVENT_COUNT: len(events),
                ATTR_EXPECTED_VERSION: expected_version,
            },
        ):
            return await self._do_append_events(
                aggregate_id, aggregate_type, events, expected_version
            )

    async def _do_append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        """Internal implementation of append_events."""
        async with self._lock:
            # Get current version for this aggregate type
            all_events = self._events[aggregate_id]
            current_events = [e for e in all_events if e.aggregate_type == aggregate_type]
            current_version = len(current_events)

            # Handle special ExpectedVersion constants
            if expected_version == ExpectedVersion.ANY:
                # Skip version check
                pass
            elif expected_version == ExpectedVersion.STREAM_EXISTS:
                # Stream must exist (have at least one event)
                if current_version == 0:
                    raise OptimisticLockError(aggregate_id, expected_version, current_version)
            elif expected_version == ExpectedVersion.NO_STREAM:
                # Stream must not exist
                if current_version != 0:
                    raise OptimisticLockError(aggregate_id, expected_version, current_version)
            else:
                # Specific version check (optimistic locking)
                if current_version != expected_version:
                    raise OptimisticLockError(aggregate_id, expected_version, current_version)

            # Stream ID for this aggregate
            stream_id = f"{aggregate_id}:{aggregate_type}"

            # Check for duplicate events (idempotency)
            appended_count = 0
            last_global_position = self._global_position
            for event in events:
                if event.event_id in self._event_ids:
                    # Event already exists, skip it (idempotent)
                    continue

                # Increment global position
                self._global_position += 1
                last_global_position = self._global_position

                # Append event
                self._events[aggregate_id].append(event)
                self._event_ids.add(event.event_id)
                self._global_events.append((event, self._global_position, stream_id))
                appended_count += 1

            # Store aggregate type mapping
            if aggregate_id not in self._aggregate_types:
                self._aggregate_types[aggregate_id] = aggregate_type

            # Calculate new version based on events of this aggregate type
            new_events = [
                e for e in self._events[aggregate_id] if e.aggregate_type == aggregate_type
            ]
            new_version = len(new_events)

            return AppendResult.successful(new_version, last_global_position)

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

        Retrieves events for the specified aggregate, optionally filtered
        by type, version range, and timestamp range.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Filter by aggregate type (optional)
            from_version: Start from this version (default: 0 = all events)
            from_timestamp: Only get events after this timestamp (optional)
            to_timestamp: Only get events before this timestamp (optional)

        Returns:
            EventStream containing the filtered events

        Example:
            >>> stream = await store.get_events(order_id, "Order")
            >>> for event in stream.events:
            ...     aggregate.apply_event(event, is_new=False)
        """
        with self._tracer.span(
            "inmemory_event_store.get_events",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type or "any",
                ATTR_FROM_VERSION: from_version,
            },
        ):
            return await self._do_get_events(
                aggregate_id, aggregate_type, from_version, from_timestamp, to_timestamp
            )

    async def _do_get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
    ) -> EventStream:
        """Internal implementation of get_events."""
        async with self._lock:
            all_events = list(self._events[aggregate_id])
            stored_aggregate_type = self._aggregate_types.get(aggregate_id, "Unknown")

            # Filter by aggregate_type if provided
            if aggregate_type:
                all_events = [e for e in all_events if e.aggregate_type == aggregate_type]
                stored_aggregate_type = aggregate_type

            # Filter by timestamp range
            if from_timestamp:
                all_events = [e for e in all_events if e.occurred_at >= from_timestamp]
            if to_timestamp:
                all_events = [e for e in all_events if e.occurred_at <= to_timestamp]

            # Filter by version (skip first N events)
            if from_version > 0:
                all_events = all_events[from_version:]

            return EventStream(
                aggregate_id=aggregate_id,
                aggregate_type=stored_aggregate_type,
                events=all_events,
                version=len(all_events),
            )

    async def get_events_by_type(
        self,
        aggregate_type: str,
        tenant_id: UUID | None = None,
        from_timestamp: datetime | None = None,
    ) -> list[DomainEvent]:
        """
        Get all events for a specific aggregate type.

        Useful for building projections that need to process all events
        of a certain type.

        Args:
            aggregate_type: Type of aggregate (e.g., 'Order')
            tenant_id: Filter by tenant ID (optional, for multi-tenant systems)
            from_timestamp: Only get events after this datetime (optional)

        Returns:
            List of events in chronological order

        Example:
            >>> from datetime import datetime, UTC, timedelta
            >>> order_events = await store.get_events_by_type(
            ...     "Order",
            ...     tenant_id=tenant_uuid,
            ...     from_timestamp=datetime.now(UTC) - timedelta(hours=1),
            ... )
        """
        validated_timestamp = validate_timestamp(from_timestamp, "from_timestamp")

        async with self._lock:
            all_events: list[DomainEvent] = []

            # Iterate through all aggregates
            for _agg_id, events in self._events.items():
                for event in events:
                    # Filter by aggregate type
                    if event.aggregate_type != aggregate_type:
                        continue

                    # Filter by tenant_id if specified
                    if tenant_id is not None and event.tenant_id != tenant_id:
                        continue

                    # Filter by timestamp if specified
                    if validated_timestamp is not None and event.occurred_at <= validated_timestamp:
                        continue

                    all_events.append(event)

            # Sort by occurred_at timestamp (chronological order)
            all_events.sort(key=lambda e: e.occurred_at)
            return all_events

    async def event_exists(self, event_id: UUID) -> bool:
        """
        Check if an event with the given ID exists.

        Args:
            event_id: ID of the event to check

        Returns:
            True if event exists, False otherwise
        """
        async with self._lock:
            return event_id in self._event_ids

    async def get_stream_version(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> int:
        """
        Get the current version of an aggregate.

        This is an optimized implementation that doesn't require
        fetching all events.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate

        Returns:
            Current version (0 if aggregate doesn't exist)
        """
        async with self._lock:
            all_events = self._events[aggregate_id]
            current_events = [e for e in all_events if e.aggregate_type == aggregate_type]
            return len(current_events)

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
        """
        if options is None:
            options = ReadOptions()

        with self._tracer.span(
            "inmemory_event_store.read_stream",
            {
                ATTR_STREAM_ID: stream_id,
                ATTR_POSITION: options.from_position,
            },
        ):
            async for stored_event in self._do_read_stream(stream_id, options):
                yield stored_event

    async def _do_read_stream(
        self,
        stream_id: str,
        options: ReadOptions,
    ) -> AsyncIterator[StoredEvent]:
        """Internal implementation of read_stream."""
        # Parse stream_id (format: "aggregate_id:aggregate_type")
        parts = stream_id.rsplit(":", 1)
        if len(parts) != 2:
            # If no colon, treat entire string as aggregate_id
            aggregate_id = UUID(stream_id)
            aggregate_type = None
        else:
            aggregate_id = UUID(parts[0])
            aggregate_type = parts[1]

        async with self._lock:
            # Get events for this aggregate
            all_events = list(self._events[aggregate_id])

            # Filter by aggregate type if specified
            if aggregate_type:
                all_events = [e for e in all_events if e.aggregate_type == aggregate_type]

            # Apply timestamp filters
            if options.from_timestamp:
                all_events = [e for e in all_events if e.occurred_at >= options.from_timestamp]
            if options.to_timestamp:
                all_events = [e for e in all_events if e.occurred_at <= options.to_timestamp]

            # Apply from_position filter
            if options.from_position > 0:
                all_events = all_events[options.from_position :]

            total_version = len(all_events)

            # Handle direction
            if options.direction == ReadDirection.BACKWARD:
                all_events = list(reversed(all_events))

            # Apply limit
            if options.limit is not None:
                all_events = all_events[: options.limit]

            # Calculate positions based on reading direction
            events_with_positions = []
            for i, event in enumerate(all_events):
                if options.direction == ReadDirection.FORWARD:
                    position = options.from_position + i + 1
                else:
                    position = total_version - i

                # Find global position for this event
                global_pos = 0
                for evt, gpos, _sid in self._global_events:
                    if evt.event_id == event.event_id:
                        global_pos = gpos
                        break

                events_with_positions.append(
                    StoredEvent(
                        event=event,
                        stream_id=stream_id,
                        stream_position=position,
                        global_position=global_pos,
                        stored_at=event.occurred_at,
                    )
                )

        # Yield events outside the lock
        for stored_event in events_with_positions:
            yield stored_event

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
            When options.tenant_id is provided, only events for that tenant
            are returned. This is useful for tenant-specific migrations.

        Example:
            >>> async for stored_event in store.read_all():
            ...     projection.handle(stored_event.event)
            >>>
            >>> # Read all events for a specific tenant
            >>> options = ReadOptions(tenant_id=my_tenant_uuid)
            >>> async for stored_event in store.read_all(options):
            ...     migrate_event(stored_event.event)
        """
        if options is None:
            options = ReadOptions()

        with self._tracer.span(
            "inmemory_event_store.read_all",
            {
                ATTR_POSITION: options.from_position,
            },
        ):
            async for stored_event in self._do_read_all(options):
                yield stored_event

    async def _do_read_all(
        self,
        options: ReadOptions,
    ) -> AsyncIterator[StoredEvent]:
        """Internal implementation of read_all."""
        async with self._lock:
            # Get all global events
            all_global = list(self._global_events)

            # Apply from_position filter
            if options.from_position > 0:
                all_global = [
                    (event, pos, sid)
                    for event, pos, sid in all_global
                    if pos > options.from_position
                ]

            # Apply timestamp filters
            if options.from_timestamp:
                all_global = [
                    (event, pos, sid)
                    for event, pos, sid in all_global
                    if event.occurred_at >= options.from_timestamp
                ]
            if options.to_timestamp:
                all_global = [
                    (event, pos, sid)
                    for event, pos, sid in all_global
                    if event.occurred_at <= options.to_timestamp
                ]

            # Apply tenant_id filter
            if options.tenant_id is not None:
                all_global = [
                    (event, pos, sid)
                    for event, pos, sid in all_global
                    if event.tenant_id == options.tenant_id
                ]

            # Handle direction
            if options.direction == ReadDirection.BACKWARD:
                all_global = list(reversed(all_global))

            # Apply limit
            if options.limit is not None:
                all_global = all_global[: options.limit]

            # Build stored events
            stored_events = []
            for event, global_pos, stream_id in all_global:
                # Calculate stream position
                # Parse stream_id to get aggregate info
                parts = stream_id.rsplit(":", 1)
                if len(parts) == 2:
                    agg_id = UUID(parts[0])
                    agg_type = parts[1]
                    # Count events in stream up to this one
                    stream_events = [
                        e for e in self._events[agg_id] if e.aggregate_type == agg_type
                    ]
                    stream_pos = 0
                    for i, e in enumerate(stream_events):
                        if e.event_id == event.event_id:
                            stream_pos = i + 1
                            break
                else:
                    stream_pos = 0

                stored_events.append(
                    StoredEvent(
                        event=event,
                        stream_id=stream_id,
                        stream_position=stream_pos,
                        global_position=global_pos,
                        stored_at=event.occurred_at,
                    )
                )

        # Yield events outside the lock
        for stored_event in stored_events:
            yield stored_event

    # Additional methods for testing support

    async def clear(self) -> None:
        """
        Clear all events from the store.

        Useful for resetting state between tests.

        Example:
            >>> await store.clear()
            >>> assert store.get_event_count() == 0
        """
        async with self._lock:
            self._events.clear()
            self._aggregate_types.clear()
            self._event_ids.clear()
            self._global_events.clear()
            self._global_position = 0

    async def get_all_events(self) -> list[DomainEvent]:
        """
        Get all events from all aggregates.

        Useful for debugging and test assertions.

        Returns:
            List of all events in chronological order
        """
        async with self._lock:
            all_events: list[DomainEvent] = []
            for events in self._events.values():
                all_events.extend(events)
            all_events.sort(key=lambda e: e.occurred_at)
            return all_events

    async def get_event_count(self) -> int:
        """
        Get total number of events stored.

        Returns:
            Total event count across all aggregates
        """
        async with self._lock:
            return len(self._event_ids)

    async def get_aggregate_ids(self) -> list[UUID]:
        """
        Get all aggregate IDs that have events.

        Returns:
            List of aggregate IDs
        """
        async with self._lock:
            return list(self._aggregate_types.keys())

    async def get_global_position(self) -> int:
        """
        Get the current global position counter.

        Returns:
            Current global position (highest assigned position)
        """
        async with self._lock:
            return self._global_position
