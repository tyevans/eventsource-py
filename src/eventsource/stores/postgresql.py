"""
PostgreSQL event store implementation.

Production-ready event store using PostgreSQL with async support,
optimistic locking, and optional outbox pattern integration.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry, default_registry
from eventsource.exceptions import OptimisticLockError
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_DB_NAME,
    ATTR_DB_SYSTEM,
    ATTR_EVENT_COUNT,
    ATTR_EVENT_TYPE,
    ATTR_EXPECTED_VERSION,
    ATTR_FROM_VERSION,
    Tracer,
    create_tracer,
)
from eventsource.stores._compat import validate_timestamp
from eventsource.stores._type_converter import (
    DefaultTypeConverter,
    TypeConverter,
)
from eventsource.stores.interface import (
    AppendResult,
    EventStore,
    EventStream,
    ExpectedVersion,
    ReadDirection,
    ReadOptions,
    StoredEvent,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class PostgreSQLEventStore(EventStore):
    """
    PostgreSQL implementation of the event store.

    Uses async SQLAlchemy for database operations. Events are persisted
    to the 'events' table with proper indexing for performance.

    Features:
    - Optimistic locking via version checking
    - Idempotent event appending (duplicate events are skipped)
    - Optional outbox pattern integration for reliable publishing
    - Optional OpenTelemetry tracing
    - Partition-aware timestamp filtering
    - Multi-tenancy support
    - Configurable UUID field detection

    Thread-safe and supports concurrent operations across multiple
    processes/workers.

    Attributes:
        _session_factory: SQLAlchemy async session factory
        _event_registry: Registry for event type lookup during deserialization
        _outbox_enabled: Whether to write to outbox on append
        _tracer: OpenTelemetry tracer (if available)

    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        >>>
        >>> engine = create_async_engine("postgresql+asyncpg://...")
        >>> session_factory = async_sessionmaker(engine, expire_on_commit=False)
        >>> store = PostgreSQLEventStore(session_factory)
        >>>
        >>> result = await store.append_events(
        ...     aggregate_id=order_id,
        ...     aggregate_type="Order",
        ...     events=[order_created],
        ...     expected_version=0,
        ... )

        # Custom UUID field detection:
        >>> store = PostgreSQLEventStore(
        ...     session_factory,
        ...     uuid_fields={"custom_reference_id", "parent_id"},
        ...     string_id_fields={"stripe_customer_id", "external_api_id"},
        ... )

        # Strict mode (explicit only, no auto-detection):
        >>> store = PostgreSQLEventStore.with_strict_uuid_detection(
        ...     session_factory,
        ...     uuid_fields={"event_id", "aggregate_id"},
        ... )
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        event_registry: EventRegistry | None = None,
        outbox_enabled: bool = False,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        type_converter: TypeConverter | None = None,
        uuid_fields: set[str] | None = None,
        string_id_fields: set[str] | None = None,
        auto_detect_uuid: bool = True,
    ) -> None:
        """
        Initialize the PostgreSQL event store.

        Args:
            session_factory: SQLAlchemy async session factory for database access
            event_registry: Event registry for deserialization (defaults to module registry)
            outbox_enabled: If True, write events to outbox table on append
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Ignored if tracer is explicitly provided.
            type_converter: Custom TypeConverter for field type conversion.
                If not provided, a DefaultTypeConverter is created using
                the uuid_fields, string_id_fields, and auto_detect_uuid params.
            uuid_fields: Additional field names to treat as UUIDs.
                These are added to the default UUID fields.
            string_id_fields: Field names that should NOT be treated as UUIDs,
                even if they match UUID patterns (e.g., end in '_id').
            auto_detect_uuid: If True (default), fields ending in '_id' are
                treated as UUIDs unless in string_id_fields.
                Set to False for explicit control only.

        Example:
            >>> engine = create_async_engine("postgresql+asyncpg://localhost/mydb")
            >>> session_factory = async_sessionmaker(engine, expire_on_commit=False)
            >>> store = PostgreSQLEventStore(
            ...     session_factory,
            ...     outbox_enabled=True,
            ...     enable_tracing=True,
            ... )

            # Add custom UUID fields:
            >>> store = PostgreSQLEventStore(
            ...     session_factory,
            ...     uuid_fields={"custom_reference_id", "parent_id"},
            ... )

            # Exclude string ID fields from auto-detection:
            >>> store = PostgreSQLEventStore(
            ...     session_factory,
            ...     string_id_fields={"stripe_customer_id", "paypal_transaction_id"},
            ... )

            # Custom type converter:
            >>> from eventsource.stores import DefaultTypeConverter
            >>> converter = DefaultTypeConverter.strict({"event_id", "aggregate_id"})
            >>> store = PostgreSQLEventStore(session_factory, type_converter=converter)
        """
        self._session_factory = session_factory
        self._event_registry = event_registry or default_registry
        self._outbox_enabled = outbox_enabled

        # Initialize tracing via composition (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        # Initialize type converter
        if type_converter is not None:
            self._type_converter = type_converter
        else:
            self._type_converter = DefaultTypeConverter(
                uuid_fields=uuid_fields,
                string_id_fields=string_id_fields,
                auto_detect_uuid=auto_detect_uuid,
            )

    @classmethod
    def with_strict_uuid_detection(
        cls,
        session_factory: async_sessionmaker[AsyncSession],
        uuid_fields: set[str],
        *,
        event_registry: EventRegistry | None = None,
        outbox_enabled: bool = False,
        enable_tracing: bool = True,
    ) -> PostgreSQLEventStore:
        """
        Create store with explicit UUID field list only (no auto-detection).

        Use this when you want full control over which fields are UUIDs.
        Only the explicitly provided uuid_fields will be treated as UUIDs;
        no auto-detection based on field name patterns will occur.

        Args:
            session_factory: SQLAlchemy async session factory for database access
            uuid_fields: Exact set of fields to treat as UUIDs
            event_registry: Event registry for deserialization (defaults to module registry)
            outbox_enabled: If True, write events to outbox table on append
            enable_tracing: If True and OpenTelemetry is available, emit traces

        Returns:
            PostgreSQLEventStore with strict UUID detection

        Example:
            >>> store = PostgreSQLEventStore.with_strict_uuid_detection(
            ...     session_factory=session_factory,
            ...     uuid_fields={"event_id", "aggregate_id", "tenant_id"},
            ... )
        """
        return cls(
            session_factory=session_factory,
            event_registry=event_registry,
            outbox_enabled=outbox_enabled,
            enable_tracing=enable_tracing,
            type_converter=DefaultTypeConverter.strict(uuid_fields),
        )

    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        """
        Append events to an aggregate's event stream.

        Implements optimistic locking and optional outbox pattern.
        Events and outbox entries are written in the same transaction
        for exactly-once delivery semantics.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate (e.g., 'Order')
            events: Events to append
            expected_version: Expected current version (0 for new aggregates).
                Use ExpectedVersion.ANY to skip version check,
                ExpectedVersion.NO_STREAM to expect no existing stream,
                ExpectedVersion.STREAM_EXISTS to expect existing stream.

        Returns:
            AppendResult with success status and new version

        Raises:
            OptimisticLockError: If expected version doesn't match current version

        Note:
            If outbox_enabled is True, each event is also written to the
            event_outbox table for reliable asynchronous publishing.
        """
        if not events:
            return AppendResult.successful(expected_version)

        with self._tracer.span(
            "postgresql_event_store.append_events",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
                ATTR_EVENT_COUNT: len(events),
                ATTR_EXPECTED_VERSION: expected_version,
                ATTR_EVENT_TYPE: ",".join(type(e).__name__ for e in events),
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_NAME: "postgresql",
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
        async with self._session_factory() as session:
            try:
                # Get current version from database for this aggregate type
                result = await session.execute(
                    text(
                        """
                        SELECT COALESCE(MAX(version), 0) as current_version
                        FROM events
                        WHERE aggregate_id = :aggregate_id
                          AND aggregate_type = :aggregate_type
                        """
                    ),
                    {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type},
                )
                row = result.fetchone()
                current_version = row[0] if row else 0

                # Handle special ExpectedVersion constants
                if expected_version == ExpectedVersion.ANY:
                    # Skip version check
                    pass
                elif expected_version == ExpectedVersion.STREAM_EXISTS:
                    # Stream must exist (have at least one event)
                    if current_version == 0:
                        logger.debug(f"Stream does not exist for {aggregate_type}/{aggregate_id}")
                        raise OptimisticLockError(aggregate_id, expected_version, current_version)
                elif expected_version == ExpectedVersion.NO_STREAM:
                    # Stream must not exist
                    if current_version != 0:
                        logger.debug(
                            f"Stream already exists for {aggregate_type}/{aggregate_id}: "
                            f"version={current_version}"
                        )
                        raise OptimisticLockError(aggregate_id, expected_version, current_version)
                else:
                    # Specific version check (optimistic locking)
                    if current_version != expected_version:
                        logger.debug(
                            f"Version conflict for {aggregate_type}/{aggregate_id}: "
                            f"expected={expected_version}, actual={current_version}"
                        )
                        raise OptimisticLockError(aggregate_id, expected_version, current_version)

                # Append events
                new_version = current_version
                last_global_position = 0
                for event in events:
                    # Check if event already exists (idempotency)
                    exists_result = await session.execute(
                        text("SELECT 1 FROM events WHERE event_id = :event_id"),
                        {"event_id": event.event_id},
                    )
                    if exists_result.fetchone():
                        logger.debug(f"Event {event.event_id} already exists, skipping")
                        continue

                    # Increment version for this event
                    new_version += 1

                    # Serialize event to JSON
                    event_data = self._serialize_event(event)

                    # Insert event and get global position
                    insert_result = await session.execute(
                        text(
                            """
                            INSERT INTO events (
                                event_id, event_type, aggregate_type, aggregate_id,
                                tenant_id, actor_id, version, timestamp, payload, created_at
                            )
                            VALUES (
                                :event_id, :event_type, :aggregate_type, :aggregate_id,
                                :tenant_id, :actor_id, :version, :timestamp, :payload, NOW()
                            )
                            RETURNING global_position
                            """
                        ),
                        {
                            "event_id": event.event_id,
                            "event_type": event.event_type,
                            "aggregate_type": aggregate_type,
                            "aggregate_id": aggregate_id,
                            "tenant_id": str(event.tenant_id) if event.tenant_id else None,
                            "actor_id": event.actor_id,
                            "version": new_version,
                            "timestamp": event.occurred_at,
                            "payload": json.dumps(event_data),
                        },
                    )
                    inserted_row = insert_result.fetchone()
                    if inserted_row:
                        last_global_position = inserted_row[0]

                    # Write to outbox if enabled
                    if self._outbox_enabled:
                        await self._write_to_outbox(session, event, aggregate_type)

                # Commit transaction
                await session.commit()

                logger.debug(
                    f"Appended {len(events)} events to {aggregate_type}/{aggregate_id}, "
                    f"new version: {new_version}"
                )
                return AppendResult.successful(new_version, last_global_position)

            except IntegrityError as e:
                await session.rollback()
                # Check if it's a version conflict from unique constraint
                if "uq_events_aggregate_version" in str(e).lower():
                    # Another transaction beat us to it
                    result = await session.execute(
                        text(
                            """
                            SELECT COALESCE(MAX(version), 0)
                            FROM events
                            WHERE aggregate_id = :aggregate_id
                              AND aggregate_type = :aggregate_type
                            """
                        ),
                        {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type},
                    )
                    row = result.fetchone()
                    actual_version = row[0] if row else 0
                    raise OptimisticLockError(aggregate_id, expected_version, actual_version) from e
                raise

    async def _write_to_outbox(
        self,
        session: AsyncSession,
        event: DomainEvent,
        aggregate_type: str,
    ) -> None:
        """Write event to outbox table for reliable publishing."""
        outbox_id = uuid4()
        now = datetime.now(UTC)

        outbox_event_data = {
            "event_id": str(event.event_id),
            "aggregate_id": str(event.aggregate_id),
            "aggregate_type": aggregate_type,
            "tenant_id": str(event.tenant_id) if event.tenant_id else None,
            "occurred_at": event.occurred_at.isoformat(),
            "payload": event.model_dump(mode="json"),
        }

        await session.execute(
            text(
                """
                INSERT INTO event_outbox (
                    id, event_id, event_type, aggregate_id, aggregate_type,
                    tenant_id, event_data, created_at, status
                )
                VALUES (
                    :id, :event_id, :event_type, :aggregate_id, :aggregate_type,
                    :tenant_id, :event_data, :created_at, 'pending'
                )
                """
            ),
            {
                "id": outbox_id,
                "event_id": event.event_id,
                "event_type": event.event_type,
                "aggregate_id": event.aggregate_id,
                "aggregate_type": aggregate_type,
                "tenant_id": str(event.tenant_id) if event.tenant_id else None,
                "event_data": json.dumps(outbox_event_data),
                "created_at": now,
            },
        )

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

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Filter by aggregate type (optional)
            from_version: Start from this version (default: 0)
            from_timestamp: Only events after this timestamp (enables partition pruning)
            to_timestamp: Only events before this timestamp (enables partition pruning)

        Returns:
            EventStream containing the events in chronological order

        Note:
            For partitioned tables, providing timestamp filters significantly
            improves query performance by enabling partition pruning.
        """
        with self._tracer.span(
            "postgresql_event_store.get_events",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type or "any",
                ATTR_FROM_VERSION: from_version,
                ATTR_DB_SYSTEM: "postgresql",
                ATTR_DB_NAME: "postgresql",
            },
        ):
            return await self._do_get_events(
                aggregate_id, aggregate_type, from_version, from_timestamp, to_timestamp
            )

    async def _do_get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None,
        from_version: int,
        from_timestamp: datetime | None,
        to_timestamp: datetime | None,
    ) -> EventStream:
        """Internal implementation of get_events."""
        async with self._session_factory() as session:
            # Build query dynamically
            query_parts = [
                """
                SELECT
                    event_id, event_type, aggregate_type, aggregate_id,
                    tenant_id, actor_id, version, timestamp, payload
                FROM events
                WHERE aggregate_id = :aggregate_id
                  AND version > :from_version
                """
            ]
            params: dict[str, Any] = {
                "aggregate_id": aggregate_id,
                "from_version": from_version,
            }

            if aggregate_type:
                query_parts.append("AND aggregate_type = :aggregate_type")
                params["aggregate_type"] = aggregate_type

            if from_timestamp is not None:
                query_parts.append("AND timestamp >= :from_timestamp")
                params["from_timestamp"] = from_timestamp

            if to_timestamp is not None:
                query_parts.append("AND timestamp <= :to_timestamp")
                params["to_timestamp"] = to_timestamp

            query_parts.append("ORDER BY version ASC")
            query = "\n".join(query_parts)

            result = await session.execute(text(query), params)
            rows = result.fetchall()

            # Deserialize events
            events: list[DomainEvent] = []
            resolved_aggregate_type = aggregate_type or "Unknown"
            current_version = from_version

            for row in rows:
                event = self._deserialize_event(
                    event_id=row[0],
                    event_type=row[1],
                    aggregate_type=row[2],
                    aggregate_id=row[3],
                    tenant_id=row[4],
                    actor_id=row[5],
                    version=row[6],
                    timestamp=row[7],
                    payload=row[8],
                )
                events.append(event)
                resolved_aggregate_type = row[2]
                current_version = row[6]

            # If no events found and we're querying from version 0, check if aggregate exists
            if not events and from_version == 0:
                agg_result = await session.execute(
                    text(
                        """
                        SELECT aggregate_type, COALESCE(MAX(version), 0)
                        FROM events
                        WHERE aggregate_id = :aggregate_id
                        GROUP BY aggregate_type
                        LIMIT 1
                        """
                    ),
                    {"aggregate_id": aggregate_id},
                )
                agg_row = agg_result.fetchone()
                if agg_row:
                    resolved_aggregate_type = agg_row[0]
                    current_version = agg_row[1]

            return EventStream(
                aggregate_id=aggregate_id,
                aggregate_type=resolved_aggregate_type,
                events=events,
                version=current_version,
            )

    async def get_events_by_type(
        self,
        aggregate_type: str,
        tenant_id: UUID | None = None,
        from_timestamp: datetime | None = None,
    ) -> list[DomainEvent]:
        """
        Get all events for a specific aggregate type.

        Args:
            aggregate_type: Type of aggregate (e.g., 'Order')
            tenant_id: Filter by tenant ID (optional)
            from_timestamp: Only events after this datetime (optional)

        Returns:
            List of events in chronological order
        """
        validated_timestamp = validate_timestamp(from_timestamp, "from_timestamp")

        async with self._session_factory() as session:
            # Build query
            query_parts = [
                """
                SELECT
                    event_id, event_type, aggregate_type, aggregate_id,
                    tenant_id, actor_id, version, timestamp, payload
                FROM events
                WHERE aggregate_type = :aggregate_type
                """
            ]
            params: dict[str, Any] = {"aggregate_type": aggregate_type}

            if tenant_id is not None:
                query_parts.append("AND tenant_id = :tenant_id")
                params["tenant_id"] = str(tenant_id)

            if validated_timestamp is not None:
                query_parts.append("AND timestamp > :from_timestamp")
                params["from_timestamp"] = validated_timestamp

            query_parts.append("ORDER BY timestamp ASC")
            query = "\n".join(query_parts)

            result = await session.execute(text(query), params)
            rows = result.fetchall()

            # Deserialize events
            events: list[DomainEvent] = []
            for row in rows:
                event = self._deserialize_event(
                    event_id=row[0],
                    event_type=row[1],
                    aggregate_type=row[2],
                    aggregate_id=row[3],
                    tenant_id=row[4],
                    actor_id=row[5],
                    version=row[6],
                    timestamp=row[7],
                    payload=row[8],
                )
                events.append(event)

            return events

    async def event_exists(self, event_id: UUID) -> bool:
        """
        Check if an event with the given ID exists.

        Args:
            event_id: ID of the event to check

        Returns:
            True if event exists, False otherwise
        """
        async with self._session_factory() as session:
            result = await session.execute(
                text("SELECT 1 FROM events WHERE event_id = :event_id LIMIT 1"),
                {"event_id": event_id},
            )
            return result.fetchone() is not None

    async def get_stream_version(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> int:
        """
        Get the current version of an aggregate.

        More efficient than the default implementation as it doesn't
        require fetching all events.

        Args:
            aggregate_id: ID of the aggregate
            aggregate_type: Type of aggregate

        Returns:
            Current version (0 if aggregate doesn't exist)
        """
        async with self._session_factory() as session:
            result = await session.execute(
                text(
                    """
                    SELECT COALESCE(MAX(version), 0)
                    FROM events
                    WHERE aggregate_id = :aggregate_id
                      AND aggregate_type = :aggregate_type
                    """
                ),
                {"aggregate_id": aggregate_id, "aggregate_type": aggregate_type},
            )
            row = result.fetchone()
            return row[0] if row else 0

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

        # Parse stream_id (format: "aggregate_id:aggregate_type")
        parts = stream_id.rsplit(":", 1)
        if len(parts) != 2:
            # If no colon, treat entire string as aggregate_id
            aggregate_id = UUID(stream_id)
            aggregate_type = None
        else:
            aggregate_id = UUID(parts[0])
            aggregate_type = parts[1]

        async with self._session_factory() as session:
            # Build query
            query_parts = [
                """
                SELECT
                    global_position, event_id, event_type, aggregate_type, aggregate_id,
                    tenant_id, actor_id, version, timestamp, payload, created_at
                FROM events
                WHERE aggregate_id = :aggregate_id
                """
            ]
            params: dict[str, Any] = {"aggregate_id": aggregate_id}

            if aggregate_type:
                query_parts.append("AND aggregate_type = :aggregate_type")
                params["aggregate_type"] = aggregate_type

            if options.from_position > 0:
                query_parts.append("AND version > :from_version")
                params["from_version"] = options.from_position

            if options.from_timestamp:
                query_parts.append("AND timestamp >= :from_timestamp")
                params["from_timestamp"] = options.from_timestamp

            if options.to_timestamp:
                query_parts.append("AND timestamp <= :to_timestamp")
                params["to_timestamp"] = options.to_timestamp

            # Add ordering based on direction
            if options.direction == ReadDirection.BACKWARD:
                query_parts.append("ORDER BY version DESC")
            else:
                query_parts.append("ORDER BY version ASC")

            if options.limit is not None:
                query_parts.append("LIMIT :limit")
                params["limit"] = options.limit

            query = "\n".join(query_parts)

            result = await session.execute(text(query), params)
            rows = result.fetchall()

            for row in rows:
                event = self._deserialize_event(
                    event_id=row[1],
                    event_type=row[2],
                    aggregate_type=row[3],
                    aggregate_id=row[4],
                    tenant_id=row[5],
                    actor_id=row[6],
                    version=row[7],
                    timestamp=row[8],
                    payload=row[9],
                )

                yield StoredEvent(
                    event=event,
                    stream_id=stream_id,
                    stream_position=row[7],  # version
                    global_position=row[0],
                    stored_at=row[10],  # created_at
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

        async with self._session_factory() as session:
            # Build query
            query_parts = [
                """
                SELECT
                    global_position, event_id, event_type, aggregate_type, aggregate_id,
                    tenant_id, actor_id, version, timestamp, payload, created_at
                FROM events
                WHERE 1=1
                """
            ]
            params: dict[str, Any] = {}

            if options.from_position > 0:
                query_parts.append("AND global_position > :from_position")
                params["from_position"] = options.from_position

            if options.from_timestamp:
                query_parts.append("AND timestamp >= :from_timestamp")
                params["from_timestamp"] = options.from_timestamp

            if options.to_timestamp:
                query_parts.append("AND timestamp <= :to_timestamp")
                params["to_timestamp"] = options.to_timestamp

            if options.tenant_id is not None:
                query_parts.append("AND tenant_id = :tenant_id")
                params["tenant_id"] = str(options.tenant_id)

            # Add ordering based on direction
            if options.direction == ReadDirection.BACKWARD:
                query_parts.append("ORDER BY global_position DESC")
            else:
                query_parts.append("ORDER BY global_position ASC")

            if options.limit is not None:
                query_parts.append("LIMIT :limit")
                params["limit"] = options.limit

            query = "\n".join(query_parts)

            result = await session.execute(text(query), params)
            rows = result.fetchall()

            for row in rows:
                event = self._deserialize_event(
                    event_id=row[1],
                    event_type=row[2],
                    aggregate_type=row[3],
                    aggregate_id=row[4],
                    tenant_id=row[5],
                    actor_id=row[6],
                    version=row[7],
                    timestamp=row[8],
                    payload=row[9],
                )

                stream_id = f"{row[4]}:{row[3]}"
                yield StoredEvent(
                    event=event,
                    stream_id=stream_id,
                    stream_position=row[7],  # version
                    global_position=row[0],
                    stored_at=row[10],  # created_at
                )

    def _serialize_event(self, event: DomainEvent) -> dict[str, Any]:
        """Serialize a domain event to dictionary for JSON storage."""
        return event.model_dump(mode="json")

    def _deserialize_event(
        self,
        event_id: UUID,
        event_type: str,
        aggregate_type: str,
        aggregate_id: UUID,
        tenant_id: str | None,
        actor_id: str | None,
        version: int,
        timestamp: datetime,
        payload: str | dict[str, Any],
    ) -> DomainEvent:
        """
        Deserialize a domain event from database row.

        Uses the event registry to look up the correct event class.

        Args:
            event_id: Event ID
            event_type: Type of event
            aggregate_type: Type of aggregate
            aggregate_id: Aggregate ID
            tenant_id: Tenant ID (may be string from DB)
            actor_id: Actor ID
            version: Event version
            timestamp: Event timestamp
            payload: JSON payload (string or dict)

        Returns:
            Deserialized domain event

        Raises:
            EventTypeNotFoundError: If event type is not registered
        """
        # Get event class from registry
        event_class = self._event_registry.get(event_type)

        # Parse payload
        event_data = payload if isinstance(payload, dict) else json.loads(payload)

        # Convert string fields to proper types for Pydantic strict validation
        event_data = self._type_converter.convert_types(event_data)

        # Create event instance
        return event_class.model_validate(event_data, strict=False)

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Get the session factory for external use (e.g., transactions)."""
        return self._session_factory

    @property
    def event_registry(self) -> EventRegistry:
        """Get the event registry used for deserialization."""
        return self._event_registry

    @property
    def outbox_enabled(self) -> bool:
        """Check if outbox pattern is enabled."""
        return self._outbox_enabled

    async def get_global_position(self) -> int:
        """
        Get the current maximum global position in the event store.

        Returns:
            The maximum global position, or 0 if empty.
        """
        query = text("SELECT COALESCE(MAX(global_position), 0) FROM events")

        async with self._session_factory() as session:
            result = await session.execute(query)
            position = result.scalar()
            return position or 0
