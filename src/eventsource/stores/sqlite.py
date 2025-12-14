"""
SQLite event store implementation.

Lightweight event store using SQLite with async support via aiosqlite,
optimistic locking, and full EventStore interface compliance.

This implementation is suitable for:
- Development and testing environments
- Single-instance deployments
- Embedded applications
- Edge computing scenarios

For high-concurrency production workloads, consider PostgreSQLEventStore.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import aiosqlite

from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry, default_registry
from eventsource.exceptions import OptimisticLockError
from eventsource.migrations import get_schema
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

logger = logging.getLogger(__name__)


class SQLiteEventStore(EventStore):
    """
    SQLite implementation of the event store.

    Uses aiosqlite for async database operations. Events are persisted
    to the 'events' table with proper indexing for performance.

    Features:
    - Optimistic locking via version checking
    - Idempotent event appending (duplicate events are skipped)
    - WAL mode for better concurrency (optional)
    - Multi-tenancy support
    - Full EventStore interface compliance
    - OpenTelemetry tracing support via Tracer composition

    SQLite-specific adaptations:
    - UUIDs stored as TEXT (36 characters, hyphenated format)
    - Timestamps stored as TEXT in ISO 8601 format
    - JSON stored as TEXT
    - Auto-increment uses INTEGER PRIMARY KEY AUTOINCREMENT

    Attributes:
        _database: Path to SQLite file or ':memory:' for in-memory database
        _event_registry: Registry for event type lookup during deserialization
        _wal_mode: Whether WAL mode is enabled
        _busy_timeout: Timeout in ms for busy database
        _connection: The aiosqlite connection (set after connect/initialize)

    Example:
        >>> store = SQLiteEventStore(":memory:", event_registry)
        >>> async with store:
        ...     await store.initialize()
        ...     result = await store.append_events(
        ...         aggregate_id=order_id,
        ...         aggregate_type="Order",
        ...         events=[order_created],
        ...         expected_version=0,
        ...     )
    """

    def __init__(
        self,
        database: str,
        event_registry: EventRegistry | None = None,
        *,
        wal_mode: bool = True,
        busy_timeout: int = 5000,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        type_converter: TypeConverter | None = None,
        uuid_fields: set[str] | None = None,
        string_id_fields: set[str] | None = None,
        auto_detect_uuid: bool = True,
    ) -> None:
        """
        Initialize the SQLite event store.

        Args:
            database: Path to SQLite database file or ':memory:' for in-memory
            event_registry: Event registry for deserialization (defaults to module registry)
            wal_mode: If True, enable WAL mode for better concurrency (default: True)
            busy_timeout: Timeout in milliseconds when database is locked (default: 5000)
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces (default: True).
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
            >>> # In-memory database for testing
            >>> store = SQLiteEventStore(":memory:", event_registry)
            >>>
            >>> # File-based database
            >>> store = SQLiteEventStore("/path/to/events.db", event_registry)
            >>>
            >>> # Without WAL mode (for read-only or single-process scenarios)
            >>> store = SQLiteEventStore("events.db", event_registry, wal_mode=False)
            >>>
            >>> # Without tracing
            >>> store = SQLiteEventStore("events.db", event_registry, enable_tracing=False)
            >>>
            >>> # Custom UUID fields
            >>> store = SQLiteEventStore(":memory:", uuid_fields={"custom_reference_id"})
            >>>
            >>> # Custom type converter
            >>> from eventsource.stores import DefaultTypeConverter
            >>> converter = DefaultTypeConverter.strict({"event_id", "aggregate_id"})
            >>> store = SQLiteEventStore(":memory:", type_converter=converter)
        """
        self._database = database
        self._event_registry = event_registry or default_registry
        self._wal_mode = wal_mode
        self._busy_timeout = busy_timeout
        self._connection: aiosqlite.Connection | None = None

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
        database: str,
        uuid_fields: set[str],
        *,
        event_registry: EventRegistry | None = None,
        wal_mode: bool = True,
        busy_timeout: int = 5000,
        enable_tracing: bool = True,
    ) -> SQLiteEventStore:
        """
        Create store with explicit UUID field list only (no auto-detection).

        Use this when you want full control over which fields are UUIDs.
        Only the explicitly provided uuid_fields will be treated as UUIDs;
        no auto-detection based on field name patterns will occur.

        Args:
            database: Path to SQLite database file or ':memory:' for in-memory
            uuid_fields: Exact set of fields to treat as UUIDs
            event_registry: Event registry for deserialization (defaults to module registry)
            wal_mode: If True, enable WAL mode for better concurrency
            busy_timeout: Timeout in milliseconds when database is locked
            enable_tracing: If True and OpenTelemetry is available, emit traces

        Returns:
            SQLiteEventStore with strict UUID detection

        Example:
            >>> store = SQLiteEventStore.with_strict_uuid_detection(
            ...     database=":memory:",
            ...     uuid_fields={"event_id", "aggregate_id", "tenant_id"},
            ... )
        """
        return cls(
            database=database,
            event_registry=event_registry,
            wal_mode=wal_mode,
            busy_timeout=busy_timeout,
            enable_tracing=enable_tracing,
            type_converter=DefaultTypeConverter.strict(uuid_fields),
        )

    async def __aenter__(self) -> SQLiteEventStore:
        """
        Async context manager entry.

        Opens the database connection and configures SQLite settings.

        Returns:
            Self for use in async with statement

        Example:
            >>> async with SQLiteEventStore(":memory:", registry) as store:
            ...     await store.initialize()
            ...     # Use store...
        """
        await self._connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """
        Async context manager exit.

        Closes the database connection.
        """
        await self.close()

    async def _connect(self) -> None:
        """
        Open the database connection and configure settings.

        This is called automatically by __aenter__ but can also be
        called directly if not using the context manager.
        """
        if self._connection is not None:
            return

        self._connection = await aiosqlite.connect(self._database)

        # Enable foreign keys
        await self._connection.execute("PRAGMA foreign_keys = ON")

        # Set busy timeout
        await self._connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout}")

        # Enable WAL mode if requested (better concurrency)
        if self._wal_mode:
            await self._connection.execute("PRAGMA journal_mode = WAL")

        # Use row factory for easier access
        self._connection.row_factory = aiosqlite.Row

        logger.debug(
            "Connected to SQLite database: %s (wal_mode=%s, busy_timeout=%d)",
            self._database,
            self._wal_mode,
            self._busy_timeout,
        )

    async def close(self) -> None:
        """
        Close the database connection.

        Safe to call multiple times. After closing, the store cannot be
        used until a new connection is established.
        """
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            logger.debug("Closed SQLite database connection: %s", self._database)

    async def initialize(self) -> None:
        """
        Initialize the database schema.

        Creates all required tables (events, event_outbox, projection_checkpoints,
        dead_letter_queue) if they don't exist.

        This method is idempotent - safe to call multiple times.

        Raises:
            RuntimeError: If not connected to database

        Example:
            >>> async with SQLiteEventStore(":memory:", registry) as store:
            ...     await store.initialize()  # Creates tables
            ...     await store.initialize()  # Safe to call again
        """
        if self._connection is None:
            await self._connect()

        assert self._connection is not None

        # Get SQLite schema from migrations
        schema = get_schema("all", backend="sqlite")

        # Execute the schema (executescript for multiple statements)
        await self._connection.executescript(schema)
        await self._connection.commit()

        logger.info("Initialized SQLite event store schema: %s", self._database)

    def _ensure_connected(self) -> aiosqlite.Connection:
        """
        Ensure we have an active connection.

        Returns:
            The active database connection

        Raises:
            RuntimeError: If not connected
        """
        if self._connection is None:
            raise RuntimeError(
                "Not connected to database. Use 'async with store:' or call '_connect()' first."
            )
        return self._connection

    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        """
        Append events to an aggregate's event stream.

        Implements optimistic locking to prevent concurrent modifications.
        Events are appended atomically within a transaction.

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
            RuntimeError: If not connected to database

        Example:
            >>> result = await store.append_events(
            ...     aggregate_id=uuid4(),
            ...     aggregate_type="Order",
            ...     events=[OrderCreated(...)],
            ...     expected_version=0,
            ... )
            >>> if result.success:
            ...     print(f"New version: {result.new_version}")
        """
        if not events:
            return AppendResult.successful(expected_version)

        with self._tracer.span(
            "sqlite_event_store.append_events",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
                ATTR_EVENT_COUNT: len(events),
                ATTR_EXPECTED_VERSION: expected_version,
                ATTR_EVENT_TYPE: ",".join(type(e).__name__ for e in events),
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_NAME: self._database,
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
        conn = self._ensure_connected()

        try:
            # Get current version from database for this aggregate type
            cursor = await conn.execute(
                """
                SELECT COALESCE(MAX(version), 0) as current_version
                FROM events
                WHERE aggregate_id = ? AND aggregate_type = ?
                """,
                (str(aggregate_id), aggregate_type),
            )
            row = await cursor.fetchone()
            current_version = row[0] if row else 0

            # Handle special ExpectedVersion constants
            if expected_version == ExpectedVersion.ANY:
                # Skip version check
                pass
            elif expected_version == ExpectedVersion.STREAM_EXISTS:
                # Stream must exist (have at least one event)
                if current_version == 0:
                    logger.debug("Stream does not exist for %s/%s", aggregate_type, aggregate_id)
                    raise OptimisticLockError(aggregate_id, expected_version, current_version)
            elif expected_version == ExpectedVersion.NO_STREAM:
                # Stream must not exist
                if current_version != 0:
                    logger.debug(
                        "Stream already exists for %s/%s: version=%d",
                        aggregate_type,
                        aggregate_id,
                        current_version,
                    )
                    raise OptimisticLockError(aggregate_id, expected_version, current_version)
            else:
                # Specific version check (optimistic locking)
                if current_version != expected_version:
                    logger.debug(
                        "Version conflict for %s/%s: expected=%d, actual=%d",
                        aggregate_type,
                        aggregate_id,
                        expected_version,
                        current_version,
                    )
                    raise OptimisticLockError(aggregate_id, expected_version, current_version)

            # Append events
            new_version = current_version
            last_global_position = 0

            for event in events:
                # Check if event already exists (idempotency)
                cursor = await conn.execute(
                    "SELECT 1 FROM events WHERE event_id = ?",
                    (str(event.event_id),),
                )
                if await cursor.fetchone():
                    logger.debug("Event %s already exists, skipping", event.event_id)
                    continue

                # Increment version for this event
                new_version += 1

                # Serialize event to JSON
                event_data = self._serialize_event(event)
                now = datetime.now(UTC).isoformat()

                # Insert event
                cursor = await conn.execute(
                    """
                    INSERT INTO events (
                        event_id, event_type, aggregate_type, aggregate_id,
                        tenant_id, actor_id, version, timestamp, payload, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(event.event_id),
                        event.event_type,
                        aggregate_type,
                        str(aggregate_id),
                        str(event.tenant_id) if event.tenant_id else None,
                        event.actor_id,
                        new_version,
                        event.occurred_at.isoformat(),
                        json.dumps(event_data),
                        now,
                    ),
                )

                # Get the auto-increment ID (global position)
                last_global_position = cursor.lastrowid or 0

            # Commit transaction
            await conn.commit()

            logger.debug(
                "Appended %d events to %s/%s, new version: %d",
                len(events),
                aggregate_type,
                aggregate_id,
                new_version,
            )
            return AppendResult.successful(new_version, last_global_position)

        except aiosqlite.IntegrityError as e:
            await conn.rollback()
            # Check if it's a version conflict from unique constraint
            error_str = str(e).lower()
            if "unique" in error_str and ("aggregate_id" in error_str or "version" in error_str):
                # Another transaction beat us to it
                cursor = await conn.execute(
                    """
                    SELECT COALESCE(MAX(version), 0)
                    FROM events
                    WHERE aggregate_id = ? AND aggregate_type = ?
                    """,
                    (str(aggregate_id), aggregate_type),
                )
                row = await cursor.fetchone()
                actual_version = row[0] if row else 0
                raise OptimisticLockError(aggregate_id, expected_version, actual_version) from e
            raise

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
            from_timestamp: Only events after this timestamp (optional)
            to_timestamp: Only events before this timestamp (optional)

        Returns:
            EventStream containing the events in chronological order

        Raises:
            RuntimeError: If not connected to database

        Example:
            >>> stream = await store.get_events(order_id, "Order")
            >>> for event in stream.events:
            ...     aggregate.apply(event)
        """
        with self._tracer.span(
            "sqlite_event_store.get_events",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type or "any",
                ATTR_FROM_VERSION: from_version,
                ATTR_DB_SYSTEM: "sqlite",
                ATTR_DB_NAME: self._database,
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
        conn = self._ensure_connected()

        # Build query dynamically
        query_parts = [
            """
            SELECT
                event_id, event_type, aggregate_type, aggregate_id,
                tenant_id, actor_id, version, timestamp, payload
            FROM events
            WHERE aggregate_id = ?
              AND version > ?
            """
        ]
        params: list[Any] = [str(aggregate_id), from_version]

        if aggregate_type:
            query_parts.append("AND aggregate_type = ?")
            params.append(aggregate_type)

        if from_timestamp is not None:
            query_parts.append("AND timestamp >= ?")
            params.append(from_timestamp.isoformat())

        if to_timestamp is not None:
            query_parts.append("AND timestamp <= ?")
            params.append(to_timestamp.isoformat())

        query_parts.append("ORDER BY version ASC")
        query = "\n".join(query_parts)

        cursor = await conn.execute(query, params)
        rows = await cursor.fetchall()

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
            cursor = await conn.execute(
                """
                SELECT aggregate_type, COALESCE(MAX(version), 0)
                FROM events
                WHERE aggregate_id = ?
                GROUP BY aggregate_type
                LIMIT 1
                """,
                (str(aggregate_id),),
            )
            agg_row = await cursor.fetchone()
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
            from_timestamp: Only events after this timestamp (optional)

        Returns:
            List of events in chronological order

        Raises:
            RuntimeError: If not connected to database

        Example:
            >>> events = await store.get_events_by_type("Order", tenant_id=my_tenant)
        """
        conn = self._ensure_connected()

        validated_timestamp = validate_timestamp(from_timestamp, "from_timestamp")

        # Build query
        query_parts = [
            """
            SELECT
                event_id, event_type, aggregate_type, aggregate_id,
                tenant_id, actor_id, version, timestamp, payload
            FROM events
            WHERE aggregate_type = ?
            """
        ]
        params: list[Any] = [aggregate_type]

        if tenant_id is not None:
            query_parts.append("AND tenant_id = ?")
            params.append(str(tenant_id))

        if validated_timestamp is not None:
            query_parts.append("AND timestamp > ?")
            params.append(validated_timestamp.isoformat())

        query_parts.append("ORDER BY timestamp ASC")
        query = "\n".join(query_parts)

        cursor = await conn.execute(query, params)
        rows = await cursor.fetchall()

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

        Useful for idempotency checks to prevent duplicate event processing.

        Args:
            event_id: ID of the event to check

        Returns:
            True if event exists, False otherwise

        Raises:
            RuntimeError: If not connected to database

        Example:
            >>> if await store.event_exists(event.event_id):
            ...     print("Already processed")
        """
        conn = self._ensure_connected()

        cursor = await conn.execute(
            "SELECT 1 FROM events WHERE event_id = ? LIMIT 1",
            (str(event_id),),
        )
        return await cursor.fetchone() is not None

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

        Raises:
            RuntimeError: If not connected to database
        """
        conn = self._ensure_connected()

        cursor = await conn.execute(
            """
            SELECT COALESCE(MAX(version), 0)
            FROM events
            WHERE aggregate_id = ? AND aggregate_type = ?
            """,
            (str(aggregate_id), aggregate_type),
        )
        row = await cursor.fetchone()
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

        Raises:
            RuntimeError: If not connected to database

        Example:
            >>> async for stored_event in store.read_stream("order-123:Order"):
            ...     print(f"Event at position {stored_event.stream_position}")
        """
        if options is None:
            options = ReadOptions()

        conn = self._ensure_connected()

        # Parse stream_id (format: "aggregate_id:aggregate_type")
        parts = stream_id.rsplit(":", 1)
        if len(parts) != 2:
            # If no colon, treat entire string as aggregate_id
            aggregate_id = UUID(stream_id)
            aggregate_type = None
        else:
            aggregate_id = UUID(parts[0])
            aggregate_type = parts[1]

        # Build query
        query_parts = [
            """
            SELECT
                global_position, event_id, event_type, aggregate_type, aggregate_id,
                tenant_id, actor_id, version, timestamp, payload, created_at
            FROM events
            WHERE aggregate_id = ?
            """
        ]
        params: list[Any] = [str(aggregate_id)]

        if aggregate_type:
            query_parts.append("AND aggregate_type = ?")
            params.append(aggregate_type)

        if options.from_position > 0:
            query_parts.append("AND version > ?")
            params.append(options.from_position)

        if options.from_timestamp:
            query_parts.append("AND timestamp >= ?")
            params.append(options.from_timestamp.isoformat())

        if options.to_timestamp:
            query_parts.append("AND timestamp <= ?")
            params.append(options.to_timestamp.isoformat())

        # Add ordering based on direction
        if options.direction == ReadDirection.BACKWARD:
            query_parts.append("ORDER BY version DESC")
        else:
            query_parts.append("ORDER BY version ASC")

        if options.limit is not None:
            query_parts.append("LIMIT ?")
            params.append(options.limit)

        query = "\n".join(query_parts)

        cursor = await conn.execute(query, params)
        rows = await cursor.fetchall()

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

            # Parse stored_at from ISO string
            stored_at = datetime.fromisoformat(row[10].replace("Z", "+00:00"))

            yield StoredEvent(
                event=event,
                stream_id=stream_id,
                stream_position=row[7],  # version
                global_position=row[0],
                stored_at=stored_at,
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

        Raises:
            RuntimeError: If not connected to database

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

        conn = self._ensure_connected()

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
        params: list[Any] = []

        if options.from_position > 0:
            query_parts.append("AND global_position > ?")
            params.append(options.from_position)

        if options.from_timestamp:
            query_parts.append("AND timestamp >= ?")
            params.append(options.from_timestamp.isoformat())

        if options.to_timestamp:
            query_parts.append("AND timestamp <= ?")
            params.append(options.to_timestamp.isoformat())

        if options.tenant_id is not None:
            query_parts.append("AND tenant_id = ?")
            params.append(str(options.tenant_id))

        # Add ordering based on direction
        if options.direction == ReadDirection.BACKWARD:
            query_parts.append("ORDER BY global_position DESC")
        else:
            query_parts.append("ORDER BY global_position ASC")

        if options.limit is not None:
            query_parts.append("LIMIT ?")
            params.append(options.limit)

        query = "\n".join(query_parts)

        cursor = await conn.execute(query, params)
        rows = await cursor.fetchall()

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

            # Parse stored_at from ISO string
            stored_at = datetime.fromisoformat(row[10].replace("Z", "+00:00"))

            stream_id = f"{row[4]}:{row[3]}"
            yield StoredEvent(
                event=event,
                stream_id=stream_id,
                stream_position=row[7],  # version
                global_position=row[0],
                stored_at=stored_at,
            )

    def _serialize_event(self, event: DomainEvent) -> dict[str, Any]:
        """Serialize a domain event to dictionary for JSON storage."""
        return event.model_dump(mode="json")

    def _deserialize_event(
        self,
        event_id: str,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        tenant_id: str | None,
        actor_id: str | None,
        version: int,
        timestamp: str,
        payload: str,
    ) -> DomainEvent:
        """
        Deserialize a domain event from database row.

        Uses the event registry to look up the correct event class.

        Args:
            event_id: Event ID (as TEXT from SQLite)
            event_type: Type of event
            aggregate_type: Type of aggregate
            aggregate_id: Aggregate ID (as TEXT from SQLite)
            tenant_id: Tenant ID (may be None)
            actor_id: Actor ID
            version: Event version
            timestamp: Event timestamp (ISO 8601 string)
            payload: JSON payload (as TEXT from SQLite)

        Returns:
            Deserialized domain event

        Raises:
            EventTypeNotFoundError: If event type is not registered
        """
        # Get event class from registry
        event_class = self._event_registry.get(event_type)

        # Parse payload
        event_data = json.loads(payload)

        # Convert string fields to proper types for Pydantic strict validation
        event_data = self._type_converter.convert_types(event_data)

        # Create event instance
        return event_class.model_validate(event_data, strict=False)

    @property
    def database(self) -> str:
        """Get the database path."""
        return self._database

    @property
    def event_registry(self) -> EventRegistry:
        """Get the event registry used for deserialization."""
        return self._event_registry

    @property
    def is_connected(self) -> bool:
        """Check if currently connected to database."""
        return self._connection is not None

    @property
    def wal_mode(self) -> bool:
        """Check if WAL mode is enabled."""
        return self._wal_mode

    @property
    def busy_timeout(self) -> int:
        """Get the busy timeout in milliseconds."""
        return self._busy_timeout

    async def get_global_position(self) -> int:
        """
        Get the current maximum global position in the event store.

        Returns:
            The maximum global position, or 0 if empty.

        Raises:
            RuntimeError: If not connected to database
        """
        conn = self._ensure_connected()

        cursor = await conn.execute("SELECT COALESCE(MAX(global_position), 0) FROM events")
        row = await cursor.fetchone()
        return row[0] if row else 0
