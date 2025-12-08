"""
SQLite Event Store Tests.

Comprehensive tests for the SQLiteEventStore implementation covering:
- WAL mode and busy timeout configuration
- File-based and in-memory persistence
- Async context manager lifecycle
- Schema initialization
- All EventStore interface methods
- Optimistic locking and version control
- Event serialization/deserialization
- Stream reading operations
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from eventsource import DomainEvent, EventRegistry, register_event
from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.exceptions import OptimisticLockError
    from eventsource.stores.interface import (
        ExpectedVersion,
        ReadDirection,
        ReadOptions,
    )
    from eventsource.stores.sqlite import SQLiteEventStore

if TYPE_CHECKING:
    pass


# ============================================================================
# Test Events
# ============================================================================


@register_event
class SQLiteTestEvent(DomainEvent):
    """Test event for SQLite store tests."""

    event_type: str = "SQLiteTestEvent"
    aggregate_type: str = "SQLiteTestAggregate"
    name: str
    value: int = 0


@register_event
class SQLiteTestUpdated(DomainEvent):
    """Test update event for SQLite store tests."""

    event_type: str = "SQLiteTestUpdated"
    aggregate_type: str = "SQLiteTestAggregate"
    name: str | None = None
    value: int | None = None


# ============================================================================
# Configuration Tests
# ============================================================================


pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


class TestSQLiteEventStoreConfiguration:
    """Tests for SQLiteEventStore configuration options."""

    async def test_wal_mode_configuration(self) -> None:
        """Test that WAL mode can be configured."""
        registry = EventRegistry()
        registry.register(SQLiteTestEvent)

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            store = SQLiteEventStore(
                database=db_path,
                event_registry=registry,
                wal_mode=True,
                busy_timeout=5000,
            )

            async with store:
                await store.initialize()

                # Verify WAL mode is enabled
                assert store.wal_mode is True

                # Query the actual journal mode from SQLite
                conn = store._connection
                assert conn is not None
                cursor = await conn.execute("PRAGMA journal_mode")
                row = await cursor.fetchone()
                assert row[0].lower() == "wal"
        finally:
            os.unlink(db_path)
            # Clean up WAL files if they exist
            for suffix in ["-wal", "-shm"]:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(db_path + suffix)

    async def test_wal_mode_disabled(self) -> None:
        """Test that WAL mode can be disabled."""
        registry = EventRegistry()

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            store = SQLiteEventStore(
                database=db_path,
                event_registry=registry,
                wal_mode=False,
                busy_timeout=5000,
            )

            async with store:
                await store.initialize()
                assert store.wal_mode is False
        finally:
            os.unlink(db_path)

    async def test_busy_timeout_configuration(self) -> None:
        """Test that busy timeout can be configured."""
        registry = EventRegistry()

        store = SQLiteEventStore(
            database=":memory:",
            event_registry=registry,
            wal_mode=False,
            busy_timeout=10000,
        )

        async with store:
            await store.initialize()
            assert store.busy_timeout == 10000

            # Query the actual busy_timeout from SQLite
            conn = store._connection
            assert conn is not None
            cursor = await conn.execute("PRAGMA busy_timeout")
            row = await cursor.fetchone()
            assert row[0] == 10000


class TestSQLiteEventStoreFilePersistence:
    """Tests for file-based SQLite persistence."""

    async def test_file_based_persistence(self) -> None:
        """Test that events persist to file and survive restart."""
        registry = EventRegistry()
        registry.register(SQLiteTestEvent)

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            aggregate_id = uuid4()

            # First session: create events
            store1 = SQLiteEventStore(
                database=db_path,
                event_registry=registry,
                wal_mode=False,
            )
            async with store1:
                await store1.initialize()

                event = SQLiteTestEvent(
                    aggregate_id=aggregate_id,
                    aggregate_version=1,
                    name="Persisted Event",
                    value=42,
                )
                await store1.append_events(
                    aggregate_id=aggregate_id,
                    aggregate_type="SQLiteTestAggregate",
                    events=[event],
                    expected_version=0,
                )

            # Second session: verify events persisted
            store2 = SQLiteEventStore(
                database=db_path,
                event_registry=registry,
                wal_mode=False,
            )
            async with store2:
                stream = await store2.get_events(aggregate_id, "SQLiteTestAggregate")
                assert len(stream.events) == 1
                assert stream.events[0].name == "Persisted Event"
                assert stream.events[0].value == 42
        finally:
            os.unlink(db_path)


class TestSQLiteEventStoreInMemoryMode:
    """Tests for in-memory SQLite mode."""

    async def test_in_memory_mode(self) -> None:
        """Test that in-memory mode works correctly."""
        registry = EventRegistry()
        registry.register(SQLiteTestEvent)

        store = SQLiteEventStore(
            database=":memory:",
            event_registry=registry,
            wal_mode=False,
        )

        async with store:
            await store.initialize()

            aggregate_id = uuid4()
            event = SQLiteTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Memory Event",
                value=100,
            )

            result = await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTestAggregate",
                events=[event],
                expected_version=0,
            )

            assert result.success is True
            assert result.new_version == 1

    async def test_in_memory_data_lost_on_close(self) -> None:
        """Test that in-memory data is lost when store is closed."""
        registry = EventRegistry()
        registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()

        # First store session
        store1 = SQLiteEventStore(
            database=":memory:",
            event_registry=registry,
        )
        async with store1:
            await store1.initialize()
            event = SQLiteTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Will Be Lost",
                value=1,
            )
            await store1.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTestAggregate",
                events=[event],
                expected_version=0,
            )

        # Second store session (new in-memory database)
        store2 = SQLiteEventStore(
            database=":memory:",
            event_registry=registry,
        )
        async with store2:
            await store2.initialize()
            stream = await store2.get_events(aggregate_id, "SQLiteTestAggregate")
            assert len(stream.events) == 0


class TestSQLiteEventStoreContextManager:
    """Tests for async context manager behavior."""

    async def test_async_context_manager_enter_exit(self) -> None:
        """Test that context manager properly connects and disconnects."""
        registry = EventRegistry()

        store = SQLiteEventStore(":memory:", registry)

        assert store.is_connected is False

        async with store:
            assert store.is_connected is True
            await store.initialize()

        assert store.is_connected is False

    async def test_async_context_manager_exception_handling(self) -> None:
        """Test that context manager cleans up on exception."""
        registry = EventRegistry()
        store = SQLiteEventStore(":memory:", registry)

        with pytest.raises(ValueError):
            async with store:
                await store.initialize()
                assert store.is_connected is True
                raise ValueError("Test exception")

        assert store.is_connected is False

    async def test_multiple_context_manager_entries(self) -> None:
        """Test that store can be reused across context manager sessions."""
        registry = EventRegistry()
        registry.register(SQLiteTestEvent)

        # Use file-based DB for persistence across sessions
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            store = SQLiteEventStore(db_path, registry, wal_mode=False)
            aggregate_id = uuid4()

            # First session
            async with store:
                await store.initialize()
                event = SQLiteTestEvent(
                    aggregate_id=aggregate_id,
                    aggregate_version=1,
                    name="First Session",
                    value=1,
                )
                await store.append_events(
                    aggregate_id=aggregate_id,
                    aggregate_type="SQLiteTestAggregate",
                    events=[event],
                    expected_version=0,
                )

            assert store.is_connected is False

            # Second session
            async with store:
                stream = await store.get_events(aggregate_id, "SQLiteTestAggregate")
                assert len(stream.events) == 1
        finally:
            os.unlink(db_path)


class TestSQLiteEventStoreInitialize:
    """Tests for initialize() method and schema creation."""

    async def test_initialize_creates_all_tables(self) -> None:
        """Test that initialize() creates all required tables."""
        registry = EventRegistry()

        store = SQLiteEventStore(":memory:", registry)

        async with store:
            await store.initialize()

            conn = store._connection
            assert conn is not None

            # Check events table exists
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
            )
            assert await cursor.fetchone() is not None

            # Check event_outbox table exists
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='event_outbox'"
            )
            assert await cursor.fetchone() is not None

            # Check projection_checkpoints table exists
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='projection_checkpoints'"
            )
            assert await cursor.fetchone() is not None

            # Check dead_letter_queue table exists
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='dead_letter_queue'"
            )
            assert await cursor.fetchone() is not None

    async def test_initialize_is_idempotent(self) -> None:
        """Test that initialize() can be called multiple times safely."""
        registry = EventRegistry()

        store = SQLiteEventStore(":memory:", registry)

        async with store:
            await store.initialize()
            await store.initialize()  # Should not raise
            await store.initialize()  # Should not raise

            # Verify tables still exist
            conn = store._connection
            assert conn is not None
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
            )
            assert await cursor.fetchone() is not None

    async def test_initialize_connects_if_not_connected(self) -> None:
        """Test that initialize() connects if not already connected."""
        registry = EventRegistry()

        store = SQLiteEventStore(":memory:", registry)

        # Not using context manager, so not connected
        assert store.is_connected is False

        await store.initialize()

        assert store.is_connected is True

        await store.close()


# ============================================================================
# EventStore Interface Tests
# ============================================================================


class TestSQLiteEventStoreAppend:
    """Tests for event append operations."""

    async def test_append_single_event(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test appending a single event to a new aggregate."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()
        event = SQLiteTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test Item",
            value=10,
        )

        result = await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 1
        assert result.global_position > 0

    async def test_append_multiple_events(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test appending multiple events to an aggregate."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)
        sqlite_event_store.event_registry.register(SQLiteTestUpdated)

        aggregate_id = uuid4()
        events = [
            SQLiteTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Test Item",
                value=10,
            ),
            SQLiteTestUpdated(
                aggregate_id=aggregate_id,
                aggregate_version=2,
                name="Updated Item",
            ),
            SQLiteTestUpdated(
                aggregate_id=aggregate_id,
                aggregate_version=3,
                value=20,
            ),
        ]

        result = await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=events,
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 3

    async def test_append_to_existing_aggregate(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test appending events to an existing aggregate."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)
        sqlite_event_store.event_registry.register(SQLiteTestUpdated)

        aggregate_id = uuid4()

        # First append
        event1 = SQLiteTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test Item",
            value=10,
        )
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event1],
            expected_version=0,
        )

        # Second append
        event2 = SQLiteTestUpdated(
            aggregate_id=aggregate_id,
            aggregate_version=2,
            name="Updated Item",
        )
        result = await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event2],
            expected_version=1,
        )

        assert result.success is True
        assert result.new_version == 2

    async def test_append_empty_events_list(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test that appending an empty list returns success with same version."""
        aggregate_id = uuid4()

        result = await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 0


class TestSQLiteEventStoreOptimisticLocking:
    """Tests for optimistic locking behavior."""

    async def test_optimistic_lock_error_on_version_conflict(
        self, sqlite_event_store: SQLiteEventStore
    ) -> None:
        """Test that version conflicts raise OptimisticLockError."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)
        sqlite_event_store.event_registry.register(SQLiteTestUpdated)

        aggregate_id = uuid4()

        # First append
        event1 = SQLiteTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test Item",
            value=10,
        )
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event1],
            expected_version=0,
        )

        # Try to append with wrong expected version
        event2 = SQLiteTestUpdated(
            aggregate_id=aggregate_id,
            aggregate_version=2,
            name="Updated Item",
        )

        with pytest.raises(OptimisticLockError) as exc_info:
            await sqlite_event_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTestAggregate",
                events=[event2],
                expected_version=0,  # Wrong - should be 1
            )

        assert exc_info.value.aggregate_id == aggregate_id
        assert exc_info.value.expected_version == 0
        assert exc_info.value.actual_version == 1

    async def test_expected_version_any(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test that ExpectedVersion.ANY skips version check."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)
        sqlite_event_store.event_registry.register(SQLiteTestUpdated)

        aggregate_id = uuid4()

        # First append
        event1 = SQLiteTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test Item",
            value=10,
        )
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event1],
            expected_version=0,
        )

        # Append with ANY (should succeed regardless of version)
        event2 = SQLiteTestUpdated(
            aggregate_id=aggregate_id,
            aggregate_version=2,
            name="Updated Item",
        )
        result = await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event2],
            expected_version=ExpectedVersion.ANY,
        )

        assert result.success is True
        assert result.new_version == 2

    async def test_expected_version_no_stream(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test that ExpectedVersion.NO_STREAM requires stream to not exist."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()

        # Should succeed for new aggregate
        event1 = SQLiteTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test Item",
            value=10,
        )
        result = await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event1],
            expected_version=ExpectedVersion.NO_STREAM,
        )
        assert result.success is True

        # Should fail for existing aggregate
        new_agg_id = uuid4()
        event2 = SQLiteTestEvent(
            aggregate_id=new_agg_id,
            aggregate_version=1,
            name="Test Item 2",
            value=10,
        )
        await sqlite_event_store.append_events(
            aggregate_id=new_agg_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event2],
            expected_version=0,
        )

        event3 = SQLiteTestEvent(
            aggregate_id=new_agg_id,
            aggregate_version=2,
            name="Should Fail",
            value=20,
        )
        with pytest.raises(OptimisticLockError):
            await sqlite_event_store.append_events(
                aggregate_id=new_agg_id,
                aggregate_type="SQLiteTestAggregate",
                events=[event3],
                expected_version=ExpectedVersion.NO_STREAM,
            )

    async def test_expected_version_stream_exists(
        self, sqlite_event_store: SQLiteEventStore
    ) -> None:
        """Test that ExpectedVersion.STREAM_EXISTS requires stream to exist."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)
        sqlite_event_store.event_registry.register(SQLiteTestUpdated)

        aggregate_id = uuid4()

        # Should fail for new aggregate
        event1 = SQLiteTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test Item",
            value=10,
        )

        with pytest.raises(OptimisticLockError):
            await sqlite_event_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SQLiteTestAggregate",
                events=[event1],
                expected_version=ExpectedVersion.STREAM_EXISTS,
            )

        # Should succeed for existing aggregate
        new_agg_id = uuid4()
        event2 = SQLiteTestEvent(
            aggregate_id=new_agg_id,
            aggregate_version=1,
            name="Test Item 2",
            value=10,
        )
        await sqlite_event_store.append_events(
            aggregate_id=new_agg_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event2],
            expected_version=0,
        )

        event3 = SQLiteTestUpdated(
            aggregate_id=new_agg_id,
            aggregate_version=2,
            name="Updated Item",
        )
        result = await sqlite_event_store.append_events(
            aggregate_id=new_agg_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event3],
            expected_version=ExpectedVersion.STREAM_EXISTS,
        )
        assert result.success is True


class TestSQLiteEventStoreRetrieval:
    """Tests for event retrieval operations."""

    async def test_get_events_for_aggregate(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test retrieving all events for an aggregate."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)
        sqlite_event_store.event_registry.register(SQLiteTestUpdated)

        aggregate_id = uuid4()

        # Append events
        events = [
            SQLiteTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Test Item",
                value=10,
            ),
            SQLiteTestUpdated(
                aggregate_id=aggregate_id,
                aggregate_version=2,
                name="Updated Item",
            ),
        ]
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=events,
            expected_version=0,
        )

        # Retrieve events
        stream = await sqlite_event_store.get_events(
            aggregate_id,
            aggregate_type="SQLiteTestAggregate",
        )

        assert len(stream.events) == 2
        assert stream.version == 2
        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == "SQLiteTestAggregate"

        # Verify event order and types
        assert stream.events[0].event_type == "SQLiteTestEvent"
        assert stream.events[1].event_type == "SQLiteTestUpdated"

    async def test_get_events_from_version(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test retrieving events from a specific version."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)
        sqlite_event_store.event_registry.register(SQLiteTestUpdated)

        aggregate_id = uuid4()

        # Append events
        events = [
            SQLiteTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Test Item",
                value=10,
            ),
            SQLiteTestUpdated(
                aggregate_id=aggregate_id,
                aggregate_version=2,
                name="Updated 1",
            ),
            SQLiteTestUpdated(
                aggregate_id=aggregate_id,
                aggregate_version=3,
                name="Updated 2",
            ),
        ]
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=events,
            expected_version=0,
        )

        # Retrieve from version 1 (should skip first event)
        stream = await sqlite_event_store.get_events(
            aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            from_version=1,
        )

        assert len(stream.events) == 2
        assert stream.events[0].aggregate_version == 2
        assert stream.events[1].aggregate_version == 3

    async def test_get_events_empty_aggregate(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test retrieving events for non-existent aggregate."""
        aggregate_id = uuid4()

        stream = await sqlite_event_store.get_events(
            aggregate_id,
            aggregate_type="SQLiteTestAggregate",
        )

        assert len(stream.events) == 0
        assert stream.version == 0

    async def test_get_events_by_type(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test retrieving events by aggregate type."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        # Create events for multiple aggregates
        agg1 = uuid4()
        agg2 = uuid4()

        event1 = SQLiteTestEvent(
            aggregate_id=agg1,
            aggregate_version=1,
            name="Item 1",
            value=10,
        )
        event2 = SQLiteTestEvent(
            aggregate_id=agg2,
            aggregate_version=1,
            name="Item 2",
            value=20,
        )

        await sqlite_event_store.append_events(
            aggregate_id=agg1,
            aggregate_type="SQLiteTestAggregate",
            events=[event1],
            expected_version=0,
        )
        await sqlite_event_store.append_events(
            aggregate_id=agg2,
            aggregate_type="SQLiteTestAggregate",
            events=[event2],
            expected_version=0,
        )

        # Retrieve by type
        events = await sqlite_event_store.get_events_by_type("SQLiteTestAggregate")

        assert len(events) == 2
        assert all(e.aggregate_type == "SQLiteTestAggregate" for e in events)

    async def test_get_events_with_timestamp_filter(
        self, sqlite_event_store: SQLiteEventStore
    ) -> None:
        """Test retrieving events with timestamp filtering."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()

        # Create event
        event = SQLiteTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test Item",
            value=10,
        )
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event],
            expected_version=0,
        )

        # Filter with future timestamp (should return no events)
        future_time = datetime.now(UTC) + timedelta(hours=1)
        stream = await sqlite_event_store.get_events(
            aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            from_timestamp=future_time,
        )
        assert len(stream.events) == 0

        # Filter with past timestamp (should return the event)
        past_time = datetime.now(UTC) - timedelta(hours=1)
        stream = await sqlite_event_store.get_events(
            aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            from_timestamp=past_time,
        )
        assert len(stream.events) == 1


class TestSQLiteEventStoreIdempotency:
    """Tests for idempotent event handling."""

    async def test_duplicate_event_id_skipped(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test that duplicate event IDs are skipped (idempotency)."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()
        event_id = uuid4()

        # Create event with specific ID
        event1 = SQLiteTestEvent(
            event_id=event_id,
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test Item",
            value=10,
        )

        # First append
        result1 = await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event1],
            expected_version=0,
        )
        assert result1.new_version == 1

        # Try to append same event again with ANY version
        # The event should be skipped due to idempotency check
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event1],
            expected_version=ExpectedVersion.ANY,
        )

        # Version should still be 1 since duplicate was skipped
        stream = await sqlite_event_store.get_events(
            aggregate_id,
            aggregate_type="SQLiteTestAggregate",
        )
        assert len(stream.events) == 1

    async def test_event_exists_check(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test checking if an event exists."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()
        event = SQLiteTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="Test Item",
            value=10,
        )

        # Before append
        exists_before = await sqlite_event_store.event_exists(event.event_id)
        assert exists_before is False

        # After append
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=[event],
            expected_version=0,
        )

        exists_after = await sqlite_event_store.event_exists(event.event_id)
        assert exists_after is True


class TestSQLiteEventStoreStreamReading:
    """Tests for stream reading operations."""

    async def test_read_stream_forward(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test reading stream in forward direction."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()

        # Create events
        events = [
            SQLiteTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                value=i * 10,
            )
            for i in range(5)
        ]
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=events,
            expected_version=0,
        )

        # Read stream forward
        stream_id = f"{aggregate_id}:SQLiteTestAggregate"
        read_events = []
        async for stored_event in sqlite_event_store.read_stream(stream_id):
            read_events.append(stored_event)

        assert len(read_events) == 5
        # Verify forward order
        for i, stored_event in enumerate(read_events):
            assert stored_event.stream_position == i + 1

    async def test_read_stream_backward(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test reading stream in backward direction."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()

        # Create events
        events = [
            SQLiteTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                value=i * 10,
            )
            for i in range(5)
        ]
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=events,
            expected_version=0,
        )

        # Read stream backward
        stream_id = f"{aggregate_id}:SQLiteTestAggregate"
        options = ReadOptions(direction=ReadDirection.BACKWARD)
        read_events = []
        async for stored_event in sqlite_event_store.read_stream(stream_id, options):
            read_events.append(stored_event)

        assert len(read_events) == 5
        # Verify backward order
        assert read_events[0].stream_position == 5
        assert read_events[-1].stream_position == 1

    async def test_read_stream_with_limit(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test reading stream with a limit."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()

        # Create events
        events = [
            SQLiteTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                value=i * 10,
            )
            for i in range(10)
        ]
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=events,
            expected_version=0,
        )

        # Read with limit
        stream_id = f"{aggregate_id}:SQLiteTestAggregate"
        options = ReadOptions(limit=3)
        read_events = []
        async for stored_event in sqlite_event_store.read_stream(stream_id, options):
            read_events.append(stored_event)

        assert len(read_events) == 3

    async def test_read_all_events(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test reading all events across all streams."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        # Create events for multiple aggregates
        agg1, agg2 = uuid4(), uuid4()

        events1 = [
            SQLiteTestEvent(
                aggregate_id=agg1,
                aggregate_version=1,
                name="Item 1",
                value=10,
            ),
        ]
        events2 = [
            SQLiteTestEvent(
                aggregate_id=agg2,
                aggregate_version=1,
                name="Item 2",
                value=20,
            ),
        ]

        await sqlite_event_store.append_events(
            aggregate_id=agg1,
            aggregate_type="SQLiteTestAggregate",
            events=events1,
            expected_version=0,
        )
        await sqlite_event_store.append_events(
            aggregate_id=agg2,
            aggregate_type="SQLiteTestAggregate",
            events=events2,
            expected_version=0,
        )

        # Read all
        all_events = []
        async for stored_event in sqlite_event_store.read_all():
            all_events.append(stored_event)

        assert len(all_events) == 2
        # Should be in global order
        assert all_events[0].global_position < all_events[1].global_position


class TestSQLiteEventStoreVersion:
    """Tests for version management operations."""

    async def test_get_stream_version(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test getting stream version."""
        sqlite_event_store.event_registry.register(SQLiteTestEvent)

        aggregate_id = uuid4()

        # New aggregate should have version 0
        version = await sqlite_event_store.get_stream_version(
            aggregate_id,
            "SQLiteTestAggregate",
        )
        assert version == 0

        # Add events
        events = [
            SQLiteTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                value=10,
            )
            for i in range(3)
        ]
        await sqlite_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="SQLiteTestAggregate",
            events=events,
            expected_version=0,
        )

        # Version should now be 3
        version = await sqlite_event_store.get_stream_version(
            aggregate_id,
            "SQLiteTestAggregate",
        )
        assert version == 3


class TestSQLiteEventStoreProperties:
    """Tests for store properties."""

    async def test_database_property(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test database property."""
        assert sqlite_event_store.database == ":memory:"

    async def test_event_registry_property(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test event_registry property."""
        assert sqlite_event_store.event_registry is not None
        assert isinstance(sqlite_event_store.event_registry, EventRegistry)

    async def test_is_connected_property(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test is_connected property."""
        assert sqlite_event_store.is_connected is True


class TestSQLiteEventStoreErrorHandling:
    """Tests for error handling scenarios."""

    async def test_not_connected_error(self) -> None:
        """Test that operations fail when not connected."""
        registry = EventRegistry()
        registry.register(SQLiteTestEvent)

        store = SQLiteEventStore(":memory:", registry)

        # Should fail - not connected
        with pytest.raises(RuntimeError, match="Not connected"):
            await store.get_events(uuid4(), "TestAggregate")

    async def test_close_multiple_times(self, sqlite_event_store: SQLiteEventStore) -> None:
        """Test that close() can be called multiple times safely."""
        # First close
        await sqlite_event_store.close()
        assert sqlite_event_store.is_connected is False

        # Second close (should not raise)
        await sqlite_event_store.close()
        assert sqlite_event_store.is_connected is False
