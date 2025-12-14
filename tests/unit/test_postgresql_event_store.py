"""
Unit tests for the PostgreSQLEventStore implementation.

Tests cover:
- Basic CRUD operations (append, get, exists)
- Optimistic concurrency control with all ExpectedVersion constants
- Idempotency (duplicate event handling)
- Event filtering (by type, version, timestamp, tenant)
- Stream reading (forward, backward, with limits)
- read_all functionality
- Outbox pattern integration
- Event deserialization
- OpenTelemetry tracing (optional)

All database interactions are mocked using unittest.mock.
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry
from eventsource.exceptions import OptimisticLockError
from eventsource.stores.interface import (
    ExpectedVersion,
    ReadDirection,
    ReadOptions,
)
from eventsource.stores.postgresql import PostgreSQLEventStore

# --- Test Event Classes ---


class SampleEvent(DomainEvent):
    """Simple test event."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "TestAggregate"
    data: str = "test"


class OrderCreated(DomainEvent):
    """Order creation event for testing."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_name: str


class OrderUpdated(DomainEvent):
    """Order update event for testing."""

    event_type: str = "OrderUpdated"
    aggregate_type: str = "Order"
    new_status: str


class UserRegistered(DomainEvent):
    """User registration event for testing multi-type scenarios."""

    event_type: str = "UserRegistered"
    aggregate_type: str = "User"
    email: str


# --- Fixtures ---


@pytest.fixture
def event_registry() -> EventRegistry:
    """Create an event registry with test events registered."""
    registry = EventRegistry()
    registry.register(SampleEvent)
    registry.register(OrderCreated)
    registry.register(OrderUpdated)
    registry.register(UserRegistered)
    return registry


@pytest.fixture
def mock_session() -> AsyncMock:
    """Create a mock async session."""
    session = AsyncMock(spec=AsyncSession)
    return session


@pytest.fixture
def mock_session_factory(mock_session: AsyncMock) -> MagicMock:
    """Create a mock session factory that returns the mock session."""
    factory = MagicMock(spec=async_sessionmaker)

    # Create async context manager
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = mock_session
    context_manager.__aexit__.return_value = None
    factory.return_value = context_manager

    return factory


@pytest.fixture
def store(
    mock_session_factory: MagicMock,
    event_registry: EventRegistry,
) -> PostgreSQLEventStore:
    """Create a PostgreSQLEventStore with mocked session factory."""
    return PostgreSQLEventStore(
        session_factory=mock_session_factory,
        event_registry=event_registry,
        outbox_enabled=False,
        enable_tracing=False,
    )


@pytest.fixture
def store_with_outbox(
    mock_session_factory: MagicMock,
    event_registry: EventRegistry,
) -> PostgreSQLEventStore:
    """Create a PostgreSQLEventStore with outbox enabled."""
    return PostgreSQLEventStore(
        session_factory=mock_session_factory,
        event_registry=event_registry,
        outbox_enabled=True,
        enable_tracing=False,
    )


@pytest.fixture
def aggregate_id() -> UUID:
    """Create a random aggregate ID."""
    return uuid4()


@pytest.fixture
def tenant_id() -> UUID:
    """Create a random tenant ID."""
    return uuid4()


# --- Helper Functions ---


def create_mock_result(rows: list[tuple[Any, ...]]) -> MagicMock:
    """Create a mock SQLAlchemy result with the given rows."""
    result = MagicMock()
    result.fetchone.return_value = rows[0] if rows else None
    result.fetchall.return_value = rows
    return result


def create_version_result(version: int) -> MagicMock:
    """Create a mock result for version queries."""
    return create_mock_result([(version,)])


def create_event_row(
    event: DomainEvent,
    version: int = 1,
    global_id: int = 1,
) -> tuple[Any, ...]:
    """Create a mock database row for an event."""
    return (
        event.event_id,
        event.event_type,
        event.aggregate_type,
        event.aggregate_id,
        str(event.tenant_id) if event.tenant_id else None,
        event.actor_id,
        version,
        event.occurred_at,
        event.model_dump(mode="json"),
    )


def create_full_event_row(
    event: DomainEvent,
    version: int = 1,
    global_id: int = 1,
    created_at: datetime | None = None,
) -> tuple[Any, ...]:
    """Create a full mock database row with global id and created_at."""
    return (
        global_id,
        event.event_id,
        event.event_type,
        event.aggregate_type,
        event.aggregate_id,
        str(event.tenant_id) if event.tenant_id else None,
        event.actor_id,
        version,
        event.occurred_at,
        event.model_dump(mode="json"),
        created_at or event.occurred_at,
    )


# --- Basic Functionality Tests ---


class TestPostgreSQLEventStoreBasic:
    """Tests for basic append and get operations."""

    @pytest.mark.asyncio
    async def test_append_single_event(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test appending a single event to a new stream."""
        event = SampleEvent(aggregate_id=aggregate_id, data="first")

        # Mock version query returning 0 (new stream)
        mock_session.execute.side_effect = [
            create_version_result(0),  # Version check
            create_mock_result([]),  # Event exists check
            create_mock_result([(1,)]),  # Insert returning id
        ]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 1
        assert result.global_position == 1
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_append_multiple_events(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test appending multiple events at once."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]

        # Mock version query and insert operations
        mock_session.execute.side_effect = [
            create_version_result(0),  # Version check
            create_mock_result([]),  # Event 1 exists check
            create_mock_result([(1,)]),  # Insert 1 returning id
            create_mock_result([]),  # Event 2 exists check
            create_mock_result([(2,)]),  # Insert 2 returning id
            create_mock_result([]),  # Event 3 exists check
            create_mock_result([(3,)]),  # Insert 3 returning id
        ]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 3
        assert result.global_position == 3
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_append_empty_list(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test appending empty event list returns success with current version."""
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[],
            expected_version=5,
        )

        assert result.success is True
        assert result.new_version == 5
        # No database operations should be performed
        mock_session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_events_from_stream(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test getting events from an existing stream."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]

        # Create mock rows for the events
        rows = [create_event_row(event, version=i + 1) for i, event in enumerate(events)]
        mock_session.execute.return_value = create_mock_result(rows)

        stream = await store.get_events(aggregate_id, "TestAggregate")

        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == "TestAggregate"
        assert len(stream.events) == 3
        assert stream.version == 3

    @pytest.mark.asyncio
    async def test_get_events_from_empty_stream(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test getting events from a non-existent stream returns empty."""
        # First query returns no events, second query for aggregate type returns nothing
        mock_session.execute.side_effect = [
            create_mock_result([]),  # No events
            create_mock_result([]),  # No aggregate type found
        ]

        stream = await store.get_events(aggregate_id, "TestAggregate")

        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == "TestAggregate"
        assert len(stream.events) == 0
        assert stream.version == 0
        assert stream.is_empty is True

    @pytest.mark.asyncio
    async def test_event_exists(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test checking if an event exists."""
        event_id = uuid4()

        # First call: event exists
        mock_session.execute.return_value = create_mock_result([(1,)])
        assert await store.event_exists(event_id) is True

        # Second call: event doesn't exist
        mock_session.execute.return_value = create_mock_result([])
        assert await store.event_exists(uuid4()) is False


# --- Optimistic Concurrency Tests ---


class TestOptimisticConcurrency:
    """Tests for optimistic locking with various ExpectedVersion values."""

    @pytest.mark.asyncio
    async def test_version_conflict_raises_error(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that version mismatch raises OptimisticLockError."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        # Current version is 5, but we expect 0
        mock_session.execute.return_value = create_version_result(5)

        with pytest.raises(OptimisticLockError) as exc_info:
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event],
                expected_version=0,  # Wrong version
            )

        assert exc_info.value.aggregate_id == aggregate_id
        assert exc_info.value.expected_version == 0
        assert exc_info.value.actual_version == 5

    @pytest.mark.asyncio
    async def test_expected_version_any(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test ExpectedVersion.ANY skips version check."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        # Current version is 5
        mock_session.execute.side_effect = [
            create_version_result(5),  # Version check
            create_mock_result([]),  # Event exists check
            create_mock_result([(6,)]),  # Insert returning id
        ]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=ExpectedVersion.ANY,
        )

        assert result.success is True
        assert result.new_version == 6

    @pytest.mark.asyncio
    async def test_expected_version_no_stream_success(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test NO_STREAM succeeds when stream doesn't exist."""
        event = SampleEvent(aggregate_id=aggregate_id, data="first")

        mock_session.execute.side_effect = [
            create_version_result(0),  # Stream doesn't exist
            create_mock_result([]),  # Event exists check
            create_mock_result([(1,)]),  # Insert returning id
        ]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=ExpectedVersion.NO_STREAM,
        )

        assert result.success is True
        assert result.new_version == 1

    @pytest.mark.asyncio
    async def test_expected_version_no_stream_fails_when_exists(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test NO_STREAM fails when stream already exists."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        # Stream exists with version 3
        mock_session.execute.return_value = create_version_result(3)

        with pytest.raises(OptimisticLockError):
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event],
                expected_version=ExpectedVersion.NO_STREAM,
            )

    @pytest.mark.asyncio
    async def test_expected_version_stream_exists_success(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test STREAM_EXISTS succeeds when stream has events."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        mock_session.execute.side_effect = [
            create_version_result(3),  # Stream exists
            create_mock_result([]),  # Event exists check
            create_mock_result([(4,)]),  # Insert returning id
        ]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=ExpectedVersion.STREAM_EXISTS,
        )

        assert result.success is True
        assert result.new_version == 4

    @pytest.mark.asyncio
    async def test_expected_version_stream_exists_fails_when_empty(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test STREAM_EXISTS fails when stream is empty."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        # Stream doesn't exist (version 0)
        mock_session.execute.return_value = create_version_result(0)

        with pytest.raises(OptimisticLockError):
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event],
                expected_version=ExpectedVersion.STREAM_EXISTS,
            )

    @pytest.mark.asyncio
    async def test_integrity_error_converts_to_optimistic_lock_error(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that IntegrityError on unique constraint becomes OptimisticLockError."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        # First execute returns version 0
        # Second execute (event exists check) returns nothing
        # Third execute (insert) raises IntegrityError
        # Fourth execute (re-check version) returns 5
        mock_session.execute.side_effect = [
            create_version_result(0),  # Version check
            create_mock_result([]),  # Event exists check
            IntegrityError(
                "uq_events_aggregate_version",
                params={},
                orig=Exception("duplicate key"),
            ),
            create_version_result(5),  # Re-check version after rollback
        ]

        with pytest.raises(OptimisticLockError) as exc_info:
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event],
                expected_version=0,
            )

        assert exc_info.value.actual_version == 5
        mock_session.rollback.assert_called_once()


# --- Idempotency Tests ---


class TestIdempotency:
    """Tests for duplicate event handling (idempotency)."""

    @pytest.mark.asyncio
    async def test_duplicate_event_skipped(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that duplicate events are silently skipped."""
        event = SampleEvent(aggregate_id=aggregate_id, data="unique")

        # Event already exists in database
        mock_session.execute.side_effect = [
            create_version_result(1),  # Version check
            create_mock_result([(1,)]),  # Event exists check (found!)
        ]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=ExpectedVersion.ANY,
        )

        # Should succeed but version stays the same
        assert result.success is True
        assert result.new_version == 1  # No new events added

    @pytest.mark.asyncio
    async def test_mixed_new_and_duplicate_events(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test batch with mix of new and duplicate events."""
        event1 = SampleEvent(aggregate_id=aggregate_id, data="existing")
        event2 = SampleEvent(aggregate_id=aggregate_id, data="new")

        mock_session.execute.side_effect = [
            create_version_result(1),  # Version check
            create_mock_result([(1,)]),  # Event 1 exists (duplicate)
            create_mock_result([]),  # Event 2 exists check (not found)
            create_mock_result([(2,)]),  # Insert event 2
        ]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event1, event2],
            expected_version=ExpectedVersion.ANY,
        )

        assert result.success is True
        assert result.new_version == 2  # Only one new event added


# --- Event Filtering Tests ---


class TestEventFiltering:
    """Tests for filtering events by various criteria."""

    @pytest.mark.asyncio
    async def test_get_events_by_aggregate_type(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test filtering by aggregate type."""
        agg_id = uuid4()
        order_event = OrderCreated(aggregate_id=agg_id, customer_name="John")

        mock_session.execute.return_value = create_mock_result(
            [create_event_row(order_event, version=1)]
        )

        events = await store.get_events_by_type("Order")

        assert len(events) == 1
        assert events[0].event_type == "OrderCreated"

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_tenant_filter(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        tenant_id: UUID,
    ) -> None:
        """Test filtering by aggregate type and tenant."""
        agg_id = uuid4()
        event = SampleEvent(aggregate_id=agg_id, data="with_tenant", tenant_id=tenant_id)

        mock_session.execute.return_value = create_mock_result([create_event_row(event, version=1)])

        events = await store.get_events_by_type("TestAggregate", tenant_id=tenant_id)

        assert len(events) == 1
        assert events[0].tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_get_events_from_version(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test getting events from a specific version."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        # Return only events after version 2
        rows = [create_event_row(events[2], version=3)]
        mock_session.execute.return_value = create_mock_result(rows)

        stream = await store.get_events(aggregate_id, "TestAggregate", from_version=2)

        assert len(stream.events) == 1
        assert stream.version == 3

    @pytest.mark.asyncio
    async def test_get_events_with_timestamp_filter(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test filtering events by timestamp range."""
        now = datetime.now(UTC)
        event = SampleEvent(aggregate_id=aggregate_id, data="recent")
        event = event.model_copy(update={"occurred_at": now})

        rows = [create_event_row(event, version=1)]
        mock_session.execute.return_value = create_mock_result(rows)

        from_ts = now - timedelta(hours=1)
        stream = await store.get_events(aggregate_id, "TestAggregate", from_timestamp=from_ts)

        assert len(stream.events) == 1

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_timestamp_filter(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test get_events_by_type with datetime timestamp filter."""
        now = datetime.now(UTC)
        agg_id = uuid4()
        event = SampleEvent(aggregate_id=agg_id, data="new")
        event = event.model_copy(update={"occurred_at": now})

        mock_session.execute.return_value = create_mock_result([create_event_row(event, version=1)])

        from_ts = now - timedelta(hours=1)
        events = await store.get_events_by_type("TestAggregate", from_timestamp=from_ts)

        assert len(events) == 1


# --- Stream Version Tests ---


class TestStreamVersion:
    """Tests for get_stream_version method."""

    @pytest.mark.asyncio
    async def test_get_stream_version(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test getting stream version."""
        mock_session.execute.return_value = create_version_result(5)

        version = await store.get_stream_version(aggregate_id, "TestAggregate")

        assert version == 5

    @pytest.mark.asyncio
    async def test_get_stream_version_empty(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test getting version of non-existent stream."""
        mock_session.execute.return_value = create_version_result(0)

        version = await store.get_stream_version(aggregate_id, "TestAggregate")

        assert version == 0


# --- Read Stream Tests ---


class TestReadStream:
    """Tests for read_stream async iterator."""

    @pytest.mark.asyncio
    async def test_read_stream_forward(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test reading stream in forward direction."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        rows = [
            create_full_event_row(event, version=i + 1, global_id=i + 1)
            for i, event in enumerate(events)
        ]
        mock_session.execute.return_value = create_mock_result(rows)

        stream_id = f"{aggregate_id}:TestAggregate"
        stored_events = []
        async for stored in store.read_stream(stream_id):
            stored_events.append(stored)

        assert len(stored_events) == 3
        assert stored_events[0].stream_position == 1
        assert stored_events[1].stream_position == 2
        assert stored_events[2].stream_position == 3
        assert stored_events[0].global_position == 1

    @pytest.mark.asyncio
    async def test_read_stream_backward(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test reading stream in backward direction."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        # Rows returned in reverse order
        rows = [
            create_full_event_row(events[2], version=3, global_id=3),
            create_full_event_row(events[1], version=2, global_id=2),
            create_full_event_row(events[0], version=1, global_id=1),
        ]
        mock_session.execute.return_value = create_mock_result(rows)

        stream_id = f"{aggregate_id}:TestAggregate"
        options = ReadOptions(direction=ReadDirection.BACKWARD)
        stored_events = []
        async for stored in store.read_stream(stream_id, options):
            stored_events.append(stored)

        assert len(stored_events) == 3
        assert stored_events[0].stream_position == 3
        assert stored_events[1].stream_position == 2
        assert stored_events[2].stream_position == 1

    @pytest.mark.asyncio
    async def test_read_stream_with_limit(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test reading stream with limit."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(2)]
        rows = [
            create_full_event_row(event, version=i + 1, global_id=i + 1)
            for i, event in enumerate(events)
        ]
        mock_session.execute.return_value = create_mock_result(rows)

        stream_id = f"{aggregate_id}:TestAggregate"
        options = ReadOptions(limit=2)
        stored_events = []
        async for stored in store.read_stream(stream_id, options):
            stored_events.append(stored)

        assert len(stored_events) == 2

    @pytest.mark.asyncio
    async def test_read_stream_from_position(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test reading stream from specific position."""
        events = [SampleEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        # Only return events after position 2
        rows = [create_full_event_row(events[2], version=3, global_id=3)]
        mock_session.execute.return_value = create_mock_result(rows)

        stream_id = f"{aggregate_id}:TestAggregate"
        options = ReadOptions(from_position=2)
        stored_events = []
        async for stored in store.read_stream(stream_id, options):
            stored_events.append(stored)

        assert len(stored_events) == 1
        assert stored_events[0].stream_position == 3

    @pytest.mark.asyncio
    async def test_read_stream_without_colon(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test read_stream with stream_id without colon (just UUID)."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")
        rows = [create_full_event_row(event, version=1, global_id=1)]
        mock_session.execute.return_value = create_mock_result(rows)

        # Use just the UUID as stream_id
        stream_id = str(aggregate_id)
        stored_events = []
        async for stored in store.read_stream(stream_id):
            stored_events.append(stored)

        assert len(stored_events) == 1


# --- Read All Tests ---


class TestReadAll:
    """Tests for read_all async iterator."""

    @pytest.mark.asyncio
    async def test_read_all_events(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test reading all events across streams."""
        agg1 = uuid4()
        agg2 = uuid4()

        event1 = SampleEvent(aggregate_id=agg1, data="agg1_event")
        event2 = OrderCreated(aggregate_id=agg2, customer_name="Jane")

        rows = [
            create_full_event_row(event1, version=1, global_id=1),
            create_full_event_row(event2, version=1, global_id=2),
        ]
        mock_session.execute.return_value = create_mock_result(rows)

        stored_events = []
        async for stored in store.read_all():
            stored_events.append(stored)

        assert len(stored_events) == 2
        assert stored_events[0].global_position == 1
        assert stored_events[1].global_position == 2

    @pytest.mark.asyncio
    async def test_read_all_with_limit(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test read_all with limit."""
        agg_id = uuid4()
        events = [SampleEvent(aggregate_id=agg_id, data=f"event_{i}") for i in range(3)]
        rows = [
            create_full_event_row(event, version=i + 1, global_id=i + 1)
            for i, event in enumerate(events)
        ]
        mock_session.execute.return_value = create_mock_result(rows)

        options = ReadOptions(limit=3)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 3

    @pytest.mark.asyncio
    async def test_read_all_backward(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test read_all in backward direction."""
        agg_id = uuid4()
        events = [SampleEvent(aggregate_id=agg_id, data=f"event_{i}") for i in range(3)]
        # Return in reverse order
        rows = [
            create_full_event_row(events[2], version=3, global_id=3),
            create_full_event_row(events[1], version=2, global_id=2),
            create_full_event_row(events[0], version=1, global_id=1),
        ]
        mock_session.execute.return_value = create_mock_result(rows)

        options = ReadOptions(direction=ReadDirection.BACKWARD)
        stored_events = []
        async for stored in store.read_all(options):
            stored_events.append(stored)

        assert len(stored_events) == 3
        assert stored_events[0].global_position == 3
        assert stored_events[1].global_position == 2
        assert stored_events[2].global_position == 1


# --- Outbox Pattern Tests ---


class TestOutboxPattern:
    """Tests for outbox pattern integration."""

    @pytest.mark.asyncio
    async def test_append_writes_to_outbox_when_enabled(
        self,
        store_with_outbox: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that events are written to outbox when enabled."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        mock_session.execute.side_effect = [
            create_version_result(0),  # Version check
            create_mock_result([]),  # Event exists check
            create_mock_result([(1,)]),  # Insert event returning id
            create_mock_result([]),  # Outbox insert
        ]

        result = await store_with_outbox.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        # Should have 4 execute calls: version, exists check, insert, outbox
        assert mock_session.execute.call_count == 4

    @pytest.mark.asyncio
    async def test_append_does_not_write_to_outbox_when_disabled(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that events are not written to outbox when disabled."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        mock_session.execute.side_effect = [
            create_version_result(0),  # Version check
            create_mock_result([]),  # Event exists check
            create_mock_result([(1,)]),  # Insert event returning id
        ]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        # Should have only 3 execute calls: version, exists check, insert
        assert mock_session.execute.call_count == 3


# --- Serialization/Deserialization Tests ---


class TestSerialization:
    """Tests for event serialization and deserialization."""

    @pytest.mark.asyncio
    async def test_deserialize_event_with_json_string_payload(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test deserializing events with JSON string payload."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        # Create row with JSON string payload
        row = (
            event.event_id,
            event.event_type,
            event.aggregate_type,
            event.aggregate_id,
            None,
            None,
            1,
            event.occurred_at,
            '{"event_type": "SampleEvent", "aggregate_type": "TestAggregate", "data": "test", '
            f'"event_id": "{event.event_id}", "aggregate_id": "{aggregate_id}", '
            f'"occurred_at": "{event.occurred_at.isoformat()}", "event_version": 1, '
            '"aggregate_version": 1, "tenant_id": null, "actor_id": null, '
            f'"correlation_id": "{event.correlation_id}", "causation_id": null, "metadata": {{}}}}',
        )
        mock_session.execute.return_value = create_mock_result([row])

        stream = await store.get_events(aggregate_id, "TestAggregate")

        assert len(stream.events) == 1
        assert stream.events[0].event_type == "SampleEvent"
        assert stream.events[0].data == "test"

    @pytest.mark.asyncio
    async def test_deserialize_event_with_dict_payload(
        self,
        store: PostgreSQLEventStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test deserializing events with dict payload (PostgreSQL JSON column)."""
        event = SampleEvent(aggregate_id=aggregate_id, data="test")

        # Create row with dict payload (as PostgreSQL would return for JSON column)
        rows = [create_event_row(event, version=1)]
        mock_session.execute.return_value = create_mock_result(rows)

        stream = await store.get_events(aggregate_id, "TestAggregate")

        assert len(stream.events) == 1
        assert stream.events[0].event_type == "SampleEvent"


# --- Properties Tests ---


class TestProperties:
    """Tests for store properties."""

    def test_session_factory_property(
        self,
        store: PostgreSQLEventStore,
        mock_session_factory: MagicMock,
    ) -> None:
        """Test session_factory property."""
        assert store.session_factory == mock_session_factory

    def test_event_registry_property(
        self,
        store: PostgreSQLEventStore,
        event_registry: EventRegistry,
    ) -> None:
        """Test event_registry property."""
        assert store.event_registry == event_registry

    def test_outbox_enabled_property(
        self,
        store: PostgreSQLEventStore,
        store_with_outbox: PostgreSQLEventStore,
    ) -> None:
        """Test outbox_enabled property."""
        assert store.outbox_enabled is False
        assert store_with_outbox.outbox_enabled is True


# --- OpenTelemetry Tracing Tests ---


class TestOpenTelemetryTracing:
    """Tests for optional OpenTelemetry tracing."""

    def test_tracing_disabled_by_default_when_otel_not_available(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """Test that tracing is disabled when OTEL is not available."""
        import eventsource.observability.tracer as tracer_module

        # Patch OTEL_AVAILABLE in the tracer module where create_tracer uses it
        with patch.object(tracer_module, "OTEL_AVAILABLE", False):
            store = PostgreSQLEventStore(
                session_factory=mock_session_factory,
                event_registry=event_registry,
                enable_tracing=True,  # Even if True, should be disabled
            )
            # With composition-based tracing, _tracer is always set but disabled
            assert store._tracer is not None
            assert store._tracer.enabled is False

    def test_tracing_disabled_when_explicitly_disabled(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """Test that tracing is disabled when explicitly set to False."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
        )
        # With composition-based tracing, _tracer is always set but disabled
        assert store._tracer is not None
        assert store._tracer.enabled is False


# --- Import Tests ---


class TestImports:
    """Tests for verifying correct module exports."""

    def test_import_from_stores(self) -> None:
        """Test importing PostgreSQLEventStore from stores module."""
        from eventsource.stores import PostgreSQLEventStore

        assert PostgreSQLEventStore is not None

    def test_import_from_main(self) -> None:
        """Test importing PostgreSQLEventStore from main eventsource module."""
        from eventsource import PostgreSQLEventStore

        assert PostgreSQLEventStore is not None


# --- Type Conversion Tests ---


class TestTypeConversion:
    """Tests for type conversion in deserialization."""

    def test_is_uuid_field_detects_uuid_fields(
        self,
        store: PostgreSQLEventStore,
    ) -> None:
        """Test that UUID fields are correctly identified."""
        # Should be UUID fields
        assert store._type_converter.is_uuid_field("event_id") is True
        assert store._type_converter.is_uuid_field("aggregate_id") is True
        assert store._type_converter.is_uuid_field("tenant_id") is True
        assert store._type_converter.is_uuid_field("correlation_id") is True
        assert store._type_converter.is_uuid_field("causation_id") is True
        assert store._type_converter.is_uuid_field("user_id") is True
        assert store._type_converter.is_uuid_field("order_id") is True

    def test_is_uuid_field_excludes_string_ids(
        self,
        store: PostgreSQLEventStore,
    ) -> None:
        """Test that string ID fields are correctly excluded."""
        # Should NOT be UUID fields (these are strings)
        assert store._type_converter.is_uuid_field("actor_id") is False
        assert store._type_converter.is_uuid_field("issuer_id") is False
        assert store._type_converter.is_uuid_field("recipient_id") is False
        assert store._type_converter.is_uuid_field("invited_by") is False

    def test_convert_types_converts_uuids(
        self,
        store: PostgreSQLEventStore,
    ) -> None:
        """Test that UUID strings are converted to UUID objects."""
        test_uuid = uuid4()
        data = {"event_id": str(test_uuid), "name": "test"}

        result = store._type_converter.convert_types(data)

        assert isinstance(result["event_id"], UUID)
        assert result["event_id"] == test_uuid
        assert result["name"] == "test"

    def test_convert_types_converts_datetimes(
        self,
        store: PostgreSQLEventStore,
    ) -> None:
        """Test that datetime strings are converted to datetime objects."""
        now = datetime.now(UTC)
        data = {"occurred_at": now.isoformat(), "name": "test"}

        result = store._type_converter.convert_types(data)

        assert isinstance(result["occurred_at"], datetime)
        assert result["name"] == "test"

    def test_convert_types_handles_nested_structures(
        self,
        store: PostgreSQLEventStore,
    ) -> None:
        """Test that nested structures are recursively converted."""
        test_uuid = uuid4()
        data = {
            "outer_id": str(test_uuid),
            "nested": {
                "inner_id": str(test_uuid),
            },
            "list_field": [
                {"item_id": str(test_uuid)},
            ],
        }

        result = store._type_converter.convert_types(data)

        assert isinstance(result["outer_id"], UUID)
        assert isinstance(result["nested"]["inner_id"], UUID)
        assert isinstance(result["list_field"][0]["item_id"], UUID)

    def test_convert_types_handles_z_timezone(
        self,
        store: PostgreSQLEventStore,
    ) -> None:
        """Test that Z timezone suffix is handled correctly."""
        data = {"occurred_at": "2024-01-01T12:00:00Z"}

        result = store._type_converter.convert_types(data)

        assert isinstance(result["occurred_at"], datetime)
        assert result["occurred_at"].tzinfo is not None


# --- Configurable UUID Detection Tests ---


class TestConfigurableUUIDDetection:
    """Tests for configurable UUID field detection."""

    def test_default_uuid_fields_detected(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """Default UUID fields are always detected."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
        )

        assert store._type_converter.is_uuid_field("event_id") is True
        assert store._type_converter.is_uuid_field("aggregate_id") is True
        assert store._type_converter.is_uuid_field("tenant_id") is True
        assert store._type_converter.is_uuid_field("correlation_id") is True
        assert store._type_converter.is_uuid_field("causation_id") is True

    def test_default_string_fields_excluded(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """Default string ID fields are excluded."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
        )

        assert store._type_converter.is_uuid_field("actor_id") is False
        assert store._type_converter.is_uuid_field("issuer_id") is False
        assert store._type_converter.is_uuid_field("recipient_id") is False
        assert store._type_converter.is_uuid_field("invited_by") is False

    def test_custom_uuid_field_added(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """Custom UUID fields are detected."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
            uuid_fields={"custom_reference_id", "special_field"},
        )

        # Custom fields should be detected
        assert store._type_converter.is_uuid_field("custom_reference_id") is True
        assert store._type_converter.is_uuid_field("special_field") is True
        # Default fields still work
        assert store._type_converter.is_uuid_field("event_id") is True

    def test_custom_string_field_excluded(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """Custom string fields are excluded from auto-detection."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
            string_id_fields={"stripe_customer_id", "external_api_id"},
        )

        # Custom exclusions should work
        # (would normally match _id suffix, but excluded)
        assert store._type_converter.is_uuid_field("stripe_customer_id") is False
        assert store._type_converter.is_uuid_field("external_api_id") is False
        # Default exclusions still work
        assert store._type_converter.is_uuid_field("actor_id") is False

    def test_auto_detect_disabled(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """auto_detect_uuid=False disables suffix detection."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
            auto_detect_uuid=False,
        )

        # Not in explicit list, suffix detection disabled
        assert store._type_converter.is_uuid_field("some_random_id") is False
        assert store._type_converter.is_uuid_field("custom_entity_id") is False
        # But explicit UUID fields still work
        assert store._type_converter.is_uuid_field("event_id") is True
        assert store._type_converter.is_uuid_field("aggregate_id") is True

    def test_auto_detect_enabled_by_default(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """Auto-detection is enabled by default."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
        )

        # Fields ending in _id should be detected
        assert store._type_converter.is_uuid_field("order_id") is True
        assert store._type_converter.is_uuid_field("customer_id") is True
        assert store._type_converter.is_uuid_field("product_id") is True

    def test_strict_uuid_detection(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """with_strict_uuid_detection uses only explicit fields."""
        store = PostgreSQLEventStore.with_strict_uuid_detection(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            uuid_fields={"event_id", "aggregate_id"},
            enable_tracing=False,
        )

        # Only explicitly listed fields are UUIDs
        assert store._type_converter.is_uuid_field("event_id") is True
        assert store._type_converter.is_uuid_field("aggregate_id") is True
        # Default UUID fields NOT in the explicit list
        assert store._type_converter.is_uuid_field("tenant_id") is False
        assert store._type_converter.is_uuid_field("correlation_id") is False
        # Auto-detection is disabled
        assert store._type_converter.is_uuid_field("any_other_id") is False
        assert store._type_converter.is_uuid_field("customer_id") is False

    def test_strict_uuid_detection_with_outbox(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """with_strict_uuid_detection passes through other options."""
        store = PostgreSQLEventStore.with_strict_uuid_detection(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            uuid_fields={"event_id"},
            outbox_enabled=True,
            enable_tracing=False,
        )

        assert store.outbox_enabled is True
        assert store._type_converter.is_uuid_field("event_id") is True

    def test_exclusion_takes_precedence_over_auto_detect(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """String ID exclusions take precedence over auto-detection."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
            string_id_fields={"external_order_id"},  # Explicitly exclude
            auto_detect_uuid=True,
        )

        # Would match _id suffix, but explicitly excluded
        assert store._type_converter.is_uuid_field("external_order_id") is False

    def test_explicit_uuid_takes_precedence_over_exclusion(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """Explicit UUID fields take precedence over exclusions."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
            uuid_fields={"special_id"},
            string_id_fields={"special_id"},  # Also in exclusions
        )

        # Explicit UUID takes precedence
        assert store._type_converter.is_uuid_field("special_id") is True

    def test_module_level_defaults_are_frozen(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """Module-level default sets are frozensets (immutable)."""
        from eventsource.stores import DEFAULT_STRING_ID_FIELDS, DEFAULT_UUID_FIELDS

        assert isinstance(DEFAULT_UUID_FIELDS, frozenset)
        assert isinstance(DEFAULT_STRING_ID_FIELDS, frozenset)

    def test_type_converter_sets_are_frozen(
        self,
        mock_session_factory: MagicMock,
        event_registry: EventRegistry,
    ) -> None:
        """TypeConverter internal field sets are frozensets (immutable)."""
        store = PostgreSQLEventStore(
            session_factory=mock_session_factory,
            event_registry=event_registry,
            enable_tracing=False,
            uuid_fields={"custom_id"},
            string_id_fields={"external_id"},
        )

        assert isinstance(store._type_converter._uuid_fields, frozenset)
        assert isinstance(store._type_converter._string_id_fields, frozenset)
