"""
Unit tests for EventStore.get_global_position() method.

Tests cover all EventStore implementations:
- InMemoryEventStore
- SQLiteEventStore
- PostgreSQLEventStore (via integration tests)

Test scenarios:
- Empty store returns 0
- Store with events returns correct max position
- Position increases after appending events
- Position reflects max across all streams
"""

from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.stores.in_memory import InMemoryEventStore

# Check for SQLite availability
try:
    import importlib.util

    AIOSQLITE_AVAILABLE = importlib.util.find_spec("aiosqlite") is not None
except ImportError:
    AIOSQLITE_AVAILABLE = False

skip_if_no_aiosqlite = pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")


# --- Test Event Classes ---


class TestEvent(DomainEvent):
    """Simple test event for position testing."""

    event_type: str = "TestEvent"
    aggregate_type: str = "TestAggregate"
    data: str = "test"


# --- InMemoryEventStore Tests ---


class TestInMemoryEventStoreGlobalPosition:
    """Tests for InMemoryEventStore.get_global_position()."""

    @pytest.fixture
    def store(self) -> InMemoryEventStore:
        """Create a fresh InMemoryEventStore for each test."""
        return InMemoryEventStore()

    @pytest.mark.asyncio
    async def test_empty_store_returns_zero(self, store: InMemoryEventStore) -> None:
        """Test that an empty store returns position 0."""
        position = await store.get_global_position()
        assert position == 0

    @pytest.mark.asyncio
    async def test_single_event_returns_one(self, store: InMemoryEventStore) -> None:
        """Test that a store with one event returns position 1."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, data="first")

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=0,
        )

        position = await store.get_global_position()
        assert position == 1

    @pytest.mark.asyncio
    async def test_multiple_events_returns_max_position(self, store: InMemoryEventStore) -> None:
        """Test that position reflects the total number of events."""
        aggregate_id = uuid4()
        events = [TestEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(5)]

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        position = await store.get_global_position()
        assert position == 5

    @pytest.mark.asyncio
    async def test_position_increases_after_append(self, store: InMemoryEventStore) -> None:
        """Test that position increases after appending more events."""
        aggregate_id = uuid4()

        # Append first batch
        events1 = [TestEvent(aggregate_id=aggregate_id, data=f"batch1_{i}") for i in range(3)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events1,
            expected_version=0,
        )

        position1 = await store.get_global_position()
        assert position1 == 3

        # Append second batch
        events2 = [TestEvent(aggregate_id=aggregate_id, data=f"batch2_{i}") for i in range(2)]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events2,
            expected_version=3,
        )

        position2 = await store.get_global_position()
        assert position2 == 5

    @pytest.mark.asyncio
    async def test_position_across_multiple_aggregates(self, store: InMemoryEventStore) -> None:
        """Test that position reflects events across all aggregates."""
        agg1 = uuid4()
        agg2 = uuid4()
        agg3 = uuid4()

        # Append events to different aggregates
        await store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestAggregate",
            events=[TestEvent(aggregate_id=agg1, data="agg1_event1")],
            expected_version=0,
        )

        await store.append_events(
            aggregate_id=agg2,
            aggregate_type="TestAggregate",
            events=[
                TestEvent(aggregate_id=agg2, data="agg2_event1"),
                TestEvent(aggregate_id=agg2, data="agg2_event2"),
            ],
            expected_version=0,
        )

        await store.append_events(
            aggregate_id=agg3,
            aggregate_type="TestAggregate",
            events=[TestEvent(aggregate_id=agg3, data="agg3_event1")],
            expected_version=0,
        )

        position = await store.get_global_position()
        assert position == 4  # Total across all aggregates

    @pytest.mark.asyncio
    async def test_position_after_clear(self, store: InMemoryEventStore) -> None:
        """Test that position returns to 0 after clearing the store."""
        aggregate_id = uuid4()
        events = [TestEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        assert await store.get_global_position() == 3

        await store.clear()

        assert await store.get_global_position() == 0


# --- SQLiteEventStore Tests ---


@skip_if_no_aiosqlite
class TestSQLiteEventStoreGlobalPosition:
    """Tests for SQLiteEventStore.get_global_position()."""

    @pytest.fixture
    async def sqlite_store(self):
        """Create an initialized SQLiteEventStore for each test."""
        from eventsource import EventRegistry
        from eventsource.stores.sqlite import SQLiteEventStore

        registry = EventRegistry()
        registry.register(TestEvent)

        store = SQLiteEventStore(
            database=":memory:",
            event_registry=registry,
            wal_mode=False,
        )

        async with store:
            await store.initialize()
            yield store

    @pytest.mark.asyncio
    async def test_empty_store_returns_zero(self, sqlite_store) -> None:
        """Test that an empty SQLite store returns position 0."""
        position = await sqlite_store.get_global_position()
        assert position == 0

    @pytest.mark.asyncio
    async def test_single_event_returns_correct_position(self, sqlite_store) -> None:
        """Test that SQLite store returns correct position after one event."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, data="first")

        result = await sqlite_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=[event],
            expected_version=0,
        )

        position = await sqlite_store.get_global_position()
        # The position should equal the global_position from append result
        assert position == result.global_position
        assert position >= 1

    @pytest.mark.asyncio
    async def test_multiple_events_returns_max_position(self, sqlite_store) -> None:
        """Test that SQLite position reflects highest id."""
        aggregate_id = uuid4()
        events = [TestEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(5)]

        result = await sqlite_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events,
            expected_version=0,
        )

        position = await sqlite_store.get_global_position()
        assert position == result.global_position
        assert position >= 5

    @pytest.mark.asyncio
    async def test_position_increases_after_append(self, sqlite_store) -> None:
        """Test that SQLite position increases after appending."""
        aggregate_id = uuid4()

        # Append first batch
        events1 = [TestEvent(aggregate_id=aggregate_id, data=f"batch1_{i}") for i in range(3)]
        result1 = await sqlite_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events1,
            expected_version=0,
        )

        position1 = await sqlite_store.get_global_position()
        assert position1 == result1.global_position

        # Append second batch
        events2 = [TestEvent(aggregate_id=aggregate_id, data=f"batch2_{i}") for i in range(2)]
        result2 = await sqlite_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=events2,
            expected_version=3,
        )

        position2 = await sqlite_store.get_global_position()
        assert position2 == result2.global_position
        assert position2 > position1

    @pytest.mark.asyncio
    async def test_position_across_multiple_aggregates(self, sqlite_store) -> None:
        """Test that SQLite position reflects events across all aggregates."""
        agg1 = uuid4()
        agg2 = uuid4()

        await sqlite_store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestAggregate",
            events=[TestEvent(aggregate_id=agg1, data="agg1_event1")],
            expected_version=0,
        )

        result = await sqlite_store.append_events(
            aggregate_id=agg2,
            aggregate_type="TestAggregate",
            events=[
                TestEvent(aggregate_id=agg2, data="agg2_event1"),
                TestEvent(aggregate_id=agg2, data="agg2_event2"),
            ],
            expected_version=0,
        )

        position = await sqlite_store.get_global_position()
        assert position == result.global_position
