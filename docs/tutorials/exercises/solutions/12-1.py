"""
Tutorial 12 - Exercise 1 Solution: SQLite Test Fixtures

This solution demonstrates how to create pytest fixtures for SQLite-based testing.
Run with: pytest 12-1.py -v

Key concepts demonstrated:
1. Creating in-memory event store fixtures
2. Proper fixture lifecycle with async context managers
3. Repository fixtures that depend on event store fixtures
4. Test isolation verification
"""

from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    EventRegistry,
    SQLiteEventStore,
    register_event,
)

# =============================================================================
# Domain Code
# =============================================================================


@register_event
class ItemCreated(DomainEvent):
    """Event emitted when an item is created."""

    event_type: str = "ItemCreated"
    aggregate_type: str = "Item"
    name: str


class ItemState(BaseModel):
    """State model for Item aggregate."""

    item_id: str
    name: str = ""


class ItemAggregate(AggregateRoot[ItemState]):
    """Simple item aggregate for testing."""

    aggregate_type = "Item"

    def _get_initial_state(self) -> ItemState:
        return ItemState(item_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, ItemCreated):
            self._state = ItemState(
                item_id=str(self.aggregate_id),
                name=event.name,
            )

    def create(self, name: str) -> None:
        """Create an item with the given name."""
        self.apply_event(
            ItemCreated(
                aggregate_id=self.aggregate_id,
                name=name,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Pytest Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def event_store():
    """
    Fixture 1: In-memory event store.

    Creates a fresh in-memory SQLite database for each test.
    Automatically initializes and cleans up.

    Key points:
    - Uses ":memory:" for in-memory database
    - Creates fresh EventRegistry for test isolation
    - Uses wal_mode=False (WAL not supported for in-memory)
    - Uses async context manager for proper cleanup
    - Yields (not returns) to allow test execution before cleanup
    """
    # Create fresh registry for this test
    registry = EventRegistry()
    registry.register(ItemCreated)

    # Create store with in-memory database
    store = SQLiteEventStore(
        database=":memory:",
        event_registry=registry,
        wal_mode=False,  # WAL not supported for in-memory
    )

    # Use async context manager for proper lifecycle
    async with store:
        # Initialize creates the schema tables
        await store.initialize()
        # Yield control to the test
        yield store
    # Cleanup happens automatically when context manager exits


@pytest_asyncio.fixture
async def item_repository(event_store):
    """
    Fixture 2: Repository using the event store fixture.

    Depends on event_store fixture for proper lifecycle.
    When event_store is cleaned up, this repository becomes invalid.

    Key points:
    - Depends on event_store fixture (pytest handles ordering)
    - Returns (not yields) since no cleanup needed
    - Repository is ready to use immediately
    """
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=ItemAggregate,
        aggregate_type="Item",
    )


# =============================================================================
# Tests Using Fixtures
# =============================================================================


class TestItemAggregate:
    """Tests demonstrating fixture usage."""

    @pytest.mark.asyncio
    async def test_create_item(self, item_repository):
        """Test creating an item through the repository."""
        item_id = uuid4()

        # Create new aggregate instance
        item = item_repository.create_new(item_id)
        item.create("Test Item")

        # Save to event store
        await item_repository.save(item)

        # Verify state
        assert item.version == 1
        assert item.state.name == "Test Item"

    @pytest.mark.asyncio
    async def test_load_item(self, item_repository):
        """Test loading an item from the repository."""
        item_id = uuid4()

        # Create and save
        item = item_repository.create_new(item_id)
        item.create("Another Item")
        await item_repository.save(item)

        # Load into new instance
        loaded = await item_repository.load(item_id)

        # Verify loaded state matches
        assert loaded.state.name == "Another Item"
        assert loaded.version == 1
        assert len(loaded.uncommitted_events) == 0  # Events are committed

    @pytest.mark.asyncio
    async def test_isolation_between_tests(self, item_repository):
        """
        Test that each test gets a fresh database.

        This test creates items that should NOT exist in other tests.
        If isolation is broken, other tests would see this data.
        """
        item_id = uuid4()
        item = item_repository.create_new(item_id)
        item.create("Isolated Item")
        await item_repository.save(item)

        # This item only exists in this test's database
        loaded = await item_repository.load(item_id)
        assert loaded.state.name == "Isolated Item"

    @pytest.mark.asyncio
    async def test_exists_method(self, item_repository):
        """Test checking if aggregate exists."""
        item_id = uuid4()

        # Should not exist yet
        assert not await item_repository.exists(item_id)

        # Create it
        item = item_repository.create_new(item_id)
        item.create("Existing Item")
        await item_repository.save(item)

        # Now should exist
        assert await item_repository.exists(item_id)


# =============================================================================
# Verification of Isolation
# =============================================================================


class TestIsolation:
    """
    Verify test isolation with separate test class.

    These tests verify that each test gets a completely fresh database.
    If isolation is working correctly:
    - No data from TestItemAggregate tests should be visible
    - The database should be empty at the start of each test
    """

    @pytest.mark.asyncio
    async def test_fresh_database(self, event_store):
        """
        Verify we get a fresh database.

        If isolation works, there should be no events from other tests.
        """
        # Read all events - should be empty
        events = []
        async for event in event_store.read_all():
            events.append(event)

        assert len(events) == 0, "Database should be empty at test start"

    @pytest.mark.asyncio
    async def test_no_cross_test_pollution(self, event_store, item_repository):
        """
        Create data in this test and verify it doesn't leak to other tests.

        Run this test multiple times - each run should see an empty database.
        """
        # Verify empty at start
        events = []
        async for event in event_store.read_all():
            events.append(event)
        assert len(events) == 0, "Should start empty"

        # Create some data
        for i in range(5):
            item = item_repository.create_new(uuid4())
            item.create(f"Item {i}")
            await item_repository.save(item)

        # Verify we have data now
        events = []
        async for event in event_store.read_all():
            events.append(event)
        assert len(events) == 5, "Should have 5 events"

        # This data will be cleaned up when the test ends
        # The next test run will see an empty database


# =============================================================================
# Additional Test Patterns
# =============================================================================


class TestAdvancedPatterns:
    """Additional test patterns for reference."""

    @pytest.mark.asyncio
    async def test_multiple_saves(self, item_repository):
        """Test saving the same aggregate multiple times."""
        item_id = uuid4()

        # First save
        item = item_repository.create_new(item_id)
        item.create("Initial Name")
        await item_repository.save(item)
        assert item.version == 1

        # Load and verify
        loaded = await item_repository.load(item_id)
        assert loaded.state.name == "Initial Name"
        assert loaded.version == 1

    @pytest.mark.asyncio
    async def test_event_stream_reading(self, event_store, item_repository):
        """Test reading the event stream directly."""
        # Register the event with the store's registry
        event_store.event_registry.register(ItemCreated)

        # Create items
        item1_id = uuid4()
        item2_id = uuid4()

        item1 = item_repository.create_new(item1_id)
        item1.create("Item One")
        await item_repository.save(item1)

        item2 = item_repository.create_new(item2_id)
        item2.create("Item Two")
        await item_repository.save(item2)

        # Read all events
        events = []
        async for stored_event in event_store.read_all():
            events.append(stored_event)

        assert len(events) == 2
        assert events[0].global_position < events[1].global_position

    @pytest.mark.asyncio
    async def test_concurrent_creates(self, item_repository):
        """Test creating multiple items (not truly concurrent in SQLite)."""
        import asyncio

        async def create_item(name: str):
            item = item_repository.create_new(uuid4())
            item.create(name)
            await item_repository.save(item)
            return item

        # Create multiple items
        items = await asyncio.gather(
            create_item("Concurrent 1"),
            create_item("Concurrent 2"),
            create_item("Concurrent 3"),
        )

        assert len(items) == 3
        for item in items:
            assert item.version == 1
