"""
Unit tests for CheckpointRepository position methods.

Tests the get_position() and save_position() methods for:
- InMemoryCheckpointRepository
- SQLCheckpointRepository

These tests verify:
- get_position() returns None for non-existent checkpoint
- save_position() creates new checkpoint with position
- save_position() updates existing checkpoint position
- events_processed increments on each save_position() call
- get_position() returns correct value after save_position()
- position is included in CheckpointData from get_all_checkpoints()

Positions are opaque tokens: `pos()` builds one per int used below.
"""

from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.ports.checkpoints import CheckpointData
from eventsource.ports.positions import Position


def pos(n: int) -> Position:
    """Build a token for a test store, standing in for an int position."""
    return Position(store_id="test", key=(n,))


class TestInMemoryCheckpointRepositoryPosition:
    """Tests for InMemoryCheckpointRepository position methods."""

    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        """Create a fresh repository for each test."""
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_get_position_returns_none_for_nonexistent(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that get_position returns None for non-existent checkpoint."""
        result = await repo.get_position("NonExistentSubscription")
        assert result is None

    @pytest.mark.asyncio
    async def test_save_position_creates_new_checkpoint(self, repo: InMemoryCheckpointRepository):
        """Test that save_position creates a new checkpoint with position."""
        subscription_id = "TestSubscription"
        event_id = uuid4()
        event_type = "TestEvent"
        position = pos(100)

        await repo.save_position(subscription_id, position, event_id, event_type)

        result = await repo.get_position(subscription_id)
        assert result == position

    @pytest.mark.asyncio
    async def test_save_position_updates_existing_checkpoint(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that save_position updates existing checkpoint position."""
        subscription_id = "TestSubscription"
        first_position = pos(100)
        second_position = pos(200)

        await repo.save_position(subscription_id, first_position, uuid4(), "Event1")
        await repo.save_position(subscription_id, second_position, uuid4(), "Event2")

        result = await repo.get_position(subscription_id)
        assert result == second_position

    @pytest.mark.asyncio
    async def test_save_position_increments_events_processed(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that events_processed increments on each save_position call."""
        subscription_id = "TestSubscription"

        # Save position three times
        for i in range(3):
            await repo.save_position(subscription_id, pos(i * 100), uuid4(), f"Event{i}")

        # Check count
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 3

    @pytest.mark.asyncio
    async def test_save_position_updates_event_id_and_type(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that save_position updates event_id and event_type."""
        subscription_id = "TestSubscription"
        event_id = uuid4()
        event_type = "TestEventType"
        position = pos(100)

        await repo.save_position(subscription_id, position, event_id, event_type)

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]
        assert checkpoint.last_event_id == event_id
        assert checkpoint.last_event_type == event_type

    @pytest.mark.asyncio
    async def test_get_position_returns_correct_value_after_save(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that get_position returns correct value after save_position."""
        subscription_id = "TestSubscription"
        positions = [pos(n) for n in (10, 50, 100, 500, 1000)]

        for position in positions:
            await repo.save_position(subscription_id, position, uuid4(), "Event")
            result = await repo.get_position(subscription_id)
            assert result == position

    @pytest.mark.asyncio
    async def test_checkpoint_data_includes_global_position(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that CheckpointData includes the position field."""
        subscription_id = "TestSubscription"
        position = pos(42)

        await repo.save_position(subscription_id, position, uuid4(), "TestEvent")

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]

        assert isinstance(checkpoint, CheckpointData)
        assert checkpoint.position == position

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_independent_positions(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that different subscriptions have independent positions."""
        sub1 = "Subscription1"
        sub2 = "Subscription2"
        pos1 = pos(100)
        pos2 = pos(200)

        await repo.save_position(sub1, pos1, uuid4(), "Event1")
        await repo.save_position(sub2, pos2, uuid4(), "Event2")

        assert await repo.get_position(sub1) == pos1
        assert await repo.get_position(sub2) == pos2

    @pytest.mark.asyncio
    async def test_get_position_returns_none_after_reset(self, repo: InMemoryCheckpointRepository):
        """Test that get_position returns None after reset_checkpoint."""
        subscription_id = "TestSubscription"

        await repo.save_position(subscription_id, pos(100), uuid4(), "Event")
        assert await repo.get_position(subscription_id) == pos(100)

        await repo.reset_checkpoint(subscription_id)
        assert await repo.get_position(subscription_id) is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_does_not_set_position(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that update_checkpoint does not set a position."""
        subscription_id = "TestSubscription"
        event_id = uuid4()

        await repo.update_checkpoint(subscription_id, event_id, "TestEvent")

        # get_position should return None since update_checkpoint doesn't set it
        result = await repo.get_position(subscription_id)
        assert result is None

        # But the checkpoint should exist
        checkpoint = await repo.get_checkpoint(subscription_id)
        assert checkpoint == event_id

    @pytest.mark.asyncio
    async def test_save_position_after_update_checkpoint(self, repo: InMemoryCheckpointRepository):
        """Test save_position works after update_checkpoint was used."""
        subscription_id = "TestSubscription"

        # First use update_checkpoint (no position)
        await repo.update_checkpoint(subscription_id, uuid4(), "Event1")
        assert await repo.get_position(subscription_id) is None

        # Then use save_position
        position = pos(500)
        await repo.save_position(subscription_id, position, uuid4(), "Event2")
        assert await repo.get_position(subscription_id) == position

        # Check events_processed was incremented
        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].events_processed == 2

    @pytest.mark.asyncio
    async def test_save_position_sets_last_processed_at(self, repo: InMemoryCheckpointRepository):
        """Test that save_position sets last_processed_at timestamp."""
        subscription_id = "TestSubscription"

        await repo.save_position(subscription_id, pos(100), uuid4(), "Event")

        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].last_processed_at is not None
        assert isinstance(checkpoints[0].last_processed_at, datetime)


class TestInMemoryCheckpointRepositoryPositionConcurrency:
    """Tests for concurrent access to position methods."""

    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        """Create a fresh repository for each test."""
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_concurrent_save_position_no_deadlock(self, repo: InMemoryCheckpointRepository):
        """Test that 100 concurrent save_position calls complete without deadlock."""
        import asyncio

        subscription_id = "TestSubscription"
        num_updates = 100

        async def save_position(i: int):
            await repo.save_position(subscription_id, pos(i), uuid4(), f"Event{i}")

        # Run 100 concurrent save_position calls
        tasks = [save_position(i) for i in range(num_updates)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify events_processed count
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == num_updates

    @pytest.mark.asyncio
    async def test_concurrent_get_and_save_position(self, repo: InMemoryCheckpointRepository):
        """Test mixed concurrent reads and writes complete without issues."""
        import asyncio

        subscription_id = "TestSubscription"
        num_operations = 50
        positions: list[Position | None] = []

        async def writer(i: int):
            await repo.save_position(subscription_id, pos(i * 10), uuid4(), f"Event{i}")

        async def reader():
            result = await repo.get_position(subscription_id)
            positions.append(result)

        # Interleave reads and writes
        tasks = []
        for i in range(num_operations):
            tasks.append(writer(i))
            tasks.append(reader())

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # All operations completed
        assert len(positions) == num_operations


# ============================================================================
# SQLCheckpointRepository Position Tests
# ============================================================================

# Check if aiosqlite is available
try:
    import aiosqlite

    from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    SQLCheckpointRepository = None  # type: ignore[assignment,misc]
    AIOSQLITE_AVAILABLE = False


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLCheckpointRepositoryPosition:
    """Tests for SQLCheckpointRepository position methods."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        """Create a SQLite engine with schema for each test."""
        from eventsource import create_async_engine
        from eventsource.migrations import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/position.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))
        yield engine
        await engine.dispose()

    @pytest.fixture
    def repo(self, sqlite_engine) -> SQLCheckpointRepository:
        """Create a SQLCheckpointRepository for each test."""
        return SQLCheckpointRepository(sqlite_engine)

    @pytest.mark.asyncio
    async def test_get_position_returns_none_for_nonexistent(self, repo: SQLCheckpointRepository):
        """Test that get_position returns None for non-existent checkpoint."""
        result = await repo.get_position("NonExistentSubscription")
        assert result is None

    @pytest.mark.asyncio
    async def test_save_position_creates_new_checkpoint(self, repo: SQLCheckpointRepository):
        """Test that save_position creates a new checkpoint with position."""
        subscription_id = "TestSubscription"
        event_id = uuid4()
        event_type = "TestEvent"
        position = pos(100)

        await repo.save_position(subscription_id, position, event_id, event_type)

        result = await repo.get_position(subscription_id)
        assert result == position

    @pytest.mark.asyncio
    async def test_save_position_updates_existing_checkpoint(self, repo: SQLCheckpointRepository):
        """Test that save_position updates existing checkpoint position."""
        subscription_id = "TestSubscription"
        first_position = pos(100)
        second_position = pos(200)

        await repo.save_position(subscription_id, first_position, uuid4(), "Event1")
        await repo.save_position(subscription_id, second_position, uuid4(), "Event2")

        result = await repo.get_position(subscription_id)
        assert result == second_position

    @pytest.mark.asyncio
    async def test_save_position_increments_events_processed(self, repo: SQLCheckpointRepository):
        """Test that events_processed increments on each save_position call."""
        subscription_id = "TestSubscription"

        # Save position three times
        for i in range(3):
            await repo.save_position(subscription_id, pos(i * 100), uuid4(), f"Event{i}")

        # Check count
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 3

    @pytest.mark.asyncio
    async def test_save_position_updates_event_id_and_type(self, repo: SQLCheckpointRepository):
        """Test that save_position updates event_id and event_type."""
        subscription_id = "TestSubscription"
        event_id = uuid4()
        event_type = "TestEventType"
        position = pos(100)

        await repo.save_position(subscription_id, position, event_id, event_type)

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]
        assert checkpoint.last_event_id == event_id
        assert checkpoint.last_event_type == event_type

    @pytest.mark.asyncio
    async def test_get_position_returns_correct_value_after_save(
        self, repo: SQLCheckpointRepository
    ):
        """Test that get_position returns correct value after save_position."""
        subscription_id = "TestSubscription"
        positions = [pos(n) for n in (10, 50, 100, 500, 1000)]

        for position in positions:
            await repo.save_position(subscription_id, position, uuid4(), "Event")
            result = await repo.get_position(subscription_id)
            assert result == position

    @pytest.mark.asyncio
    async def test_checkpoint_data_includes_global_position(self, repo: SQLCheckpointRepository):
        """Test that CheckpointData includes the position field."""
        subscription_id = "TestSubscription"
        position = pos(42)

        await repo.save_position(subscription_id, position, uuid4(), "TestEvent")

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]

        assert isinstance(checkpoint, CheckpointData)
        assert checkpoint.position == position

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_independent_positions(
        self, repo: SQLCheckpointRepository
    ):
        """Test that different subscriptions have independent positions."""
        sub1 = "Subscription1"
        sub2 = "Subscription2"
        pos1 = pos(100)
        pos2 = pos(200)

        await repo.save_position(sub1, pos1, uuid4(), "Event1")
        await repo.save_position(sub2, pos2, uuid4(), "Event2")

        assert await repo.get_position(sub1) == pos1
        assert await repo.get_position(sub2) == pos2

    @pytest.mark.asyncio
    async def test_get_position_returns_none_after_reset(self, repo: SQLCheckpointRepository):
        """Test that get_position returns None after reset_checkpoint."""
        subscription_id = "TestSubscription"

        await repo.save_position(subscription_id, pos(100), uuid4(), "Event")
        assert await repo.get_position(subscription_id) == pos(100)

        await repo.reset_checkpoint(subscription_id)
        assert await repo.get_position(subscription_id) is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_does_not_set_position(self, repo: SQLCheckpointRepository):
        """Test that update_checkpoint does not set a position."""
        subscription_id = "TestSubscription"
        event_id = uuid4()

        await repo.update_checkpoint(subscription_id, event_id, "TestEvent")

        # get_position should return None since update_checkpoint doesn't set it
        result = await repo.get_position(subscription_id)
        assert result is None

        # But the checkpoint should exist
        checkpoint = await repo.get_checkpoint(subscription_id)
        assert checkpoint == event_id

    @pytest.mark.asyncio
    async def test_save_position_after_update_checkpoint(self, repo: SQLCheckpointRepository):
        """Test save_position works after update_checkpoint was used."""
        subscription_id = "TestSubscription"

        # First use update_checkpoint (no position)
        await repo.update_checkpoint(subscription_id, uuid4(), "Event1")
        assert await repo.get_position(subscription_id) is None

        # Then use save_position
        position = pos(500)
        await repo.save_position(subscription_id, position, uuid4(), "Event2")
        assert await repo.get_position(subscription_id) == position

        # Check events_processed was incremented
        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].events_processed == 2

    @pytest.mark.asyncio
    async def test_save_position_sets_last_processed_at(self, repo: SQLCheckpointRepository):
        """Test that save_position sets last_processed_at timestamp."""
        subscription_id = "TestSubscription"

        await repo.save_position(subscription_id, pos(100), uuid4(), "Event")

        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].last_processed_at is not None
        assert isinstance(checkpoints[0].last_processed_at, datetime)

    @pytest.mark.asyncio
    async def test_sqlitestore_position_round_trip_through_checkpoint_repo(
        self, repo: SQLCheckpointRepository, tmp_path
    ) -> None:
        """Round-trip: SQLiteEventStore position → checkpoint → read → compare.

        Tests the full codec round-trip: a position produced by a real
        SQLiteEventStore append, saved through SQLCheckpointRepository.save_position,
        read back, and asserted to equal the original. This proves store_id matching
        and position ordering work end-to-end, covering the position token codec
        round-trip through the checkpoint table.
        """
        from eventsource.adapters.sqlite import SQLiteEventStore
        from eventsource.events.registry import EventRegistry
        from eventsource.ports import ExpectedVersion
        from eventsource.testing.conformance_ports._fixtures import (
            ConformanceEvent,
            make_stream,
        )

        # Create a fresh registry for the SQLiteEventStore
        registry = EventRegistry()
        registry.register(ConformanceEvent)

        # Create a real SQLiteEventStore in a temp location
        db_path = str(tmp_path / "store.db")
        store = SQLiteEventStore(db_path, event_registry=registry)

        try:
            # Append first event and capture its position
            stream1 = make_stream()
            result1 = await store.append(
                stream1,
                [ConformanceEvent(aggregate_id=stream1.aggregate_id, payload="first")],
                ExpectedVersion.any_(),
            )
            position1 = result1.position
            assert position1 is not None

            # Append second event and capture its position
            stream2 = make_stream()
            result2 = await store.append(
                stream2,
                [ConformanceEvent(aggregate_id=stream2.aggregate_id, payload="second")],
                ExpectedVersion.any_(),
            )
            position2 = result2.position
            assert position2 is not None

            # Save position1 through the checkpoint repository
            await repo.save_position("S", position1, uuid4(), "Created")

            # Read it back
            read_position = await repo.get_position("S")

            # Assert the round-trip succeeded
            assert read_position is not None
            assert read_position == position1
            assert read_position.store_id == position1.store_id

            # Assert comparison with a later position works correctly
            assert position1 < position2
            assert read_position < position2
        finally:
            await store.close()
