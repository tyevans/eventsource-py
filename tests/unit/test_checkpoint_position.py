"""
Unit tests for CheckpointRepository position methods.

Tests the get_position() and save_position() methods for:
- InMemoryCheckpointRepository
- SQLiteCheckpointRepository

These tests verify:
- get_position() returns None for non-existent checkpoint
- save_position() creates new checkpoint with position
- save_position() updates existing checkpoint position
- events_processed increments on each save_position() call
- get_position() returns correct value after save_position()
- global_position is included in CheckpointData from get_all_checkpoints()
"""

from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.repositories.checkpoint import (
    CheckpointData,
    InMemoryCheckpointRepository,
)


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
        position = 100

        await repo.save_position(subscription_id, position, event_id, event_type)

        result = await repo.get_position(subscription_id)
        assert result == position

    @pytest.mark.asyncio
    async def test_save_position_updates_existing_checkpoint(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that save_position updates existing checkpoint position."""
        subscription_id = "TestSubscription"
        first_position = 100
        second_position = 200

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
            await repo.save_position(subscription_id, i * 100, uuid4(), f"Event{i}")

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
        position = 100

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
        positions = [10, 50, 100, 500, 1000]

        for pos in positions:
            await repo.save_position(subscription_id, pos, uuid4(), "Event")
            result = await repo.get_position(subscription_id)
            assert result == pos

    @pytest.mark.asyncio
    async def test_checkpoint_data_includes_global_position(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that CheckpointData includes global_position field."""
        subscription_id = "TestSubscription"
        position = 42

        await repo.save_position(subscription_id, position, uuid4(), "TestEvent")

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]

        assert isinstance(checkpoint, CheckpointData)
        assert checkpoint.global_position == position

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_independent_positions(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that different subscriptions have independent positions."""
        sub1 = "Subscription1"
        sub2 = "Subscription2"
        pos1 = 100
        pos2 = 200

        await repo.save_position(sub1, pos1, uuid4(), "Event1")
        await repo.save_position(sub2, pos2, uuid4(), "Event2")

        assert await repo.get_position(sub1) == pos1
        assert await repo.get_position(sub2) == pos2

    @pytest.mark.asyncio
    async def test_get_position_returns_none_after_reset(self, repo: InMemoryCheckpointRepository):
        """Test that get_position returns None after reset_checkpoint."""
        subscription_id = "TestSubscription"

        await repo.save_position(subscription_id, 100, uuid4(), "Event")
        assert await repo.get_position(subscription_id) == 100

        await repo.reset_checkpoint(subscription_id)
        assert await repo.get_position(subscription_id) is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_does_not_set_position(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that update_checkpoint does not set global_position."""
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
        position = 500
        await repo.save_position(subscription_id, position, uuid4(), "Event2")
        assert await repo.get_position(subscription_id) == position

        # Check events_processed was incremented
        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].events_processed == 2

    @pytest.mark.asyncio
    async def test_save_position_sets_last_processed_at(self, repo: InMemoryCheckpointRepository):
        """Test that save_position sets last_processed_at timestamp."""
        subscription_id = "TestSubscription"

        await repo.save_position(subscription_id, 100, uuid4(), "Event")

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
            await repo.save_position(subscription_id, i, uuid4(), f"Event{i}")

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
        positions: list[int | None] = []

        async def writer(i: int):
            await repo.save_position(subscription_id, i * 10, uuid4(), f"Event{i}")

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
# SQLiteCheckpointRepository Position Tests
# ============================================================================

# Check if aiosqlite is available
try:
    import aiosqlite

    from eventsource.repositories.checkpoint import SQLiteCheckpointRepository

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    SQLiteCheckpointRepository = None  # type: ignore[assignment,misc]
    AIOSQLITE_AVAILABLE = False


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteCheckpointRepositoryPosition:
    """Tests for SQLiteCheckpointRepository position methods."""

    @pytest.fixture
    async def db_connection(self) -> "aiosqlite.Connection":
        """Create an in-memory SQLite database with schema for each test."""
        conn = await aiosqlite.connect(":memory:")

        # Create the projection_checkpoints table with global_position column
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS projection_checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_event_id TEXT,
                last_event_type TEXT,
                last_processed_at TEXT,
                global_position INTEGER,
                events_processed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        await conn.commit()

        yield conn

        await conn.close()

    @pytest.fixture
    def repo(self, db_connection: "aiosqlite.Connection") -> SQLiteCheckpointRepository:
        """Create a SQLiteCheckpointRepository for each test."""
        return SQLiteCheckpointRepository(db_connection)

    @pytest.mark.asyncio
    async def test_get_position_returns_none_for_nonexistent(
        self, repo: SQLiteCheckpointRepository
    ):
        """Test that get_position returns None for non-existent checkpoint."""
        result = await repo.get_position("NonExistentSubscription")
        assert result is None

    @pytest.mark.asyncio
    async def test_save_position_creates_new_checkpoint(self, repo: SQLiteCheckpointRepository):
        """Test that save_position creates a new checkpoint with position."""
        subscription_id = "TestSubscription"
        event_id = uuid4()
        event_type = "TestEvent"
        position = 100

        await repo.save_position(subscription_id, position, event_id, event_type)

        result = await repo.get_position(subscription_id)
        assert result == position

    @pytest.mark.asyncio
    async def test_save_position_updates_existing_checkpoint(
        self, repo: SQLiteCheckpointRepository
    ):
        """Test that save_position updates existing checkpoint position."""
        subscription_id = "TestSubscription"
        first_position = 100
        second_position = 200

        await repo.save_position(subscription_id, first_position, uuid4(), "Event1")
        await repo.save_position(subscription_id, second_position, uuid4(), "Event2")

        result = await repo.get_position(subscription_id)
        assert result == second_position

    @pytest.mark.asyncio
    async def test_save_position_increments_events_processed(
        self, repo: SQLiteCheckpointRepository
    ):
        """Test that events_processed increments on each save_position call."""
        subscription_id = "TestSubscription"

        # Save position three times
        for i in range(3):
            await repo.save_position(subscription_id, i * 100, uuid4(), f"Event{i}")

        # Check count
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 3

    @pytest.mark.asyncio
    async def test_save_position_updates_event_id_and_type(self, repo: SQLiteCheckpointRepository):
        """Test that save_position updates event_id and event_type."""
        subscription_id = "TestSubscription"
        event_id = uuid4()
        event_type = "TestEventType"
        position = 100

        await repo.save_position(subscription_id, position, event_id, event_type)

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]
        assert checkpoint.last_event_id == event_id
        assert checkpoint.last_event_type == event_type

    @pytest.mark.asyncio
    async def test_get_position_returns_correct_value_after_save(
        self, repo: SQLiteCheckpointRepository
    ):
        """Test that get_position returns correct value after save_position."""
        subscription_id = "TestSubscription"
        positions = [10, 50, 100, 500, 1000]

        for pos in positions:
            await repo.save_position(subscription_id, pos, uuid4(), "Event")
            result = await repo.get_position(subscription_id)
            assert result == pos

    @pytest.mark.asyncio
    async def test_checkpoint_data_includes_global_position(self, repo: SQLiteCheckpointRepository):
        """Test that CheckpointData includes global_position field."""
        subscription_id = "TestSubscription"
        position = 42

        await repo.save_position(subscription_id, position, uuid4(), "TestEvent")

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]

        assert isinstance(checkpoint, CheckpointData)
        assert checkpoint.global_position == position

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_independent_positions(
        self, repo: SQLiteCheckpointRepository
    ):
        """Test that different subscriptions have independent positions."""
        sub1 = "Subscription1"
        sub2 = "Subscription2"
        pos1 = 100
        pos2 = 200

        await repo.save_position(sub1, pos1, uuid4(), "Event1")
        await repo.save_position(sub2, pos2, uuid4(), "Event2")

        assert await repo.get_position(sub1) == pos1
        assert await repo.get_position(sub2) == pos2

    @pytest.mark.asyncio
    async def test_get_position_returns_none_after_reset(self, repo: SQLiteCheckpointRepository):
        """Test that get_position returns None after reset_checkpoint."""
        subscription_id = "TestSubscription"

        await repo.save_position(subscription_id, 100, uuid4(), "Event")
        assert await repo.get_position(subscription_id) == 100

        await repo.reset_checkpoint(subscription_id)
        assert await repo.get_position(subscription_id) is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_does_not_set_position(self, repo: SQLiteCheckpointRepository):
        """Test that update_checkpoint does not set global_position."""
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
    async def test_save_position_after_update_checkpoint(self, repo: SQLiteCheckpointRepository):
        """Test save_position works after update_checkpoint was used."""
        subscription_id = "TestSubscription"

        # First use update_checkpoint (no position)
        await repo.update_checkpoint(subscription_id, uuid4(), "Event1")
        assert await repo.get_position(subscription_id) is None

        # Then use save_position
        position = 500
        await repo.save_position(subscription_id, position, uuid4(), "Event2")
        assert await repo.get_position(subscription_id) == position

        # Check events_processed was incremented
        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].events_processed == 2

    @pytest.mark.asyncio
    async def test_save_position_sets_last_processed_at(self, repo: SQLiteCheckpointRepository):
        """Test that save_position sets last_processed_at timestamp."""
        subscription_id = "TestSubscription"

        await repo.save_position(subscription_id, 100, uuid4(), "Event")

        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].last_processed_at is not None
        assert isinstance(checkpoints[0].last_processed_at, datetime)
