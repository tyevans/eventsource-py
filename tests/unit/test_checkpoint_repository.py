"""
Unit tests for CheckpointRepository implementations.

Tests the InMemoryCheckpointRepository for:
- Checkpoint storage and retrieval
- Update with event tracking
- Lag metrics calculation
- Reset functionality
"""

from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.repositories.checkpoint import (
    CheckpointData,
    CheckpointRepository,
    InMemoryCheckpointRepository,
    LagMetrics,
)


class TestInMemoryCheckpointRepository:
    """Tests for InMemoryCheckpointRepository."""

    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        """Create a fresh repository for each test."""
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_get_checkpoint_returns_none_when_empty(self, repo: InMemoryCheckpointRepository):
        """Test that get_checkpoint returns None for non-existent projection."""
        result = await repo.get_checkpoint("NonExistentProjection")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_and_get_checkpoint(self, repo: InMemoryCheckpointRepository):
        """Test updating and retrieving a checkpoint."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await repo.update_checkpoint(projection_name, event_id, event_type)

        result = await repo.get_checkpoint(projection_name)
        assert result == event_id

    @pytest.mark.asyncio
    async def test_update_checkpoint_increments_count(self, repo: InMemoryCheckpointRepository):
        """Test that updating checkpoint increments the events_processed count."""
        projection_name = "TestProjection"

        # Update three times
        for i in range(3):
            event_id = uuid4()
            await repo.update_checkpoint(projection_name, event_id, f"Event{i}")

        # Check count
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 3

    @pytest.mark.asyncio
    async def test_update_checkpoint_overwrites_previous(self, repo: InMemoryCheckpointRepository):
        """Test that updating checkpoint replaces the previous event_id."""
        projection_name = "TestProjection"
        first_event_id = uuid4()
        second_event_id = uuid4()

        await repo.update_checkpoint(projection_name, first_event_id, "Event1")
        await repo.update_checkpoint(projection_name, second_event_id, "Event2")

        result = await repo.get_checkpoint(projection_name)
        assert result == second_event_id

    @pytest.mark.asyncio
    async def test_multiple_projections_independent(self, repo: InMemoryCheckpointRepository):
        """Test that different projections have independent checkpoints."""
        proj1 = "Projection1"
        proj2 = "Projection2"
        event_id_1 = uuid4()
        event_id_2 = uuid4()

        await repo.update_checkpoint(proj1, event_id_1, "Event1")
        await repo.update_checkpoint(proj2, event_id_2, "Event2")

        assert await repo.get_checkpoint(proj1) == event_id_1
        assert await repo.get_checkpoint(proj2) == event_id_2

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_none_when_no_checkpoint(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that lag metrics returns None for non-existent projection."""
        result = await repo.get_lag_metrics("NonExistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_data_for_existing_checkpoint(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that lag metrics returns data for existing checkpoint."""
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert isinstance(result, LagMetrics)
        assert result.projection_name == projection_name
        assert result.last_event_id == str(event_id)
        assert result.events_processed == 1

    @pytest.mark.asyncio
    async def test_reset_checkpoint_removes_checkpoint(self, repo: InMemoryCheckpointRepository):
        """Test that reset_checkpoint removes the checkpoint."""
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")
        assert await repo.get_checkpoint(projection_name) == event_id

        await repo.reset_checkpoint(projection_name)
        assert await repo.get_checkpoint(projection_name) is None

    @pytest.mark.asyncio
    async def test_reset_checkpoint_does_not_affect_others(
        self, repo: InMemoryCheckpointRepository
    ):
        """Test that reset only affects the specified projection."""
        proj1 = "Projection1"
        proj2 = "Projection2"
        event_id_1 = uuid4()
        event_id_2 = uuid4()

        await repo.update_checkpoint(proj1, event_id_1, "Event1")
        await repo.update_checkpoint(proj2, event_id_2, "Event2")

        await repo.reset_checkpoint(proj1)

        assert await repo.get_checkpoint(proj1) is None
        assert await repo.get_checkpoint(proj2) == event_id_2

    @pytest.mark.asyncio
    async def test_reset_nonexistent_checkpoint_no_error(self, repo: InMemoryCheckpointRepository):
        """Test that resetting non-existent checkpoint doesn't raise error."""
        await repo.reset_checkpoint("NonExistent")  # Should not raise

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_empty(self, repo: InMemoryCheckpointRepository):
        """Test get_all_checkpoints returns empty list when no checkpoints."""
        result = await repo.get_all_checkpoints()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_returns_all(self, repo: InMemoryCheckpointRepository):
        """Test get_all_checkpoints returns all checkpoints sorted by name."""
        projections = ["Zebra", "Apple", "Middle"]
        for proj in projections:
            await repo.update_checkpoint(proj, uuid4(), "TestEvent")

        result = await repo.get_all_checkpoints()
        assert len(result) == 3
        # Should be sorted alphabetically
        assert [c.projection_name for c in result] == ["Apple", "Middle", "Zebra"]

    @pytest.mark.asyncio
    async def test_clear_removes_all_checkpoints(self, repo: InMemoryCheckpointRepository):
        """Test that clear removes all checkpoints."""
        for i in range(3):
            await repo.update_checkpoint(f"Proj{i}", uuid4(), "Event")

        await repo.clear()

        result = await repo.get_all_checkpoints()
        assert result == []

    @pytest.mark.asyncio
    async def test_checkpoint_data_structure(self, repo: InMemoryCheckpointRepository):
        """Test that checkpoint data has correct structure."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEventType"

        await repo.update_checkpoint(projection_name, event_id, event_type)

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]

        assert isinstance(checkpoint, CheckpointData)
        assert checkpoint.projection_name == projection_name
        assert checkpoint.last_event_id == event_id
        assert checkpoint.last_event_type == event_type
        assert checkpoint.events_processed == 1
        assert checkpoint.last_processed_at is not None

    @pytest.mark.asyncio
    async def test_lag_metrics_has_timestamp_info(self, repo: InMemoryCheckpointRepository):
        """Test that lag metrics includes timestamp information."""
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert result.last_processed_at is not None
        # Should be a valid ISO timestamp string
        datetime.fromisoformat(result.last_processed_at)


class TestCheckpointRepositoryProtocol:
    """Tests to verify InMemoryCheckpointRepository implements the protocol."""

    def test_implements_protocol(self):
        """Test that InMemoryCheckpointRepository implements CheckpointRepository protocol."""
        repo = InMemoryCheckpointRepository()
        # The protocol is runtime checkable
        assert isinstance(repo, CheckpointRepository)


class TestInMemoryCheckpointRepositoryConcurrency:
    """Tests for concurrent access to InMemoryCheckpointRepository.

    These tests verify that asyncio.Lock properly serializes concurrent
    operations without deadlocks or data corruption.
    """

    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        """Create a fresh repository for each test."""
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_concurrent_updates_no_deadlock(self, repo: InMemoryCheckpointRepository):
        """Test that 100 concurrent updates complete without deadlock."""
        import asyncio

        projection_name = "TestProjection"
        num_updates = 100

        async def update_checkpoint(i: int):
            event_id = uuid4()
            await repo.update_checkpoint(projection_name, event_id, f"Event{i}")

        # Run 100 concurrent updates
        tasks = [update_checkpoint(i) for i in range(num_updates)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify events_processed count
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == num_updates

    @pytest.mark.asyncio
    async def test_concurrent_read_write(self, repo: InMemoryCheckpointRepository):
        """Test mixed concurrent reads and writes complete without issues."""
        import asyncio

        projection_name = "TestProjection"
        num_operations = 50
        read_results: list[bool] = []

        async def writer(i: int):
            event_id = uuid4()
            await repo.update_checkpoint(projection_name, event_id, f"Event{i}")

        async def reader():
            result = await repo.get_checkpoint(projection_name)
            read_results.append(result is not None or True)

        # Interleave reads and writes
        tasks = []
        for i in range(num_operations):
            tasks.append(writer(i))
            tasks.append(reader())

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # All operations completed
        assert len(read_results) == num_operations

    @pytest.mark.asyncio
    async def test_concurrent_multiple_projections(self, repo: InMemoryCheckpointRepository):
        """Test concurrent updates to different projections."""
        import asyncio

        num_projections = 10
        updates_per_projection = 10

        async def update_projection(proj_id: int, update_id: int):
            event_id = uuid4()
            await repo.update_checkpoint(
                f"Projection{proj_id}",
                event_id,
                f"Event{update_id}",
            )

        tasks = []
        for proj in range(num_projections):
            for update in range(updates_per_projection):
                tasks.append(update_projection(proj, update))

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify all projections exist with correct counts
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == num_projections
        for checkpoint in checkpoints:
            assert checkpoint.events_processed == updates_per_projection

    @pytest.mark.asyncio
    async def test_concurrent_reset_and_update(self, repo: InMemoryCheckpointRepository):
        """Test concurrent reset and update operations."""
        import asyncio

        projection_name = "TestProjection"

        # First, create the checkpoint
        await repo.update_checkpoint(projection_name, uuid4(), "Initial")

        async def updater():
            for _ in range(10):
                await repo.update_checkpoint(projection_name, uuid4(), "Update")

        async def resetter():
            for _ in range(3):
                await repo.reset_checkpoint(projection_name)
                await asyncio.sleep(0.001)

        # Run concurrent updates and resets - should not deadlock
        await asyncio.wait_for(
            asyncio.gather(updater(), resetter()),
            timeout=5.0,
        )


# ============================================================================
# SQLiteCheckpointRepository Tests
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
class TestSQLiteCheckpointRepository:
    """Tests for SQLiteCheckpointRepository using in-memory SQLite database."""

    @pytest.fixture
    async def db_connection(self) -> "aiosqlite.Connection":
        """Create an in-memory SQLite database with schema for each test."""
        conn = await aiosqlite.connect(":memory:")

        # Create the projection_checkpoints table
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

        # Create the events table for lag metrics tests
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                tenant_id TEXT,
                actor_id TEXT,
                version INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
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
    async def test_get_checkpoint_returns_none_when_empty(self, repo: SQLiteCheckpointRepository):
        """Test that get_checkpoint returns None for non-existent projection."""
        result = await repo.get_checkpoint("NonExistentProjection")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_and_get_checkpoint(self, repo: SQLiteCheckpointRepository):
        """Test updating and retrieving a checkpoint."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await repo.update_checkpoint(projection_name, event_id, event_type)

        result = await repo.get_checkpoint(projection_name)
        assert result == event_id

    @pytest.mark.asyncio
    async def test_update_checkpoint_increments_count(self, repo: SQLiteCheckpointRepository):
        """Test that updating checkpoint increments the events_processed count."""
        projection_name = "TestProjection"

        # Update three times
        for i in range(3):
            event_id = uuid4()
            await repo.update_checkpoint(projection_name, event_id, f"Event{i}")

        # Check count
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 3

    @pytest.mark.asyncio
    async def test_update_checkpoint_overwrites_previous(self, repo: SQLiteCheckpointRepository):
        """Test that updating checkpoint replaces the previous event_id."""
        projection_name = "TestProjection"
        first_event_id = uuid4()
        second_event_id = uuid4()

        await repo.update_checkpoint(projection_name, first_event_id, "Event1")
        await repo.update_checkpoint(projection_name, second_event_id, "Event2")

        result = await repo.get_checkpoint(projection_name)
        assert result == second_event_id

    @pytest.mark.asyncio
    async def test_multiple_projections_independent(self, repo: SQLiteCheckpointRepository):
        """Test that different projections have independent checkpoints."""
        proj1 = "Projection1"
        proj2 = "Projection2"
        event_id_1 = uuid4()
        event_id_2 = uuid4()

        await repo.update_checkpoint(proj1, event_id_1, "Event1")
        await repo.update_checkpoint(proj2, event_id_2, "Event2")

        assert await repo.get_checkpoint(proj1) == event_id_1
        assert await repo.get_checkpoint(proj2) == event_id_2

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_none_when_no_checkpoint(
        self, repo: SQLiteCheckpointRepository
    ):
        """Test that lag metrics returns None for non-existent projection."""
        result = await repo.get_lag_metrics("NonExistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_data_for_existing_checkpoint(
        self, repo: SQLiteCheckpointRepository
    ):
        """Test that lag metrics returns data for existing checkpoint."""
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert isinstance(result, LagMetrics)
        assert result.projection_name == projection_name
        assert result.last_event_id == str(event_id)
        assert result.events_processed == 1

    @pytest.mark.asyncio
    async def test_reset_checkpoint_removes_checkpoint(self, repo: SQLiteCheckpointRepository):
        """Test that reset_checkpoint removes the checkpoint."""
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")
        assert await repo.get_checkpoint(projection_name) == event_id

        await repo.reset_checkpoint(projection_name)
        assert await repo.get_checkpoint(projection_name) is None

    @pytest.mark.asyncio
    async def test_reset_checkpoint_does_not_affect_others(self, repo: SQLiteCheckpointRepository):
        """Test that reset only affects the specified projection."""
        proj1 = "Projection1"
        proj2 = "Projection2"
        event_id_1 = uuid4()
        event_id_2 = uuid4()

        await repo.update_checkpoint(proj1, event_id_1, "Event1")
        await repo.update_checkpoint(proj2, event_id_2, "Event2")

        await repo.reset_checkpoint(proj1)

        assert await repo.get_checkpoint(proj1) is None
        assert await repo.get_checkpoint(proj2) == event_id_2

    @pytest.mark.asyncio
    async def test_reset_nonexistent_checkpoint_no_error(self, repo: SQLiteCheckpointRepository):
        """Test that resetting non-existent checkpoint doesn't raise error."""
        await repo.reset_checkpoint("NonExistent")  # Should not raise

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_empty(self, repo: SQLiteCheckpointRepository):
        """Test get_all_checkpoints returns empty list when no checkpoints."""
        result = await repo.get_all_checkpoints()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_returns_all(self, repo: SQLiteCheckpointRepository):
        """Test get_all_checkpoints returns all checkpoints sorted by name."""
        projections = ["Zebra", "Apple", "Middle"]
        for proj in projections:
            await repo.update_checkpoint(proj, uuid4(), "TestEvent")

        result = await repo.get_all_checkpoints()
        assert len(result) == 3
        # Should be sorted alphabetically
        assert [c.projection_name for c in result] == ["Apple", "Middle", "Zebra"]

    @pytest.mark.asyncio
    async def test_checkpoint_data_structure(self, repo: SQLiteCheckpointRepository):
        """Test that checkpoint data has correct structure."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEventType"

        await repo.update_checkpoint(projection_name, event_id, event_type)

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]

        assert isinstance(checkpoint, CheckpointData)
        assert checkpoint.projection_name == projection_name
        assert checkpoint.last_event_id == event_id
        assert checkpoint.last_event_type == event_type
        assert checkpoint.events_processed == 1
        assert checkpoint.last_processed_at is not None

    @pytest.mark.asyncio
    async def test_lag_metrics_has_timestamp_info(self, repo: SQLiteCheckpointRepository):
        """Test that lag metrics includes timestamp information."""
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert result.last_processed_at is not None
        # Should be a valid ISO timestamp string
        datetime.fromisoformat(result.last_processed_at)

    @pytest.mark.asyncio
    async def test_lag_metrics_with_events(
        self, repo: "SQLiteCheckpointRepository", db_connection: "aiosqlite.Connection"
    ):
        """Test that lag metrics can calculate lag against events table."""
        from datetime import UTC
        from datetime import datetime as dt

        projection_name = "TestProjection"
        event_id = uuid4()

        # Add an event to the events table
        now = dt.now(UTC).isoformat()
        await db_connection.execute(
            """
            INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                               version, timestamp, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (str(event_id), "TestEvent", "TestAggregate", str(uuid4()), 1, now, "{}"),
        )
        await db_connection.commit()

        # Update checkpoint
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        # Get lag metrics
        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert result.projection_name == projection_name
        assert result.latest_event_id == str(event_id)

    @pytest.mark.asyncio
    async def test_lag_metrics_with_event_type_filter(
        self, repo: "SQLiteCheckpointRepository", db_connection: "aiosqlite.Connection"
    ):
        """Test that lag metrics correctly filters by event types."""
        from datetime import UTC
        from datetime import datetime as dt

        projection_name = "TestProjection"
        event_id_1 = uuid4()
        event_id_2 = uuid4()

        # Add events of different types
        now = dt.now(UTC).isoformat()
        await db_connection.execute(
            """
            INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                               version, timestamp, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (str(event_id_1), "TypeA", "TestAggregate", str(uuid4()), 1, now, "{}"),
        )
        await db_connection.execute(
            """
            INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                               version, timestamp, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (str(event_id_2), "TypeB", "TestAggregate", str(uuid4()), 1, now, "{}"),
        )
        await db_connection.commit()

        # Update checkpoint
        await repo.update_checkpoint(projection_name, event_id_1, "TypeA")

        # Get lag metrics filtered to TypeA only
        result = await repo.get_lag_metrics(projection_name, event_types=["TypeA"])
        assert result is not None
        assert result.latest_event_id == str(event_id_1)


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteCheckpointRepositoryProtocol:
    """Tests to verify SQLiteCheckpointRepository implements the protocol."""

    @pytest.fixture
    async def db_connection(self) -> "aiosqlite.Connection":
        """Create an in-memory SQLite database with schema."""
        conn = await aiosqlite.connect(":memory:")

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

    def test_implements_protocol(self, db_connection: "aiosqlite.Connection"):
        """Test that SQLiteCheckpointRepository implements CheckpointRepository protocol."""
        repo = SQLiteCheckpointRepository(db_connection)
        # The protocol is runtime checkable
        assert isinstance(repo, CheckpointRepository)
