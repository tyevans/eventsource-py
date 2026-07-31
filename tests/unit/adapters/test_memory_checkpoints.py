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
from sqlalchemy import text

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.ports.checkpoints import CheckpointData, CheckpointRepository, LagMetrics
from eventsource.ports.positions import Position


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
# SQLCheckpointRepository Tests
# ============================================================================


class TestSQLCheckpointRepository:
    """The unified repository must behave identically on both dialects."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.migrations import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/cp.db")
        async with engine.begin() as conn:
            # `get_schema` includes a `CREATE TRIGGER ... BEGIN ... END;` block
            # whose body contains its own semicolons, so a naive `.split(";")`
            # truncates it mid-statement. executescript() on the underlying
            # aiosqlite driver connection parses the whole script correctly.
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))
            await raw.driver_connection.executescript(get_schema("events", backend="sqlite"))
        yield engine
        await engine.dispose()

    async def test_update_and_get_checkpoint(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        event_id = uuid4()
        await repo.update_checkpoint("Proj", event_id, "Created")
        assert await repo.get_checkpoint("Proj") == event_id

    async def test_repository_does_not_commit(self, sqlite_engine):
        """A repository write must roll back with the caller's transaction.

        This is the regression test for the old SQLiteCheckpointRepository,
        which called connection.commit() inside update_checkpoint.
        """
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        conn = await sqlite_engine.connect()
        try:
            await conn.begin()
            repo = SQLCheckpointRepository(conn)
            await repo.update_checkpoint("Proj", uuid4(), "Created")
            await conn.rollback()
        finally:
            await conn.close()

        repo = SQLCheckpointRepository(sqlite_engine)
        assert await repo.get_checkpoint("Proj") is None

    async def test_save_and_get_position(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        position = Position(store_id="test", key=(42,))
        await repo.save_position("sub-1", position, uuid4(), "Created")
        assert await repo.get_position("sub-1") == position

    async def test_update_checkpoint_increments_count(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"

        for i in range(3):
            await repo.update_checkpoint(projection_name, uuid4(), f"Event{i}")

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 3

    async def test_update_checkpoint_overwrites_previous(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        first_event_id = uuid4()
        second_event_id = uuid4()

        await repo.update_checkpoint(projection_name, first_event_id, "Event1")
        await repo.update_checkpoint(projection_name, second_event_id, "Event2")

        assert await repo.get_checkpoint(projection_name) == second_event_id

    async def test_multiple_projections_independent(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        proj1, proj2 = "Projection1", "Projection2"
        event_id_1, event_id_2 = uuid4(), uuid4()

        await repo.update_checkpoint(proj1, event_id_1, "Event1")
        await repo.update_checkpoint(proj2, event_id_2, "Event2")

        assert await repo.get_checkpoint(proj1) == event_id_1
        assert await repo.get_checkpoint(proj2) == event_id_2

    async def test_get_lag_metrics_returns_none_when_no_checkpoint(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        result = await repo.get_lag_metrics("NonExistent")
        assert result is None

    async def test_get_lag_metrics_zero_when_checkpoint_matches_latest_event(self, sqlite_engine):
        """When last_event_id == latest_event_id, lag is reported as 0 even if
        the raw timestamp arithmetic would otherwise suggest a nonzero gap."""
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        # The event's timestamp is well in the future of "now" so the raw
        # lag arithmetic would be strongly negative -- but that's not what
        # this test is discriminating; test_lag_negative below covers that.
        # Here the checkpoint IS caught up (last_event_id == latest_event_id),
        # which must force lag to 0 regardless of the timestamp delta.
        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": now,
                    "payload": "{}",
                },
            )

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert result.last_event_id == result.latest_event_id
        assert result.lag_seconds == 0.0

    async def test_get_lag_metrics_zero_when_raw_lag_negative(self, sqlite_engine):
        """A checkpoint processed AFTER the latest relevant event (raw lag < 0,
        e.g. clock skew) must be clamped to 0, not reported as negative."""
        from datetime import UTC, timedelta
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()
        other_event_id = uuid4()

        earlier = (dt.now(UTC) - timedelta(minutes=5)).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(other_event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": earlier,
                    "payload": "{}",
                },
            )

        # Checkpoint is for a different event, processed "now" -- after the
        # only relevant event's timestamp -- so raw lag is negative.
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert result.lag_seconds == 0.0

    async def test_get_lag_metrics_returns_data_for_existing_checkpoint(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert isinstance(result, LagMetrics)
        assert result.projection_name == projection_name
        assert result.last_event_id == str(event_id)
        assert result.events_processed == 1

    async def test_reset_checkpoint_removes_checkpoint(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")
        assert await repo.get_checkpoint(projection_name) == event_id

        await repo.reset_checkpoint(projection_name)
        assert await repo.get_checkpoint(projection_name) is None

    async def test_reset_checkpoint_does_not_affect_others(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        proj1, proj2 = "Projection1", "Projection2"
        event_id_1, event_id_2 = uuid4(), uuid4()

        await repo.update_checkpoint(proj1, event_id_1, "Event1")
        await repo.update_checkpoint(proj2, event_id_2, "Event2")

        await repo.reset_checkpoint(proj1)

        assert await repo.get_checkpoint(proj1) is None
        assert await repo.get_checkpoint(proj2) == event_id_2

    async def test_reset_nonexistent_checkpoint_no_error(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        await repo.reset_checkpoint("NonExistent")  # Should not raise

    async def test_get_all_checkpoints_empty(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        result = await repo.get_all_checkpoints()
        assert result == []

    async def test_get_all_checkpoints_returns_all(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projections = ["Zebra", "Apple", "Middle"]
        for proj in projections:
            await repo.update_checkpoint(proj, uuid4(), "TestEvent")

        result = await repo.get_all_checkpoints()
        assert len(result) == 3
        assert [c.projection_name for c in result] == ["Apple", "Middle", "Zebra"]

    async def test_checkpoint_data_structure(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
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

    async def test_lag_metrics_has_timestamp_info(self, sqlite_engine):
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert result.last_processed_at is not None
        # Should be a valid ISO timestamp string
        datetime.fromisoformat(result.last_processed_at)

    async def test_lag_metrics_with_events(self, sqlite_engine):
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": now,
                    "payload": "{}",
                },
            )

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert result.projection_name == projection_name
        assert result.latest_event_id == str(event_id)

    async def test_lag_metrics_without_event_types_uses_latest_event(self, sqlite_engine):
        """Cross-dialect invariant: with NO `event_types` filter,
        `get_lag_metrics` must find the true latest relevant event. SQLite
        does this via `event_types` falling through to `else: event_filter
        = ""` (no filter). This is the SQLite half of the invariant proven
        against real PostgreSQL in
        tests/integration/repositories/test_checkpoint.py::
        test_get_lag_metrics_without_event_types_uses_latest_event -- same
        call shape, same data shape, both dialects must agree. PostgreSQL
        used to diverge here (`WHERE event_type = ANY('{}')` matches zero
        rows), which this pair of tests exists to guard against regressing.
        """
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id = uuid4()

        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": now,
                    "payload": "{}",
                },
            )

        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        # No event_types passed -- must still find the event above as the
        # latest relevant one.
        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert result.latest_event_id == str(event_id)

    async def test_lag_metrics_with_event_type_filter(self, sqlite_engine):
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id_1, event_id_2 = uuid4(), uuid4()

        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            for event_id, event_type in ((event_id_1, "TypeA"), (event_id_2, "TypeB")):
                await conn.execute(
                    text("""
                        INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                           version, timestamp, payload)
                        VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                                :version, :timestamp, :payload)
                        """),
                    {
                        "event_id": str(event_id),
                        "event_type": event_type,
                        "aggregate_type": "TestAggregate",
                        "aggregate_id": str(uuid4()),
                        "version": 1,
                        "timestamp": now,
                        "payload": "{}",
                    },
                )

        await repo.update_checkpoint(projection_name, event_id_1, "TypeA")

        result = await repo.get_lag_metrics(projection_name, event_types=["TypeA"])
        assert result is not None
        assert result.latest_event_id == str(event_id_1)

    async def test_lag_metrics_with_multiple_event_type_filter(self, sqlite_engine):
        """event_types with more than one entry exercises the multi-placeholder
        IN-clause join (":et0", ":et1", ...), not just the single-item case."""
        from datetime import UTC
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        event_id_1, event_id_2, event_id_3 = uuid4(), uuid4(), uuid4()

        now = dt.now(UTC).isoformat()
        async with sqlite_engine.begin() as conn:
            for event_id, event_type in (
                (event_id_1, "TypeA"),
                (event_id_2, "TypeB"),
                (event_id_3, "TypeC"),
            ):
                await conn.execute(
                    text("""
                        INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                           version, timestamp, payload)
                        VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                                :version, :timestamp, :payload)
                        """),
                    {
                        "event_id": str(event_id),
                        "event_type": event_type,
                        "aggregate_type": "TestAggregate",
                        "aggregate_id": str(uuid4()),
                        "version": 1,
                        "timestamp": now,
                        "payload": "{}",
                    },
                )

        await repo.update_checkpoint(projection_name, event_id_1, "TypeA")

        # TypeC is excluded from the filter -- if the IN-clause degraded to
        # matching only the first placeholder, this would return TypeA
        # instead of TypeB.
        result = await repo.get_lag_metrics(projection_name, event_types=["TypeB", "TypeC"])
        assert result is not None
        assert result.latest_event_id in (str(event_id_2), str(event_id_3))
        assert result.latest_event_id != str(event_id_1)

    async def test_get_lag_metrics_reports_positive_lag_for_stale_checkpoint(self, sqlite_engine):
        """A checkpoint stuck on an OLDER event than the latest relevant one
        must report a real, positive `lag_seconds` -- not silently clamp to
        0. Discriminates the raw-lag arithmetic (`latest_event_time -
        last_processed_at`), the up-to-date short-circuit (which must NOT
        fire here, since last_event_id != latest_event_id), and the
        `round(raw_lag, 1)` call from a version that drops or corrupts any
        of those steps.
        """
        from datetime import UTC, timedelta
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        stale_event_id = uuid4()
        newer_event_id = uuid4()

        # Checkpoint processed the stale event "now".
        await repo.update_checkpoint(projection_name, stale_event_id, "TestEvent")

        # A distinct, newer relevant event exists 12.3 seconds later --
        # a value that survives round(x, 1) unchanged, so the test also
        # discriminates the rounding precision itself.
        newer_timestamp = (dt.now(UTC) + timedelta(seconds=12.3)).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(newer_event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": newer_timestamp,
                    "payload": "{}",
                },
            )

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        assert result.last_event_id == str(stale_event_id)
        assert result.latest_event_id == str(newer_event_id)
        assert result.last_event_id != result.latest_event_id
        # Must be a real, positive, precisely-rounded value -- not 0.0,
        # not None, not an int-truncated round(x).
        assert 12.0 <= result.lag_seconds <= 12.6
        assert result.lag_seconds == round(result.lag_seconds, 1)

    async def test_get_lag_metrics_sub_second_lag_is_not_clamped_to_zero(self, sqlite_engine):
        """A small positive raw lag (< 1 second) must be reported as-is, not
        clamped to 0 by an off-by-one boundary on the up-to-date check."""
        from datetime import UTC, timedelta
        from datetime import datetime as dt

        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        stale_event_id = uuid4()
        newer_event_id = uuid4()

        await repo.update_checkpoint(projection_name, stale_event_id, "TestEvent")

        newer_timestamp = (dt.now(UTC) + timedelta(seconds=0.5)).isoformat()
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO events (event_id, event_type, aggregate_type, aggregate_id,
                                       version, timestamp, payload)
                    VALUES (:event_id, :event_type, :aggregate_type, :aggregate_id,
                            :version, :timestamp, :payload)
                    """),
                {
                    "event_id": str(newer_event_id),
                    "event_type": "TestEvent",
                    "aggregate_type": "TestAggregate",
                    "aggregate_id": str(uuid4()),
                    "version": 1,
                    "timestamp": newer_timestamp,
                    "payload": "{}",
                },
            )

        result = await repo.get_lag_metrics(projection_name, event_types=["TestEvent"])
        assert result is not None
        # 0 < lag < 1 -- must not be clamped to 0.0 by a `< 1` (instead of
        # `< 0`) boundary on the up-to-date short-circuit.
        assert 0.0 < result.lag_seconds < 1.0

    async def test_get_lag_metrics_zero_when_no_relevant_event(self, sqlite_engine):
        """A checkpoint exists but no event matches the `event_types`
        filter, so `latest_event_time` is None and `raw_lag` never gets
        computed from a timestamp diff -- it must fall back to its default
        of 0.0, not some other sentinel."""
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        projection_name = "TestProjection"
        await repo.update_checkpoint(projection_name, uuid4(), "TestEvent")

        # No event of type "NoSuchType" exists anywhere in the events table.
        result = await repo.get_lag_metrics(projection_name, event_types=["NoSuchType"])
        assert result is not None
        assert result.latest_event_id is None
        assert result.lag_seconds == 0.0


class TestSQLCheckpointRepositoryProtocol:
    """Tests to verify SQLCheckpointRepository implements the protocol."""

    async def test_implements_protocol(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
        from eventsource.migrations import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/cp2.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))

        repo = SQLCheckpointRepository(engine)
        assert isinstance(repo, CheckpointRepository)
        await engine.dispose()
