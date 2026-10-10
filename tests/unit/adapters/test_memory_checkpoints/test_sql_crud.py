"""Unit tests for SQLCheckpointRepository CRUD operations, positions, resets, and protocol."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.ports.checkpoints import CheckpointData, CheckpointRepository
from eventsource.ports.positions import Position


class TestSQLCheckpointRepositoryCrud:
    """The unified repository must behave identically on both dialects - CRUD & basic ops."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.adapters.sql.schemas import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/cp.db")
        async with engine.begin() as conn:
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


class TestSQLCheckpointRepositoryProtocol:
    """Tests to verify SQLCheckpointRepository implements the protocol."""

    async def test_implements_protocol(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
        from eventsource.adapters.sql.schemas import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/cp2.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))

        repo = SQLCheckpointRepository(engine)
        assert isinstance(repo, CheckpointRepository)
        await engine.dispose()
