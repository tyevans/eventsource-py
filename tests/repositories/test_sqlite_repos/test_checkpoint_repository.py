"""Tests for SQLCheckpointRepository against SQLite."""

from __future__ import annotations

from uuid import uuid4

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
    from eventsource.ports.checkpoints import CheckpointRepository

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


class TestSQLCheckpointRepositoryProtocol:
    """Verify SQLCheckpointRepository implements the protocol."""

    def test_implements_protocol(self, sqlite_checkpoint_repo: SQLCheckpointRepository) -> None:
        assert isinstance(sqlite_checkpoint_repo, CheckpointRepository)


class TestSQLCheckpointRepositoryGetCheckpoint:
    """Tests for get_checkpoint method."""

    async def test_get_checkpoint_returns_none_for_new_projection(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        checkpoint = await sqlite_checkpoint_repo.get_checkpoint("NewProjection")
        assert checkpoint is None

    async def test_get_checkpoint_returns_saved_checkpoint(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        event_id = uuid4()
        projection_name = "TestProjection"
        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id, event_type="TestEvent"
        )
        checkpoint = await sqlite_checkpoint_repo.get_checkpoint(projection_name)
        assert checkpoint == event_id


class TestSQLCheckpointRepositoryUpdateCheckpoint:
    """Tests for update_checkpoint method."""

    async def test_update_checkpoint_creates_new(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        event_id = uuid4()
        projection_name = "NewProjection"
        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id, event_type="FirstEvent"
        )
        checkpoint = await sqlite_checkpoint_repo.get_checkpoint(projection_name)
        assert checkpoint == event_id

    async def test_update_checkpoint_updates_existing(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name = "UpdatedProjection"
        event_id_1, event_id_2 = uuid4(), uuid4()
        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id_1, event_type="FirstEvent"
        )
        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id_2, event_type="SecondEvent"
        )
        checkpoint = await sqlite_checkpoint_repo.get_checkpoint(projection_name)
        assert checkpoint == event_id_2

    async def test_update_checkpoint_preserves_processed_count(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name = "CountingProjection"
        for i in range(5):
            await sqlite_checkpoint_repo.update_checkpoint(
                projection_name=projection_name, event_id=uuid4(), event_type=f"Event{i}"
            )
        checkpoints = await sqlite_checkpoint_repo.get_all_checkpoints()
        checkpoint = next((cp for cp in checkpoints if cp.projection_name == projection_name), None)
        assert checkpoint is not None
        assert checkpoint.events_processed == 5


class TestSQLCheckpointRepositoryGetAllCheckpoints:
    """Tests for get_all_checkpoints method."""

    async def test_get_all_checkpoints_empty(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        checkpoints = await sqlite_checkpoint_repo.get_all_checkpoints()
        assert checkpoints == []

    async def test_get_all_checkpoints_returns_all(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projections = ["Projection1", "Projection2", "Projection3"]
        for projection in projections:
            await sqlite_checkpoint_repo.update_checkpoint(
                projection_name=projection, event_id=uuid4(), event_type="TestEvent"
            )
        checkpoints = await sqlite_checkpoint_repo.get_all_checkpoints()
        assert len(checkpoints) == 3
        assert {cp.projection_name for cp in checkpoints} == set(projections)


class TestSQLCheckpointRepositoryResetCheckpoint:
    """Tests for reset_checkpoint method."""

    async def test_reset_checkpoint_deletes_checkpoint(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name = "ToBeReset"
        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=uuid4(), event_type="TestEvent"
        )
        assert await sqlite_checkpoint_repo.get_checkpoint(projection_name) is not None
        await sqlite_checkpoint_repo.reset_checkpoint(projection_name)
        assert await sqlite_checkpoint_repo.get_checkpoint(projection_name) is None

    async def test_reset_nonexistent_checkpoint(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        await sqlite_checkpoint_repo.reset_checkpoint("NonExistent")


class TestSQLCheckpointRepositoryGetLagMetrics:
    """Tests for get_lag_metrics method."""

    async def test_get_lag_metrics_returns_none_for_new_projection(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        metrics = await sqlite_checkpoint_repo.get_lag_metrics("NonExistent")
        assert metrics is None

    async def test_get_lag_metrics_returns_metrics_for_existing(
        self, sqlite_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name = "MetricsProjection"
        event_id = uuid4()
        await sqlite_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id, event_type="TestEvent"
        )
        metrics = await sqlite_checkpoint_repo.get_lag_metrics(projection_name)
        assert metrics is not None
        assert metrics.projection_name == projection_name
        assert metrics.last_event_id == str(event_id)
        assert metrics.events_processed == 1
