"""Integration tests for PostgreSQL Checkpoint Repository."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource import SQLCheckpointRepository
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion

from ..conftest import TestItemCreated, skip_if_no_postgres_infra

pytestmark = [pytest.mark.integration, pytest.mark.postgres, skip_if_no_postgres_infra]


class TestSQLCheckpointRepositoryBasics:
    async def test_get_checkpoint_nonexistent(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        assert await postgres_checkpoint_repo.get_checkpoint("NonExistentProjection") is None

    async def test_update_and_get_checkpoint(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name, event_id, event_type = "TestProjection", uuid4(), "TestItemCreated"
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id, event_type=event_type
        )
        assert await postgres_checkpoint_repo.get_checkpoint(projection_name) == event_id

    async def test_update_checkpoint_increments_count(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name = "CountingProjection"
        for _ in range(5):
            await postgres_checkpoint_repo.update_checkpoint(
                projection_name=projection_name, event_id=uuid4(), event_type="TestEvent"
            )
        all_checkpoints = await postgres_checkpoint_repo.get_all_checkpoints()
        checkpoint = next(
            (c for c in all_checkpoints if c.projection_name == projection_name), None
        )
        assert checkpoint is not None and checkpoint.events_processed == 5

    async def test_update_checkpoint_upsert_behavior(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name = "UpsertProjection"
        event_id_1, event_id_2 = uuid4(), uuid4()
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id_1, event_type="TestEvent1"
        )
        assert await postgres_checkpoint_repo.get_checkpoint(projection_name) == event_id_1
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id_2, event_type="TestEvent2"
        )
        assert await postgres_checkpoint_repo.get_checkpoint(projection_name) == event_id_2
        all_checkpoints = await postgres_checkpoint_repo.get_all_checkpoints()
        assert len([c for c in all_checkpoints if c.projection_name == projection_name]) == 1


class TestSQLCheckpointRepositoryReset:
    async def test_reset_checkpoint(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name, event_id = "ResetProjection", uuid4()
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id, event_type="TestEvent"
        )
        assert await postgres_checkpoint_repo.get_checkpoint(projection_name) == event_id
        await postgres_checkpoint_repo.reset_checkpoint(projection_name)
        assert await postgres_checkpoint_repo.get_checkpoint(projection_name) is None

    async def test_reset_nonexistent_checkpoint(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        await postgres_checkpoint_repo.reset_checkpoint("NonExistentProjection")


class TestSQLCheckpointRepositoryAllCheckpoints:
    async def test_get_all_checkpoints_empty(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        assert await postgres_checkpoint_repo.get_all_checkpoints() == []

    async def test_get_all_checkpoints_multiple(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        for projection in ["Projection1", "Projection2", "Projection3"]:
            await postgres_checkpoint_repo.update_checkpoint(
                projection_name=projection, event_id=uuid4(), event_type="TestEvent"
            )
        result = await postgres_checkpoint_repo.get_all_checkpoints()
        assert len(result) == 3
        names = [c.projection_name for c in result]
        assert names == sorted(names)

    async def test_checkpoint_data_fields(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name, event_id, event_type = "FieldsProjection", uuid4(), "TestItemCreated"
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id, event_type=event_type
        )
        all_checkpoints = await postgres_checkpoint_repo.get_all_checkpoints()
        checkpoint = next(
            (c for c in all_checkpoints if c.projection_name == projection_name), None
        )
        assert checkpoint is not None and checkpoint.projection_name == projection_name
        assert checkpoint.last_event_id == event_id and checkpoint.last_event_type == event_type
        assert checkpoint.last_processed_at is not None and checkpoint.events_processed == 1


class TestSQLCheckpointRepositoryLagMetrics:
    async def test_get_lag_metrics_no_checkpoint(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        assert await postgres_checkpoint_repo.get_lag_metrics("NonExistent") is None

    async def test_get_lag_metrics_with_checkpoint(
        self, postgres_checkpoint_repo: SQLCheckpointRepository
    ) -> None:
        projection_name, event_id = "LagProjection", uuid4()
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event_id, event_type="TestEvent"
        )
        result = await postgres_checkpoint_repo.get_lag_metrics(projection_name)
        assert result is not None and result.projection_name == projection_name
        assert result.last_event_id == str(event_id)
        assert result.events_processed == 1 and result.last_processed_at is not None

    async def test_get_lag_metrics_with_event_types_filter(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
        postgres_event_store,
        sample_aggregate_id,
    ) -> None:
        projection_name = "FilteredLagProjection"
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id, aggregate_version=1, name="Test Item", quantity=10
        )
        stream_id = StreamId(aggregate_id=sample_aggregate_id, category="TestItem")
        await postgres_event_store.append(stream_id, [event], ExpectedVersion.no_stream())
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event.event_id, event_type="TestItemCreated"
        )
        result = await postgres_checkpoint_repo.get_lag_metrics(
            projection_name, event_types=["TestItemCreated"]
        )
        assert result is not None and result.lag_seconds >= 0

    async def test_get_lag_metrics_without_event_types_uses_latest_event(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
        postgres_event_store,
        sample_aggregate_id,
    ) -> None:
        projection_name = "NoFilterLagProjection"
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id, aggregate_version=1, name="Test Item", quantity=10
        )
        stream_id = StreamId(aggregate_id=sample_aggregate_id, category="TestItem")
        await postgres_event_store.append(stream_id, [event], ExpectedVersion.no_stream())
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name, event_id=event.event_id, event_type="TestItemCreated"
        )
        result = await postgres_checkpoint_repo.get_lag_metrics(projection_name)
        assert result is not None and result.latest_event_id == str(event.event_id)

    async def test_get_lag_metrics_reports_nonzero_lag_for_stale_checkpoint(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
        postgres_event_store,
        sample_aggregate_id,
    ) -> None:
        projection_name = "StaleLagProjection"
        stale_event = TestItemCreated(
            aggregate_id=sample_aggregate_id, aggregate_version=1, name="Stale Item", quantity=1
        )
        stream_id = StreamId(aggregate_id=sample_aggregate_id, category="TestItem")
        await postgres_event_store.append(stream_id, [stale_event], ExpectedVersion.no_stream())
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=stale_event.event_id,
            event_type="TestItemCreated",
        )
        await asyncio.sleep(1.1)
        newer_event = TestItemCreated(
            aggregate_id=sample_aggregate_id, aggregate_version=2, name="Newer Item", quantity=2
        )
        await postgres_event_store.append(stream_id, [newer_event], ExpectedVersion.exact(1))
        result = await postgres_checkpoint_repo.get_lag_metrics(
            projection_name, event_types=["TestItemCreated"]
        )
        assert result is not None and result.last_event_id != result.latest_event_id
        assert result.latest_event_id == str(newer_event.event_id) and result.lag_seconds >= 1.0
