"""
Integration tests for PostgreSQL Checkpoint Repository.

These tests verify actual database operations for checkpoint tracking including:
- Checkpoint creation and retrieval
- Checkpoint updates with UPSERT
- Lag metrics calculation
- Checkpoint reset operations
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from eventsource import SQLCheckpointRepository
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion

from ..conftest import (
    TestItemCreated,
    skip_if_no_postgres_infra,
)

if TYPE_CHECKING:
    pass


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestSQLCheckpointRepositoryBasics:
    """Basic checkpoint repository operations."""

    async def test_get_checkpoint_nonexistent(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test getting a checkpoint that doesn't exist returns None."""
        result = await postgres_checkpoint_repo.get_checkpoint("NonExistentProjection")
        assert result is None

    async def test_update_and_get_checkpoint(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test updating and retrieving a checkpoint."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestItemCreated"

        # Update checkpoint
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id,
            event_type=event_type,
        )

        # Retrieve checkpoint
        result = await postgres_checkpoint_repo.get_checkpoint(projection_name)
        assert result == event_id

    async def test_update_checkpoint_increments_count(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test that updating checkpoint increments event count."""
        projection_name = "CountingProjection"

        # Update multiple times
        for _ in range(5):
            event_id = uuid4()
            await postgres_checkpoint_repo.update_checkpoint(
                projection_name=projection_name,
                event_id=event_id,
                event_type="TestEvent",
            )

        # Get all checkpoints and verify count
        all_checkpoints = await postgres_checkpoint_repo.get_all_checkpoints()
        checkpoint = next(
            (c for c in all_checkpoints if c.projection_name == projection_name),
            None,
        )

        assert checkpoint is not None
        assert checkpoint.events_processed == 5

    async def test_update_checkpoint_upsert_behavior(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test that update uses UPSERT and updates existing checkpoint."""
        projection_name = "UpsertProjection"
        event_id_1 = uuid4()
        event_id_2 = uuid4()

        # First update
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id_1,
            event_type="TestEvent1",
        )

        result1 = await postgres_checkpoint_repo.get_checkpoint(projection_name)
        assert result1 == event_id_1

        # Second update (should update, not insert)
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id_2,
            event_type="TestEvent2",
        )

        result2 = await postgres_checkpoint_repo.get_checkpoint(projection_name)
        assert result2 == event_id_2

        # Should still be only one checkpoint
        all_checkpoints = await postgres_checkpoint_repo.get_all_checkpoints()
        matching = [c for c in all_checkpoints if c.projection_name == projection_name]
        assert len(matching) == 1


class TestSQLCheckpointRepositoryReset:
    """Tests for checkpoint reset operations."""

    async def test_reset_checkpoint(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test resetting a checkpoint."""
        projection_name = "ResetProjection"
        event_id = uuid4()

        # Create checkpoint
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id,
            event_type="TestEvent",
        )

        # Verify it exists
        result = await postgres_checkpoint_repo.get_checkpoint(projection_name)
        assert result == event_id

        # Reset checkpoint
        await postgres_checkpoint_repo.reset_checkpoint(projection_name)

        # Verify it's gone
        result = await postgres_checkpoint_repo.get_checkpoint(projection_name)
        assert result is None

    async def test_reset_nonexistent_checkpoint(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test resetting a checkpoint that doesn't exist (should not error)."""
        # Should not raise
        await postgres_checkpoint_repo.reset_checkpoint("NonExistentProjection")


class TestSQLCheckpointRepositoryAllCheckpoints:
    """Tests for getting all checkpoints."""

    async def test_get_all_checkpoints_empty(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test getting all checkpoints when none exist."""
        result = await postgres_checkpoint_repo.get_all_checkpoints()
        assert result == []

    async def test_get_all_checkpoints_multiple(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test getting all checkpoints with multiple projections."""
        projections = ["Projection1", "Projection2", "Projection3"]

        for projection in projections:
            await postgres_checkpoint_repo.update_checkpoint(
                projection_name=projection,
                event_id=uuid4(),
                event_type="TestEvent",
            )

        result = await postgres_checkpoint_repo.get_all_checkpoints()

        assert len(result) == 3
        # Should be sorted by name
        names = [c.projection_name for c in result]
        assert names == sorted(names)

    async def test_checkpoint_data_fields(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test that checkpoint data has all expected fields."""
        projection_name = "FieldsProjection"
        event_id = uuid4()
        event_type = "TestItemCreated"

        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id,
            event_type=event_type,
        )

        all_checkpoints = await postgres_checkpoint_repo.get_all_checkpoints()
        checkpoint = next(
            (c for c in all_checkpoints if c.projection_name == projection_name),
            None,
        )

        assert checkpoint is not None
        assert checkpoint.projection_name == projection_name
        assert checkpoint.last_event_id == event_id
        assert checkpoint.last_event_type == event_type
        assert checkpoint.last_processed_at is not None
        assert checkpoint.events_processed == 1


class TestSQLCheckpointRepositoryLagMetrics:
    """Tests for lag metrics calculation."""

    async def test_get_lag_metrics_no_checkpoint(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test getting lag metrics for non-existent projection."""
        result = await postgres_checkpoint_repo.get_lag_metrics("NonExistent")
        assert result is None

    async def test_get_lag_metrics_with_checkpoint(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
    ) -> None:
        """Test getting lag metrics for existing projection."""
        projection_name = "LagProjection"
        event_id = uuid4()

        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event_id,
            event_type="TestEvent",
        )

        result = await postgres_checkpoint_repo.get_lag_metrics(projection_name)

        assert result is not None
        assert result.projection_name == projection_name
        assert result.last_event_id == str(event_id)
        assert result.events_processed == 1
        assert result.last_processed_at is not None

    async def test_get_lag_metrics_with_event_types_filter(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
        postgres_event_store,  # Need event store to create actual events
        sample_aggregate_id,
    ) -> None:
        """Test getting lag metrics with event type filtering."""
        projection_name = "FilteredLagProjection"

        # Create some events in the event store
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await postgres_event_store.append(
            StreamId(aggregate_id=sample_aggregate_id, category="TestItem"),
            [event],
            ExpectedVersion.no_stream(),
        )

        # Update checkpoint
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event.event_id,
            event_type="TestItemCreated",
        )

        # Get lag metrics with filter
        result = await postgres_checkpoint_repo.get_lag_metrics(
            projection_name,
            event_types=["TestItemCreated"],
        )

        assert result is not None
        # When caught up, lag should be 0
        assert result.lag_seconds >= 0

    async def test_get_lag_metrics_without_event_types_uses_latest_event(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
        postgres_event_store,
        sample_aggregate_id,
    ) -> None:
        """With no `event_types` filter, `get_lag_metrics` must find the
        TRUE latest event -- on both dialects. SQLite already gets this
        right (an empty/`None` `event_types` falls through to `else:
        event_filter = ""`, i.e. no filter). PostgreSQL must agree: it must
        NOT take the `ANY(:event_types)` branch when `event_types` is
        empty, because `WHERE event_type = ANY('{}')` matches zero rows in
        PostgreSQL -- silently reporting "no relevant event" (and therefore
        no/wrong lag) for a projection that is, in reality, caught up or
        behind by a knowable amount. This is a monitoring path: a wrong
        answer here doesn't crash, it just lies to whoever is watching."""
        projection_name = "NoFilterLagProjection"

        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await postgres_event_store.append(
            StreamId(aggregate_id=sample_aggregate_id, category="TestItem"),
            [event],
            ExpectedVersion.no_stream(),
        )

        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event.event_id,
            event_type="TestItemCreated",
        )

        # No event_types passed -- must still find the event above as the
        # latest relevant one, exactly as SQLite would with the same data.
        result = await postgres_checkpoint_repo.get_lag_metrics(projection_name)

        assert result is not None
        assert result.latest_event_id == str(event.event_id)

    async def test_get_lag_metrics_reports_nonzero_lag_for_stale_checkpoint(
        self,
        postgres_checkpoint_repo: SQLCheckpointRepository,
        postgres_event_store,
        sample_aggregate_id,
    ) -> None:
        """A checkpoint behind a genuinely newer, different relevant event
        must report a positive lag -- not silently clamp to 0. Exercises the
        real PostgreSQL ANY() filter with a match, plus the raw-lag/rounding
        arithmetic end to end against a real server."""
        import asyncio

        projection_name = "StaleLagProjection"

        stale_event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Stale Item",
            quantity=1,
        )
        await postgres_event_store.append(
            StreamId(aggregate_id=sample_aggregate_id, category="TestItem"),
            [stale_event],
            ExpectedVersion.no_stream(),
        )
        await postgres_checkpoint_repo.update_checkpoint(
            projection_name=projection_name,
            event_id=stale_event.event_id,
            event_type="TestItemCreated",
        )

        # A real, measurable gap between the checkpoint's last_processed_at
        # and the newer event's timestamp.
        await asyncio.sleep(1.1)

        newer_event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=2,
            name="Newer Item",
            quantity=2,
        )
        await postgres_event_store.append(
            StreamId(aggregate_id=sample_aggregate_id, category="TestItem"),
            [newer_event],
            ExpectedVersion.exact(1),
        )

        result = await postgres_checkpoint_repo.get_lag_metrics(
            projection_name,
            event_types=["TestItemCreated"],
        )

        assert result is not None
        assert result.last_event_id != result.latest_event_id
        assert result.latest_event_id == str(newer_event.event_id)
        # Real, positive lag -- not clamped to 0 -- with second-level precision.
        assert result.lag_seconds >= 1.0
