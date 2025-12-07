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

from eventsource import PostgreSQLCheckpointRepository

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


class TestPostgreSQLCheckpointRepositoryBasics:
    """Basic checkpoint repository operations."""

    async def test_get_checkpoint_nonexistent(
        self,
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
    ) -> None:
        """Test getting a checkpoint that doesn't exist returns None."""
        result = await postgres_checkpoint_repo.get_checkpoint("NonExistentProjection")
        assert result is None

    async def test_update_and_get_checkpoint(
        self,
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
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
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
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
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
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


class TestPostgreSQLCheckpointRepositoryReset:
    """Tests for checkpoint reset operations."""

    async def test_reset_checkpoint(
        self,
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
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
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
    ) -> None:
        """Test resetting a checkpoint that doesn't exist (should not error)."""
        # Should not raise
        await postgres_checkpoint_repo.reset_checkpoint("NonExistentProjection")


class TestPostgreSQLCheckpointRepositoryAllCheckpoints:
    """Tests for getting all checkpoints."""

    async def test_get_all_checkpoints_empty(
        self,
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
    ) -> None:
        """Test getting all checkpoints when none exist."""
        result = await postgres_checkpoint_repo.get_all_checkpoints()
        assert result == []

    async def test_get_all_checkpoints_multiple(
        self,
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
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
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
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


class TestPostgreSQLCheckpointRepositoryLagMetrics:
    """Tests for lag metrics calculation."""

    async def test_get_lag_metrics_no_checkpoint(
        self,
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
    ) -> None:
        """Test getting lag metrics for non-existent projection."""
        result = await postgres_checkpoint_repo.get_lag_metrics("NonExistent")
        assert result is None

    async def test_get_lag_metrics_with_checkpoint(
        self,
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
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
        postgres_checkpoint_repo: PostgreSQLCheckpointRepository,
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
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event],
            expected_version=0,
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
