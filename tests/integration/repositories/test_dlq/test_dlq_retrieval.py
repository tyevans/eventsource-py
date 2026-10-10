"""Integration tests for DLQ retrieval operations."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource import SQLDLQRepository
from tests.integration.conftest import skip_if_no_postgres_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestSQLDLQRepositoryRetrieval:
    """Tests for DLQ retrieval operations."""

    async def test_get_failed_events_by_projection(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test filtering failed events by projection name."""
        # Add events for different projections
        for i, proj in enumerate(["ProjectionA", "ProjectionB", "ProjectionA"]):
            await postgres_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name=proj,
                event_type="TestEvent",
                event_data={"index": i},
                error=ValueError(f"Error {i}"),
            )

        # Get for ProjectionA only
        events_a = await postgres_dlq_repo.get_failed_events(projection_name="ProjectionA")
        assert len(events_a) == 2

        # Get for ProjectionB only
        events_b = await postgres_dlq_repo.get_failed_events(projection_name="ProjectionB")
        assert len(events_b) == 1

    async def test_get_failed_events_by_status(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test filtering failed events by status."""
        event_id = uuid4()
        projection_name = "StatusProjection"

        # Add failed event
        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={},
            error=ValueError("Error"),
        )

        # Get failed events (default status)
        failed = await postgres_dlq_repo.get_failed_events(status="failed")
        assert len(failed) >= 1

        # Mark as retrying
        dlq_id = failed[0].id
        await postgres_dlq_repo.mark_retrying(dlq_id)

        # Get retrying events
        retrying = await postgres_dlq_repo.get_failed_events(status="retrying")
        assert any(e.id == dlq_id for e in retrying)

    async def test_get_failed_events_with_limit(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test limiting number of returned events."""
        # Add 10 events
        for i in range(10):
            await postgres_dlq_repo.add_failed_event(
                event_id=uuid4(),
                projection_name="LimitProjection",
                event_type="TestEvent",
                event_data={"index": i},
                error=ValueError(f"Error {i}"),
            )

        # Get with limit
        events = await postgres_dlq_repo.get_failed_events(
            projection_name="LimitProjection",
            limit=5,
        )

        assert len(events) == 5

    async def test_get_failed_event_by_id(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test retrieving a specific failed event by DLQ ID."""
        event_id = uuid4()

        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="ByIdProjection",
            event_type="TestEvent",
            event_data={"specific": True},
            error=ValueError("Specific error"),
        )

        # Get all to find the ID
        all_events = await postgres_dlq_repo.get_failed_events(projection_name="ByIdProjection")
        dlq_id = all_events[0].id

        # Get by ID
        result = await postgres_dlq_repo.get_failed_event_by_id(dlq_id)

        assert result is not None
        assert result.event_id == event_id
        assert result.projection_name == "ByIdProjection"

    async def test_get_failed_event_by_id_nonexistent(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test getting non-existent event returns None."""
        result = await postgres_dlq_repo.get_failed_event_by_id(999999)
        assert result is None
