"""Integration tests for basic DLQ operations and status transitions."""

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


class TestSQLDLQRepositoryBasics:
    """Basic DLQ repository operations."""

    async def test_add_failed_event(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test adding a failed event to the DLQ."""
        event_id = uuid4()
        projection_name = "TestProjection"
        event_type = "TestEvent"
        event_data = {"key": "value", "number": 42}
        error = ValueError("Test error message")

        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type=event_type,
            event_data=event_data,
            error=error,
            retry_count=0,
        )

        failed_events = await postgres_dlq_repo.get_failed_events(projection_name=projection_name)

        assert len(failed_events) == 1
        assert failed_events[0].event_id == event_id
        assert failed_events[0].projection_name == projection_name
        assert failed_events[0].event_type == event_type
        assert failed_events[0].error_message == "Test error message"
        assert failed_events[0].status == "failed"

    async def test_add_failed_event_with_stacktrace(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test that stacktrace is captured when adding failed event."""
        event_id = uuid4()

        try:
            raise RuntimeError("Intentional error for test")
        except RuntimeError as e:
            await postgres_dlq_repo.add_failed_event(
                event_id=event_id,
                projection_name="StacktraceProjection",
                event_type="TestEvent",
                event_data={"test": True},
                error=e,
                retry_count=0,
            )

        failed_events = await postgres_dlq_repo.get_failed_events(
            projection_name="StacktraceProjection"
        )

        assert len(failed_events) == 1
        assert failed_events[0].error_stacktrace is not None
        assert "RuntimeError" in failed_events[0].error_stacktrace
        assert "Intentional error for test" in failed_events[0].error_stacktrace

    async def test_add_failed_event_upsert_on_retry(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test that adding same event/projection updates retry count."""
        event_id = uuid4()
        projection_name = "RetryProjection"

        # First failure
        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={"attempt": 1},
            error=ValueError("First failure"),
            retry_count=0,
        )

        # Second failure (same event, same projection)
        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={"attempt": 2},
            error=ValueError("Second failure"),
            retry_count=1,
        )

        failed_events = await postgres_dlq_repo.get_failed_events(projection_name=projection_name)

        assert len(failed_events) == 1
        assert failed_events[0].retry_count == 1
        assert failed_events[0].error_message == "Second failure"


class TestSQLDLQRepositoryStatusTransitions:
    """Tests for DLQ status transition operations."""

    async def test_mark_resolved(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test marking a DLQ entry as resolved."""
        event_id = uuid4()
        resolver_id = "user123"

        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="ResolveProjection",
            event_type="TestEvent",
            event_data={},
            error=ValueError("Error"),
        )

        events = await postgres_dlq_repo.get_failed_events(projection_name="ResolveProjection")
        dlq_id = events[0].id

        await postgres_dlq_repo.mark_resolved(dlq_id, resolver_id)

        result = await postgres_dlq_repo.get_failed_event_by_id(dlq_id)
        assert result is not None
        assert result.status == "resolved"
        assert result.resolved_by == resolver_id
        assert result.resolved_at is not None

    async def test_mark_retrying(
        self,
        postgres_dlq_repo: SQLDLQRepository,
    ) -> None:
        """Test marking a DLQ entry as retrying."""
        event_id = uuid4()

        await postgres_dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="RetryingProjection",
            event_type="TestEvent",
            event_data={},
            error=ValueError("Error"),
        )

        events = await postgres_dlq_repo.get_failed_events(projection_name="RetryingProjection")
        dlq_id = events[0].id

        await postgres_dlq_repo.mark_retrying(dlq_id)

        result = await postgres_dlq_repo.get_failed_event_by_id(dlq_id)
        assert result is not None
        assert result.status == "retrying"
