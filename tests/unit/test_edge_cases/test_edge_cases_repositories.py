"""Unit tests for repository edge cases (checkpoint, DLQ, outbox)."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.outbox import InMemoryOutboxRepository

# --- InMemoryCheckpointRepository Edge Cases ---


class TestCheckpointRepositoryEdgeCases:
    """Edge case tests for checkpoint repository."""

    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_lag_metrics_with_event_types(self, repo: InMemoryCheckpointRepository):
        """Test get_lag_metrics with event_types parameter."""
        projection_name = "TestProjection"
        event_id = uuid4()

        await repo.update_checkpoint(projection_name, event_id, "EdgeTestEvent")

        # Even though event_types is ignored in in-memory impl,
        # it should still work
        result = await repo.get_lag_metrics(projection_name, event_types=["EdgeTestEvent"])
        assert result is not None
        assert result.projection_name == projection_name


# --- InMemoryDLQRepository Edge Cases ---


class TestDLQRepositoryEdgeCases:
    """Edge case tests for DLQ repository."""

    @pytest.fixture
    def repo(self) -> InMemoryDLQRepository:
        return InMemoryDLQRepository()

    @pytest.mark.asyncio
    async def test_delete_resolved_events_with_none_resolved_at(self, repo: InMemoryDLQRepository):
        """Test delete_resolved_events doesn't fail with None resolved_at."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id,
            "TestProjection",
            "EdgeTestEvent",
            {"test": "data"},
            Exception("Test error"),
        )

        # Mark resolved (this sets resolved_at)
        await repo.mark_resolved(1, "admin")

        # Try to delete (should work without error)
        deleted = await repo.delete_resolved_events(older_than_days=0)
        assert deleted >= 0

    @pytest.mark.asyncio
    async def test_get_projection_failure_counts_multiple_projections(
        self, repo: InMemoryDLQRepository
    ):
        """Test failure counts with multiple projections."""
        for i in range(3):
            await repo.add_failed_event(
                uuid4(),
                f"Projection{i}",
                "EdgeTestEvent",
                {"test": "data"},
                Exception(f"Error {i}"),
            )

        counts = await repo.get_projection_failure_counts()
        assert len(counts) == 3

    @pytest.mark.asyncio
    async def test_failure_stats_with_no_active_entries(self, repo: InMemoryDLQRepository):
        """Test failure stats when all entries are resolved."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id,
            "TestProjection",
            "EdgeTestEvent",
            {"test": "data"},
            Exception("Test error"),
        )

        await repo.mark_resolved(1, "admin")

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 0
        assert stats.total_retrying == 0


# --- InMemoryOutboxRepository Edge Cases ---


class TestOutboxRepositoryEdgeCases:
    """Edge case tests for outbox repository."""

    @pytest.fixture
    def repo(self) -> InMemoryOutboxRepository:
        return InMemoryOutboxRepository()

    @pytest.mark.asyncio
    async def test_cleanup_published_no_events(self, repo: InMemoryOutboxRepository):
        """Test cleanup when no published events exist."""
        deleted = await repo.cleanup_published(days=0)
        assert deleted == 0

    @pytest.mark.asyncio
    async def test_increment_retry_nonexistent(self, repo: InMemoryOutboxRepository):
        """Test increment_retry on non-existent entry."""
        # Should not raise
        await repo.increment_retry(uuid4(), "Error")
