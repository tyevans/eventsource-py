"""Unit tests for InMemoryCheckpointRepository implementation and protocol compliance."""

from __future__ import annotations

import asyncio
from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.ports.checkpoints import CheckpointData, CheckpointRepository, LagMetrics


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
        """Test that lag metrics returns data when checkpoint exists."""
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
        """Test that resetting a checkpoint removes it."""
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
        """Test that resetting one checkpoint leaves others intact."""
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
        """Test that resetting non-existent checkpoint does not raise."""
        # Should complete without error
        await repo.reset_checkpoint("NonExistent")

    @pytest.mark.asyncio
    async def test_clear_removes_all_checkpoints(self, repo: InMemoryCheckpointRepository):
        """Test that clear removes all checkpoints."""
        await repo.update_checkpoint("P1", uuid4(), "E1")
        await repo.update_checkpoint("P2", uuid4(), "E2")

        await repo.clear()

        assert await repo.get_checkpoint("P1") is None
        assert await repo.get_checkpoint("P2") is None

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_empty(self, repo: InMemoryCheckpointRepository):
        """Test get_all_checkpoints on empty repository."""
        result = await repo.get_all_checkpoints()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_returns_all(self, repo: InMemoryCheckpointRepository):
        """Test that get_all_checkpoints returns all registered checkpoints."""
        projections = ["Zebra", "Apple", "Middle"]
        for proj in projections:
            await repo.update_checkpoint(proj, uuid4(), "TestEvent")

        result = await repo.get_all_checkpoints()
        assert len(result) == 3
        # Should be ordered by projection name
        assert [c.projection_name for c in result] == ["Apple", "Middle", "Zebra"]

    @pytest.mark.asyncio
    async def test_checkpoint_data_structure(self, repo: InMemoryCheckpointRepository):
        """Test that CheckpointData has all expected fields."""
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
        """Test that LagMetrics includes timestamp information."""
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
        """Test that InMemoryCheckpointRepository is an instance of CheckpointRepository."""
        repo = InMemoryCheckpointRepository()
        assert isinstance(repo, CheckpointRepository)


class TestInMemoryCheckpointRepositoryConcurrency:
    """Tests for concurrent access to InMemoryCheckpointRepository."""

    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        """Create a fresh repository for each test."""
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_concurrent_updates_no_deadlock(self, repo: InMemoryCheckpointRepository):
        """Test that 100 concurrent updates complete without deadlock."""
        projection_name = "TestProjection"
        num_updates = 100

        async def update_checkpoint(i: int):
            event_id = uuid4()
            await repo.update_checkpoint(projection_name, event_id, f"Event{i}")

        # Run 100 concurrent updates
        tasks = [update_checkpoint(i) for i in range(num_updates)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify final state
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == num_updates

    @pytest.mark.asyncio
    async def test_concurrent_multiple_projections(self, repo: InMemoryCheckpointRepository):
        """Test concurrent updates across multiple projections."""
        num_projections = 10
        updates_per_proj = 20

        async def update_projection(proj_idx: int):
            projection_name = f"Projection{proj_idx}"
            for i in range(updates_per_proj):
                event_id = uuid4()
                await repo.update_checkpoint(projection_name, event_id, f"Event{i}")

        # Run concurrent updates for multiple projections
        tasks = [update_projection(i) for i in range(num_projections)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify all projections exist with correct counts
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == num_projections
        for cp in checkpoints:
            assert cp.events_processed == updates_per_proj

    @pytest.mark.asyncio
    async def test_concurrent_read_write(self, repo: InMemoryCheckpointRepository):
        """Test concurrent reads and writes."""
        projection_name = "TestProjection"
        num_operations = 50
        read_results = []

        async def writer(i: int):
            event_id = uuid4()
            await repo.update_checkpoint(projection_name, event_id, f"Event{i}")

        async def reader():
            result = await repo.get_checkpoint(projection_name)
            read_results.append(result)

        # Interleave reads and writes
        tasks = []
        for i in range(num_operations):
            tasks.append(writer(i))
            tasks.append(reader())

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # All operations completed
        assert len(read_results) == num_operations

    @pytest.mark.asyncio
    async def test_concurrent_reset_and_update(self, repo: InMemoryCheckpointRepository):
        """Test concurrent reset and update operations don't cause state corruption."""
        projection_name = "TestProjection"

        # Pre-populate
        await repo.update_checkpoint(projection_name, uuid4(), "Initial")

        async def do_updates():
            for i in range(20):
                await repo.update_checkpoint(projection_name, uuid4(), f"Event{i}")

        async def do_resets():
            for _ in range(5):
                await repo.reset_checkpoint(projection_name)

        # Run concurrently
        await asyncio.wait_for(asyncio.gather(do_updates(), do_resets()), timeout=5.0)

        # Repository should be in a valid state (either None or an Event ID)
        result = await repo.get_checkpoint(projection_name)
        assert result is None or isinstance(result, type(uuid4()))
