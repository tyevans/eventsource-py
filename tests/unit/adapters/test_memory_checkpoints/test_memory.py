"""Unit tests for InMemoryCheckpointRepository implementation and protocol compliance."""

from __future__ import annotations

import asyncio
from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.ports.checkpoints import CheckpointData, CheckpointRepository, LagMetrics


class TestInMemoryCheckpointRepository:
    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_get_checkpoint_returns_none_when_empty(self, repo: InMemoryCheckpointRepository):
        assert await repo.get_checkpoint("NonExistentProjection") is None

    @pytest.mark.asyncio
    async def test_update_and_get_checkpoint(self, repo: InMemoryCheckpointRepository):
        projection_name, event_id = "TestProjection", uuid4()
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")
        assert await repo.get_checkpoint(projection_name) == event_id

    @pytest.mark.asyncio
    async def test_update_checkpoint_increments_count(self, repo: InMemoryCheckpointRepository):
        for i in range(3):
            await repo.update_checkpoint("TestProjection", uuid4(), f"Event{i}")
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1 and checkpoints[0].events_processed == 3

    @pytest.mark.asyncio
    async def test_update_checkpoint_overwrites_previous(self, repo: InMemoryCheckpointRepository):
        first_event_id, second_event_id = uuid4(), uuid4()
        await repo.update_checkpoint("TestProjection", first_event_id, "Event1")
        await repo.update_checkpoint("TestProjection", second_event_id, "Event2")
        assert await repo.get_checkpoint("TestProjection") == second_event_id

    @pytest.mark.asyncio
    async def test_multiple_projections_independent(self, repo: InMemoryCheckpointRepository):
        event_id_1, event_id_2 = uuid4(), uuid4()
        await repo.update_checkpoint("Projection1", event_id_1, "Event1")
        await repo.update_checkpoint("Projection2", event_id_2, "Event2")
        assert await repo.get_checkpoint("Projection1") == event_id_1
        assert await repo.get_checkpoint("Projection2") == event_id_2

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_none_when_no_checkpoint(
        self, repo: InMemoryCheckpointRepository
    ):
        assert await repo.get_lag_metrics("NonExistent") is None

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_data_for_existing_checkpoint(
        self, repo: InMemoryCheckpointRepository
    ):
        projection_name, event_id = "TestProjection", uuid4()
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")
        result = await repo.get_lag_metrics(projection_name)
        assert isinstance(result, LagMetrics)
        assert (result.projection_name, result.last_event_id, result.events_processed) == (
            projection_name,
            str(event_id),
            1,
        )

    @pytest.mark.asyncio
    async def test_reset_checkpoint_removes_checkpoint(self, repo: InMemoryCheckpointRepository):
        projection_name, event_id = "TestProjection", uuid4()
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")
        assert await repo.get_checkpoint(projection_name) == event_id
        await repo.reset_checkpoint(projection_name)
        assert await repo.get_checkpoint(projection_name) is None

    @pytest.mark.asyncio
    async def test_reset_checkpoint_does_not_affect_others(
        self, repo: InMemoryCheckpointRepository
    ):
        event_id_1, event_id_2 = uuid4(), uuid4()
        await repo.update_checkpoint("Projection1", event_id_1, "Event1")
        await repo.update_checkpoint("Projection2", event_id_2, "Event2")
        await repo.reset_checkpoint("Projection1")
        assert await repo.get_checkpoint("Projection1") is None
        assert await repo.get_checkpoint("Projection2") == event_id_2

    @pytest.mark.asyncio
    async def test_reset_nonexistent_checkpoint_no_error(self, repo: InMemoryCheckpointRepository):
        await repo.reset_checkpoint("NonExistent")

    @pytest.mark.asyncio
    async def test_clear_removes_all_checkpoints(self, repo: InMemoryCheckpointRepository):
        await repo.update_checkpoint("P1", uuid4(), "E1")
        await repo.update_checkpoint("P2", uuid4(), "E2")
        await repo.clear()
        assert await repo.get_checkpoint("P1") is None
        assert await repo.get_checkpoint("P2") is None

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_empty(self, repo: InMemoryCheckpointRepository):
        assert await repo.get_all_checkpoints() == []

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_returns_all(self, repo: InMemoryCheckpointRepository):
        for proj in ["Zebra", "Apple", "Middle"]:
            await repo.update_checkpoint(proj, uuid4(), "TestEvent")
        result = await repo.get_all_checkpoints()
        assert [c.projection_name for c in result] == ["Apple", "Middle", "Zebra"]

    @pytest.mark.asyncio
    async def test_checkpoint_data_structure(self, repo: InMemoryCheckpointRepository):
        projection_name, event_id, event_type = "TestProjection", uuid4(), "TestEventType"
        await repo.update_checkpoint(projection_name, event_id, event_type)
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        cp = checkpoints[0]
        assert isinstance(cp, CheckpointData)
        assert (cp.projection_name, cp.last_event_id, cp.last_event_type) == (
            projection_name,
            event_id,
            event_type,
        )
        assert cp.events_processed == 1 and cp.last_processed_at is not None

    @pytest.mark.asyncio
    async def test_lag_metrics_has_timestamp_info(self, repo: InMemoryCheckpointRepository):
        await repo.update_checkpoint("TestProjection", uuid4(), "TestEvent")
        result = await repo.get_lag_metrics("TestProjection")
        assert result is not None and result.last_processed_at is not None
        datetime.fromisoformat(result.last_processed_at)


class TestCheckpointRepositoryProtocol:
    def test_implements_protocol(self):
        assert isinstance(InMemoryCheckpointRepository(), CheckpointRepository)


class TestInMemoryCheckpointRepositoryConcurrency:
    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_concurrent_updates_no_deadlock(self, repo: InMemoryCheckpointRepository):
        async def update_checkpoint(i: int):
            await repo.update_checkpoint("TestProjection", uuid4(), f"Event{i}")

        await asyncio.wait_for(
            asyncio.gather(*[update_checkpoint(i) for i in range(100)]), timeout=5.0
        )
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1 and checkpoints[0].events_processed == 100

    @pytest.mark.asyncio
    async def test_concurrent_multiple_projections(self, repo: InMemoryCheckpointRepository):
        async def update_projection(proj_idx: int):
            for i in range(20):
                await repo.update_checkpoint(f"Projection{proj_idx}", uuid4(), f"Event{i}")

        await asyncio.wait_for(
            asyncio.gather(*[update_projection(i) for i in range(10)]), timeout=5.0
        )
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 10 and all(cp.events_processed == 20 for cp in checkpoints)

    @pytest.mark.asyncio
    async def test_concurrent_read_write(self, repo: InMemoryCheckpointRepository):
        read_results = []

        async def writer(i: int):
            await repo.update_checkpoint("TestProjection", uuid4(), f"Event{i}")

        async def reader():
            read_results.append(await repo.get_checkpoint("TestProjection"))

        tasks = [t for i in range(50) for t in (writer(i), reader())]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
        assert len(read_results) == 50

    @pytest.mark.asyncio
    async def test_concurrent_reset_and_update(self, repo: InMemoryCheckpointRepository):
        await repo.update_checkpoint("TestProjection", uuid4(), "Initial")

        async def do_updates():
            for i in range(20):
                await repo.update_checkpoint("TestProjection", uuid4(), f"Event{i}")

        async def do_resets():
            for _ in range(5):
                await repo.reset_checkpoint("TestProjection")

        await asyncio.wait_for(asyncio.gather(do_updates(), do_resets()), timeout=5.0)
        result = await repo.get_checkpoint("TestProjection")
        assert result is None or isinstance(result, type(uuid4()))
