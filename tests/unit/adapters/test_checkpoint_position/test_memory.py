"""Unit tests for InMemoryCheckpointRepository position methods."""

from __future__ import annotations

import asyncio
from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.ports.checkpoints import CheckpointData
from eventsource.ports.positions import Position

from ._helpers import pos


class TestInMemoryCheckpointRepositoryPosition:
    """Tests for InMemoryCheckpointRepository position methods."""

    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_get_position_returns_none_for_nonexistent(
        self, repo: InMemoryCheckpointRepository
    ):
        assert await repo.get_position("NonExistentSubscription") is None

    @pytest.mark.asyncio
    async def test_save_position_creates_new_checkpoint(self, repo: InMemoryCheckpointRepository):
        position = pos(100)
        await repo.save_position("TestSubscription", position, uuid4(), "TestEvent")
        assert await repo.get_position("TestSubscription") == position

    @pytest.mark.asyncio
    async def test_save_position_updates_existing_checkpoint(
        self, repo: InMemoryCheckpointRepository
    ):
        await repo.save_position("TestSubscription", pos(100), uuid4(), "Event1")
        await repo.save_position("TestSubscription", pos(200), uuid4(), "Event2")
        assert await repo.get_position("TestSubscription") == pos(200)

    @pytest.mark.asyncio
    async def test_save_position_increments_events_processed(
        self, repo: InMemoryCheckpointRepository
    ):
        for i in range(3):
            await repo.save_position("TestSubscription", pos(i * 100), uuid4(), f"Event{i}")
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1 and checkpoints[0].events_processed == 3

    @pytest.mark.asyncio
    async def test_save_position_updates_event_id_and_type(
        self, repo: InMemoryCheckpointRepository
    ):
        event_id = uuid4()
        await repo.save_position("TestSubscription", pos(100), event_id, "TestEventType")
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].last_event_id == event_id
        assert checkpoints[0].last_event_type == "TestEventType"

    @pytest.mark.asyncio
    async def test_get_position_returns_correct_value_after_save(
        self, repo: InMemoryCheckpointRepository
    ):
        for position in [pos(n) for n in (10, 50, 100, 500, 1000)]:
            await repo.save_position("TestSubscription", position, uuid4(), "Event")
            assert await repo.get_position("TestSubscription") == position

    @pytest.mark.asyncio
    async def test_checkpoint_data_includes_global_position(
        self, repo: InMemoryCheckpointRepository
    ):
        position = pos(42)
        await repo.save_position("TestSubscription", position, uuid4(), "TestEvent")
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert isinstance(checkpoints[0], CheckpointData)
        assert checkpoints[0].position == position

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_independent_positions(
        self, repo: InMemoryCheckpointRepository
    ):
        await repo.save_position("Subscription1", pos(100), uuid4(), "Event1")
        await repo.save_position("Subscription2", pos(200), uuid4(), "Event2")
        assert await repo.get_position("Subscription1") == pos(100)
        assert await repo.get_position("Subscription2") == pos(200)

    @pytest.mark.asyncio
    async def test_get_position_returns_none_after_reset(self, repo: InMemoryCheckpointRepository):
        await repo.save_position("TestSubscription", pos(100), uuid4(), "Event")
        assert await repo.get_position("TestSubscription") == pos(100)
        await repo.reset_checkpoint("TestSubscription")
        assert await repo.get_position("TestSubscription") is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_does_not_set_position(
        self, repo: InMemoryCheckpointRepository
    ):
        event_id = uuid4()
        await repo.update_checkpoint("TestSubscription", event_id, "TestEvent")
        assert await repo.get_position("TestSubscription") is None
        assert await repo.get_checkpoint("TestSubscription") == event_id

    @pytest.mark.asyncio
    async def test_save_position_after_update_checkpoint(self, repo: InMemoryCheckpointRepository):
        await repo.update_checkpoint("TestSubscription", uuid4(), "Event1")
        assert await repo.get_position("TestSubscription") is None
        position = pos(500)
        await repo.save_position("TestSubscription", position, uuid4(), "Event2")
        assert await repo.get_position("TestSubscription") == position
        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].events_processed == 2

    @pytest.mark.asyncio
    async def test_save_position_sets_last_processed_at(self, repo: InMemoryCheckpointRepository):
        await repo.save_position("TestSubscription", pos(100), uuid4(), "Event")
        checkpoints = await repo.get_all_checkpoints()
        assert isinstance(checkpoints[0].last_processed_at, datetime)


class TestInMemoryCheckpointRepositoryPositionConcurrency:
    """Tests for concurrent access to position methods."""

    @pytest.fixture
    def repo(self) -> InMemoryCheckpointRepository:
        return InMemoryCheckpointRepository()

    @pytest.mark.asyncio
    async def test_concurrent_save_position_no_deadlock(self, repo: InMemoryCheckpointRepository):
        num_updates = 100

        async def save_position(i: int):
            await repo.save_position("TestSubscription", pos(i), uuid4(), f"Event{i}")

        await asyncio.wait_for(
            asyncio.gather(*[save_position(i) for i in range(num_updates)]), timeout=5.0
        )
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1 and checkpoints[0].events_processed == num_updates

    @pytest.mark.asyncio
    async def test_concurrent_get_and_save_position(self, repo: InMemoryCheckpointRepository):
        num_operations = 50
        positions: list[Position | None] = []

        async def writer(i: int):
            await repo.save_position("TestSubscription", pos(i * 10), uuid4(), f"Event{i}")

        async def reader():
            positions.append(await repo.get_position("TestSubscription"))

        tasks = [t for i in range(num_operations) for t in (writer(i), reader())]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
        assert len(positions) == num_operations
