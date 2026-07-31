"""Conformance tests for InMemoryCheckpointRepository against the port suites."""

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryCheckpointRepository
from eventsource.testing.conformance_ports import CheckpointRepositoryConformance


class TestMemoryCheckpointRepository(CheckpointRepositoryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryCheckpointRepository]:
        yield InMemoryCheckpointRepository()

    async def test_memory_lag_metrics_use_the_documented_placeholder_shape(
        self, store: InMemoryCheckpointRepository
    ) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        metrics = await store.get_lag_metrics("P")
        assert metrics is not None
        assert metrics.latest_event_id is None
        assert metrics.lag_seconds == 0.0

    async def test_clear_empties_the_repository(self, store: InMemoryCheckpointRepository) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        await store.clear()
        assert await store.get_all_checkpoints() == []
