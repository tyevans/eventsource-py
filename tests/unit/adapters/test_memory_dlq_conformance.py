"""Conformance tests for InMemoryDLQRepository against the port suites."""

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryDLQRepository
from eventsource.testing.conformance_ports import DLQRepositoryConformance


class TestMemoryDLQRepository(DLQRepositoryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryDLQRepository]:
        yield InMemoryDLQRepository()

    async def test_clear_empties_the_repository(self, store: InMemoryDLQRepository) -> None:
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        await store.clear()
        assert await store.get_failed_events() == []
