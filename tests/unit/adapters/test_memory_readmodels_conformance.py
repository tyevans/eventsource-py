"""Conformance tests for InMemoryReadModelRepository against the port suite."""

from collections.abc import AsyncIterator

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.testing.conformance_ports import ReadModelRepositoryConformance
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel


class TestMemoryReadModelRepository(ReadModelRepositoryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryReadModelRepository[ConformanceReadModel]]:
        yield InMemoryReadModelRepository(ConformanceReadModel, enable_tracing=False)
