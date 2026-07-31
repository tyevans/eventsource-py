"""Conformance tests for SQLiteReadModelRepository against the port suite."""

from collections.abc import AsyncIterator

import aiosqlite
import pytest

from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.testing.conformance_ports import ReadModelRepositoryConformance
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel

pytestmark = pytest.mark.sqlite


class TestSQLiteReadModelRepository(ReadModelRepositoryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteReadModelRepository[ConformanceReadModel]]:
        async with aiosqlite.connect(":memory:") as conn:
            await conn.executescript(generate_schema(ConformanceReadModel, dialect="sqlite"))
            await conn.commit()
            yield SQLiteReadModelRepository(conn, ConformanceReadModel, enable_tracing=False)
