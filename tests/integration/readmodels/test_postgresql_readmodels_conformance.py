"""Conformance tests for PostgreSQLReadModelRepository against the port suite."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from sqlalchemy import text

from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository
from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.testing.conformance_ports import ReadModelRepositoryConformance
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


class TestPostgreSQLReadModelRepository(ReadModelRepositoryConformance):
    @pytest_asyncio.fixture
    async def store(
        self, postgres_engine: AsyncEngine
    ) -> AsyncIterator[PostgreSQLReadModelRepository[ConformanceReadModel]]:
        table = ConformanceReadModel.table_name()
        async with postgres_engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            await conn.execute(text(generate_schema(ConformanceReadModel, dialect="postgresql")))
        yield PostgreSQLReadModelRepository(
            postgres_engine, ConformanceReadModel, enable_tracing=False
        )
        async with postgres_engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
