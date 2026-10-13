"""Shared fixtures and test model for enhanced read model features tests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.ports.readmodels import ReadModel

if TYPE_CHECKING:
    from uuid import UUID


class EnhancedTestModel(ReadModel):
    """Test model for enhanced feature integration tests."""

    name: str
    category: str = "default"
    value: int = 0
    amount: Decimal = Decimal("0.00")


@pytest_asyncio.fixture
async def enhanced_inmemory_repo() -> AsyncIterator[InMemoryReadModelRepository[EnhancedTestModel]]:
    """In-memory repository for enhanced feature tests."""
    repo: InMemoryReadModelRepository[EnhancedTestModel] = InMemoryReadModelRepository(
        EnhancedTestModel, enable_tracing=False
    )
    yield repo
    await repo.truncate()


@pytest_asyncio.fixture
async def enhanced_sqlite_repo(tmp_path: Any) -> AsyncIterator[Any]:
    """SQLite repository for enhanced feature tests."""
    try:
        import aiosqlite
    except ImportError:
        pytest.skip("aiosqlite not installed")

    from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository

    db_path = tmp_path / "test_enhanced_features.db"
    async with aiosqlite.connect(db_path) as db:
        schema_sql = generate_schema(EnhancedTestModel, dialect="sqlite", if_not_exists=False)
        await db.execute(schema_sql)
        await db.commit()

        repo: SQLiteReadModelRepository[EnhancedTestModel] = SQLiteReadModelRepository(
            db, EnhancedTestModel, enable_tracing=False
        )
        yield repo
        await repo.truncate()


@pytest_asyncio.fixture
async def enhanced_postgresql_repo(postgres_engine: Any) -> AsyncIterator[Any]:
    """PostgreSQL repository for enhanced feature tests."""
    from sqlalchemy import text

    from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository

    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {EnhancedTestModel.table_name()}"))
        schema_sql = generate_schema(EnhancedTestModel, dialect="postgresql", if_not_exists=False)
        await conn.execute(text(schema_sql))

    repo: PostgreSQLReadModelRepository[EnhancedTestModel] = PostgreSQLReadModelRepository(
        postgres_engine, EnhancedTestModel, enable_tracing=False
    )
    yield repo
    await repo.truncate()


@pytest.fixture
def enhanced_model_factory() -> Callable[..., EnhancedTestModel]:
    """Factory for creating test EnhancedTestModel instances."""

    def _create(
        id: UUID | None = None,
        name: str = "test_model",
        category: str = "default",
        value: int = 0,
        amount: Decimal = Decimal("0.00"),
    ) -> EnhancedTestModel:
        return EnhancedTestModel(
            id=id or uuid4(),
            name=name,
            category=category,
            value=value,
            amount=amount,
        )

    return _create


@pytest.fixture(params=["inmemory", "sqlite", "postgresql"])
def enhanced_repo_type(request: pytest.FixtureRequest) -> str:
    """Return the repository type for parametrization."""
    return str(request.param)


@pytest_asyncio.fixture
async def enhanced_repo(
    enhanced_repo_type: str,
    enhanced_inmemory_repo: InMemoryReadModelRepository[EnhancedTestModel],
    enhanced_sqlite_repo: Any,
    enhanced_postgresql_repo: Any,
) -> AsyncIterator[Any]:
    """Parametrized fixture providing all repository implementations."""
    if enhanced_repo_type == "inmemory":
        yield enhanced_inmemory_repo
    elif enhanced_repo_type == "sqlite":
        yield enhanced_sqlite_repo
    elif enhanced_repo_type == "postgresql":
        try:
            yield enhanced_postgresql_repo
        except pytest.skip.Exception:
            pytest.skip("PostgreSQL test infrastructure not available")
    else:
        pytest.fail(f"Unknown repository type: {enhanced_repo_type}")
