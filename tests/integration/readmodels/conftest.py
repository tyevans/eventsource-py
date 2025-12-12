"""
Fixtures for read model repository integration tests.

Provides repository instances for all three backends:
- InMemory (always available)
- PostgreSQL (requires running database via testcontainers)
- SQLite (file-based or in-memory)
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource.readmodels import (
    InMemoryReadModelRepository,
    ReadModel,
    generate_schema,
)

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine


# ============================================================================
# Test Read Model
# ============================================================================


class OrderSummary(ReadModel):
    """Test read model for integration tests."""

    order_number: str
    customer_name: str
    status: str
    total_amount: Decimal
    item_count: int = 0


# ============================================================================
# Factory Fixtures
# ============================================================================


@pytest.fixture
def order_summary_factory() -> Callable[..., OrderSummary]:
    """Factory for creating test OrderSummary instances."""

    def _create(
        id: UUID | None = None,
        order_number: str = "ORD-001",
        customer_name: str = "Test Customer",
        status: str = "pending",
        total_amount: Decimal = Decimal("99.99"),
        item_count: int = 1,
    ) -> OrderSummary:
        return OrderSummary(
            id=id or uuid4(),
            order_number=order_number,
            customer_name=customer_name,
            status=status,
            total_amount=total_amount,
            item_count=item_count,
        )

    return _create


# ============================================================================
# InMemory Repository Fixture
# ============================================================================


@pytest_asyncio.fixture
async def inmemory_repo() -> AsyncGenerator[InMemoryReadModelRepository[OrderSummary], None]:
    """In-memory repository for testing."""
    repo: InMemoryReadModelRepository[OrderSummary] = InMemoryReadModelRepository(
        OrderSummary, enable_tracing=False
    )
    yield repo
    await repo.clear()


# ============================================================================
# SQLite Repository Fixture
# ============================================================================


@pytest_asyncio.fixture
async def sqlite_repo(
    tmp_path: Any,
) -> AsyncGenerator[Any, None]:
    """SQLite repository for testing."""
    try:
        import aiosqlite
    except ImportError:
        pytest.skip("aiosqlite not installed")

    from eventsource.readmodels import SQLiteReadModelRepository

    db_path = tmp_path / "test_readmodels.db"
    async with aiosqlite.connect(db_path) as db:
        # Create table using schema generation utility
        schema_sql = generate_schema(OrderSummary, dialect="sqlite", if_not_exists=False)
        await db.execute(schema_sql)
        await db.commit()

        repo: SQLiteReadModelRepository[OrderSummary] = SQLiteReadModelRepository(
            db, OrderSummary, enable_tracing=False
        )
        yield repo
        # Cleanup
        await repo.truncate()


# ============================================================================
# PostgreSQL Repository Fixture
# ============================================================================


@pytest_asyncio.fixture
async def postgresql_repo(
    postgres_engine: AsyncEngine,
) -> AsyncGenerator[Any, None]:
    """PostgreSQL repository for testing."""
    from sqlalchemy import text

    from eventsource.readmodels import PostgreSQLReadModelRepository

    # Create table using schema generation utility
    async with postgres_engine.begin() as conn:
        # Drop and recreate for clean state
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))
        schema_sql = generate_schema(OrderSummary, dialect="postgresql", if_not_exists=False)
        await conn.execute(text(schema_sql))

    # Create repository with engine
    repo: PostgreSQLReadModelRepository[OrderSummary] = PostgreSQLReadModelRepository(
        postgres_engine, OrderSummary, enable_tracing=False
    )
    yield repo
    # Cleanup
    await repo.truncate()


# ============================================================================
# Parametrized Repository Fixture
# ============================================================================


@pytest.fixture(params=["inmemory", "sqlite", "postgresql"])
def repo_type(request: pytest.FixtureRequest) -> str:
    """Return the repository type for parametrization."""
    return request.param


@pytest_asyncio.fixture
async def repo(
    repo_type: str,
    inmemory_repo: InMemoryReadModelRepository[OrderSummary],
    sqlite_repo: Any,
    postgresql_repo: Any,
    request: pytest.FixtureRequest,
) -> AsyncGenerator[Any, None]:
    """
    Parametrized fixture providing all repository implementations.

    Yields the appropriate repository based on repo_type parameter.
    Skips PostgreSQL tests if infrastructure is not available.
    """
    if repo_type == "inmemory":
        yield inmemory_repo
    elif repo_type == "sqlite":
        yield sqlite_repo
    elif repo_type == "postgresql":
        # Check if postgres_engine fixture is available
        # This will skip if testcontainers/docker not available
        try:
            yield postgresql_repo
        except pytest.skip.Exception:
            pytest.skip("PostgreSQL test infrastructure not available")
    else:
        pytest.fail(f"Unknown repository type: {repo_type}")
