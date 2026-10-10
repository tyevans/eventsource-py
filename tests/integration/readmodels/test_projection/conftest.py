"""Shared models, events, projections, and fixtures for ReadModelProjection integration tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest_asyncio

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.application.projections import handles
from eventsource.domain.event import DomainEvent
from eventsource.ports.readmodels import ReadModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker


# ============================================================================
# Test Events
# ============================================================================


class OrderCreated(DomainEvent):
    """Test event for order creation."""

    aggregate_type: str = "Order"
    order_number: str
    customer_name: str
    total_amount: Decimal


class OrderShipped(DomainEvent):
    """Test event for order shipping."""

    aggregate_type: str = "Order"
    tracking_number: str


class OrderCancelled(DomainEvent):
    """Test event for order cancellation."""

    aggregate_type: str = "Order"
    reason: str


class OrderCompleted(DomainEvent):
    """Test event for order completion (no handler defined - tests unhandled events)."""

    aggregate_type: str = "Order"


# ============================================================================
# Test Read Model
# ============================================================================


class OrderSummary(ReadModel):
    """Test read model for integration tests."""

    order_number: str
    customer_name: str
    status: str
    total_amount: Decimal
    tracking_number: str | None = None


# ============================================================================
# Test Projections
# ============================================================================


class OrderProjection(ReadModelProjection[OrderSummary]):
    """Test projection for integration tests."""

    @handles(OrderCreated)
    async def _on_created(self, repo: Any, event: OrderCreated) -> None:
        """Handle order creation by creating a new read model."""
        await repo.save(
            OrderSummary(
                id=event.aggregate_id,
                order_number=event.order_number,
                customer_name=event.customer_name,
                status="pending",
                total_amount=event.total_amount,
            )
        )

    @handles(OrderShipped)
    async def _on_shipped(self, repo: Any, event: OrderShipped) -> None:
        """Handle order shipping by updating status and tracking number."""
        summary = await repo.get(event.aggregate_id)
        if summary:
            summary.status = "shipped"
            summary.tracking_number = event.tracking_number
            await repo.save(summary)

    @handles(OrderCancelled)
    async def _on_cancelled(self, repo: Any, event: OrderCancelled) -> None:
        """Handle order cancellation by soft deleting the read model."""
        await repo.soft_delete(event.aggregate_id)


class MixedHandlerProjection(ReadModelProjection[OrderSummary]):
    """Test projection with mixed handler signatures (1-param and 2-param)."""

    events_without_repo: list[DomainEvent]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.events_without_repo = []

    @handles(OrderCreated)
    async def _on_created(self, repo: Any, event: OrderCreated) -> None:
        """Two-param handler with repository access."""
        await repo.save(
            OrderSummary(
                id=event.aggregate_id,
                order_number=event.order_number,
                customer_name=event.customer_name,
                status="pending",
                total_amount=event.total_amount,
            )
        )

    @handles(OrderShipped)
    async def _on_shipped(self, event: OrderShipped) -> None:
        """Single-param handler without repository access."""
        self.events_without_repo.append(event)


class FailingProjection(ReadModelProjection[OrderSummary]):
    """Test projection that fails on specific events."""

    MAX_RETRIES = 1
    RETRY_BACKOFF_BASE = 0

    @handles(OrderCreated)
    async def _on_created(self, repo: Any, event: OrderCreated) -> None:
        """Handler that always fails."""
        raise ValueError("Simulated handler failure")


# ============================================================================
# PostgreSQL Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def postgresql_projection(
    postgres_engine: AsyncEngine,
    postgres_session_factory: async_sessionmaker[Any],
) -> AsyncIterator[OrderProjection]:
    """Create projection with PostgreSQL backend."""
    from sqlalchemy import text

    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))
        schema_sql = generate_schema(OrderSummary, dialect="postgresql", if_not_exists=False)
        await conn.execute(text(schema_sql))

    projection = OrderProjection(
        session_factory=postgres_session_factory,
        model_class=OrderSummary,
        checkpoint_repo=InMemoryCheckpointRepository(enable_tracing=False),
        enable_tracing=False,
    )

    yield projection

    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))


@pytest_asyncio.fixture
async def postgresql_mixed_projection(
    postgres_engine: AsyncEngine,
    postgres_session_factory: async_sessionmaker[Any],
) -> AsyncIterator[MixedHandlerProjection]:
    """Create mixed handler projection with PostgreSQL backend."""
    from sqlalchemy import text

    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))
        schema_sql = generate_schema(OrderSummary, dialect="postgresql", if_not_exists=False)
        await conn.execute(text(schema_sql))

    projection = MixedHandlerProjection(
        session_factory=postgres_session_factory,
        model_class=OrderSummary,
        checkpoint_repo=InMemoryCheckpointRepository(enable_tracing=False),
        enable_tracing=False,
    )

    yield projection

    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))


@pytest_asyncio.fixture
async def postgresql_failing_projection(
    postgres_engine: AsyncEngine,
    postgres_session_factory: async_sessionmaker[Any],
) -> AsyncIterator[FailingProjection]:
    """Create failing projection with PostgreSQL backend."""
    from sqlalchemy import text

    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))
        schema_sql = generate_schema(OrderSummary, dialect="postgresql", if_not_exists=False)
        await conn.execute(text(schema_sql))

    checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)
    dlq_repo = InMemoryDLQRepository()

    projection = FailingProjection(
        session_factory=postgres_session_factory,
        model_class=OrderSummary,
        checkpoint_repo=checkpoint_repo,
        dlq_repo=dlq_repo,
        enable_tracing=False,
    )

    yield projection

    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))


# ============================================================================
# SQLite Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def sqlite_projection(tmp_path: Any) -> AsyncIterator[OrderProjection]:
    """Create projection with SQLite backend."""
    import aiosqlite
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    db_path = tmp_path / "test_projection.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with aiosqlite.connect(db_path) as db:
        schema_sql = generate_schema(OrderSummary, dialect="sqlite", if_not_exists=False)
        await db.execute(schema_sql)
        await db.commit()

    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    projection = OrderProjection(
        session_factory=session_factory,
        model_class=OrderSummary,
        checkpoint_repo=InMemoryCheckpointRepository(enable_tracing=False),
        enable_tracing=False,
    )

    yield projection

    await engine.dispose()


@pytest_asyncio.fixture
async def sqlite_mixed_projection(tmp_path: Any) -> AsyncIterator[MixedHandlerProjection]:
    """Create mixed handler projection with SQLite backend."""
    import aiosqlite
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    db_path = tmp_path / "test_mixed_projection.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with aiosqlite.connect(db_path) as db:
        schema_sql = generate_schema(OrderSummary, dialect="sqlite", if_not_exists=False)
        await db.execute(schema_sql)
        await db.commit()

    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    projection = MixedHandlerProjection(
        session_factory=session_factory,
        model_class=OrderSummary,
        checkpoint_repo=InMemoryCheckpointRepository(enable_tracing=False),
        enable_tracing=False,
    )

    yield projection

    await engine.dispose()


@pytest_asyncio.fixture
async def sqlite_failing_projection(tmp_path: Any) -> AsyncIterator[FailingProjection]:
    """Create failing projection with SQLite backend."""
    import aiosqlite
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    db_path = tmp_path / "test_failing_projection.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with aiosqlite.connect(db_path) as db:
        schema_sql = generate_schema(OrderSummary, dialect="sqlite", if_not_exists=False)
        await db.execute(schema_sql)
        await db.commit()

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)
    dlq_repo = InMemoryDLQRepository()

    projection = FailingProjection(
        session_factory=session_factory,
        model_class=OrderSummary,
        checkpoint_repo=checkpoint_repo,
        dlq_repo=dlq_repo,
        enable_tracing=False,
    )

    yield projection

    await engine.dispose()
