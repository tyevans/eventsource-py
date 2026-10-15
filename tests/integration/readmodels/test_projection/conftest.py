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


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    customer_name: str
    total_amount: Decimal


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str


class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"
    reason: str


class OrderCompleted(DomainEvent):
    aggregate_type: str = "Order"


class OrderSummary(ReadModel):
    order_number: str
    customer_name: str
    status: str
    total_amount: Decimal
    tracking_number: str | None = None


def _new_order_summary(event: OrderCreated) -> OrderSummary:
    return OrderSummary(
        id=event.aggregate_id,
        order_number=event.order_number,
        customer_name=event.customer_name,
        status="pending",
        total_amount=event.total_amount,
    )


class OrderProjection(ReadModelProjection[OrderSummary]):
    @handles(OrderCreated)
    async def _on_created(self, repo: Any, event: OrderCreated) -> None:
        await repo.save(_new_order_summary(event))

    @handles(OrderShipped)
    async def _on_shipped(self, repo: Any, event: OrderShipped) -> None:
        summary = await repo.get(event.aggregate_id)
        if summary:
            summary.status = "shipped"
            summary.tracking_number = event.tracking_number
            await repo.save(summary)

    @handles(OrderCancelled)
    async def _on_cancelled(self, repo: Any, event: OrderCancelled) -> None:
        await repo.soft_delete(event.aggregate_id)


class MixedHandlerProjection(ReadModelProjection[OrderSummary]):
    events_without_repo: list[DomainEvent]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.events_without_repo = []

    @handles(OrderCreated)
    async def _on_created(self, repo: Any, event: OrderCreated) -> None:
        await repo.save(_new_order_summary(event))

    @handles(OrderShipped)
    async def _on_shipped(self, event: OrderShipped) -> None:
        self.events_without_repo.append(event)


class FailingProjection(ReadModelProjection[OrderSummary]):
    MAX_RETRIES = 1
    RETRY_BACKOFF_BASE = 0

    @handles(OrderCreated)
    async def _on_created(self, repo: Any, event: OrderCreated) -> None:
        raise ValueError("Simulated handler failure")


async def _reset_postgres_table(postgres_engine: AsyncEngine) -> None:
    from sqlalchemy import text

    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))
        schema_sql = generate_schema(OrderSummary, dialect="postgresql", if_not_exists=False)
        await conn.execute(text(schema_sql))


async def _drop_postgres_table(postgres_engine: AsyncEngine) -> None:
    from sqlalchemy import text

    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))


async def _init_sqlite_engine(tmp_path: Any, db_name: str) -> tuple[Any, Any]:
    import aiosqlite
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    db_path = tmp_path / db_name
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with aiosqlite.connect(db_path) as db:
        await db.execute(generate_schema(OrderSummary, dialect="sqlite", if_not_exists=False))
        await db.commit()
    return engine, async_sessionmaker(engine, expire_on_commit=False)


def _make_projection(cls: type[Any], session_factory: Any, **extra: Any) -> Any:
    return cls(
        session_factory=session_factory,
        model_class=OrderSummary,
        checkpoint_repo=InMemoryCheckpointRepository(enable_tracing=False),
        enable_tracing=False,
        **extra,
    )


@pytest_asyncio.fixture
async def postgresql_projection(
    postgres_engine: AsyncEngine,
    postgres_session_factory: async_sessionmaker[Any],
) -> AsyncIterator[OrderProjection]:
    await _reset_postgres_table(postgres_engine)
    yield _make_projection(OrderProjection, postgres_session_factory)
    await _drop_postgres_table(postgres_engine)


@pytest_asyncio.fixture
async def postgresql_mixed_projection(
    postgres_engine: AsyncEngine,
    postgres_session_factory: async_sessionmaker[Any],
) -> AsyncIterator[MixedHandlerProjection]:
    await _reset_postgres_table(postgres_engine)
    yield _make_projection(MixedHandlerProjection, postgres_session_factory)
    await _drop_postgres_table(postgres_engine)


@pytest_asyncio.fixture
async def postgresql_failing_projection(
    postgres_engine: AsyncEngine,
    postgres_session_factory: async_sessionmaker[Any],
) -> AsyncIterator[FailingProjection]:
    await _reset_postgres_table(postgres_engine)
    yield _make_projection(
        FailingProjection, postgres_session_factory, dlq_repo=InMemoryDLQRepository()
    )
    await _drop_postgres_table(postgres_engine)


@pytest_asyncio.fixture
async def sqlite_projection(tmp_path: Any) -> AsyncIterator[OrderProjection]:
    engine, session_factory = await _init_sqlite_engine(tmp_path, "test_projection.db")
    yield _make_projection(OrderProjection, session_factory)
    await engine.dispose()


@pytest_asyncio.fixture
async def sqlite_mixed_projection(tmp_path: Any) -> AsyncIterator[MixedHandlerProjection]:
    engine, session_factory = await _init_sqlite_engine(tmp_path, "test_mixed_projection.db")
    yield _make_projection(MixedHandlerProjection, session_factory)
    await engine.dispose()


@pytest_asyncio.fixture
async def sqlite_failing_projection(tmp_path: Any) -> AsyncIterator[FailingProjection]:
    engine, session_factory = await _init_sqlite_engine(tmp_path, "test_failing_projection.db")
    yield _make_projection(FailingProjection, session_factory, dlq_repo=InMemoryDLQRepository())
    await engine.dispose()
