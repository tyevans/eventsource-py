"""Integration tests for DatabaseProjection SQL execution and transaction management."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.domain.decorators import handles
from tests.integration.conftest import skip_if_no_postgres_infra

from .conftest import OrderCreated

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

pytestmark = [
    skip_if_no_postgres_infra,
    pytest.mark.integration,
    pytest.mark.postgres,
]


class TestDatabaseProjectionTransactions:
    """Integration tests for DatabaseProjection transaction behavior."""

    @pytest.mark.asyncio
    async def test_sql_execution_in_handler(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
        postgres_engine: AsyncEngine,
    ) -> None:
        class OrderProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
                await conn.execute(
                    text(
                        "INSERT INTO test_orders (id, order_number, amount, status) "
                        "VALUES (:id, :order_number, :amount, 'created')"
                    ),
                    {
                        "id": event.aggregate_id,
                        "order_number": event.order_number,
                        "amount": event.amount,
                    },
                )

        projection = OrderProjection(session_factory=postgres_session_factory)
        aggregate_id = uuid4()
        await projection.handle(
            OrderCreated(aggregate_id=aggregate_id, order_number="ORD-001", amount=99.99)
        )

        async with postgres_engine.begin() as conn:
            result = await conn.execute(
                text("SELECT order_number, amount, status FROM test_orders WHERE id = :id"),
                {"id": aggregate_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] == "ORD-001"
        assert float(row[1]) == 99.99
        assert row[2] == "created"

    @pytest.mark.asyncio
    async def test_transaction_commits_on_success(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
        postgres_engine: AsyncEngine,
    ) -> None:
        class OrderProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
                await conn.execute(
                    text(
                        "INSERT INTO test_orders (id, order_number, amount) "
                        "VALUES (:id, :order_number, :amount)"
                    ),
                    {
                        "id": event.aggregate_id,
                        "order_number": event.order_number,
                        "amount": event.amount,
                    },
                )

        projection = OrderProjection(session_factory=postgres_session_factory)
        aggregate_id = uuid4()
        await projection.handle(
            OrderCreated(aggregate_id=aggregate_id, order_number="ORD-002", amount=149.99)
        )

        async with postgres_engine.begin() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM test_orders WHERE id = :id"), {"id": aggregate_id}
            )
            count = result.scalar()

        assert count == 1

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_handler_error(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
        postgres_engine: AsyncEngine,
    ) -> None:
        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
                await conn.execute(
                    text(
                        "INSERT INTO test_orders (id, order_number, amount) "
                        "VALUES (:id, :order_number, :amount)"
                    ),
                    {
                        "id": event.aggregate_id,
                        "order_number": event.order_number,
                        "amount": event.amount,
                    },
                )
                raise ValueError("Simulated handler error")

        projection = FailingProjection(
            session_factory=postgres_session_factory,
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
        )
        aggregate_id = uuid4()
        event = OrderCreated(aggregate_id=aggregate_id, order_number="ORD-003", amount=199.99)

        with pytest.raises(ValueError, match="Simulated handler error"):
            await projection.handle(event)

        async with postgres_engine.begin() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM test_orders WHERE id = :id"), {"id": aggregate_id}
            )
            count = result.scalar()

        assert count == 0

    @pytest.mark.asyncio
    async def test_partial_handler_failure_rollback(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
        postgres_engine: AsyncEngine,
    ) -> None:
        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
                sql = text(
                    "INSERT INTO test_orders (id, order_number, amount) "
                    "VALUES (:id, :order_number, :amount)"
                )
                await conn.execute(
                    sql,
                    {
                        "id": event.aggregate_id,
                        "order_number": event.order_number,
                        "amount": event.amount,
                    },
                )
                await conn.execute(
                    sql,
                    {
                        "id": uuid4(),
                        "order_number": f"{event.order_number}-RELATED",
                        "amount": event.amount / 2,
                    },
                )
                raise ValueError("Simulated failure after inserts")

        projection = FailingProjection(
            session_factory=postgres_session_factory,
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
        )
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-007", amount=200.00)

        with pytest.raises(ValueError):
            await projection.handle(event)

        async with postgres_engine.begin() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM test_orders"))
            count = result.scalar()

        assert count == 0
