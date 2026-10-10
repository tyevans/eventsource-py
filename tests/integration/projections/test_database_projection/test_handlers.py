"""Integration tests for DatabaseProjection handler signatures and sequential event handling."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.domain.decorators import handles
from tests.integration.conftest import skip_if_no_postgres_infra

from .conftest import OrderCreated, OrderShipped

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

pytestmark = [
    skip_if_no_postgres_infra,
    pytest.mark.integration,
    pytest.mark.postgres,
]


class TestDatabaseProjectionHandlers:
    """Integration tests for DatabaseProjection handler patterns."""

    @pytest.mark.asyncio
    async def test_multiple_events_processed_sequentially(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
        postgres_engine: AsyncEngine,
    ) -> None:
        """Multiple events are processed correctly in sequence."""

        class OrderProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
                await conn.execute(
                    text("""
                        INSERT INTO test_orders (id, order_number, amount, status)
                        VALUES (:id, :order_number, :amount, 'created')
                    """),
                    {
                        "id": event.aggregate_id,
                        "order_number": event.order_number,
                        "amount": event.amount,
                    },
                )

            @handles(OrderShipped)
            async def _handle_order_shipped(self, conn: Any, event: OrderShipped) -> None:
                await conn.execute(
                    text("""
                        UPDATE test_orders
                        SET status = 'shipped', tracking_number = :tracking_number
                        WHERE id = :id
                    """),
                    {
                        "id": event.aggregate_id,
                        "tracking_number": event.tracking_number,
                    },
                )

        projection = OrderProjection(session_factory=postgres_session_factory)

        aggregate_id = uuid4()
        created = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-006",
            amount=129.99,
        )
        await projection.handle(created)

        shipped = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRK-12345",
        )
        await projection.handle(shipped)

        async with postgres_engine.begin() as conn:
            result = await conn.execute(
                text("""
                    SELECT order_number, status, tracking_number
                    FROM test_orders WHERE id = :id
                """),
                {"id": aggregate_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] == "ORD-006"
        assert row[1] == "shipped"
        assert row[2] == "TRK-12345"

    @pytest.mark.asyncio
    async def test_single_param_handler_with_database(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
    ) -> None:
        """Single parameter handlers work within DatabaseProjection."""
        events_handled: list[OrderCreated] = []

        class MixedProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                events_handled.append(event)

        projection = MixedProjection(session_factory=postgres_session_factory)

        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-008",
            amount=89.99,
        )
        await projection.handle(event)

        assert len(events_handled) == 1
        assert events_handled[0] == event

    @pytest.mark.asyncio
    async def test_mixed_handler_signatures(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
        postgres_engine: AsyncEngine,
    ) -> None:
        """Projection can have both 1-param and 2-param handlers."""
        one_param_events: list[OrderShipped] = []

        class MixedProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
                await conn.execute(
                    text("""
                        INSERT INTO test_orders (id, order_number, amount)
                        VALUES (:id, :order_number, :amount)
                    """),
                    {
                        "id": event.aggregate_id,
                        "order_number": event.order_number,
                        "amount": event.amount,
                    },
                )

            @handles(OrderShipped)
            async def _handle_order_shipped(self, event: OrderShipped) -> None:
                one_param_events.append(event)

        projection = MixedProjection(session_factory=postgres_session_factory)

        aggregate_id = uuid4()

        created = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-009",
            amount=119.99,
        )
        shipped = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRK-99999",
        )

        await projection.handle(created)
        await projection.handle(shipped)

        async with postgres_engine.begin() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM test_orders WHERE id = :id"),
                {"id": aggregate_id},
            )
            count = result.scalar()

        assert count == 1
        assert len(one_param_events) == 1
