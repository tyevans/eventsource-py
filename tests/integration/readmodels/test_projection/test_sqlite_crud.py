"""Integration tests for SQLite CRUD, checkpoint, and reset operations with ReadModelProjection."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from .conftest import OrderCancelled, OrderCreated, OrderShipped

if TYPE_CHECKING:
    from .conftest import OrderProjection

pytestmark = [pytest.mark.integration]


class TestSQLiteReadModelProjectionCRUD:
    """Test CRUD operations via projection handlers with SQLite."""

    @pytest.mark.asyncio
    async def test_create_via_handler(self, sqlite_projection: OrderProjection) -> None:
        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-001",
            customer_name="Alice",
            total_amount=Decimal("99.99"),
        )
        await sqlite_projection.handle(event)

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is not None
        assert summary.order_number == "ORD-001"
        assert summary.customer_name == "Alice"
        assert summary.status == "pending"
        assert abs(float(summary.total_amount) - 99.99) < 0.01

    @pytest.mark.asyncio
    async def test_update_via_handler(self, sqlite_projection: OrderProjection) -> None:
        order_id = uuid4()
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-002",
            customer_name="Bob",
            total_amount=Decimal("150.00"),
        )
        await sqlite_projection.handle(create_event)

        ship_event = OrderShipped(aggregate_id=order_id, tracking_number="TRK-12345")
        await sqlite_projection.handle(ship_event)

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is not None
        assert summary.status == "shipped"
        assert summary.tracking_number == "TRK-12345"
        assert summary.version == 2

    @pytest.mark.asyncio
    async def test_soft_delete_via_handler(self, sqlite_projection: OrderProjection) -> None:
        order_id = uuid4()
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-003",
            customer_name="Charlie",
            total_amount=Decimal("50.00"),
        )
        await sqlite_projection.handle(create_event)

        cancel_event = OrderCancelled(aggregate_id=order_id, reason="Customer request")
        await sqlite_projection.handle(cancel_event)

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is None


class TestSQLiteReadModelProjectionCheckpoint:
    """Test checkpoint tracking integration with SQLite."""

    @pytest.mark.asyncio
    async def test_checkpoint_updated_after_handle(
        self, sqlite_projection: OrderProjection
    ) -> None:
        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-004",
            customer_name="Diana",
            total_amount=Decimal("200.00"),
        )
        await sqlite_projection.handle(event)

        checkpoint = await sqlite_projection.get_checkpoint()
        assert checkpoint == str(event.event_id)

    @pytest.mark.asyncio
    async def test_checkpoint_tracks_multiple_events(
        self, sqlite_projection: OrderProjection
    ) -> None:
        order_id = uuid4()
        events = [
            OrderCreated(
                aggregate_id=order_id,
                order_number="ORD-005",
                customer_name="Eve",
                total_amount=Decimal("100.00"),
            ),
            OrderShipped(aggregate_id=order_id, tracking_number="TRK-11111"),
        ]
        for event in events:
            await sqlite_projection.handle(event)

        checkpoint = await sqlite_projection.get_checkpoint()
        assert checkpoint == str(events[-1].event_id)


class TestSQLiteReadModelProjectionReset:
    """Test reset/truncate functionality with SQLite."""

    @pytest.mark.asyncio
    async def test_reset_truncates_data(self, sqlite_projection: OrderProjection) -> None:
        for i in range(3):
            event = OrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:03d}",
                customer_name=f"Customer {i}",
                total_amount=Decimal(str(i * 100)),
            )
            await sqlite_projection.handle(event)

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            count_before = await repo.count()
        assert count_before == 3

        await sqlite_projection.reset()

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            count_after = await repo.count()
        assert count_after == 0

    @pytest.mark.asyncio
    async def test_reset_clears_checkpoint(self, sqlite_projection: OrderProjection) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-006",
            customer_name="Frank",
            total_amount=Decimal("300.00"),
        )
        await sqlite_projection.handle(event)
        assert await sqlite_projection.get_checkpoint() is not None

        await sqlite_projection.reset()
        assert await sqlite_projection.get_checkpoint() is None
