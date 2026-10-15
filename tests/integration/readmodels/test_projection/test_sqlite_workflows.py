"""Integration tests for SQLite event routing, mixed handlers, error handling, and workflows with ReadModelProjection."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from .conftest import OrderCancelled, OrderCompleted, OrderCreated, OrderShipped

if TYPE_CHECKING:
    from .conftest import FailingProjection, MixedHandlerProjection, OrderProjection

pytestmark = [pytest.mark.integration]


class TestSQLiteReadModelProjectionEventRouting:
    @pytest.mark.asyncio
    async def test_multiple_events_routed_correctly(
        self, sqlite_projection: OrderProjection
    ) -> None:
        order_id = uuid4()
        events = [
            OrderCreated(
                aggregate_id=order_id,
                order_number="ORD-007",
                customer_name="Grace",
                total_amount=Decimal("300.00"),
            ),
            OrderShipped(aggregate_id=order_id, tracking_number="TRK-99999"),
        ]
        for event in events:
            await sqlite_projection.handle(event)

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is not None
        assert summary.status == "shipped"
        assert summary.tracking_number == "TRK-99999"

    @pytest.mark.asyncio
    async def test_subscribed_to_returns_all_event_types(
        self, sqlite_projection: OrderProjection
    ) -> None:
        subscribed = sqlite_projection.subscribed_to()
        assert OrderCreated in subscribed
        assert OrderShipped in subscribed
        assert OrderCancelled in subscribed
        assert len(subscribed) == 3

    @pytest.mark.asyncio
    async def test_unhandled_event_is_ignored(self, sqlite_projection: OrderProjection) -> None:
        event = OrderCompleted(aggregate_id=uuid4())
        await sqlite_projection.handle(event)
        assert await sqlite_projection.get_checkpoint() == str(event.event_id)


class TestSQLiteMixedHandlerProjection:
    @pytest.mark.asyncio
    async def test_two_param_handler_with_repo_access(
        self, sqlite_mixed_projection: MixedHandlerProjection
    ) -> None:
        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-008",
            customer_name="Henry",
            total_amount=Decimal("400.00"),
        )
        await sqlite_mixed_projection.handle(event)

        async with sqlite_mixed_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_mixed_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is not None
        assert summary.order_number == "ORD-008"

    @pytest.mark.asyncio
    async def test_single_param_handler_without_repo_access(
        self, sqlite_mixed_projection: MixedHandlerProjection
    ) -> None:
        order_id = uuid4()
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-009",
            customer_name="Ivy",
            total_amount=Decimal("500.00"),
        )
        await sqlite_mixed_projection.handle(create_event)

        ship_event = OrderShipped(aggregate_id=order_id, tracking_number="TRK-22222")
        await sqlite_mixed_projection.handle(ship_event)

        assert sqlite_mixed_projection.events_without_repo == [ship_event]


class TestSQLiteReadModelProjectionErrorHandling:
    @pytest.mark.asyncio
    async def test_handler_failure_sends_to_dlq(
        self, sqlite_failing_projection: FailingProjection
    ) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-010",
            customer_name="Jack",
            total_amount=Decimal("600.00"),
        )
        with pytest.raises(ValueError, match="Simulated handler failure"):
            await sqlite_failing_projection.handle(event)

        assert sqlite_failing_projection._dlq_repo is not None
        failed_events = await sqlite_failing_projection._dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event.event_id
        assert "Simulated handler failure" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_checkpoint_not_updated_on_failure(
        self, sqlite_failing_projection: FailingProjection
    ) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-011",
            customer_name="Kate",
            total_amount=Decimal("700.00"),
        )
        with pytest.raises(ValueError):
            await sqlite_failing_projection.handle(event)

        assert await sqlite_failing_projection.get_checkpoint() is None


class TestSQLiteReadModelProjectionWorkflow:
    @pytest.mark.asyncio
    async def test_full_order_lifecycle(self, sqlite_projection: OrderProjection) -> None:
        order_id = uuid4()
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-LIFECYCLE",
            customer_name="Lifecycle Test",
            total_amount=Decimal("999.99"),
        )
        await sqlite_projection.handle(create_event)

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None and summary.status == "pending"

        ship_event = OrderShipped(aggregate_id=order_id, tracking_number="TRK-LIFECYCLE")
        await sqlite_projection.handle(ship_event)

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None
        assert summary.status == "shipped"
        assert summary.tracking_number == "TRK-LIFECYCLE"
        assert summary.version == 2

        cancel_event = OrderCancelled(aggregate_id=order_id, reason="Test cancellation")
        await sqlite_projection.handle(cancel_event)

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is None

    @pytest.mark.asyncio
    async def test_multiple_orders_processed_independently(
        self, sqlite_projection: OrderProjection
    ) -> None:
        order_ids = [uuid4() for _ in range(3)]
        for i, order_id in enumerate(order_ids):
            event = OrderCreated(
                aggregate_id=order_id,
                order_number=f"ORD-MULTI-{i:03d}",
                customer_name=f"Customer {i}",
                total_amount=Decimal(str((i + 1) * 100)),
            )
            await sqlite_projection.handle(event)

        await sqlite_projection.handle(
            OrderShipped(aggregate_id=order_ids[0], tracking_number="TRK-FIRST")
        )
        await sqlite_projection.handle(
            OrderCancelled(aggregate_id=order_ids[2], reason="Cancel last")
        )

        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            order0 = await repo.get(order_ids[0])
            assert order0 is not None
            assert order0.status == "shipped" and order0.tracking_number == "TRK-FIRST"
            order1 = await repo.get(order_ids[1])
            assert order1 is not None and order1.status == "pending"
            assert await repo.get(order_ids[2]) is None
