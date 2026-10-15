"""Integration tests for PostgreSQL workflows and ReadModelProjection properties."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from tests.integration.conftest import skip_if_no_postgres_infra

from .conftest import OrderCancelled, OrderCreated, OrderShipped, OrderSummary

if TYPE_CHECKING:
    from .conftest import FailingProjection, MixedHandlerProjection, OrderProjection

pytestmark = [pytest.mark.integration]


@skip_if_no_postgres_infra
@pytest.mark.postgres
class TestPostgreSQLReadModelProjectionEventRouting:
    """Test event routing to correct handlers with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_multiple_events_routed_correctly(
        self, postgresql_projection: OrderProjection
    ) -> None:
        order_id = uuid4()
        events = [
            OrderCreated(
                aggregate_id=order_id,
                order_number="PG-ORD-005",
                customer_name="PG Eve",
                total_amount=Decimal("300.00"),
            ),
            OrderShipped(aggregate_id=order_id, tracking_number="PG-TRK-99999"),
        ]
        for event in events:
            await postgresql_projection.handle(event)

        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is not None
        assert summary.status == "shipped"
        assert summary.tracking_number == "PG-TRK-99999"

    @pytest.mark.asyncio
    async def test_subscribed_to_returns_all_event_types(
        self, postgresql_projection: OrderProjection
    ) -> None:
        subscribed = postgresql_projection.subscribed_to()
        assert OrderCreated in subscribed
        assert OrderShipped in subscribed
        assert OrderCancelled in subscribed
        assert len(subscribed) == 3


@skip_if_no_postgres_infra
@pytest.mark.postgres
class TestPostgreSQLMixedHandlerProjection:
    """Test projections with mixed handler signatures with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_mixed_handlers_work_correctly(
        self, postgresql_mixed_projection: MixedHandlerProjection
    ) -> None:
        order_id = uuid4()
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-MIXED",
            customer_name="PG Mixed Test",
            total_amount=Decimal("500.00"),
        )
        await postgresql_mixed_projection.handle(create_event)

        async with postgresql_mixed_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_mixed_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None

        ship_event = OrderShipped(aggregate_id=order_id, tracking_number="PG-TRK-MIXED")
        await postgresql_mixed_projection.handle(ship_event)

        assert len(postgresql_mixed_projection.events_without_repo) == 1
        assert postgresql_mixed_projection.events_without_repo[0] == ship_event


@skip_if_no_postgres_infra
@pytest.mark.postgres
class TestPostgreSQLReadModelProjectionErrorHandling:
    """Test error handling and transaction rollback with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_handler_failure_sends_to_dlq(
        self, postgresql_failing_projection: FailingProjection
    ) -> None:
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="PG-ORD-FAIL",
            customer_name="PG Fail Test",
            total_amount=Decimal("600.00"),
        )
        with pytest.raises(ValueError, match="Simulated handler failure"):
            await postgresql_failing_projection.handle(event)

        assert postgresql_failing_projection._dlq_repo is not None
        failed_events = await postgresql_failing_projection._dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event.event_id


@skip_if_no_postgres_infra
@pytest.mark.postgres
class TestPostgreSQLReadModelProjectionWorkflow:
    """Test full end-to-end workflows with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_full_order_lifecycle(self, postgresql_projection: OrderProjection) -> None:
        order_id = uuid4()
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-LIFECYCLE",
            customer_name="PG Lifecycle Test",
            total_amount=Decimal("999.99"),
        )
        await postgresql_projection.handle(create_event)

        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None and summary.status == "pending"

        ship_event = OrderShipped(aggregate_id=order_id, tracking_number="PG-TRK-LIFECYCLE")
        await postgresql_projection.handle(ship_event)

        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None
        assert summary.status == "shipped"
        assert summary.tracking_number == "PG-TRK-LIFECYCLE"

        cancel_event = OrderCancelled(aggregate_id=order_id, reason="PG Test cancellation")
        await postgresql_projection.handle(cancel_event)

        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is None


class TestReadModelProjectionProperties:
    """Test ReadModelProjection property accessors."""

    @pytest.mark.asyncio
    async def test_model_class_property(self, sqlite_projection: OrderProjection) -> None:
        assert sqlite_projection.model_class is OrderSummary

    @pytest.mark.asyncio
    async def test_repository_property_outside_handle(
        self, sqlite_projection: OrderProjection
    ) -> None:
        assert sqlite_projection.repository is None

    @pytest.mark.asyncio
    async def test_projection_name_property(self, sqlite_projection: OrderProjection) -> None:
        assert sqlite_projection.projection_name == "OrderProjection"

    @pytest.mark.asyncio
    async def test_repr(self, sqlite_projection: OrderProjection) -> None:
        repr_str = repr(sqlite_projection)
        assert "OrderProjection" in repr_str
        assert "OrderSummary" in repr_str
        assert "order_summaries" in repr_str
