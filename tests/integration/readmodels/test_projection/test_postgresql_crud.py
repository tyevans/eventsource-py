"""Integration tests for PostgreSQL CRUD, checkpoint, and reset operations with ReadModelProjection."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from tests.integration.conftest import skip_if_no_postgres_infra

from .conftest import (
    OrderCancelled,
    OrderCreated,
    OrderShipped,
)

if TYPE_CHECKING:
    from .conftest import OrderProjection

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestPostgreSQLReadModelProjectionCRUD:
    """Test CRUD operations via projection handlers with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_create_via_handler(self, postgresql_projection: OrderProjection) -> None:
        """Test creating a read model via handler."""
        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-001",
            customer_name="PG Alice",
            total_amount=Decimal("99.99"),
        )

        await postgresql_projection.handle(event)

        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is not None
        assert summary.order_number == "PG-ORD-001"
        assert summary.customer_name == "PG Alice"
        assert summary.status == "pending"
        assert summary.total_amount == Decimal("99.99")

    @pytest.mark.asyncio
    async def test_update_via_handler(self, postgresql_projection: OrderProjection) -> None:
        """Test updating a read model via handler."""
        order_id = uuid4()

        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-002",
            customer_name="PG Bob",
            total_amount=Decimal("150.00"),
        )
        await postgresql_projection.handle(create_event)

        ship_event = OrderShipped(
            aggregate_id=order_id,
            tracking_number="PG-TRK-12345",
        )
        await postgresql_projection.handle(ship_event)

        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is not None
        assert summary.status == "shipped"
        assert summary.tracking_number == "PG-TRK-12345"
        assert summary.version == 2

    @pytest.mark.asyncio
    async def test_soft_delete_via_handler(self, postgresql_projection: OrderProjection) -> None:
        """Test soft deleting via handler."""
        order_id = uuid4()

        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-003",
            customer_name="PG Charlie",
            total_amount=Decimal("50.00"),
        )
        await postgresql_projection.handle(create_event)

        cancel_event = OrderCancelled(
            aggregate_id=order_id,
            reason="PG Customer request",
        )
        await postgresql_projection.handle(cancel_event)

        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is None


class TestPostgreSQLReadModelProjectionCheckpoint:
    """Test checkpoint tracking integration with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_checkpoint_updated_after_handle(
        self, postgresql_projection: OrderProjection
    ) -> None:
        """Test that checkpoint is updated after successful handle."""
        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-004",
            customer_name="PG Diana",
            total_amount=Decimal("200.00"),
        )

        await postgresql_projection.handle(event)

        checkpoint = await postgresql_projection.get_checkpoint()
        assert checkpoint == str(event.event_id)


class TestPostgreSQLReadModelProjectionReset:
    """Test reset/truncate functionality with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_reset_truncates_data(self, postgresql_projection: OrderProjection) -> None:
        """Test that reset() truncates all read model data."""
        for i in range(3):
            event = OrderCreated(
                aggregate_id=uuid4(),
                order_number=f"PG-ORD-{i:03d}",
                customer_name=f"PG Customer {i}",
                total_amount=Decimal(str(i * 100)),
            )
            await postgresql_projection.handle(event)

        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            count_before = await repo.count()

        assert count_before == 3

        await postgresql_projection.reset()

        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            count_after = await repo.count()

        assert count_after == 0
