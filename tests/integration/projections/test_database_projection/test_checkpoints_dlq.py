"""Integration tests for DatabaseProjection checkpoint tracking and DLQ routing."""

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

    from sqlalchemy.ext.asyncio import async_sessionmaker

pytestmark = [
    skip_if_no_postgres_infra,
    pytest.mark.integration,
    pytest.mark.postgres,
]


class TestDatabaseProjectionCheckpointsAndDLQ:
    """Integration tests for DatabaseProjection checkpoints and DLQ."""

    @pytest.mark.asyncio
    async def test_checkpoint_not_updated_on_error(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
    ) -> None:
        """Checkpoint is NOT updated when handler raises error."""
        checkpoint_repo = InMemoryCheckpointRepository()
        dlq_repo = InMemoryDLQRepository()

        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
                raise ValueError("Simulated error")

        projection = FailingProjection(
            session_factory=postgres_session_factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-004",
            amount=59.99,
        )

        with pytest.raises(ValueError):
            await projection.handle(event)

        checkpoint = await checkpoint_repo.get_checkpoint("FailingProjection")
        assert checkpoint is None

    @pytest.mark.asyncio
    async def test_checkpoint_updated_on_success(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
    ) -> None:
        """Checkpoint is updated after successful event processing."""
        checkpoint_repo = InMemoryCheckpointRepository()

        class OrderProjection(DatabaseProjection):
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

        projection = OrderProjection(
            session_factory=postgres_session_factory,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-005",
            amount=79.99,
        )
        await projection.handle(event)

        checkpoint = await checkpoint_repo.get_checkpoint("OrderProjection")
        assert checkpoint == event.event_id

    @pytest.mark.asyncio
    async def test_event_goes_to_dlq_on_persistent_failure(
        self,
        postgres_session_factory: async_sessionmaker[Any],
        orders_table: AsyncGenerator[None],
    ) -> None:
        """Failed events go to DLQ after retries exhausted."""
        checkpoint_repo = InMemoryCheckpointRepository()
        dlq_repo = InMemoryDLQRepository()

        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 2
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn: Any, event: OrderCreated) -> None:
                raise ValueError("Persistent handler failure")

        projection = FailingProjection(
            session_factory=postgres_session_factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-010",
            amount=299.99,
        )

        with pytest.raises(ValueError):
            await projection.handle(event)

        failed_events = await dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event.event_id
        assert "Persistent handler failure" in failed_events[0].error_message
