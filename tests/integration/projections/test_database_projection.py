"""
Integration tests for DatabaseProjection with real PostgreSQL database.

These tests verify that DatabaseProjection works correctly with a real database,
including:
- Handler receives valid database connection
- SQL operations execute successfully
- Transactions commit on success
- Transactions rollback on error
- Checkpoint is updated correctly
- Transaction atomicity is maintained

Tests require Docker and testcontainers for PostgreSQL.
"""

from uuid import uuid4

import pytest
from pydantic import Field
from sqlalchemy import text

from eventsource.events.base import DomainEvent
from eventsource.handlers import handles
from eventsource.projections.base import DatabaseProjection
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository

# Import integration test fixtures
from tests.integration.conftest import (
    skip_if_no_postgres_infra,
)


# Test events
class OrderCreated(DomainEvent):
    """Test event for order creation."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")
    amount: float = Field(default=0.0, description="Order amount")


class OrderShipped(DomainEvent):
    """Test event for order shipping."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class OrderCancelled(DomainEvent):
    """Test event for order cancellation."""

    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"
    reason: str = Field(..., description="Cancellation reason")


# Test table schema
TEST_ORDERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS test_orders (
    id UUID PRIMARY KEY,
    order_number VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL DEFAULT 'created',
    tracking_number VARCHAR(100),
    cancelled_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


@pytest.fixture
async def orders_table(postgres_engine):
    """Create and clean up test orders table."""
    async with postgres_engine.begin() as conn:
        await conn.execute(text(TEST_ORDERS_SCHEMA))
        await conn.execute(text("TRUNCATE TABLE test_orders CASCADE"))

    yield

    async with postgres_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE test_orders CASCADE"))


@skip_if_no_postgres_infra
@pytest.mark.integration
@pytest.mark.postgres
class TestDatabaseProjectionIntegration:
    """Integration tests for DatabaseProjection with real PostgreSQL."""

    @pytest.mark.asyncio
    async def test_sql_execution_in_handler(
        self,
        postgres_session_factory,
        orders_table,
        postgres_engine,
    ) -> None:
        """Handler can execute SQL against real PostgreSQL."""

        class OrderProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
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

        projection = OrderProjection(session_factory=postgres_session_factory)

        # Handle event
        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            amount=99.99,
        )
        await projection.handle(event)

        # Verify data was inserted
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
        postgres_session_factory,
        orders_table,
        postgres_engine,
    ) -> None:
        """Transaction commits successfully when handler completes."""

        class OrderProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
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

        projection = OrderProjection(session_factory=postgres_session_factory)

        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-002",
            amount=149.99,
        )
        await projection.handle(event)

        # Transaction should be committed - data should persist
        async with postgres_engine.begin() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM test_orders WHERE id = :id"),
                {"id": aggregate_id},
            )
            count = result.scalar()

        assert count == 1

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_handler_error(
        self,
        postgres_session_factory,
        orders_table,
        postgres_engine,
    ) -> None:
        """Transaction is rolled back when handler raises error."""
        checkpoint_repo = InMemoryCheckpointRepository()
        dlq_repo = InMemoryDLQRepository()

        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                # First insert the order
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
                # Then fail
                raise ValueError("Simulated handler error")

        projection = FailingProjection(
            session_factory=postgres_session_factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-003",
            amount=199.99,
        )

        with pytest.raises(ValueError, match="Simulated handler error"):
            await projection.handle(event)

        # Transaction should be rolled back - data should NOT persist
        async with postgres_engine.begin() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM test_orders WHERE id = :id"),
                {"id": aggregate_id},
            )
            count = result.scalar()

        assert count == 0

    @pytest.mark.asyncio
    async def test_checkpoint_not_updated_on_error(
        self,
        postgres_session_factory,
        orders_table,
    ) -> None:
        """Checkpoint is NOT updated when handler raises error."""
        checkpoint_repo = InMemoryCheckpointRepository()
        dlq_repo = InMemoryDLQRepository()

        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
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

        # Checkpoint should NOT be updated
        checkpoint = await checkpoint_repo.get_checkpoint("FailingProjection")
        assert checkpoint is None

    @pytest.mark.asyncio
    async def test_checkpoint_updated_on_success(
        self,
        postgres_session_factory,
        orders_table,
    ) -> None:
        """Checkpoint is updated after successful event processing."""
        checkpoint_repo = InMemoryCheckpointRepository()

        class OrderProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
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

        # Checkpoint should be updated
        checkpoint = await checkpoint_repo.get_checkpoint("OrderProjection")
        assert checkpoint == event.event_id

    @pytest.mark.asyncio
    async def test_multiple_events_processed_sequentially(
        self,
        postgres_session_factory,
        orders_table,
        postgres_engine,
    ) -> None:
        """Multiple events are processed correctly in sequence."""

        class OrderProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
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
            async def _handle_order_shipped(self, conn, event: OrderShipped) -> None:
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

        # Create order
        aggregate_id = uuid4()
        created = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-006",
            amount=129.99,
        )
        await projection.handle(created)

        # Ship order
        shipped = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRK-12345",
        )
        await projection.handle(shipped)

        # Verify final state
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
    async def test_partial_handler_failure_rollback(
        self,
        postgres_session_factory,
        orders_table,
        postgres_engine,
    ) -> None:
        """Multiple inserts in one handler are rolled back together on error."""
        checkpoint_repo = InMemoryCheckpointRepository()
        dlq_repo = InMemoryDLQRepository()

        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                # Insert first order
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

                # Insert second order (related)
                await conn.execute(
                    text("""
                        INSERT INTO test_orders (id, order_number, amount)
                        VALUES (:id, :order_number, :amount)
                    """),
                    {
                        "id": uuid4(),
                        "order_number": f"{event.order_number}-RELATED",
                        "amount": event.amount / 2,
                    },
                )

                # Then fail
                raise ValueError("Simulated failure after inserts")

        projection = FailingProjection(
            session_factory=postgres_session_factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-007",
            amount=200.00,
        )

        with pytest.raises(ValueError):
            await projection.handle(event)

        # BOTH inserts should be rolled back
        async with postgres_engine.begin() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM test_orders"))
            count = result.scalar()

        assert count == 0

    @pytest.mark.asyncio
    async def test_single_param_handler_with_database(
        self,
        postgres_session_factory,
        orders_table,
    ) -> None:
        """Single parameter handlers work within DatabaseProjection."""
        events_handled = []

        class MixedProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                # Handler without database connection
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
        postgres_session_factory,
        orders_table,
        postgres_engine,
    ) -> None:
        """Projection can have both 1-param and 2-param handlers."""
        one_param_events = []

        class MixedProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                # Two-param handler with database access
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
                # Single-param handler without database access
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

        # Verify database write
        async with postgres_engine.begin() as conn:
            result = await conn.execute(
                text("SELECT COUNT(*) FROM test_orders WHERE id = :id"),
                {"id": aggregate_id},
            )
            count = result.scalar()

        assert count == 1

        # Verify single-param handler was called
        assert len(one_param_events) == 1

    @pytest.mark.asyncio
    async def test_event_goes_to_dlq_on_persistent_failure(
        self,
        postgres_session_factory,
        orders_table,
    ) -> None:
        """Failed events go to DLQ after retries exhausted."""
        checkpoint_repo = InMemoryCheckpointRepository()
        dlq_repo = InMemoryDLQRepository()

        class FailingProjection(DatabaseProjection):
            MAX_RETRIES = 2
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
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

        # Event should be in DLQ
        failed_events = await dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event.event_id
        assert "Persistent handler failure" in failed_events[0].error_message
