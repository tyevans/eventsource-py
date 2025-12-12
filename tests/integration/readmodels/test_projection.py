"""
Integration tests for ReadModelProjection.

Tests the projection with real database backends to ensure
handler routing and repository operations work correctly.

These tests verify:
- Event processing via handler methods
- Repository operations (save, update, soft delete)
- Checkpoint tracking integration
- Reset/truncate functionality
- Event routing to correct handlers
- Multi-event workflows (create -> update -> delete)
- Error handling and transaction rollback
- Handler parameter introspection (1-param vs 2-param handlers)

Tests run against:
- SQLite (always available)
- PostgreSQL (when Docker/testcontainers available)
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource.events import DomainEvent
from eventsource.projections import handles
from eventsource.readmodels import (
    ReadModel,
    ReadModelProjection,
    generate_schema,
)
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository

from ..conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from typing import Any

    from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker


pytestmark = [
    pytest.mark.integration,
]


# ============================================================================
# Test Events
# ============================================================================


class OrderCreated(DomainEvent):
    """Test event for order creation."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    order_number: str
    customer_name: str
    total_amount: Decimal


class OrderShipped(DomainEvent):
    """Test event for order shipping."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


class OrderCancelled(DomainEvent):
    """Test event for order cancellation."""

    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"
    reason: str


class OrderCompleted(DomainEvent):
    """Test event for order completion (no handler defined - tests unhandled events)."""

    event_type: str = "OrderCompleted"
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
# Test Projection
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

    # Track events handled without repository access
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
) -> OrderProjection:
    """Create projection with PostgreSQL backend."""
    from sqlalchemy import text

    # Create table
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

    # Cleanup
    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))


@pytest_asyncio.fixture
async def postgresql_mixed_projection(
    postgres_engine: AsyncEngine,
    postgres_session_factory: async_sessionmaker[Any],
) -> MixedHandlerProjection:
    """Create mixed handler projection with PostgreSQL backend."""
    from sqlalchemy import text

    # Create table
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

    # Cleanup
    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))


@pytest_asyncio.fixture
async def postgresql_failing_projection(
    postgres_engine: AsyncEngine,
    postgres_session_factory: async_sessionmaker[Any],
) -> FailingProjection:
    """Create failing projection with PostgreSQL backend."""
    from sqlalchemy import text

    # Create table
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

    # Cleanup
    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {OrderSummary.table_name()}"))


# ============================================================================
# SQLite Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def sqlite_projection(tmp_path: Any) -> OrderProjection:
    """Create projection with SQLite backend."""
    import aiosqlite
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    db_path = tmp_path / "test_projection.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    # Create table using raw aiosqlite for setup
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
async def sqlite_mixed_projection(tmp_path: Any) -> MixedHandlerProjection:
    """Create mixed handler projection with SQLite backend."""
    import aiosqlite
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    db_path = tmp_path / "test_mixed_projection.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    # Create table using raw aiosqlite for setup
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
async def sqlite_failing_projection(tmp_path: Any) -> FailingProjection:
    """Create failing projection with SQLite backend."""
    import aiosqlite
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    db_path = tmp_path / "test_failing_projection.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    # Create table using raw aiosqlite for setup
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


# ============================================================================
# SQLite CRUD Tests
# ============================================================================


class TestSQLiteReadModelProjectionCRUD:
    """Test CRUD operations via projection handlers with SQLite."""

    @pytest.mark.asyncio
    async def test_create_via_handler(self, sqlite_projection: OrderProjection) -> None:
        """Test creating a read model via handler."""
        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-001",
            customer_name="Alice",
            total_amount=Decimal("99.99"),
        )

        await sqlite_projection.handle(event)

        # Verify via direct repository access
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
        """Test updating a read model via handler."""
        order_id = uuid4()

        # Create
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-002",
            customer_name="Bob",
            total_amount=Decimal("150.00"),
        )
        await sqlite_projection.handle(create_event)

        # Update
        ship_event = OrderShipped(
            aggregate_id=order_id,
            tracking_number="TRK-12345",
        )
        await sqlite_projection.handle(ship_event)

        # Verify
        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is not None
        assert summary.status == "shipped"
        assert summary.tracking_number == "TRK-12345"
        assert summary.version == 2  # Updated once

    @pytest.mark.asyncio
    async def test_soft_delete_via_handler(self, sqlite_projection: OrderProjection) -> None:
        """Test soft deleting via handler."""
        order_id = uuid4()

        # Create
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-003",
            customer_name="Charlie",
            total_amount=Decimal("50.00"),
        )
        await sqlite_projection.handle(create_event)

        # Cancel (soft delete)
        cancel_event = OrderCancelled(
            aggregate_id=order_id,
            reason="Customer request",
        )
        await sqlite_projection.handle(cancel_event)

        # Verify - should be hidden from normal get
        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is None  # Hidden due to soft delete


# ============================================================================
# SQLite Checkpoint Tests
# ============================================================================


class TestSQLiteReadModelProjectionCheckpoint:
    """Test checkpoint tracking integration with SQLite."""

    @pytest.mark.asyncio
    async def test_checkpoint_updated_after_handle(
        self, sqlite_projection: OrderProjection
    ) -> None:
        """Test that checkpoint is updated after successful handle."""
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
        """Test that checkpoint tracks the latest processed event."""
        order_id = uuid4()

        # Process multiple events
        events = [
            OrderCreated(
                aggregate_id=order_id,
                order_number="ORD-005",
                customer_name="Eve",
                total_amount=Decimal("100.00"),
            ),
            OrderShipped(
                aggregate_id=order_id,
                tracking_number="TRK-11111",
            ),
        ]

        for event in events:
            await sqlite_projection.handle(event)

        # Checkpoint should be the last event
        checkpoint = await sqlite_projection.get_checkpoint()
        assert checkpoint == str(events[-1].event_id)


# ============================================================================
# SQLite Reset Tests
# ============================================================================


class TestSQLiteReadModelProjectionReset:
    """Test reset/truncate functionality with SQLite."""

    @pytest.mark.asyncio
    async def test_reset_truncates_data(self, sqlite_projection: OrderProjection) -> None:
        """Test that reset() truncates all read model data."""
        # Create some data
        for i in range(3):
            event = OrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:03d}",
                customer_name=f"Customer {i}",
                total_amount=Decimal(str(i * 100)),
            )
            await sqlite_projection.handle(event)

        # Verify data exists
        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            count_before = await repo.count()

        assert count_before == 3

        # Reset
        await sqlite_projection.reset()

        # Verify data cleared
        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            count_after = await repo.count()

        assert count_after == 0

    @pytest.mark.asyncio
    async def test_reset_clears_checkpoint(self, sqlite_projection: OrderProjection) -> None:
        """Test that reset() clears the checkpoint."""
        # Process an event
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-006",
            customer_name="Frank",
            total_amount=Decimal("300.00"),
        )
        await sqlite_projection.handle(event)

        # Verify checkpoint exists
        checkpoint = await sqlite_projection.get_checkpoint()
        assert checkpoint is not None

        # Reset
        await sqlite_projection.reset()

        # Checkpoint should be cleared
        checkpoint = await sqlite_projection.get_checkpoint()
        assert checkpoint is None


# ============================================================================
# SQLite Event Routing Tests
# ============================================================================


class TestSQLiteReadModelProjectionEventRouting:
    """Test event routing to correct handlers with SQLite."""

    @pytest.mark.asyncio
    async def test_multiple_events_routed_correctly(
        self, sqlite_projection: OrderProjection
    ) -> None:
        """Test that different events route to correct handlers."""
        order_id = uuid4()

        events = [
            OrderCreated(
                aggregate_id=order_id,
                order_number="ORD-007",
                customer_name="Grace",
                total_amount=Decimal("300.00"),
            ),
            OrderShipped(
                aggregate_id=order_id,
                tracking_number="TRK-99999",
            ),
        ]

        for event in events:
            await sqlite_projection.handle(event)

        # Verify final state
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
        """Test that subscribed_to returns all handled event types."""
        subscribed = sqlite_projection.subscribed_to()

        assert OrderCreated in subscribed
        assert OrderShipped in subscribed
        assert OrderCancelled in subscribed
        assert len(subscribed) == 3

    @pytest.mark.asyncio
    async def test_unhandled_event_is_ignored(self, sqlite_projection: OrderProjection) -> None:
        """Test that unhandled events are silently ignored."""
        # OrderCompleted has no handler defined
        event = OrderCompleted(
            aggregate_id=uuid4(),
        )

        # Should not raise, should be silently ignored
        await sqlite_projection.handle(event)

        # Checkpoint should still be updated (event was processed)
        checkpoint = await sqlite_projection.get_checkpoint()
        assert checkpoint == str(event.event_id)


# ============================================================================
# SQLite Mixed Handler Tests
# ============================================================================


class TestSQLiteMixedHandlerProjection:
    """Test projections with mixed handler signatures (1-param and 2-param)."""

    @pytest.mark.asyncio
    async def test_two_param_handler_with_repo_access(
        self, sqlite_mixed_projection: MixedHandlerProjection
    ) -> None:
        """Test that 2-param handlers receive repository and work correctly."""
        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-008",
            customer_name="Henry",
            total_amount=Decimal("400.00"),
        )

        await sqlite_mixed_projection.handle(event)

        # Verify data was saved
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
        """Test that 1-param handlers work without repository."""
        order_id = uuid4()

        # First create the order
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-009",
            customer_name="Ivy",
            total_amount=Decimal("500.00"),
        )
        await sqlite_mixed_projection.handle(create_event)

        # Now ship it (1-param handler)
        ship_event = OrderShipped(
            aggregate_id=order_id,
            tracking_number="TRK-22222",
        )
        await sqlite_mixed_projection.handle(ship_event)

        # Verify the event was tracked in the list
        assert len(sqlite_mixed_projection.events_without_repo) == 1
        assert sqlite_mixed_projection.events_without_repo[0] == ship_event


# ============================================================================
# SQLite Error Handling Tests
# ============================================================================


class TestSQLiteReadModelProjectionErrorHandling:
    """Test error handling and transaction rollback with SQLite."""

    @pytest.mark.asyncio
    async def test_handler_failure_sends_to_dlq(
        self, sqlite_failing_projection: FailingProjection
    ) -> None:
        """Test that failed events are sent to DLQ after retries exhausted."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-010",
            customer_name="Jack",
            total_amount=Decimal("600.00"),
        )

        with pytest.raises(ValueError, match="Simulated handler failure"):
            await sqlite_failing_projection.handle(event)

        # Verify event is in DLQ
        failed_events = await sqlite_failing_projection._dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event.event_id
        assert "Simulated handler failure" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_checkpoint_not_updated_on_failure(
        self, sqlite_failing_projection: FailingProjection
    ) -> None:
        """Test that checkpoint is NOT updated when handler fails."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-011",
            customer_name="Kate",
            total_amount=Decimal("700.00"),
        )

        with pytest.raises(ValueError):
            await sqlite_failing_projection.handle(event)

        # Checkpoint should NOT be updated
        checkpoint = await sqlite_failing_projection.get_checkpoint()
        assert checkpoint is None


# ============================================================================
# SQLite End-to-End Workflow Tests
# ============================================================================


class TestSQLiteReadModelProjectionWorkflow:
    """Test full end-to-end workflows with SQLite."""

    @pytest.mark.asyncio
    async def test_full_order_lifecycle(self, sqlite_projection: OrderProjection) -> None:
        """Test complete order lifecycle: create -> ship -> cancel."""
        order_id = uuid4()

        # Create order
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="ORD-LIFECYCLE",
            customer_name="Lifecycle Test",
            total_amount=Decimal("999.99"),
        )
        await sqlite_projection.handle(create_event)

        # Verify created
        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None
        assert summary.status == "pending"

        # Ship order
        ship_event = OrderShipped(
            aggregate_id=order_id,
            tracking_number="TRK-LIFECYCLE",
        )
        await sqlite_projection.handle(ship_event)

        # Verify shipped
        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None
        assert summary.status == "shipped"
        assert summary.tracking_number == "TRK-LIFECYCLE"
        assert summary.version == 2

        # Cancel order (soft delete)
        cancel_event = OrderCancelled(
            aggregate_id=order_id,
            reason="Test cancellation",
        )
        await sqlite_projection.handle(cancel_event)

        # Verify cancelled (soft deleted - not visible)
        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is None  # Soft deleted

    @pytest.mark.asyncio
    async def test_multiple_orders_processed_independently(
        self, sqlite_projection: OrderProjection
    ) -> None:
        """Test processing multiple orders independently."""
        order_ids = [uuid4() for _ in range(3)]

        # Create orders
        for i, order_id in enumerate(order_ids):
            event = OrderCreated(
                aggregate_id=order_id,
                order_number=f"ORD-MULTI-{i:03d}",
                customer_name=f"Customer {i}",
                total_amount=Decimal(str((i + 1) * 100)),
            )
            await sqlite_projection.handle(event)

        # Ship only the first order
        ship_event = OrderShipped(
            aggregate_id=order_ids[0],
            tracking_number="TRK-FIRST",
        )
        await sqlite_projection.handle(ship_event)

        # Cancel only the last order
        cancel_event = OrderCancelled(
            aggregate_id=order_ids[2],
            reason="Cancel last",
        )
        await sqlite_projection.handle(cancel_event)

        # Verify states
        async with sqlite_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await sqlite_projection._create_repository(conn)

            # First order: shipped
            order0 = await repo.get(order_ids[0])
            assert order0 is not None
            assert order0.status == "shipped"
            assert order0.tracking_number == "TRK-FIRST"

            # Second order: still pending
            order1 = await repo.get(order_ids[1])
            assert order1 is not None
            assert order1.status == "pending"

            # Third order: soft deleted (not visible)
            order2 = await repo.get(order_ids[2])
            assert order2 is None


# ============================================================================
# PostgreSQL Tests (marked to skip if infrastructure not available)
# ============================================================================


@skip_if_no_postgres_infra
@pytest.mark.postgres
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

        # Verify via direct repository access
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

        # Create
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-002",
            customer_name="PG Bob",
            total_amount=Decimal("150.00"),
        )
        await postgresql_projection.handle(create_event)

        # Update
        ship_event = OrderShipped(
            aggregate_id=order_id,
            tracking_number="PG-TRK-12345",
        )
        await postgresql_projection.handle(ship_event)

        # Verify
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

        # Create
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-003",
            customer_name="PG Charlie",
            total_amount=Decimal("50.00"),
        )
        await postgresql_projection.handle(create_event)

        # Cancel (soft delete)
        cancel_event = OrderCancelled(
            aggregate_id=order_id,
            reason="PG Customer request",
        )
        await postgresql_projection.handle(cancel_event)

        # Verify - should be hidden from normal get
        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)

        assert summary is None


@skip_if_no_postgres_infra
@pytest.mark.postgres
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


@skip_if_no_postgres_infra
@pytest.mark.postgres
class TestPostgreSQLReadModelProjectionReset:
    """Test reset/truncate functionality with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_reset_truncates_data(self, postgresql_projection: OrderProjection) -> None:
        """Test that reset() truncates all read model data."""
        # Create some data
        for i in range(3):
            event = OrderCreated(
                aggregate_id=uuid4(),
                order_number=f"PG-ORD-{i:03d}",
                customer_name=f"PG Customer {i}",
                total_amount=Decimal(str(i * 100)),
            )
            await postgresql_projection.handle(event)

        # Verify data exists
        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            count_before = await repo.count()

        assert count_before == 3

        # Reset
        await postgresql_projection.reset()

        # Verify data cleared
        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            count_after = await repo.count()

        assert count_after == 0


@skip_if_no_postgres_infra
@pytest.mark.postgres
class TestPostgreSQLReadModelProjectionEventRouting:
    """Test event routing to correct handlers with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_multiple_events_routed_correctly(
        self, postgresql_projection: OrderProjection
    ) -> None:
        """Test that different events route to correct handlers."""
        order_id = uuid4()

        events = [
            OrderCreated(
                aggregate_id=order_id,
                order_number="PG-ORD-005",
                customer_name="PG Eve",
                total_amount=Decimal("300.00"),
            ),
            OrderShipped(
                aggregate_id=order_id,
                tracking_number="PG-TRK-99999",
            ),
        ]

        for event in events:
            await postgresql_projection.handle(event)

        # Verify final state
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
        """Test that subscribed_to returns all handled event types."""
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
        """Test both 1-param and 2-param handlers work in PostgreSQL."""
        order_id = uuid4()

        # Create via 2-param handler
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-MIXED",
            customer_name="PG Mixed Test",
            total_amount=Decimal("500.00"),
        )
        await postgresql_mixed_projection.handle(create_event)

        # Verify data was saved
        async with postgresql_mixed_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_mixed_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None

        # Ship via 1-param handler
        ship_event = OrderShipped(
            aggregate_id=order_id,
            tracking_number="PG-TRK-MIXED",
        )
        await postgresql_mixed_projection.handle(ship_event)

        # Verify 1-param handler received the event
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
        """Test that failed events are sent to DLQ after retries exhausted."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="PG-ORD-FAIL",
            customer_name="PG Fail Test",
            total_amount=Decimal("600.00"),
        )

        with pytest.raises(ValueError, match="Simulated handler failure"):
            await postgresql_failing_projection.handle(event)

        # Verify event is in DLQ
        failed_events = await postgresql_failing_projection._dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event.event_id


@skip_if_no_postgres_infra
@pytest.mark.postgres
class TestPostgreSQLReadModelProjectionWorkflow:
    """Test full end-to-end workflows with PostgreSQL."""

    @pytest.mark.asyncio
    async def test_full_order_lifecycle(self, postgresql_projection: OrderProjection) -> None:
        """Test complete order lifecycle: create -> ship -> cancel."""
        order_id = uuid4()

        # Create order
        create_event = OrderCreated(
            aggregate_id=order_id,
            order_number="PG-ORD-LIFECYCLE",
            customer_name="PG Lifecycle Test",
            total_amount=Decimal("999.99"),
        )
        await postgresql_projection.handle(create_event)

        # Verify created
        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None
        assert summary.status == "pending"

        # Ship order
        ship_event = OrderShipped(
            aggregate_id=order_id,
            tracking_number="PG-TRK-LIFECYCLE",
        )
        await postgresql_projection.handle(ship_event)

        # Verify shipped
        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is not None
        assert summary.status == "shipped"
        assert summary.tracking_number == "PG-TRK-LIFECYCLE"

        # Cancel order
        cancel_event = OrderCancelled(
            aggregate_id=order_id,
            reason="PG Test cancellation",
        )
        await postgresql_projection.handle(cancel_event)

        # Verify cancelled (soft deleted)
        async with postgresql_projection._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await postgresql_projection._create_repository(conn)
            summary = await repo.get(order_id)
        assert summary is None


# ============================================================================
# Projection Properties Tests
# ============================================================================


class TestReadModelProjectionProperties:
    """Test ReadModelProjection property accessors."""

    @pytest.mark.asyncio
    async def test_model_class_property(self, sqlite_projection: OrderProjection) -> None:
        """Test that model_class property returns the correct type."""
        assert sqlite_projection.model_class is OrderSummary

    @pytest.mark.asyncio
    async def test_repository_property_outside_handle(
        self, sqlite_projection: OrderProjection
    ) -> None:
        """Test that repository property returns None outside handle()."""
        assert sqlite_projection.repository is None

    @pytest.mark.asyncio
    async def test_projection_name_property(self, sqlite_projection: OrderProjection) -> None:
        """Test that projection_name returns the class name."""
        assert sqlite_projection.projection_name == "OrderProjection"

    @pytest.mark.asyncio
    async def test_repr(self, sqlite_projection: OrderProjection) -> None:
        """Test string representation."""
        repr_str = repr(sqlite_projection)
        assert "OrderProjection" in repr_str
        assert "OrderSummary" in repr_str
        assert "order_summaries" in repr_str
