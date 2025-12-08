"""
End-to-end integration tests for complete event sourcing workflows.

These tests verify the full event flow from command to projection:
Command -> Aggregate -> EventStore -> EventBus -> Projection -> ReadModel

Tests cover:
- Full aggregate lifecycle with PostgreSQL persistence
- Event publishing through the bus
- Projection updates from events
- Repository pattern integration
- Multi-aggregate scenarios
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    DomainEvent,
    InMemoryEventBus,
    PostgreSQLEventStore,
)

from ..conftest import (
    TestOrderAggregate,
    TestOrderCompleted,
    TestOrderCreated,
    TestOrderItemAdded,
    skip_if_no_postgres_infra,
)

if TYPE_CHECKING:
    pass


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.e2e,
    skip_if_no_postgres_infra,
]


# =============================================================================
# Simple Projection for Testing
# =============================================================================


class OrderSummary(BaseModel):
    """Read model for order summary."""

    order_id: UUID
    customer_id: UUID
    item_count: int = 0
    total_amount: float = 0.0
    status: str = "pending"
    completed_at: datetime | None = None


class TestOrderProjection:
    """Simple projection that builds OrderSummary read models."""

    def __init__(self) -> None:
        self._orders: dict[UUID, OrderSummary] = {}
        self._events_processed: int = 0

    @property
    def orders(self) -> dict[UUID, OrderSummary]:
        return self._orders

    @property
    def events_processed(self) -> int:
        return self._events_processed

    async def handle(self, event: DomainEvent) -> None:
        """Handle events to update read model."""
        if isinstance(event, TestOrderCreated):
            self._orders[event.aggregate_id] = OrderSummary(
                order_id=event.aggregate_id,
                customer_id=event.customer_id,
                total_amount=event.total_amount,
                status="created",
            )
        elif isinstance(event, TestOrderItemAdded):
            if event.aggregate_id in self._orders:
                order = self._orders[event.aggregate_id]
                self._orders[event.aggregate_id] = order.model_copy(
                    update={
                        "item_count": order.item_count + 1,
                        "total_amount": order.total_amount + (event.quantity * event.unit_price),
                    }
                )
        elif isinstance(event, TestOrderCompleted) and event.aggregate_id in self._orders:
            order = self._orders[event.aggregate_id]
            self._orders[event.aggregate_id] = order.model_copy(
                update={
                    "status": "completed",
                    "completed_at": event.completed_at,
                }
            )

        self._events_processed += 1


# =============================================================================
# Full Flow Tests
# =============================================================================


class TestCommandToProjectionFlow:
    """Tests for complete command-to-projection flow."""

    async def test_create_order_flow(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test creating an order through the full flow."""
        # Setup
        event_bus = InMemoryEventBus()
        projection = TestOrderProjection()
        event_bus.subscribe(TestOrderCreated, projection)
        event_bus.subscribe(TestOrderItemAdded, projection)
        event_bus.subscribe(TestOrderCompleted, projection)

        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
            event_publisher=event_bus,
        )

        # Execute command
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id, total_amount=0.0)

        await repo.save(order)

        # Verify projection was updated
        assert order_id in projection.orders
        assert projection.orders[order_id].customer_id == sample_customer_id
        assert projection.orders[order_id].status == "created"

    async def test_add_items_flow(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test adding items to an order through the full flow."""
        # Setup
        event_bus = InMemoryEventBus()
        projection = TestOrderProjection()
        event_bus.subscribe(TestOrderCreated, projection)
        event_bus.subscribe(TestOrderItemAdded, projection)
        event_bus.subscribe(TestOrderCompleted, projection)

        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
            event_publisher=event_bus,
        )

        # Create order
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id)
        await repo.save(order)

        # Add items
        order = await repo.load(order_id)
        order.add_item(
            item_id=uuid4(),
            item_name="Widget",
            quantity=2,
            unit_price=10.0,
        )
        order.add_item(
            item_id=uuid4(),
            item_name="Gadget",
            quantity=1,
            unit_price=25.0,
        )
        await repo.save(order)

        # Verify projection
        summary = projection.orders[order_id]
        assert summary.item_count == 2
        assert summary.total_amount == 45.0  # 2*10 + 1*25

    async def test_complete_order_flow(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test completing an order through the full flow."""
        # Setup
        event_bus = InMemoryEventBus()
        projection = TestOrderProjection()
        event_bus.subscribe(TestOrderCreated, projection)
        event_bus.subscribe(TestOrderItemAdded, projection)
        event_bus.subscribe(TestOrderCompleted, projection)

        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
            event_publisher=event_bus,
        )

        # Create and add item
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id)
        await repo.save(order)

        order = await repo.load(order_id)
        order.add_item(
            item_id=uuid4(),
            item_name="Product",
            quantity=1,
            unit_price=50.0,
        )
        await repo.save(order)

        # Complete order
        order = await repo.load(order_id)
        order.complete_order()
        await repo.save(order)

        # Verify projection
        summary = projection.orders[order_id]
        assert summary.status == "completed"
        assert summary.completed_at is not None


class TestAggregateRehydration:
    """Tests for aggregate rehydration from event store."""

    async def test_load_aggregate_from_history(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test that aggregate can be fully reconstructed from events."""
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
        )

        # Create order with items
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id)
        await repo.save(order)

        order = await repo.load(order_id)
        order.add_item(
            item_id=uuid4(),
            item_name="Item 1",
            quantity=3,
            unit_price=15.0,
        )
        order.add_item(
            item_id=uuid4(),
            item_name="Item 2",
            quantity=2,
            unit_price=20.0,
        )
        await repo.save(order)

        # Load fresh instance
        loaded_order = await repo.load(order_id)

        assert loaded_order.version == 3
        assert loaded_order.state is not None
        assert loaded_order.state.customer_id == sample_customer_id
        assert len(loaded_order.state.items) == 2
        assert loaded_order.state.total_amount == 85.0  # 3*15 + 2*20

    async def test_load_nonexistent_aggregate(
        self,
        postgres_event_store: PostgreSQLEventStore,
    ) -> None:
        """Test that loading non-existent aggregate raises error."""
        from eventsource import AggregateNotFoundError

        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
        )

        with pytest.raises(AggregateNotFoundError):
            await repo.load(uuid4())

    async def test_load_or_create(
        self,
        postgres_event_store: PostgreSQLEventStore,
    ) -> None:
        """Test load_or_create returns empty aggregate if not found."""
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
        )

        order_id = uuid4()
        order = await repo.load_or_create(order_id)

        assert order.version == 0
        assert order.aggregate_id == order_id


class TestOptimisticLocking:
    """Tests for optimistic locking in concurrent scenarios."""

    async def test_concurrent_modifications_fail(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test that concurrent modifications to same aggregate fail."""
        from eventsource import OptimisticLockError

        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
        )

        # Create order
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id)
        await repo.save(order)

        # Load two instances
        order1 = await repo.load(order_id)
        order2 = await repo.load(order_id)

        # Modify and save first
        order1.add_item(uuid4(), "Item 1", 1, 10.0)
        await repo.save(order1)

        # Try to modify and save second (should fail)
        order2.add_item(uuid4(), "Item 2", 1, 20.0)

        with pytest.raises(OptimisticLockError):
            await repo.save(order2)


class TestMultiAggregateScenarios:
    """Tests involving multiple aggregates."""

    async def test_multiple_orders_independence(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test that multiple orders are independent."""
        event_bus = InMemoryEventBus()
        projection = TestOrderProjection()
        event_bus.subscribe(TestOrderCreated, projection)
        event_bus.subscribe(TestOrderItemAdded, projection)

        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
            event_publisher=event_bus,
        )

        # Create multiple orders
        order_ids = []
        for i in range(3):
            order_id = uuid4()
            order_ids.append(order_id)

            order = TestOrderAggregate(order_id)
            order.create_order(customer_id=sample_customer_id)
            await repo.save(order)

            order = await repo.load(order_id)
            order.add_item(uuid4(), f"Item for order {i}", i + 1, 10.0)
            await repo.save(order)

        # Verify each order has correct state
        for i, order_id in enumerate(order_ids):
            order = await repo.load(order_id)
            assert order.state.total_amount == (i + 1) * 10.0

            summary = projection.orders[order_id]
            assert summary.item_count == 1
            assert summary.total_amount == (i + 1) * 10.0


class TestEventStreamOperations:
    """Tests for event stream operations."""

    async def test_read_events_from_position(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test reading events from a specific position."""
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
        )

        # Create order with multiple events
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id)
        await repo.save(order)

        order = await repo.load(order_id)
        for i in range(3):
            order.add_item(uuid4(), f"Item {i}", 1, 10.0)
        await repo.save(order)

        # Read all events
        stream = await postgres_event_store.get_events(order_id, "TestOrder")
        assert len(stream.events) == 4  # 1 created + 3 items

        # Read from version 2
        stream_partial = await postgres_event_store.get_events(
            order_id,
            "TestOrder",
            from_version=2,
        )
        assert len(stream_partial.events) == 2  # Events 3 and 4

    async def test_read_all_events_global(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test reading all events across aggregates."""
        # Create multiple aggregates with events
        for _ in range(3):
            order_id = uuid4()
            event = TestOrderCreated(
                aggregate_id=order_id,
                aggregate_version=1,
                customer_id=sample_customer_id,
                total_amount=0.0,
            )
            await postgres_event_store.append_events(
                aggregate_id=order_id,
                aggregate_type="TestOrder",
                events=[event],
                expected_version=0,
            )

        # Read all events
        all_events = []
        async for stored_event in postgres_event_store.read_all():
            all_events.append(stored_event)

        assert len(all_events) >= 3

        # Verify global ordering
        positions = [e.global_position for e in all_events]
        assert positions == sorted(positions)


class TestProjectionRebuilding:
    """Tests for projection rebuilding scenarios."""

    async def test_rebuild_projection_from_events(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test rebuilding a projection by replaying all events."""
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
            aggregate_type="TestOrder",
        )

        # Create some orders without projection
        order_ids = []
        for i in range(3):
            order_id = uuid4()
            order_ids.append(order_id)

            order = TestOrderAggregate(order_id)
            order.create_order(customer_id=sample_customer_id)
            await repo.save(order)

            order = await repo.load(order_id)
            order.add_item(uuid4(), f"Item {i}", i + 1, 10.0)
            await repo.save(order)

        # Now "rebuild" projection by reading all events
        projection = TestOrderProjection()

        async for stored_event in postgres_event_store.read_all():
            await projection.handle(stored_event.event)

        # Verify projection has correct state
        assert len(projection.orders) == 3
        assert projection.events_processed >= 6  # 3 creates + 3 items


class TestOutboxIntegration:
    """Tests for outbox pattern integration."""

    async def test_outbox_enabled_stores_events(
        self,
        postgres_event_store_with_outbox: PostgreSQLEventStore,
        postgres_outbox_repo,
        sample_customer_id: UUID,
    ) -> None:
        """Test that events are stored in outbox when enabled."""
        order_id = uuid4()
        event = TestOrderCreated(
            aggregate_id=order_id,
            aggregate_version=1,
            customer_id=sample_customer_id,
            total_amount=100.0,
        )

        await postgres_event_store_with_outbox.append_events(
            aggregate_id=order_id,
            aggregate_type="TestOrder",
            events=[event],
            expected_version=0,
        )

        # Verify event is in outbox
        pending = await postgres_outbox_repo.get_pending_events()
        matching = [e for e in pending if e.event_id == event.event_id]
        assert len(matching) == 1
        assert matching[0].event_type == "TestOrderCreated"
