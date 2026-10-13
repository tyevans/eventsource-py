"""Integration tests for command-to-projection flow."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource import (
    AggregateRepository,
    DomainEvent,
    InMemoryEventBus,
)
from eventsource.adapters.postgresql import PostgreSQLEventStore
from tests.integration.conftest import (
    TestOrderAggregate,
    TestOrderCompleted,
    TestOrderCreated,
    TestOrderItemAdded,
    skip_if_no_postgres_infra,
)

from ._models import OrderSummary

if TYPE_CHECKING:
    pass


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.e2e,
    skip_if_no_postgres_infra,
]


class TestOrderProjection:
    """Simple projection that builds OrderSummary read models."""

    __test__ = False

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
