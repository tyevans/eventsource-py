"""Integration tests for aggregate rehydration, optimistic locking, and multi-aggregate scenarios."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource import (
    AggregateNotFoundError,
    AggregateRepository,
    InMemoryEventBus,
    OptimisticLockError,
)
from eventsource.adapters.postgresql import PostgreSQLEventStore
from tests.integration.conftest import (
    TestOrderAggregate,
    TestOrderCreated,
    TestOrderItemAdded,
    skip_if_no_postgres_infra,
)

from .test_order_projection import TestOrderProjection

if TYPE_CHECKING:
    pass


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.e2e,
    skip_if_no_postgres_infra,
]


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
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
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
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
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
