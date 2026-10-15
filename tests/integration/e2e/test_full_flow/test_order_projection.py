"""Integration tests for command-to-projection flow."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource import AggregateRepository, DomainEvent, InMemoryEventBus
from eventsource.adapters.postgresql import PostgreSQLEventStore
from tests.integration.conftest import (
    TestOrderAggregate,
    TestOrderCompleted,
    TestOrderCreated,
    TestOrderItemAdded,
    skip_if_no_postgres_infra,
)

from ._models import OrderSummary

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
        elif isinstance(event, TestOrderItemAdded) and event.aggregate_id in self._orders:
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
                update={"status": "completed", "completed_at": event.completed_at}
            )
        self._events_processed += 1


def _build_flow(
    postgres_event_store: PostgreSQLEventStore,
) -> tuple[AggregateRepository, TestOrderProjection]:
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
    return repo, projection


class TestCommandToProjectionFlow:
    """Tests for complete command-to-projection flow."""

    async def test_create_order_flow(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        repo, projection = _build_flow(postgres_event_store)
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id, total_amount=0.0)
        await repo.save(order)
        assert order_id in projection.orders
        assert projection.orders[order_id].customer_id == sample_customer_id
        assert projection.orders[order_id].status == "created"

    async def test_add_items_flow(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        repo, projection = _build_flow(postgres_event_store)
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id)
        await repo.save(order)

        order = await repo.load(order_id)
        order.add_item(item_id=uuid4(), item_name="Widget", quantity=2, unit_price=10.0)
        order.add_item(item_id=uuid4(), item_name="Gadget", quantity=1, unit_price=25.0)
        await repo.save(order)

        summary = projection.orders[order_id]
        assert summary.item_count == 2 and summary.total_amount == 45.0

    async def test_complete_order_flow(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        repo, projection = _build_flow(postgres_event_store)
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id)
        await repo.save(order)

        order = await repo.load(order_id)
        order.add_item(item_id=uuid4(), item_name="Product", quantity=1, unit_price=50.0)
        await repo.save(order)

        order = await repo.load(order_id)
        order.complete_order()
        await repo.save(order)

        summary = projection.orders[order_id]
        assert summary.status == "completed" and summary.completed_at is not None
