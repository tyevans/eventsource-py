"""Test domain events, states, and aggregates for integration tests."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from eventsource import (
    DeclarativeAggregate,
    DomainEvent,
    handles,
    register_event,
)


@register_event
class TestItemCreated(DomainEvent):
    """Test event for item creation."""

    aggregate_type: str = "TestItem"
    name: str
    quantity: int = 1


@register_event
class TestItemUpdated(DomainEvent):
    """Test event for item updates."""

    aggregate_type: str = "TestItem"
    name: str | None = None
    quantity: int | None = None


@register_event
class TestItemDeleted(DomainEvent):
    """Test event for item deletion."""

    aggregate_type: str = "TestItem"


@register_event
class TestOrderCreated(DomainEvent):
    """Test event for order creation (used in e2e tests)."""

    aggregate_type: str = "TestOrder"
    customer_id: UUID
    total_amount: float = 0.0


@register_event
class TestOrderItemAdded(DomainEvent):
    """Test event for adding items to an order."""

    aggregate_type: str = "TestOrder"
    item_id: UUID
    item_name: str
    quantity: int
    unit_price: float


@register_event
class TestOrderCompleted(DomainEvent):
    """Test event for order completion."""

    aggregate_type: str = "TestOrder"
    completed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TestOrderState(BaseModel):
    """State for TestOrderAggregate."""

    order_id: UUID
    customer_id: UUID | None = None
    items: list[dict[str, Any]] = []
    total_amount: float = 0.0
    status: str = "pending"
    completed_at: datetime | None = None


class TestOrderAggregate(DeclarativeAggregate[TestOrderState]):
    """Test aggregate for integration tests."""

    aggregate_type = "TestOrder"

    def _get_initial_state(self) -> TestOrderState:
        return TestOrderState(order_id=self.aggregate_id)

    @handles(TestOrderCreated)
    def _on_order_created(self, event: TestOrderCreated) -> None:
        self._state = TestOrderState(
            order_id=self.aggregate_id,
            customer_id=event.customer_id,
            total_amount=event.total_amount,
            status="created",
        )

    @handles(TestOrderItemAdded)
    def _on_item_added(self, event: TestOrderItemAdded) -> None:
        if self._state:
            item = {
                "item_id": str(event.item_id),
                "name": event.item_name,
                "quantity": event.quantity,
                "unit_price": event.unit_price,
            }
            new_items = [*self._state.items, item]
            new_total = self._state.total_amount + (event.quantity * event.unit_price)
            self._state = self._state.model_copy(
                update={"items": new_items, "total_amount": new_total}
            )

    @handles(TestOrderCompleted)
    def _on_order_completed(self, event: TestOrderCompleted) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={"status": "completed", "completed_at": event.completed_at}
            )

    def create_order(self, customer_id: UUID, total_amount: float = 0.0) -> None:
        """Create a new order."""
        if self.version > 0:
            raise ValueError("Order already exists")
        event = TestOrderCreated(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
            customer_id=customer_id,
            total_amount=total_amount,
        )
        self._raise_event(event)

    def add_item(
        self,
        item_id: UUID,
        item_name: str,
        quantity: int,
        unit_price: float,
    ) -> None:
        """Add an item to the order."""
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot add items to this order")
        event = TestOrderItemAdded(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
            item_id=item_id,
            item_name=item_name,
            quantity=quantity,
            unit_price=unit_price,
        )
        self._raise_event(event)

    def complete_order(self) -> None:
        """Complete the order."""
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot complete this order")
        event = TestOrderCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        )
        self._raise_event(event)
