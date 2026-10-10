"""Shared test events and mock aggregates for TenantAwareRepository tests."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import TenantDomainEvent
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent


# Test events
class OrderCreated(TenantDomainEvent):
    """Test event with tenant_id."""

    aggregate_type: str = "Order"
    customer_id: UUID


class NonTenantOrderCreated(DomainEvent):
    """Test event without tenant_id (uses base DomainEvent)."""

    aggregate_type: str = "Order"
    customer_id: UUID


# Mock aggregate for testing
class MockAggregate:
    """Mock aggregate for testing."""

    __test__ = False

    def __init__(
        self,
        aggregate_id: UUID | None = None,
        uncommitted_events: list[DomainEvent] | None = None,
        version: int = 0,
    ) -> None:
        self.aggregate_id = aggregate_id or uuid4()
        self.uncommitted_events = uncommitted_events or []
        self.version = version
        self._state: Any = None


class TenantOrderState(BaseModel):
    """State for the real aggregate used in the read-isolation tests."""

    order_id: UUID
    customer_id: UUID | None = None


class TenantOrderAggregate(AggregateRoot[TenantOrderState]):
    """Real aggregate over a real store, so reads exercise the actual load path."""

    __test__ = False

    aggregate_type = "Order"

    def _get_initial_state(self) -> TenantOrderState:
        return TenantOrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(update={"customer_id": event.customer_id})

    def create(self, customer_id: UUID) -> None:
        self.create_event(OrderCreated, customer_id=customer_id)


__all__ = [
    "MockAggregate",
    "NonTenantOrderCreated",
    "OrderCreated",
    "TenantOrderAggregate",
    "TenantOrderState",
]
