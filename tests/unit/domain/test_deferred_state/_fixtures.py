"""Test fixtures, models, and aggregate implementations for deferred state tests."""

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from eventsource.domain.aggregate import DeclarativeAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent


class ExtractionState(BaseModel):
    """State model for Extraction aggregate."""

    page_id: UUID
    status: str


class ExtractionRequested(DomainEvent):
    """Event for extraction request."""

    aggregate_type: str = "Extraction"
    page_id: UUID
    config: dict[str, Any]


class ExtractionCompleted(DomainEvent):
    """Event for extraction completion."""

    aggregate_type: str = "Extraction"
    result: str


class OrderState(BaseModel):
    """State model for Order aggregate."""

    order_id: UUID
    status: str = "pending"


class OrderCreated(DomainEvent):
    """Event for order creation."""

    aggregate_type: str = "Order"
    customer_id: UUID


class OrderShipped(DomainEvent):
    """Event for order shipment."""

    aggregate_type: str = "Order"
    tracking_number: str


class ExtractionProcess(DeclarativeAggregate[ExtractionState]):
    """Aggregate that requires a creation event (deferred state)."""

    aggregate_type = "Extraction"
    requires_creation_event = True

    def request(self, page_id: UUID, config: dict[str, Any]) -> ExtractionRequested:
        """Request an extraction."""
        return self.create_event(ExtractionRequested, page_id=page_id, config=config)

    def complete(self, result: str) -> ExtractionCompleted:
        """Complete the extraction."""
        return self.create_event(ExtractionCompleted, result=result)

    @handles(ExtractionRequested)
    def _on_requested(self, event: ExtractionRequested) -> None:
        self._state = ExtractionState(
            page_id=event.page_id,
            status="requested",
        )

    @handles(ExtractionCompleted)
    def _on_completed(self, event: ExtractionCompleted) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "completed"})


class Order(DeclarativeAggregate[OrderState]):
    """Traditional aggregate with initial state."""

    aggregate_type = "Order"
    # requires_creation_event = False (default)

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def create(self, customer_id: UUID) -> OrderCreated:
        """Create the order."""
        return self.create_event(OrderCreated, customer_id=customer_id)

    def ship(self, tracking_number: str) -> OrderShipped:
        """Ship the order."""
        return self.create_event(OrderShipped, tracking_number=tracking_number)

    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        # Traditional pattern: check for None and initialize
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"status": "created"})

    @handles(OrderShipped)
    def _on_shipped(self, event: OrderShipped) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "shipped"})
