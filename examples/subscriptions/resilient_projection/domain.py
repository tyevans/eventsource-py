"""Domain events and aggregate for resilient projection example."""

from uuid import UUID

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    register_event,
)


@register_event
class PaymentReceived(DomainEvent):
    """Event emitted when a payment is received."""

    aggregate_type: str = "Payment"

    order_id: UUID
    amount: float
    currency: str = "USD"


@register_event
class PaymentFailed(DomainEvent):
    """Event emitted when a payment fails."""

    aggregate_type: str = "Payment"

    order_id: UUID
    reason: str


@register_event
class PaymentRefunded(DomainEvent):
    """Event emitted when a payment is refunded."""

    aggregate_type: str = "Payment"

    order_id: UUID
    amount: float
    reason: str


class PaymentState(BaseModel):
    """Payment aggregate state."""

    payment_id: UUID
    order_id: UUID | None = None
    amount: float = 0.0
    status: str = "pending"


class PaymentAggregate(AggregateRoot[PaymentState]):
    """Event-sourced payment aggregate."""

    aggregate_type = "Payment"

    def _get_initial_state(self) -> PaymentState:
        return PaymentState(payment_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, PaymentReceived):
            self._state = PaymentState(
                payment_id=self.aggregate_id,
                order_id=event.order_id,
                amount=event.amount,
                status="received",
            )
        elif isinstance(event, PaymentFailed):
            if self._state:
                self._state = self._state.model_copy(update={"status": "failed"})
        elif isinstance(event, PaymentRefunded) and self._state:
            self._state = self._state.model_copy(update={"status": "refunded"})

    def receive(self, order_id: UUID, amount: float) -> None:
        """Record a payment receipt."""
        event = PaymentReceived(
            aggregate_id=self.aggregate_id,
            order_id=order_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def fail(self, order_id: UUID, reason: str) -> None:
        """Record a payment failure."""
        event = PaymentFailed(
            aggregate_id=self.aggregate_id,
            order_id=order_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def refund(self, order_id: UUID, amount: float, reason: str) -> None:
        """Record a payment refund."""
        event = PaymentRefunded(
            aggregate_id=self.aggregate_id,
            order_id=order_id,
            amount=amount,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)
