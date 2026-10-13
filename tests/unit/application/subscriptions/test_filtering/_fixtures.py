"""Shared fixtures and domain events for subscription filtering tests."""

from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent

# --- Sample Event Classes ---


class OrderCreated(DomainEvent):
    """Order created event."""

    aggregate_type: str = "Order"
    customer_id: str = "test"


class OrderShipped(DomainEvent):
    """Order shipped event."""

    aggregate_type: str = "Order"
    tracking_number: str = "123"


class OrderCancelled(DomainEvent):
    """Order cancelled event."""

    aggregate_type: str = "Order"
    reason: str = ""


class PaymentReceived(DomainEvent):
    """Payment received event."""

    aggregate_type: str = "Payment"
    amount: float = 0.0


class UserRegistered(DomainEvent):
    """User registered event."""

    aggregate_type: str = "User"
    email: str = "test@example.com"


class UserUpdatedEvent(DomainEvent):
    """User updated event with 'Event' suffix."""

    aggregate_type: str = "User"
    name: str = ""


# --- Mock Subscribers ---


class MockOrderSubscriber:
    """Subscriber for order events only."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        pass


class MockAllEventsSubscriber:
    """Subscriber for all events (empty subscribed_to)."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return []

    async def handle(self, event: DomainEvent) -> None:
        pass


# --- Fixtures ---


@pytest.fixture
def order_created() -> OrderCreated:
    """Create an OrderCreated event."""
    return OrderCreated(aggregate_id=uuid4())


@pytest.fixture
def order_shipped() -> OrderShipped:
    """Create an OrderShipped event."""
    return OrderShipped(aggregate_id=uuid4())


@pytest.fixture
def order_cancelled() -> OrderCancelled:
    """Create an OrderCancelled event."""
    return OrderCancelled(aggregate_id=uuid4())


@pytest.fixture
def payment_received() -> PaymentReceived:
    """Create a PaymentReceived event."""
    return PaymentReceived(aggregate_id=uuid4())


@pytest.fixture
def user_registered() -> UserRegistered:
    """Create a UserRegistered event."""
    return UserRegistered(aggregate_id=uuid4())


__all__ = [  # pragma: allowlist secret
    "MockAllEventsSubscriber",
    "MockOrderSubscriber",
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "PaymentReceived",
    "UserRegistered",
    "UserUpdatedEvent",
    "order_cancelled",
    "order_created",
    "order_shipped",
    "payment_received",
    "user_registered",
]
