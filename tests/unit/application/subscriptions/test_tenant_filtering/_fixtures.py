"""Shared fixtures and sample domain events for tenant filtering unit tests."""

from uuid import UUID, uuid4

import pytest

from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    """Order created event."""

    aggregate_type: str = "Order"
    customer_id: str = "test"


class OrderShipped(DomainEvent):
    """Order shipped event."""

    aggregate_type: str = "Order"
    tracking_number: str = "123"


class PaymentReceived(DomainEvent):
    """Payment received event."""

    aggregate_type: str = "Payment"
    amount: float = 0.0


class MockOrderSubscriber:
    """Subscriber for order events only."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        pass


@pytest.fixture
def tenant_id_1() -> UUID:
    """First test tenant ID."""
    return UUID("11111111-1111-1111-1111-111111111111")


@pytest.fixture
def tenant_id_2() -> UUID:
    """Second test tenant ID."""
    return UUID("22222222-2222-2222-2222-222222222222")


@pytest.fixture
def order_created_tenant_1(tenant_id_1: UUID) -> OrderCreated:
    """Create an OrderCreated event for tenant 1."""
    return OrderCreated(aggregate_id=uuid4(), tenant_id=tenant_id_1)


@pytest.fixture
def order_created_tenant_2(tenant_id_2: UUID) -> OrderCreated:
    """Create an OrderCreated event for tenant 2."""
    return OrderCreated(aggregate_id=uuid4(), tenant_id=tenant_id_2)


@pytest.fixture
def order_created_no_tenant() -> OrderCreated:
    """Create an OrderCreated event with no tenant."""
    return OrderCreated(aggregate_id=uuid4(), tenant_id=None)


@pytest.fixture
def order_shipped_tenant_1(tenant_id_1: UUID) -> OrderShipped:
    """Create an OrderShipped event for tenant 1."""
    return OrderShipped(aggregate_id=uuid4(), tenant_id=tenant_id_1)


@pytest.fixture
def payment_received_tenant_1(tenant_id_1: UUID) -> PaymentReceived:
    """Create a PaymentReceived event for tenant 1."""
    return PaymentReceived(aggregate_id=uuid4(), tenant_id=tenant_id_1)


__all__ = [  # pragma: allowlist secret
    "MockOrderSubscriber",
    "OrderCreated",
    "OrderShipped",
    "PaymentReceived",
    "order_created_no_tenant",
    "order_created_tenant_1",
    "order_created_tenant_2",
    "order_shipped_tenant_1",
    "payment_received_tenant_1",
    "tenant_id_1",
    "tenant_id_2",
]
