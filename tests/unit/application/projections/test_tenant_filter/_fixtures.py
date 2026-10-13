"""Shared fixtures and sample models for tenant filter tests."""

from uuid import UUID, uuid4

import pytest

from eventsource.application.projections import DeclarativeProjection, TenantFilter
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent

# --- Sample Event Classes ---


class TenantEvent(DomainEvent):
    """Event with tenant_id field."""

    aggregate_type: str = "Order"
    tenant_id: UUID  # Override to make required for testing


class NonTenantEvent(DomainEvent):
    """Event without tenant_id field (legacy/system event)."""

    aggregate_type: str = "System"


class OrderCreated(DomainEvent):
    """Order created event for handler tests."""

    aggregate_type: str = "Order"
    order_number: str = "ORD-001"


class OrderShipped(DomainEvent):
    """Order shipped event for handler tests."""

    aggregate_type: str = "Order"
    tracking_number: str = "TRK-001"


# --- Sample Projection Classes ---


class SampleProjection(DeclarativeProjection):
    """Sample projection that tracks processed events."""

    def __init__(
        self,
        *,
        tenant_filter: TenantFilter = None,
    ) -> None:
        super().__init__(tenant_filter=tenant_filter)
        self.processed_events: list[DomainEvent] = []

    @handles(TenantEvent)
    async def _handle_tenant_event(self, event: TenantEvent) -> None:
        self.processed_events.append(event)

    @handles(NonTenantEvent)
    async def _handle_non_tenant_event(self, event: NonTenantEvent) -> None:
        self.processed_events.append(event)

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.processed_events.append(event)


# --- Fixtures ---


@pytest.fixture
def tenant_id_1() -> UUID:
    """First test tenant ID."""
    return UUID("11111111-1111-1111-1111-111111111111")


@pytest.fixture
def tenant_id_2() -> UUID:
    """Second test tenant ID."""
    return UUID("22222222-2222-2222-2222-222222222222")


@pytest.fixture
def tenant_event_1(tenant_id_1: UUID) -> TenantEvent:
    """Create a TenantEvent for tenant 1."""
    return TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_1)


@pytest.fixture
def tenant_event_2(tenant_id_2: UUID) -> TenantEvent:
    """Create a TenantEvent for tenant 2."""
    return TenantEvent(aggregate_id=uuid4(), tenant_id=tenant_id_2)


@pytest.fixture
def non_tenant_event() -> NonTenantEvent:
    """Create a NonTenantEvent (no tenant_id)."""
    return NonTenantEvent(aggregate_id=uuid4())


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
