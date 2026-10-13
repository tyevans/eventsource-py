"""Pytest fixtures for tenant filter tests."""

from tests.unit.application.projections.test_tenant_filter._fixtures import (
    NonTenantEvent,
    OrderCreated,
    OrderShipped,
    SampleProjection,
    TenantEvent,
    non_tenant_event,
    order_created_no_tenant,
    order_created_tenant_1,
    order_created_tenant_2,
    tenant_event_1,
    tenant_event_2,
    tenant_id_1,
    tenant_id_2,
)

__all__ = [  # pragma: allowlist secret
    "NonTenantEvent",
    "OrderCreated",
    "OrderShipped",
    "SampleProjection",
    "TenantEvent",
    "non_tenant_event",
    "order_created_no_tenant",
    "order_created_tenant_1",
    "order_created_tenant_2",
    "tenant_event_1",
    "tenant_event_2",
    "tenant_id_1",
    "tenant_id_2",
]
