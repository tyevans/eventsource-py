"""Pytest configuration and shared fixtures for tenant filtering unit tests."""

from tests.unit.application.subscriptions.test_tenant_filtering._fixtures import (
    MockOrderSubscriber,
    OrderCreated,
    OrderShipped,
    PaymentReceived,
    order_created_no_tenant,
    order_created_tenant_1,
    order_created_tenant_2,
    order_shipped_tenant_1,
    payment_received_tenant_1,
    tenant_id_1,
    tenant_id_2,
)

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
