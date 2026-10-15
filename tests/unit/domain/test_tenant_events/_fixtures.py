"""Shared event fixtures for TenantDomainEvent unit tests."""

from __future__ import annotations

from uuid import UUID

from eventsource import TenantDomainEvent


class OrderCreated(TenantDomainEvent):
    """Test event class for testing TenantDomainEvent."""

    aggregate_type: str = "Order"
    customer_id: UUID
    total_amount: float = 0.0


class ProductAdded(TenantDomainEvent):
    """Another test event class."""

    aggregate_type: str = "Catalog"
    product_name: str
    price: float


__all__ = ["OrderCreated", "ProductAdded"]
