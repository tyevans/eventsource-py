"""
Multi-Subscriber Example - Multiple Projections Running Concurrently

This example demonstrates:
- Running multiple projections with a single SubscriptionManager
- Different configurations per projection (start positions, batch sizes)
- Health monitoring across all projections
- Independent error handling per subscription
- Querying subscription status and metrics

This pattern is common in CQRS architectures where multiple read models
are built from the same event stream.

Run with: python -m examples.subscriptions.multi_subscriber
"""

from __future__ import annotations

from examples.subscriptions.multi_subscriber.domain import (
    InventoryAdded,
    InventoryReserved,
    InventorySold,
    ProductAggregate,
    ProductCreated,
    ProductPriceChanged,
    ProductState,
)
from examples.subscriptions.multi_subscriber.projections import (
    InventoryDashboardProjection,
    ProductCatalogProjection,
    SalesAnalyticsProjection,
)
from examples.subscriptions.multi_subscriber.runner import main

__all__ = [
    "InventoryAdded",
    "InventoryDashboardProjection",
    "InventoryReserved",
    "InventorySold",
    "ProductAggregate",
    "ProductCatalogProjection",
    "ProductCreated",
    "ProductPriceChanged",
    "ProductState",
    "SalesAnalyticsProjection",
    "main",
]
