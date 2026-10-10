"""
Projections for multi-subscriber example.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any
from uuid import UUID

from eventsource import DomainEvent
from eventsource.ports.handlers import EventSubscriber
from examples.subscriptions.multi_subscriber.domain import (
    InventoryAdded,
    InventoryReserved,
    InventorySold,
    ProductCreated,
    ProductPriceChanged,
)

logger = logging.getLogger(__name__)


class ProductCatalogProjection(EventSubscriber):
    """
    Projection that maintains a browsable product catalog.

    Use case: Product listing pages, search results, category browsing.
    Optimized for: Fast reads, simple queries by category.
    """

    def __init__(self) -> None:
        # Products indexed by ID
        self.products: dict[UUID, dict[str, Any]] = {}
        # Products indexed by category
        self.by_category: dict[str, list[UUID]] = defaultdict(list)
        self.events_processed: int = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Only interested in product metadata events."""
        return [ProductCreated, ProductPriceChanged]

    async def handle(self, event: DomainEvent) -> None:
        """Update the catalog read model."""
        self.events_processed += 1

        if isinstance(event, ProductCreated):
            self.products[event.aggregate_id] = {
                "id": event.aggregate_id,
                "name": event.name,
                "price": event.price,
                "category": event.category,
                "created_at": event.occurred_at,
            }
            self.by_category[event.category].append(event.aggregate_id)
            logger.debug(f"Catalog: Added product {event.name}")

        elif isinstance(event, ProductPriceChanged):
            if event.aggregate_id in self.products:
                self.products[event.aggregate_id]["price"] = event.new_price
                logger.debug(f"Catalog: Updated price to ${event.new_price:.2f}")

    def get_all_products(self) -> list[dict[str, Any]]:
        """Get all products in the catalog."""
        return list(self.products.values())

    def get_by_category(self, category: str) -> list[dict[str, Any]]:
        """Get products in a category."""
        product_ids = self.by_category.get(category, [])
        return [self.products[pid] for pid in product_ids if pid in self.products]

    def get_stats(self) -> dict[str, Any]:
        """Get catalog statistics."""
        return {
            "total_products": len(self.products),
            "categories": list(self.by_category.keys()),
            "events_processed": self.events_processed,
        }


class InventoryDashboardProjection(EventSubscriber):
    """
    Projection that maintains real-time inventory levels.

    Use case: Warehouse management, stock alerts, reorder triggers.
    Optimized for: Real-time accuracy, low-stock alerts.
    """

    def __init__(self, low_stock_threshold: int = 10) -> None:
        self.low_stock_threshold = low_stock_threshold
        # Inventory levels by product
        self.inventory: dict[UUID, int] = defaultdict(int)
        # Products by warehouse
        self.by_warehouse: dict[str, dict[UUID, int]] = defaultdict(lambda: defaultdict(int))
        # Low stock alerts
        self.low_stock_alerts: list[dict[str, Any]] = []
        self.events_processed: int = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Interested in inventory-related events."""
        return [InventoryAdded, InventoryReserved, InventorySold]

    async def handle(self, event: DomainEvent) -> None:
        """Update inventory tracking."""
        self.events_processed += 1

        if isinstance(event, InventoryAdded):
            self.inventory[event.aggregate_id] += event.quantity
            self.by_warehouse[event.warehouse][event.aggregate_id] += event.quantity
            logger.debug(f"Inventory: +{event.quantity} at {event.warehouse}")
            # Clear low stock alert if level is now OK
            self._check_low_stock(event.aggregate_id)

        elif isinstance(event, InventoryReserved):
            self.inventory[event.aggregate_id] -= event.quantity
            logger.debug(f"Inventory: -{event.quantity} reserved for order {event.order_id}")
            # Check for low stock
            self._check_low_stock(event.aggregate_id)

        elif isinstance(event, InventorySold):
            # Inventory already reduced during reservation
            logger.debug(f"Inventory: Sale confirmed for order {event.order_id}")

    def _check_low_stock(self, product_id: UUID) -> None:
        """Check and record low stock alerts."""
        level = self.inventory.get(product_id, 0)
        if level < self.low_stock_threshold:
            alert = {
                "product_id": product_id,
                "current_level": level,
                "threshold": self.low_stock_threshold,
                "timestamp": datetime.now(),
            }
            # Avoid duplicate alerts
            existing = [a for a in self.low_stock_alerts if a["product_id"] == product_id]
            if not existing:
                self.low_stock_alerts.append(alert)
                logger.warning(f"LOW STOCK ALERT: Product {product_id} at {level} units")

    def get_inventory_levels(self) -> dict[UUID, int]:
        """Get current inventory levels for all products."""
        return dict(self.inventory)

    def get_low_stock_products(self) -> list[dict[str, Any]]:
        """Get products below threshold."""
        return [
            {"product_id": pid, "level": level}
            for pid, level in self.inventory.items()
            if level < self.low_stock_threshold
        ]

    def get_warehouse_levels(self, warehouse: str) -> dict[UUID, int]:
        """Get inventory levels for a specific warehouse."""
        return dict(self.by_warehouse.get(warehouse, {}))

    def get_stats(self) -> dict[str, Any]:
        """Get inventory statistics."""
        return {
            "total_products_tracked": len(self.inventory),
            "total_inventory": sum(self.inventory.values()),
            "low_stock_count": len(self.get_low_stock_products()),
            "warehouses": list(self.by_warehouse.keys()),
            "events_processed": self.events_processed,
        }


class SalesAnalyticsProjection(EventSubscriber):
    """
    Projection that maintains sales metrics and analytics.

    Use case: Business intelligence, revenue reporting, trend analysis.
    Optimized for: Aggregations, time-series data.
    """

    def __init__(self) -> None:
        # Revenue by product
        self.revenue_by_product: dict[UUID, float] = defaultdict(float)
        # Sales count by product
        self.sales_count: dict[UUID, int] = defaultdict(int)
        # Daily revenue
        self.daily_revenue: dict[str, float] = defaultdict(float)
        # Price change history
        self.price_changes: list[dict[str, Any]] = []
        self.events_processed: int = 0
        # Cache product prices for revenue calculation
        self._product_prices: dict[UUID, float] = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Interested in price and sales events."""
        return [ProductCreated, ProductPriceChanged, InventorySold]

    async def handle(self, event: DomainEvent) -> None:
        """Update sales analytics."""
        self.events_processed += 1

        if isinstance(event, ProductCreated):
            # Cache the initial price
            self._product_prices[event.aggregate_id] = event.price
            logger.debug(f"Analytics: Tracking product at ${event.price:.2f}")

        elif isinstance(event, ProductPriceChanged):
            # Record price change and update cache
            self.price_changes.append(
                {
                    "product_id": event.aggregate_id,
                    "old_price": event.old_price,
                    "new_price": event.new_price,
                    "changed_at": event.occurred_at,
                }
            )
            self._product_prices[event.aggregate_id] = event.new_price
            logger.debug("Analytics: Price change recorded")

        elif isinstance(event, InventorySold):
            # Calculate revenue using cached price
            price = self._product_prices.get(event.aggregate_id, 0)
            revenue = price * event.quantity
            self.revenue_by_product[event.aggregate_id] += revenue
            self.sales_count[event.aggregate_id] += event.quantity

            # Track daily revenue
            date_key = event.occurred_at.strftime("%Y-%m-%d")
            self.daily_revenue[date_key] += revenue
            logger.debug(f"Analytics: Sale recorded, revenue=${revenue:.2f}")

    def get_total_revenue(self) -> float:
        """Get total revenue across all products."""
        return sum(self.revenue_by_product.values())

    def get_top_products(self, limit: int = 5) -> list[dict[str, Any]]:
        """Get top products by revenue."""
        sorted_products = sorted(
            self.revenue_by_product.items(),
            key=lambda x: x[1],
            reverse=True,
        )
        return [
            {"product_id": pid, "revenue": rev, "units_sold": self.sales_count[pid]}
            for pid, rev in sorted_products[:limit]
        ]

    def get_daily_revenue(self) -> dict[str, float]:
        """Get revenue by day."""
        return dict(self.daily_revenue)

    def get_stats(self) -> dict[str, Any]:
        """Get analytics statistics."""
        return {
            "total_revenue": self.get_total_revenue(),
            "total_units_sold": sum(self.sales_count.values()),
            "products_sold": len([p for p, c in self.sales_count.items() if c > 0]),
            "price_changes": len(self.price_changes),
            "events_processed": self.events_processed,
        }


__all__ = [
    "InventoryDashboardProjection",
    "ProductCatalogProjection",
    "SalesAnalyticsProjection",
]
