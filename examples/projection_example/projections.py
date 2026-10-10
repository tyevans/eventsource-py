"""
Read model projections for order processing demo.

These projections implement the Subscriber protocol for use with
SubscriptionManager. Each projection declares which events it handles
and provides a handle() method to process events.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any
from uuid import UUID

from eventsource import DomainEvent
from eventsource.ports.handlers import EventSubscriber
from examples.projection_example.domain import (
    OrderCancelled,
    OrderDelivered,
    OrderPlaced,
    OrderShipped,
)

logger = logging.getLogger(__name__)


class OrderListProjection(EventSubscriber):
    """
    Projection that maintains a list of orders for quick querying.

    This is an in-memory read model for demonstration.
    In production, you'd write to a database table.

    Implements the Subscriber protocol required by SubscriptionManager:
    - subscribed_to(): Returns list of event types this projection handles
    - handle(): Processes a single event
    """

    def __init__(self) -> None:
        # In-memory read model
        self.orders: dict[UUID, dict[str, Any]] = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which event types this projection handles."""
        return [OrderPlaced, OrderShipped, OrderDelivered, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event and update the read model."""
        if isinstance(event, OrderPlaced):
            self.orders[event.aggregate_id] = {
                "order_id": event.aggregate_id,
                "customer_id": event.customer_id,
                "customer_name": event.customer_name,
                "total_amount": event.total_amount,
                "item_count": event.item_count,
                "status": "placed",
                "placed_at": event.occurred_at,
                "tracking_number": None,
                "carrier": None,
            }
            logger.debug(f"Order placed: {event.aggregate_id}")

        elif isinstance(event, OrderShipped):
            if event.aggregate_id in self.orders:
                self.orders[event.aggregate_id].update(
                    {
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                        "carrier": event.carrier,
                        "shipped_at": event.occurred_at,
                    }
                )
                logger.debug(f"Order shipped: {event.aggregate_id}")

        elif isinstance(event, OrderDelivered):
            if event.aggregate_id in self.orders:
                self.orders[event.aggregate_id].update(
                    {
                        "status": "delivered",
                        "delivered_at": event.delivered_at,
                    }
                )
                logger.debug(f"Order delivered: {event.aggregate_id}")

        elif isinstance(event, OrderCancelled):
            if event.aggregate_id in self.orders:
                self.orders[event.aggregate_id].update(
                    {
                        "status": "cancelled",
                        "cancelled_at": event.occurred_at,
                        "cancellation_reason": event.reason,
                    }
                )
                logger.debug(f"Order cancelled: {event.aggregate_id}")

    # Query methods

    def get_all_orders(self) -> list[dict[str, Any]]:
        """Get all orders."""
        return list(self.orders.values())

    def get_orders_by_status(self, status: str) -> list[dict[str, Any]]:
        """Get orders by status."""
        return [o for o in self.orders.values() if o["status"] == status]

    def get_orders_by_customer(self, customer_id: UUID) -> list[dict[str, Any]]:
        """Get orders for a customer."""
        return [o for o in self.orders.values() if o["customer_id"] == customer_id]


class CustomerStatsProjection(EventSubscriber):
    """
    Projection that maintains customer statistics.

    Demonstrates aggregating data across multiple events.
    """

    def __init__(self) -> None:
        # Customer stats read model
        self.stats: dict[UUID, dict[str, Any]] = defaultdict(
            lambda: {
                "customer_id": None,
                "customer_name": "",
                "total_orders": 0,
                "total_spent": 0.0,
                "delivered_orders": 0,
                "cancelled_orders": 0,
            }
        )
        # Track order->customer mapping for updates
        self._order_to_customer: dict[UUID, UUID] = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which event types this projection handles."""
        return [OrderPlaced, OrderDelivered, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event and update the read model."""
        if isinstance(event, OrderPlaced):
            stats = self.stats[event.customer_id]
            stats["customer_id"] = event.customer_id
            stats["customer_name"] = event.customer_name
            stats["total_orders"] += 1
            stats["total_spent"] += event.total_amount
            # Track order->customer for later updates
            self._order_to_customer[event.aggregate_id] = event.customer_id
            logger.debug(f"Customer stats updated for {event.customer_name}")

        elif isinstance(event, OrderDelivered):
            customer_id = self._order_to_customer.get(event.aggregate_id)
            if customer_id and customer_id in self.stats:
                self.stats[customer_id]["delivered_orders"] += 1

        elif isinstance(event, OrderCancelled):
            customer_id = self._order_to_customer.get(event.aggregate_id)
            if customer_id and customer_id in self.stats:
                self.stats[customer_id]["cancelled_orders"] += 1

    # Query methods

    def get_customer_stats(self, customer_id: UUID) -> dict[str, Any] | None:
        """Get stats for a customer."""
        if customer_id in self.stats:
            return dict(self.stats[customer_id])
        return None

    def get_top_customers(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get top customers by total spent."""
        customers = list(self.stats.values())
        customers.sort(key=lambda x: x["total_spent"], reverse=True)
        return customers[:limit]


class DailyRevenueProjection(EventSubscriber):
    """
    Projection that tracks daily revenue.

    Demonstrates time-series aggregation.
    """

    def __init__(self) -> None:
        # Daily revenue read model
        self.daily_revenue: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"date": None, "order_count": 0, "total_revenue": 0.0}
        )

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which event types this projection handles."""
        return [OrderPlaced]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event and update the read model."""
        if isinstance(event, OrderPlaced):
            date_str = event.occurred_at.strftime("%Y-%m-%d")
            self.daily_revenue[date_str]["date"] = date_str
            self.daily_revenue[date_str]["order_count"] += 1
            self.daily_revenue[date_str]["total_revenue"] += event.total_amount
            logger.debug(f"Daily revenue updated for {date_str}")

    # Query methods

    def get_daily_revenue(self, date_str: str) -> dict[str, Any] | None:
        """Get revenue for a specific date."""
        if date_str in self.daily_revenue:
            return dict(self.daily_revenue[date_str])
        return None

    def get_revenue_report(self) -> list[dict[str, Any]]:
        """Get all daily revenue sorted by date."""
        revenue = list(self.daily_revenue.values())
        revenue.sort(key=lambda x: x["date"] or "")
        return revenue


__all__ = [
    "CustomerStatsProjection",
    "DailyRevenueProjection",
    "OrderListProjection",
]
