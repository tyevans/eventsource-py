"""Read model projections for order processing demo."""

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
    """In-memory projection maintaining a list of orders for quick querying."""

    def __init__(self) -> None:
        self.orders: dict[UUID, dict[str, Any]] = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped, OrderDelivered, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
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
        elif isinstance(event, OrderShipped) and event.aggregate_id in self.orders:
            self.orders[event.aggregate_id].update(
                {
                    "status": "shipped",
                    "tracking_number": event.tracking_number,
                    "carrier": event.carrier,
                    "shipped_at": event.occurred_at,
                }
            )
            logger.debug(f"Order shipped: {event.aggregate_id}")
        elif isinstance(event, OrderDelivered) and event.aggregate_id in self.orders:
            self.orders[event.aggregate_id].update(
                {"status": "delivered", "delivered_at": event.delivered_at}
            )
            logger.debug(f"Order delivered: {event.aggregate_id}")
        elif isinstance(event, OrderCancelled) and event.aggregate_id in self.orders:
            self.orders[event.aggregate_id].update(
                {
                    "status": "cancelled",
                    "cancelled_at": event.occurred_at,
                    "cancellation_reason": event.reason,
                }
            )
            logger.debug(f"Order cancelled: {event.aggregate_id}")

    def get_all_orders(self) -> list[dict[str, Any]]:
        return list(self.orders.values())

    def get_orders_by_status(self, status: str) -> list[dict[str, Any]]:
        return [o for o in self.orders.values() if o["status"] == status]

    def get_orders_by_customer(self, customer_id: UUID) -> list[dict[str, Any]]:
        return [o for o in self.orders.values() if o["customer_id"] == customer_id]


class CustomerStatsProjection(EventSubscriber):
    """Projection that maintains customer statistics across multiple events."""

    def __init__(self) -> None:
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
        self._order_to_customer: dict[UUID, UUID] = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderDelivered, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            stats = self.stats[event.customer_id]
            stats["customer_id"] = event.customer_id
            stats["customer_name"] = event.customer_name
            stats["total_orders"] += 1
            stats["total_spent"] += event.total_amount
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

    def get_customer_stats(self, customer_id: UUID) -> dict[str, Any] | None:
        return dict(self.stats[customer_id]) if customer_id in self.stats else None

    def get_top_customers(self, limit: int = 10) -> list[dict[str, Any]]:
        customers = list(self.stats.values())
        customers.sort(key=lambda x: x["total_spent"], reverse=True)
        return customers[:limit]


class DailyRevenueProjection(EventSubscriber):
    """Projection that tracks daily revenue."""

    def __init__(self) -> None:
        self.daily_revenue: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"date": None, "order_count": 0, "total_revenue": 0.0}
        )

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            date_str = event.occurred_at.strftime("%Y-%m-%d")
            self.daily_revenue[date_str]["date"] = date_str
            self.daily_revenue[date_str]["order_count"] += 1
            self.daily_revenue[date_str]["total_revenue"] += event.total_amount
            logger.debug(f"Daily revenue updated for {date_str}")

    def get_daily_revenue(self, date_str: str) -> dict[str, Any] | None:
        return dict(self.daily_revenue[date_str]) if date_str in self.daily_revenue else None

    def get_revenue_report(self) -> list[dict[str, Any]]:
        revenue = list(self.daily_revenue.values())
        revenue.sort(key=lambda x: x["date"] or "")
        return revenue


__all__ = ["CustomerStatsProjection", "DailyRevenueProjection", "OrderListProjection"]
