"""
Projection Example

This example demonstrates:
- Building read models from event streams
- DeclarativeProjection with @handles decorator
- Checkpoint tracking for recovery
- Integration with event bus

Run with: python -m eventsource.examples.projection_example
"""

import asyncio
from collections import defaultdict
from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventBus,
    InMemoryEventStore,
    register_event,
)
from eventsource.projections import DeclarativeProjection, handles
from eventsource.repositories import InMemoryCheckpointRepository, InMemoryDLQRepository

# =============================================================================
# Domain Events
# =============================================================================


@register_event
class OrderPlaced(DomainEvent):
    """Event emitted when an order is placed."""

    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: UUID
    customer_name: str
    total_amount: float
    item_count: int


@register_event
class OrderShipped(DomainEvent):
    """Event emitted when an order is shipped."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str
    carrier: str


@register_event
class OrderDelivered(DomainEvent):
    """Event emitted when an order is delivered."""

    event_type: str = "OrderDelivered"
    aggregate_type: str = "Order"

    delivered_at: datetime


@register_event
class OrderCancelled(DomainEvent):
    """Event emitted when an order is cancelled."""

    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"

    reason: str


# =============================================================================
# Aggregate State and Implementation
# =============================================================================


class OrderState(BaseModel):
    """Order aggregate state."""

    order_id: UUID
    customer_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
    item_count: int = 0
    status: str = "draft"
    tracking_number: str | None = None
    carrier: str | None = None


class OrderAggregate(AggregateRoot[OrderState]):
    """Simple order aggregate."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                customer_name=event.customer_name,
                total_amount=event.total_amount,
                item_count=event.item_count,
                status="placed",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                        "carrier": event.carrier,
                    }
                )
        elif isinstance(event, OrderDelivered):
            if self._state:
                self._state = self._state.model_copy(update={"status": "delivered"})
        elif isinstance(event, OrderCancelled) and self._state:
            self._state = self._state.model_copy(update={"status": "cancelled"})

    def place(
        self, customer_id: UUID, customer_name: str, total_amount: float, item_count: int
    ) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        event = OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            customer_name=customer_name,
            total_amount=total_amount,
            item_count=item_count,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def ship(self, tracking_number: str, carrier: str) -> None:
        if not self.state or self.state.status != "placed":
            raise ValueError("Order must be placed to ship")
        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            carrier=carrier,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def deliver(self) -> None:
        if not self.state or self.state.status != "shipped":
            raise ValueError("Order must be shipped to deliver")
        event = OrderDelivered(
            aggregate_id=self.aggregate_id,
            delivered_at=datetime.now(),
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def cancel(self, reason: str) -> None:
        if not self.state or self.state.status in ("delivered", "cancelled"):
            raise ValueError("Cannot cancel this order")
        event = OrderCancelled(
            aggregate_id=self.aggregate_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Projections - Read Models
# =============================================================================


class OrderListProjection(DeclarativeProjection):
    """
    Projection that maintains a list of orders for quick querying.

    This is an in-memory read model for demonstration.
    In production, you'd write to a database table.
    """

    def __init__(self):
        super().__init__(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
        )
        # In-memory read model
        self.orders: dict[UUID, dict] = {}

    @handles(OrderPlaced)
    async def _handle_order_placed(self, event: OrderPlaced) -> None:
        """Create order in read model."""
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

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        """Update order status to shipped."""
        if event.aggregate_id in self.orders:
            self.orders[event.aggregate_id].update(
                {
                    "status": "shipped",
                    "tracking_number": event.tracking_number,
                    "carrier": event.carrier,
                    "shipped_at": event.occurred_at,
                }
            )

    @handles(OrderDelivered)
    async def _handle_order_delivered(self, event: OrderDelivered) -> None:
        """Update order status to delivered."""
        if event.aggregate_id in self.orders:
            self.orders[event.aggregate_id].update(
                {
                    "status": "delivered",
                    "delivered_at": event.delivered_at,
                }
            )

    @handles(OrderCancelled)
    async def _handle_order_cancelled(self, event: OrderCancelled) -> None:
        """Update order status to cancelled."""
        if event.aggregate_id in self.orders:
            self.orders[event.aggregate_id].update(
                {
                    "status": "cancelled",
                    "cancelled_at": event.occurred_at,
                    "cancellation_reason": event.reason,
                }
            )

    async def _truncate_read_models(self) -> None:
        """Clear all orders for reset."""
        self.orders.clear()

    # Query methods

    def get_all_orders(self) -> list[dict]:
        """Get all orders."""
        return list(self.orders.values())

    def get_orders_by_status(self, status: str) -> list[dict]:
        """Get orders by status."""
        return [o for o in self.orders.values() if o["status"] == status]

    def get_orders_by_customer(self, customer_id: UUID) -> list[dict]:
        """Get orders for a customer."""
        return [o for o in self.orders.values() if o["customer_id"] == customer_id]


class CustomerStatsProjection(DeclarativeProjection):
    """
    Projection that maintains customer statistics.

    Demonstrates aggregating data across multiple events.
    """

    def __init__(self):
        super().__init__(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
        )
        # Customer stats read model
        self.stats: dict[UUID, dict] = defaultdict(
            lambda: {
                "customer_id": None,
                "customer_name": "",
                "total_orders": 0,
                "total_spent": 0.0,
                "delivered_orders": 0,
                "cancelled_orders": 0,
            }
        )

    @handles(OrderPlaced)
    async def _handle_order_placed(self, event: OrderPlaced) -> None:
        """Track new order for customer."""
        stats = self.stats[event.customer_id]
        stats["customer_id"] = event.customer_id
        stats["customer_name"] = event.customer_name
        stats["total_orders"] += 1
        stats["total_spent"] += event.total_amount

    @handles(OrderDelivered)
    async def _handle_order_delivered(self, event: OrderDelivered) -> None:
        """Track delivered order."""
        # Need to find customer from order - in production, you'd have a lookup
        # For demo, we iterate
        for _customer_id, stats in self.stats.items():
            if stats["customer_id"]:
                # In a real system, you'd look this up properly
                pass
        # Note: This is simplified. In production, you'd store order->customer mapping

    @handles(OrderCancelled)
    async def _handle_order_cancelled(self, event: OrderCancelled) -> None:
        """Track cancelled order and adjust stats."""
        # Similar to above - would need order->customer mapping
        pass

    async def _truncate_read_models(self) -> None:
        """Clear all stats for reset."""
        self.stats.clear()

    # Query methods

    def get_customer_stats(self, customer_id: UUID) -> dict | None:
        """Get stats for a customer."""
        if customer_id in self.stats:
            return dict(self.stats[customer_id])
        return None

    def get_top_customers(self, limit: int = 10) -> list[dict]:
        """Get top customers by total spent."""
        customers = list(self.stats.values())
        customers.sort(key=lambda x: x["total_spent"], reverse=True)
        return customers[:limit]


class DailyRevenueProjection(DeclarativeProjection):
    """
    Projection that tracks daily revenue.

    Demonstrates time-series aggregation.
    """

    def __init__(self):
        super().__init__(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
        )
        # Daily revenue read model
        self.daily_revenue: dict[str, dict] = defaultdict(
            lambda: {"date": None, "order_count": 0, "total_revenue": 0.0}
        )

    @handles(OrderPlaced)
    async def _handle_order_placed(self, event: OrderPlaced) -> None:
        """Add order to daily revenue."""
        date_str = event.occurred_at.strftime("%Y-%m-%d")
        self.daily_revenue[date_str]["date"] = date_str
        self.daily_revenue[date_str]["order_count"] += 1
        self.daily_revenue[date_str]["total_revenue"] += event.total_amount

    async def _truncate_read_models(self) -> None:
        """Clear all daily revenue for reset."""
        self.daily_revenue.clear()

    # Query methods

    def get_daily_revenue(self, date_str: str) -> dict | None:
        """Get revenue for a specific date."""
        if date_str in self.daily_revenue:
            return dict(self.daily_revenue[date_str])
        return None

    def get_revenue_report(self) -> list[dict]:
        """Get all daily revenue sorted by date."""
        revenue = list(self.daily_revenue.values())
        revenue.sort(key=lambda x: x["date"] or "")
        return revenue


# =============================================================================
# Demo
# =============================================================================


async def main():
    """Demonstrate projection patterns."""
    print("=" * 60)
    print("Projection Example")
    print("=" * 60)

    # Setup infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()

    # Create projections
    order_list = OrderListProjection()
    customer_stats = CustomerStatsProjection()
    daily_revenue = DailyRevenueProjection()

    # Subscribe projections to event bus
    event_bus.subscribe_all(order_list)
    event_bus.subscribe_all(customer_stats)
    event_bus.subscribe_all(daily_revenue)

    # Create repository with event publishing
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,
    )

    # Create some customers
    customers = [
        (uuid4(), "Alice Johnson"),
        (uuid4(), "Bob Smith"),
        (uuid4(), "Carol Williams"),
    ]

    # Create orders
    print("\n1. Creating orders")

    orders_data = [
        (customers[0], 150.00, 3),
        (customers[0], 75.50, 2),
        (customers[1], 200.00, 5),
        (customers[1], 99.99, 1),
        (customers[2], 450.00, 10),
    ]

    order_ids = []
    for (customer_id, customer_name), total, items in orders_data:
        order_id = uuid4()
        order_ids.append(order_id)

        order = repo.create_new(order_id)
        order.place(customer_id, customer_name, total, items)
        await repo.save(order)
        print(f"   Order {order_id}: ${total:.2f} for {customer_name}")

    # Ship some orders
    print("\n2. Shipping orders")

    for i, order_id in enumerate(order_ids[:3]):
        order = await repo.load(order_id)
        order.ship(f"TRACK-{1000 + i}", "FedEx")
        await repo.save(order)
        print(f"   Shipped order {order_id}")

    # Deliver one order
    print("\n3. Delivering order")
    order = await repo.load(order_ids[0])
    order.deliver()
    await repo.save(order)
    print(f"   Delivered order {order_ids[0]}")

    # Cancel one order
    print("\n4. Cancelling order")
    order = await repo.load(order_ids[3])
    order.cancel("Customer requested cancellation")
    await repo.save(order)
    print(f"   Cancelled order {order_ids[3]}")

    # Query projections
    print("\n5. Querying Order List Projection")
    print(f"   Total orders: {len(order_list.get_all_orders())}")
    print(f"   Placed orders: {len(order_list.get_orders_by_status('placed'))}")
    print(f"   Shipped orders: {len(order_list.get_orders_by_status('shipped'))}")
    print(f"   Delivered orders: {len(order_list.get_orders_by_status('delivered'))}")
    print(f"   Cancelled orders: {len(order_list.get_orders_by_status('cancelled'))}")

    print("\n6. Customer Stats Projection")
    for customer_id, customer_name in customers:
        stats = customer_stats.get_customer_stats(customer_id)
        if stats:
            print(f"   {customer_name}:")
            print(f"      Orders: {stats['total_orders']}")
            print(f"      Total spent: ${stats['total_spent']:.2f}")

    print("\n7. Daily Revenue Projection")
    for daily in daily_revenue.get_revenue_report():
        print(f"   {daily['date']}: {daily['order_count']} orders, ${daily['total_revenue']:.2f}")

    print("\n8. Checkpoint tracking")
    checkpoint = await order_list.get_checkpoint()
    print(f"   Order List checkpoint: {checkpoint}")

    # Demonstrate projection reset
    print("\n9. Resetting projections")
    await order_list.reset()
    print(f"   Orders after reset: {len(order_list.get_all_orders())}")

    # Replay all events to rebuild projection
    print("\n10. Rebuilding projection from event stream")
    async for stored_event in event_store.read_all():
        if isinstance(
            stored_event.event, OrderPlaced | OrderShipped | OrderDelivered | OrderCancelled
        ):
            await order_list.handle(stored_event.event)

    print(f"    Rebuilt orders: {len(order_list.get_all_orders())}")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
