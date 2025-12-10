"""
Projection Example with SubscriptionManager

This example demonstrates:
- Building read models (projections) from event streams
- SubscriptionManager for coordinated catch-up and live subscriptions
- Checkpoint tracking for resumable processing
- Multiple projections with different read models

The SubscriptionManager is the recommended pattern for production use as it:
- Catches up on historical events from the event store
- Seamlessly transitions to live events from the event bus
- Tracks checkpoints for resumable processing
- Supports multiple concurrent projections

Run with: python -m examples.projection_example
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryCheckpointRepository,
    InMemoryEventBus,
    InMemoryEventStore,
    register_event,
)
from eventsource.subscriptions import SubscriptionConfig, SubscriptionManager

# Configure logging to see subscription lifecycle messages
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

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
# These projections implement the Subscriber protocol for use with
# SubscriptionManager. Each projection declares which events it handles
# and provides a handle() method to process events.


class OrderListProjection:
    """
    Projection that maintains a list of orders for quick querying.

    This is an in-memory read model for demonstration.
    In production, you'd write to a database table.

    Implements the Subscriber protocol required by SubscriptionManager:
    - subscribed_to(): Returns list of event types this projection handles
    - handle(): Processes a single event
    """

    def __init__(self):
        # In-memory read model
        self.orders: dict[UUID, dict] = {}

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

    def get_all_orders(self) -> list[dict]:
        """Get all orders."""
        return list(self.orders.values())

    def get_orders_by_status(self, status: str) -> list[dict]:
        """Get orders by status."""
        return [o for o in self.orders.values() if o["status"] == status]

    def get_orders_by_customer(self, customer_id: UUID) -> list[dict]:
        """Get orders for a customer."""
        return [o for o in self.orders.values() if o["customer_id"] == customer_id]


class CustomerStatsProjection:
    """
    Projection that maintains customer statistics.

    Demonstrates aggregating data across multiple events.
    """

    def __init__(self):
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


class DailyRevenueProjection:
    """
    Projection that tracks daily revenue.

    Demonstrates time-series aggregation.
    """

    def __init__(self):
        # Daily revenue read model
        self.daily_revenue: dict[str, dict] = defaultdict(
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
    """Demonstrate projection patterns with SubscriptionManager."""
    print("=" * 60)
    print("Projection Example with SubscriptionManager")
    print("=" * 60)

    # -------------------------------------------------------------------------
    # Step 1: Setup infrastructure
    # -------------------------------------------------------------------------
    print("\n1. Setting up infrastructure...")

    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    # Create repository with event publishing
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,
    )

    print("   - Event store: InMemoryEventStore")
    print("   - Event bus: InMemoryEventBus")
    print("   - Checkpoint repo: InMemoryCheckpointRepository")

    # -------------------------------------------------------------------------
    # Step 2: Create historical events (before subscriptions start)
    # -------------------------------------------------------------------------
    print("\n2. Creating historical orders (before subscriptions)...")

    customers = [
        (uuid4(), "Alice Johnson"),
        (uuid4(), "Bob Smith"),
        (uuid4(), "Carol Williams"),
    ]

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
        print(f"   Created order: ${total:.2f} for {customer_name}")

    # Ship some orders
    for i, order_id in enumerate(order_ids[:3]):
        order = await repo.load(order_id)
        order.ship(f"TRACK-{1000 + i}", "FedEx")
        await repo.save(order)

    # Deliver one order
    order = await repo.load(order_ids[0])
    order.deliver()
    await repo.save(order)

    # Cancel one order
    order = await repo.load(order_ids[3])
    order.cancel("Customer requested cancellation")
    await repo.save(order)

    print("   - 3 orders shipped, 1 delivered, 1 cancelled")

    # -------------------------------------------------------------------------
    # Step 3: Create projections and SubscriptionManager
    # -------------------------------------------------------------------------
    print("\n3. Creating SubscriptionManager and projections...")

    # Create projections
    order_list = OrderListProjection()
    customer_stats = CustomerStatsProjection()
    daily_revenue = DailyRevenueProjection()

    # Create SubscriptionManager - the key component that coordinates
    # catch-up from event store with live subscriptions from event bus
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Configure subscriptions to start from the beginning
    # This ensures all historical events are processed during catch-up
    config = SubscriptionConfig(
        start_from="beginning",
        batch_size=100,
    )

    # Subscribe all projections
    await manager.subscribe(order_list, config=config, name="OrderList")
    await manager.subscribe(customer_stats, config=config, name="CustomerStats")
    await manager.subscribe(daily_revenue, config=config, name="DailyRevenue")

    print("   - Subscribed: OrderList, CustomerStats, DailyRevenue")
    print("   - Start position: beginning (will catch-up on historical events)")

    # -------------------------------------------------------------------------
    # Step 4: Start the subscription manager (catch-up phase)
    # -------------------------------------------------------------------------
    print("\n4. Starting SubscriptionManager (catching up on history)...")

    await manager.start()

    # Wait for catch-up to complete
    await asyncio.sleep(0.5)

    # -------------------------------------------------------------------------
    # Step 5: Query projections (now have historical data)
    # -------------------------------------------------------------------------
    print("\n5. Querying projections after catch-up:")

    print("\n   Order List Projection:")
    print(f"      Total orders: {len(order_list.get_all_orders())}")
    print(f"      Placed: {len(order_list.get_orders_by_status('placed'))}")
    print(f"      Shipped: {len(order_list.get_orders_by_status('shipped'))}")
    print(f"      Delivered: {len(order_list.get_orders_by_status('delivered'))}")
    print(f"      Cancelled: {len(order_list.get_orders_by_status('cancelled'))}")

    print("\n   Customer Stats Projection:")
    for customer_id, customer_name in customers:
        stats = customer_stats.get_customer_stats(customer_id)
        if stats:
            print(
                f"      {customer_name}: {stats['total_orders']} orders, ${stats['total_spent']:.2f}"
            )

    print("\n   Daily Revenue Projection:")
    for daily in daily_revenue.get_revenue_report():
        print(
            f"      {daily['date']}: {daily['order_count']} orders, ${daily['total_revenue']:.2f}"
        )

    # -------------------------------------------------------------------------
    # Step 6: Create new events (live subscription)
    # -------------------------------------------------------------------------
    print("\n6. Creating new orders (live events)...")

    # These events will be delivered via live subscription (not catch-up)
    new_customer_id = uuid4()
    new_order_id = uuid4()
    order = repo.create_new(new_order_id)
    order.place(new_customer_id, "David Brown", 500.00, 8)
    await repo.save(order)
    print("   New order placed for David Brown: $500.00")

    # Wait for live event to be processed
    await asyncio.sleep(0.2)

    # -------------------------------------------------------------------------
    # Step 7: Verify live updates
    # -------------------------------------------------------------------------
    print("\n7. Verifying live updates:")
    print(f"   Total orders now: {len(order_list.get_all_orders())}")

    # Check subscription status
    status = manager.get_all_statuses()
    for name, sub_status in status.items():
        print(f"\n   Subscription '{name}':")
        print(f"      State: {sub_status.state}")
        print(f"      Events processed: {sub_status.events_processed}")

    # -------------------------------------------------------------------------
    # Step 8: Demonstrate checkpoint tracking
    # -------------------------------------------------------------------------
    print("\n8. Checkpoint tracking:")

    checkpoint = await checkpoint_repo.get_checkpoint("OrderList")
    print(f"   OrderList checkpoint: {checkpoint}")

    checkpoint = await checkpoint_repo.get_checkpoint("CustomerStats")
    print(f"   CustomerStats checkpoint: {checkpoint}")

    # -------------------------------------------------------------------------
    # Step 9: Graceful shutdown
    # -------------------------------------------------------------------------
    print("\n9. Stopping SubscriptionManager...")

    await manager.stop()
    print("   Manager stopped gracefully")

    # Verify final checkpoints were saved
    final_checkpoint = await checkpoint_repo.get_checkpoint("OrderList")
    if final_checkpoint:
        print(f"   Final checkpoint saved: {final_checkpoint}")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)
    print("\nKey takeaways:")
    print("- SubscriptionManager handles catch-up AND live subscriptions")
    print("- Projections receive ALL historical events on startup")
    print("- Checkpoints are tracked for resumable processing")
    print("- Graceful shutdown preserves progress")


if __name__ == "__main__":
    asyncio.run(main())
