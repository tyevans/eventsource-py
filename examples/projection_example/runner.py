"""
Execution runner for projection example.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from eventsource import (
    AggregateRepository,
    InMemoryCheckpointRepository,
    InMemoryEventBus,
    InMemoryEventStore,
)
from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager
from examples.projection_example.domain import OrderAggregate
from examples.projection_example.projections import (
    CustomerStatsProjection,
    DailyRevenueProjection,
    OrderListProjection,
)

# Configure logging to see subscription lifecycle messages
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
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


__all__ = [
    "main",
]
