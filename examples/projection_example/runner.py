"""Execution runner for projection example."""

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

    # Step 1: Setup infrastructure
    print("\n1. Setting up infrastructure...")
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        event_publisher=event_bus,
    )
    print("   - Event store: InMemoryEventStore")
    print("   - Event bus: InMemoryEventBus")
    print("   - Checkpoint repo: InMemoryCheckpointRepository")

    # Step 2: Create historical events (before subscriptions start)
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

    for i, order_id in enumerate(order_ids[:3]):
        order = await repo.load(order_id)
        order.ship(f"TRACK-{1000 + i}", "FedEx")
        await repo.save(order)

    order = await repo.load(order_ids[0])
    order.deliver()
    await repo.save(order)

    order = await repo.load(order_ids[3])
    order.cancel("Customer requested cancellation")
    await repo.save(order)
    print("   - 3 orders shipped, 1 delivered, 1 cancelled")

    # Step 3: Create projections and SubscriptionManager
    print("\n3. Creating SubscriptionManager and projections...")
    order_list = OrderListProjection()
    customer_stats = CustomerStatsProjection()
    daily_revenue = DailyRevenueProjection()

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )
    config = SubscriptionConfig(start_from="beginning", batch_size=100)

    await manager.subscribe(order_list, config=config, name="OrderList")
    await manager.subscribe(customer_stats, config=config, name="CustomerStats")
    await manager.subscribe(daily_revenue, config=config, name="DailyRevenue")
    print("   - Subscribed: OrderList, CustomerStats, DailyRevenue")
    print("   - Start position: beginning (will catch-up on historical events)")

    # Step 4: Start the subscription manager (catch-up phase)
    print("\n4. Starting SubscriptionManager (catching up on history)...")
    await manager.start()
    await asyncio.sleep(0.5)

    # Step 5: Query projections (now have historical data)
    print("\n5. Querying projections after catch-up:")
    print("\n   Order List Projection:")
    print(f"      Total orders: {len(order_list.get_all_orders())}")
    for status_name in ("placed", "shipped", "delivered", "cancelled"):
        count = len(order_list.get_orders_by_status(status_name))
        print(f"      {status_name.capitalize()}: {count}")

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

    # Step 6: Create new events (live subscription)
    print("\n6. Creating new orders (live events)...")
    order = repo.create_new(uuid4())
    order.place(uuid4(), "David Brown", 500.00, 8)
    await repo.save(order)
    print("   New order placed for David Brown: $500.00")
    await asyncio.sleep(0.2)

    # Step 7: Verify live updates
    print("\n7. Verifying live updates:")
    print(f"   Total orders now: {len(order_list.get_all_orders())}")
    for name, sub_status in manager.get_all_statuses().items():
        print(f"\n   Subscription '{name}':")
        print(f"      State: {sub_status.state}")
        print(f"      Events processed: {sub_status.events_processed}")

    # Step 8: Demonstrate checkpoint tracking
    print("\n8. Checkpoint tracking:")
    for sub_name in ("OrderList", "CustomerStats"):
        checkpoint = await checkpoint_repo.get_checkpoint(sub_name)
        print(f"   {sub_name} checkpoint: {checkpoint}")

    # Step 9: Graceful shutdown
    print("\n9. Stopping SubscriptionManager...")
    await manager.stop()
    print("   Manager stopped gracefully")

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


__all__ = ["main"]
