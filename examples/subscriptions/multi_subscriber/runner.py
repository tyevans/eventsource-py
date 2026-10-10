"""
Execution runner for multi-subscriber example.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from eventsource import (
    AggregateRepository,
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
    InMemoryEventBus,
    InMemoryEventStore,
)
from eventsource.application.subscriptions import (
    HealthCheckConfig,
    SubscriptionConfig,
    SubscriptionManager,
)
from examples.subscriptions.multi_subscriber.domain import ProductAggregate
from examples.subscriptions.multi_subscriber.projections import (
    InventoryDashboardProjection,
    ProductCatalogProjection,
    SalesAnalyticsProjection,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Demonstrate multiple projections with different configurations."""
    print("=" * 60)
    print("Multi-Subscriber Example")
    print("Multiple Projections Running Concurrently")
    print("=" * 60)

    # -------------------------------------------------------------------------
    # Step 1: Set up infrastructure
    # -------------------------------------------------------------------------
    print("\n1. Setting up infrastructure...")

    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()
    dlq_repo = InMemoryDLQRepository()

    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=ProductAggregate,
        event_publisher=event_bus,
    )

    # -------------------------------------------------------------------------
    # Step 2: Create projections
    # -------------------------------------------------------------------------
    print("\n2. Creating projections...")

    catalog_projection = ProductCatalogProjection()
    inventory_projection = InventoryDashboardProjection(low_stock_threshold=5)
    analytics_projection = SalesAnalyticsProjection()

    print("   - ProductCatalogProjection (browsing)")
    print("   - InventoryDashboardProjection (operations)")
    print("   - SalesAnalyticsProjection (reporting)")

    # -------------------------------------------------------------------------
    # Step 3: Create subscription manager with health monitoring
    # -------------------------------------------------------------------------
    print("\n3. Creating SubscriptionManager with health monitoring...")

    health_config = HealthCheckConfig(
        max_lag_events_warning=100,  # Warn if more than 100 events behind
        max_errors_warning=10,  # Warn at 10 errors
        max_errors_critical=50,  # Critical at 50 errors
        max_error_rate_per_minute=10.0,  # Error rate threshold
    )

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        dlq_repo=dlq_repo,
        health_check_config=health_config,
    )

    # -------------------------------------------------------------------------
    # Step 4: Subscribe projections with different configurations
    # -------------------------------------------------------------------------
    print("\n4. Subscribing projections with different configurations...")

    # Catalog: Start from beginning, needs full history
    catalog_config = SubscriptionConfig(
        start_from="beginning",
        batch_size=200,  # Larger batches for catch-up
    )
    await manager.subscribe(
        catalog_projection,
        config=catalog_config,
        name="ProductCatalog",
    )
    print("   ProductCatalog: start=beginning, batch_size=200")

    # Inventory: Start from beginning, smaller batches
    inventory_config = SubscriptionConfig(
        start_from="beginning",
        batch_size=50,  # Smaller batches for faster processing
    )
    await manager.subscribe(
        inventory_projection,
        config=inventory_config,
        name="InventoryDashboard",
    )
    print("   InventoryDashboard: start=beginning, batch_size=50")

    # Analytics: Start from checkpoint (demonstrates resumability)
    analytics_config = SubscriptionConfig(
        start_from="beginning",  # Would be "checkpoint" in production
        batch_size=100,
    )
    await manager.subscribe(
        analytics_projection,
        config=analytics_config,
        name="SalesAnalytics",
    )
    print("   SalesAnalytics: start=beginning, batch_size=100")

    # -------------------------------------------------------------------------
    # Step 5: Create test data
    # -------------------------------------------------------------------------
    print("\n5. Creating test data (products, inventory, sales)...")

    # Create products
    products_data = [
        ("Laptop Pro 15", 1299.99, "Electronics"),
        ("Wireless Mouse", 29.99, "Electronics"),
        ("Coffee Mug", 12.99, "Home & Kitchen"),
        ("Running Shoes", 89.99, "Sports"),
        ("Python Book", 49.99, "Books"),
    ]

    product_ids = []
    for name, price, category in products_data:
        product_id = uuid4()
        product_ids.append(product_id)

        product = repo.create_new(product_id)
        product.create(name, price, category)
        await repo.save(product)

    print(f"   Created {len(products_data)} products")

    # Add inventory
    for pid in product_ids:
        product = await repo.load(pid)
        product.add_inventory(20, "Warehouse-A")
        await repo.save(product)

    print("   Added inventory to all products")

    # Simulate sales
    order_ids = [uuid4() for _ in range(10)]
    for i, order_id in enumerate(order_ids):
        product_id = product_ids[i % len(product_ids)]
        product = await repo.load(product_id)
        product.reserve_inventory(2, order_id)
        await repo.save(product)

        product = await repo.load(product_id)
        product.confirm_sale(2, order_id)
        await repo.save(product)

    print(f"   Simulated {len(order_ids)} sales")

    # Price change
    product = await repo.load(product_ids[0])
    product.change_price(1199.99)
    await repo.save(product)
    print("   Applied one price change")

    # -------------------------------------------------------------------------
    # Step 6: Start all subscriptions concurrently
    # -------------------------------------------------------------------------
    print("\n6. Starting all subscriptions (concurrent)...")

    start_results = await manager.start(concurrent=True)

    # Check for failures
    failures = {k: v for k, v in start_results.items() if v is not None}
    if failures:
        print(f"   WARNING: Some subscriptions failed: {failures}")
    else:
        print("   All subscriptions started successfully")

    # Wait for catch-up
    await asyncio.sleep(1.0)

    # -------------------------------------------------------------------------
    # Step 7: Query each projection's read model
    # -------------------------------------------------------------------------
    print("\n7. Querying projection read models:")

    # Catalog projection
    print("\n   === Product Catalog ===")
    catalog_stats = catalog_projection.get_stats()
    print(f"   Total products: {catalog_stats['total_products']}")
    print(f"   Categories: {catalog_stats['categories']}")
    print(f"   Events processed: {catalog_stats['events_processed']}")

    electronics = catalog_projection.get_by_category("Electronics")
    print(f"   Electronics products: {len(electronics)}")

    # Inventory projection
    print("\n   === Inventory Dashboard ===")
    inventory_stats = inventory_projection.get_stats()
    print(f"   Products tracked: {inventory_stats['total_products_tracked']}")
    print(f"   Total inventory: {inventory_stats['total_inventory']} units")
    print(f"   Low stock alerts: {inventory_stats['low_stock_count']}")
    print(f"   Events processed: {inventory_stats['events_processed']}")

    low_stock = inventory_projection.get_low_stock_products()
    if low_stock:
        print(f"   Low stock products: {len(low_stock)}")

    # Analytics projection
    print("\n   === Sales Analytics ===")
    analytics_stats = analytics_projection.get_stats()
    print(f"   Total revenue: ${analytics_stats['total_revenue']:.2f}")
    print(f"   Units sold: {analytics_stats['total_units_sold']}")
    print(f"   Products with sales: {analytics_stats['products_sold']}")
    print(f"   Events processed: {analytics_stats['events_processed']}")

    top_products = analytics_projection.get_top_products(limit=3)
    print("   Top 3 products by revenue:")
    for i, p in enumerate(top_products, 1):
        print(f"     {i}. ${p['revenue']:.2f} ({p['units_sold']} units)")

    # -------------------------------------------------------------------------
    # Step 8: Monitor health across all subscriptions
    # -------------------------------------------------------------------------
    print("\n8. Health monitoring:")

    # Get comprehensive health
    health = await manager.health_check()
    print(f"\n   Overall Status: {health.status}")
    print(f"   Running: {health.running}")
    print(f"   Total subscriptions: {health.subscription_count}")
    print(f"   Healthy: {health.healthy_count}")
    print(f"   Degraded: {health.degraded_count}")
    print(f"   Unhealthy: {health.unhealthy_count}")
    print(f"   Total events processed: {health.total_events_processed}")
    print(f"   Total lag: {health.total_lag_events} events")

    # Individual subscription health
    print("\n   Subscription Status:")
    for sub_health in health.subscriptions:
        print(f"   - {sub_health.name}:")
        print(f"       State: {sub_health.state}")
        print(f"       Status: {sub_health.status}")
        print(f"       Events: {sub_health.events_processed}")
        print(f"       Lag: {sub_health.lag_events}")

    # -------------------------------------------------------------------------
    # Step 9: Demonstrate pause/resume
    # -------------------------------------------------------------------------
    print("\n9. Demonstrating pause/resume:")

    # Pause one subscription
    paused = await manager.pause_subscription("InventoryDashboard")
    print(f"   Paused InventoryDashboard: {paused}")

    # Check paused subscriptions
    paused_names = manager.paused_subscription_names
    print(f"   Currently paused: {paused_names}")

    # Resume
    resumed = await manager.resume_subscription("InventoryDashboard")
    print(f"   Resumed InventoryDashboard: {resumed}")

    # -------------------------------------------------------------------------
    # Step 10: Readiness and liveness checks
    # -------------------------------------------------------------------------
    print("\n10. Kubernetes-style health probes:")

    readiness = await manager.readiness_check()
    print(f"   Readiness: ready={readiness.ready}, reason={readiness.reason}")

    liveness = await manager.liveness_check()
    print(f"   Liveness: alive={liveness.alive}, reason={liveness.reason}")

    # -------------------------------------------------------------------------
    # Step 11: Clean shutdown
    # -------------------------------------------------------------------------
    print("\n11. Shutting down all subscriptions...")

    await manager.stop()

    # Verify checkpoints
    print("\n   Checkpoints saved:")
    for name in ["ProductCatalog", "InventoryDashboard", "SalesAnalytics"]:
        checkpoint = await checkpoint_repo.get_checkpoint(name)
        if checkpoint:
            print(f"   - {name}: position {checkpoint}")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)
    print("\nKey Takeaways:")
    print("- Multiple projections can share one SubscriptionManager")
    print("- Each projection can have different configurations")
    print("- Health monitoring aggregates across all subscriptions")
    print("- Pause/resume allows maintenance without full restart")
    print("- Checkpoints ensure each projection resumes correctly")


__all__ = [
    "main",
]
