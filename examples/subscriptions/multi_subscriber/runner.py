"""Execution runner for multi-subscriber example."""

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    """Demonstrate multiple projections with different configurations."""
    sep = "=" * 60
    print(f"{sep}\nMulti-Subscriber Example\nMultiple Projections Running Concurrently\n{sep}")

    print("\n1. Setting up infrastructure...")
    event_store, event_bus = InMemoryEventStore(), InMemoryEventBus()
    checkpoint_repo, dlq_repo = InMemoryCheckpointRepository(), InMemoryDLQRepository()
    repo = AggregateRepository(
        event_store=event_store, aggregate_factory=ProductAggregate, event_publisher=event_bus
    )

    print("\n2. Creating projections...")
    catalog_projection = ProductCatalogProjection()
    inventory_projection = InventoryDashboardProjection(low_stock_threshold=5)
    analytics_projection = SalesAnalyticsProjection()
    print(
        "   - ProductCatalogProjection (browsing)\n"
        "   - InventoryDashboardProjection (operations)\n"
        "   - SalesAnalyticsProjection (reporting)"
    )

    print("\n3. Creating SubscriptionManager with health monitoring...")
    health_config = HealthCheckConfig(
        max_lag_events_warning=100,
        max_errors_warning=10,
        max_errors_critical=50,
        max_error_rate_per_minute=10.0,
    )
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        dlq_repo=dlq_repo,
        health_check_config=health_config,
    )

    print("\n4. Subscribing projections with different configurations...")
    for proj, name, batch_size in [
        (catalog_projection, "ProductCatalog", 200),
        (inventory_projection, "InventoryDashboard", 50),
        (analytics_projection, "SalesAnalytics", 100),
    ]:
        cfg = SubscriptionConfig(start_from="beginning", batch_size=batch_size)
        await manager.subscribe(proj, config=cfg, name=name)
        print(f"   {name}: start=beginning, batch_size={batch_size}")

    print("\n5. Creating test data (products, inventory, sales)...")
    products_data = [
        ("Laptop Pro 15", 1299.99, "Electronics"),
        ("Wireless Mouse", 29.99, "Electronics"),
        ("Coffee Mug", 12.99, "Home & Kitchen"),
        ("Running Shoes", 89.99, "Sports"),
        ("Python Book", 49.99, "Books"),
    ]
    product_ids = []
    for name, price, category in products_data:
        product_ids.append(product_id := uuid4())
        product = repo.create_new(product_id)
        product.create(name, price, category)
        await repo.save(product)
    print(f"   Created {len(products_data)} products")

    for pid in product_ids:
        product = await repo.load(pid)
        product.add_inventory(20, "Warehouse-A")
        await repo.save(product)
    print("   Added inventory to all products")

    order_ids = [uuid4() for _ in range(10)]
    for i, order_id in enumerate(order_ids):
        pid = product_ids[i % len(product_ids)]
        product = await repo.load(pid)
        product.reserve_inventory(2, order_id)
        await repo.save(product)
        product = await repo.load(pid)
        product.confirm_sale(2, order_id)
        await repo.save(product)
    print(f"   Simulated {len(order_ids)} sales")

    product = await repo.load(product_ids[0])
    product.change_price(1199.99)
    await repo.save(product)
    print("   Applied one price change")

    print("\n6. Starting all subscriptions (concurrent)...")
    start_results = await manager.start(concurrent=True)
    failures = {k: v for k, v in start_results.items() if v is not None}
    ok_msg = "All subscriptions started successfully"
    print(f"   {f'WARNING: Some subscriptions failed: {failures}' if failures else ok_msg}")

    await asyncio.sleep(1.0)

    print("\n7. Querying projection read models:\n\n   === Product Catalog ===")
    c_stats = catalog_projection.get_stats()
    elec_count = len(catalog_projection.get_by_category("Electronics"))
    print(
        f"   Total products: {c_stats['total_products']}\n"
        f"   Categories: {c_stats['categories']}\n"
        f"   Events processed: {c_stats['events_processed']}\n"
        f"   Electronics products: {elec_count}"
    )

    print("\n   === Inventory Dashboard ===")
    i_stats = inventory_projection.get_stats()
    print(
        f"   Products tracked: {i_stats['total_products_tracked']}\n"
        f"   Total inventory: {i_stats['total_inventory']} units\n"
        f"   Low stock alerts: {i_stats['low_stock_count']}\n"
        f"   Events processed: {i_stats['events_processed']}"
    )
    if low_stock := inventory_projection.get_low_stock_products():
        print(f"   Low stock products: {len(low_stock)}")

    print("\n   === Sales Analytics ===")
    a_stats = analytics_projection.get_stats()
    print(
        f"   Total revenue: ${a_stats['total_revenue']:.2f}\n"
        f"   Units sold: {a_stats['total_units_sold']}\n"
        f"   Products with sales: {a_stats['products_sold']}\n"
        f"   Events processed: {a_stats['events_processed']}\n"
        "   Top 3 products by revenue:"
    )
    for i, p in enumerate(analytics_projection.get_top_products(limit=3), 1):
        print(f"     {i}. ${p['revenue']:.2f} ({p['units_sold']} units)")

    print("\n8. Health monitoring:")
    health = await manager.health_check()
    print(
        f"\n   Overall Status: {health.status} (Running: {health.running})\n"
        f"   Subscriptions: {health.subscription_count} (Healthy: {health.healthy_count}, "
        f"Degraded: {health.degraded_count}, Unhealthy: {health.unhealthy_count})\n"
        f"   Total events processed: {health.total_events_processed}, Lag: {health.total_lag_events} events\n"
        "\n   Subscription Status:"
    )
    for sh in health.subscriptions:
        print(
            f"   - {sh.name}: state={sh.state}, status={sh.status}, "
            f"events={sh.events_processed}, lag={sh.lag_events}"
        )

    print("\n9. Demonstrating pause/resume:")
    paused = await manager.pause_subscription("InventoryDashboard")
    paused_names = manager.paused_subscription_names
    print(f"   Paused InventoryDashboard: {paused} (Currently paused: {paused_names})")
    resumed = await manager.resume_subscription("InventoryDashboard")
    print(f"   Resumed InventoryDashboard: {resumed}")

    print("\n10. Kubernetes-style health probes:")
    readiness, liveness = await manager.readiness_check(), await manager.liveness_check()
    print(f"   Readiness: ready={readiness.ready}, reason={readiness.reason}")
    print(f"   Liveness: alive={liveness.alive}, reason={liveness.reason}")

    print("\n11. Shutting down all subscriptions...")
    await manager.stop()
    print("\n   Checkpoints saved:")
    for name in ["ProductCatalog", "InventoryDashboard", "SalesAnalytics"]:
        if checkpoint := await checkpoint_repo.get_checkpoint(name):
            print(f"   - {name}: position {checkpoint}")

    print(
        f"\n{sep}\nExample completed successfully!\n{sep}\n\nKey Takeaways:\n"
        "- Multiple projections can share one SubscriptionManager\n"
        "- Each projection can have different configurations\n"
        "- Health monitoring aggregates across all subscriptions\n"
        "- Pause/resume allows maintenance without full restart\n"
        "- Checkpoints ensure each projection resumes correctly"
    )


__all__ = ["main"]
