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
    InMemoryDLQRepository,
    InMemoryEventBus,
    InMemoryEventStore,
    register_event,
)
from eventsource.subscriptions import (
    HealthCheckConfig,
    SubscriptionConfig,
    SubscriptionManager,
)

# =============================================================================
# Configure Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Domain Events - E-Commerce Domain
# =============================================================================


@register_event
class ProductCreated(DomainEvent):
    """Event emitted when a product is created."""

    event_type: str = "ProductCreated"
    aggregate_type: str = "Product"

    name: str
    price: float
    category: str


@register_event
class ProductPriceChanged(DomainEvent):
    """Event emitted when a product price changes."""

    event_type: str = "ProductPriceChanged"
    aggregate_type: str = "Product"

    old_price: float
    new_price: float


@register_event
class InventoryAdded(DomainEvent):
    """Event emitted when inventory is added."""

    event_type: str = "InventoryAdded"
    aggregate_type: str = "Product"

    quantity: int
    warehouse: str


@register_event
class InventoryReserved(DomainEvent):
    """Event emitted when inventory is reserved for an order."""

    event_type: str = "InventoryReserved"
    aggregate_type: str = "Product"

    quantity: int
    order_id: UUID


@register_event
class InventorySold(DomainEvent):
    """Event emitted when inventory is sold."""

    event_type: str = "InventorySold"
    aggregate_type: str = "Product"

    quantity: int
    order_id: UUID


# =============================================================================
# Product Aggregate
# =============================================================================


class ProductState(BaseModel):
    """Product aggregate state."""

    product_id: UUID
    name: str = ""
    price: float = 0.0
    category: str = ""
    inventory: int = 0


class ProductAggregate(AggregateRoot[ProductState]):
    """Event-sourced product aggregate."""

    aggregate_type = "Product"

    def _get_initial_state(self) -> ProductState:
        return ProductState(product_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, ProductCreated):
            self._state = ProductState(
                product_id=self.aggregate_id,
                name=event.name,
                price=event.price,
                category=event.category,
                inventory=0,
            )
        elif isinstance(event, ProductPriceChanged):
            if self._state:
                self._state = self._state.model_copy(update={"price": event.new_price})
        elif isinstance(event, InventoryAdded):
            if self._state:
                new_inventory = self._state.inventory + event.quantity
                self._state = self._state.model_copy(update={"inventory": new_inventory})
        elif isinstance(event, InventoryReserved):
            if self._state:
                new_inventory = self._state.inventory - event.quantity
                self._state = self._state.model_copy(update={"inventory": new_inventory})
        elif isinstance(event, InventorySold):
            # Inventory already reduced during reservation
            pass

    def create(self, name: str, price: float, category: str) -> None:
        """Create a new product."""
        if self.version > 0:
            raise ValueError("Product already created")

        event = ProductCreated(
            aggregate_id=self.aggregate_id,
            name=name,
            price=price,
            category=category,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def change_price(self, new_price: float) -> None:
        """Change the product price."""
        if not self.state:
            raise ValueError("Product not created")

        event = ProductPriceChanged(
            aggregate_id=self.aggregate_id,
            old_price=self.state.price,
            new_price=new_price,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def add_inventory(self, quantity: int, warehouse: str) -> None:
        """Add inventory for the product."""
        if not self.state:
            raise ValueError("Product not created")

        event = InventoryAdded(
            aggregate_id=self.aggregate_id,
            quantity=quantity,
            warehouse=warehouse,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def reserve_inventory(self, quantity: int, order_id: UUID) -> None:
        """Reserve inventory for an order."""
        if not self.state:
            raise ValueError("Product not created")
        if self.state.inventory < quantity:
            raise ValueError("Insufficient inventory")

        event = InventoryReserved(
            aggregate_id=self.aggregate_id,
            quantity=quantity,
            order_id=order_id,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def confirm_sale(self, quantity: int, order_id: UUID) -> None:
        """Confirm a sale (after reservation)."""
        event = InventorySold(
            aggregate_id=self.aggregate_id,
            quantity=quantity,
            order_id=order_id,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Projection 1: Product Catalog (for browsing)
# =============================================================================


class ProductCatalogProjection:
    """
    Projection that maintains a browsable product catalog.

    Use case: Product listing pages, search results, category browsing.
    Optimized for: Fast reads, simple queries by category.
    """

    def __init__(self):
        # Products indexed by ID
        self.products: dict[UUID, dict] = {}
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

    def get_all_products(self) -> list[dict]:
        """Get all products in the catalog."""
        return list(self.products.values())

    def get_by_category(self, category: str) -> list[dict]:
        """Get products in a category."""
        product_ids = self.by_category.get(category, [])
        return [self.products[pid] for pid in product_ids if pid in self.products]

    def get_stats(self) -> dict:
        """Get catalog statistics."""
        return {
            "total_products": len(self.products),
            "categories": list(self.by_category.keys()),
            "events_processed": self.events_processed,
        }


# =============================================================================
# Projection 2: Inventory Dashboard (for operations)
# =============================================================================


class InventoryDashboardProjection:
    """
    Projection that maintains real-time inventory levels.

    Use case: Warehouse management, stock alerts, reorder triggers.
    Optimized for: Real-time accuracy, low-stock alerts.
    """

    def __init__(self, low_stock_threshold: int = 10):
        self.low_stock_threshold = low_stock_threshold
        # Inventory levels by product
        self.inventory: dict[UUID, int] = defaultdict(int)
        # Products by warehouse
        self.by_warehouse: dict[str, dict[UUID, int]] = defaultdict(lambda: defaultdict(int))
        # Low stock alerts
        self.low_stock_alerts: list[dict] = []
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

    def get_low_stock_products(self) -> list[dict]:
        """Get products below threshold."""
        return [
            {"product_id": pid, "level": level}
            for pid, level in self.inventory.items()
            if level < self.low_stock_threshold
        ]

    def get_warehouse_levels(self, warehouse: str) -> dict[UUID, int]:
        """Get inventory levels for a specific warehouse."""
        return dict(self.by_warehouse.get(warehouse, {}))

    def get_stats(self) -> dict:
        """Get inventory statistics."""
        return {
            "total_products_tracked": len(self.inventory),
            "total_inventory": sum(self.inventory.values()),
            "low_stock_count": len(self.get_low_stock_products()),
            "warehouses": list(self.by_warehouse.keys()),
            "events_processed": self.events_processed,
        }


# =============================================================================
# Projection 3: Sales Analytics (for reporting)
# =============================================================================


class SalesAnalyticsProjection:
    """
    Projection that maintains sales metrics and analytics.

    Use case: Business intelligence, revenue reporting, trend analysis.
    Optimized for: Aggregations, time-series data.
    """

    def __init__(self):
        # Revenue by product
        self.revenue_by_product: dict[UUID, float] = defaultdict(float)
        # Sales count by product
        self.sales_count: dict[UUID, int] = defaultdict(int)
        # Daily revenue
        self.daily_revenue: dict[str, float] = defaultdict(float)
        # Price change history
        self.price_changes: list[dict] = []
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

    def get_top_products(self, limit: int = 5) -> list[dict]:
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

    def get_stats(self) -> dict:
        """Get analytics statistics."""
        return {
            "total_revenue": self.get_total_revenue(),
            "total_units_sold": sum(self.sales_count.values()),
            "products_sold": len([p for p, c in self.sales_count.items() if c > 0]),
            "price_changes": len(self.price_changes),
            "events_processed": self.events_processed,
        }


# =============================================================================
# Main Demo
# =============================================================================


async def main():
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
        aggregate_type="Product",
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


if __name__ == "__main__":
    asyncio.run(main())
