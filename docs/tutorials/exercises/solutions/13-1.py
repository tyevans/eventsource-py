"""
Tutorial 13 - Exercise 1 Solution: Production Subscription Setup

This example demonstrates a production-ready subscription setup with:
- Proper configuration
- Health monitoring
- Error handling
- Graceful shutdown

Run with: python 13-1.py
"""

import asyncio
from uuid import uuid4

from eventsource import (
    DeclarativeProjection,
    DomainEvent,
    InMemoryCheckpointRepository,
    InMemoryEventBus,
    InMemoryEventStore,
    handles,
    register_event,
)
from eventsource.stores.interface import ExpectedVersion
from eventsource.subscriptions import (
    CheckpointStrategy,
    ErrorInfo,
    ErrorSeverity,
    SubscriptionConfig,
    SubscriptionManager,
)

# =============================================================================
# Events
# =============================================================================


@register_event
class ProductAdded(DomainEvent):
    event_type: str = "ProductAdded"
    aggregate_type: str = "Inventory"
    name: str
    quantity: int


@register_event
class ProductSold(DomainEvent):
    event_type: str = "ProductSold"
    aggregate_type: str = "Inventory"
    quantity: int


# =============================================================================
# Projection
# =============================================================================


class InventoryProjection(DeclarativeProjection):
    """Tracks inventory levels across all products."""

    def __init__(self):
        super().__init__()
        self.products: dict[str, int] = {}  # product_id -> quantity
        self.total_items = 0
        self.total_products = 0

    @handles(ProductAdded)
    async def _on_product_added(self, event: ProductAdded) -> None:
        """Handle product addition."""
        product_id = str(event.aggregate_id)

        if product_id not in self.products:
            self.products[product_id] = 0
            self.total_products += 1

        self.products[product_id] += event.quantity
        self.total_items += event.quantity

    @handles(ProductSold)
    async def _on_product_sold(self, event: ProductSold) -> None:
        """Handle product sale."""
        product_id = str(event.aggregate_id)

        if product_id in self.products:
            self.products[product_id] -= event.quantity
            self.total_items -= event.quantity

            # Clean up if no inventory left
            if self.products[product_id] <= 0:
                del self.products[product_id]
                self.total_products -= 1

    async def _truncate_read_models(self) -> None:
        """Reset projection state for rebuilds."""
        self.products.clear()
        self.total_items = 0
        self.total_products = 0

    def get_inventory_summary(self) -> dict:
        """Get a summary of current inventory."""
        return {
            "total_products": self.total_products,
            "total_items": self.total_items,
            "products": dict(self.products),
        }


# =============================================================================
# Health Check Function
# =============================================================================


async def health_check(manager: SubscriptionManager) -> dict:
    """
    Perform comprehensive health check on the subscription manager.

    Returns:
        Dictionary with health status and details
    """
    health = await manager.health_check()

    # Determine overall status based on health data
    if health.status == "healthy":
        status = "healthy"
    elif health.status == "degraded":
        status = "degraded"
    else:
        status = "unhealthy"

    return {
        "status": status,
        "running": health.running,
        "subscription_count": health.subscription_count,
        "healthy_count": health.healthy_count,
        "degraded_count": health.degraded_count,
        "unhealthy_count": health.unhealthy_count,
        "total_events_processed": health.total_events_processed,
        "total_events_failed": health.total_events_failed,
        "total_lag": health.total_lag_events,
        "uptime_seconds": health.uptime_seconds,
    }


# =============================================================================
# Main Application
# =============================================================================


async def main():
    print("=== Exercise 13-1: Production Subscription Setup ===\n")

    # =========================================================================
    # Step 1: Create infrastructure
    # =========================================================================
    print("Step 1: Setting up infrastructure...")

    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    print("   Event store: InMemoryEventStore")
    print("   Event bus: InMemoryEventBus")
    print("   Checkpoint repo: InMemoryCheckpointRepository")

    # =========================================================================
    # Step 2: Create SubscriptionManager with production settings
    # =========================================================================
    print("\nStep 2: Creating SubscriptionManager...")

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        shutdown_timeout=15.0,  # 15 second total shutdown timeout
        drain_timeout=5.0,  # 5 seconds to drain in-flight events
    )

    print("   Shutdown timeout: 15.0s")
    print("   Drain timeout: 5.0s")

    # =========================================================================
    # Step 3: Configure and subscribe projection
    # =========================================================================
    print("\nStep 3: Configuring projection subscription...")

    projection = InventoryProjection()

    config = SubscriptionConfig(
        # Resume from checkpoint, or start from beginning if none exists
        start_from="checkpoint",
        # Batch settings for efficient processing
        batch_size=100,
        max_in_flight=1000,
        # Balanced checkpoint strategy for production
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        # Continue processing even if some events fail
        continue_on_error=True,
        # Reasonable timeout for event processing
        processing_timeout=30.0,
    )

    subscription = await manager.subscribe(
        projection,
        config=config,
        name="inventory-projection",
    )

    print(f"   Subscription name: {subscription.name}")
    print(f"   Start from: {config.start_from}")
    print(f"   Checkpoint strategy: {config.checkpoint_strategy.value}")
    print(f"   Continue on error: {config.continue_on_error}")

    # =========================================================================
    # Step 4: Set up error handling callbacks
    # =========================================================================
    print("\nStep 4: Setting up error handling...")

    errors_captured: list[ErrorInfo] = []

    async def log_all_errors(error_info: ErrorInfo):
        """Callback for all errors."""
        errors_captured.append(error_info)
        print(f"   [ERROR] {error_info.error_type}: {error_info.error_message[:50]}")

    async def alert_critical_errors(error_info: ErrorInfo):
        """Callback for critical errors only."""
        print(f"   [CRITICAL ALERT] {error_info.error_message}")
        # In production, this would send to PagerDuty, Slack, etc.

    # Register error callbacks
    manager.on_error(log_all_errors)
    manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical_errors)

    print("   Registered: log_all_errors callback")
    print("   Registered: alert_critical_errors callback (CRITICAL severity)")

    # =========================================================================
    # Step 5: Start manager and create test events
    # =========================================================================
    print("\nStep 5: Starting manager and creating test data...")

    await manager.start()
    print("   Manager started!")

    # Create some test events directly in the event store
    product_ids = [uuid4() for _ in range(5)]

    for i, product_id in enumerate(product_ids):
        # Add product to inventory
        event = ProductAdded(
            aggregate_id=product_id,
            name=f"Product {i + 1}",
            quantity=10 * (i + 1),  # 10, 20, 30, 40, 50 items
            aggregate_version=1,
        )

        await event_store.append_events(
            aggregate_id=product_id,
            aggregate_type="Inventory",
            events=[event],
            expected_version=ExpectedVersion.NO_STREAM,
        )

        # Publish to bus so live subscription receives it
        await event_bus.publish(event)

    print(f"   Created {len(product_ids)} products")

    # Wait for events to be processed
    await asyncio.sleep(0.5)

    # Show projection state
    summary = projection.get_inventory_summary()
    print("\n   Inventory Summary:")
    print(f"     Total products: {summary['total_products']}")
    print(f"     Total items: {summary['total_items']}")

    # =========================================================================
    # Step 6: Perform health check
    # =========================================================================
    print("\nStep 6: Performing health check...")

    health_status = await health_check(manager)

    print(f"   Status: {health_status['status']}")
    print(f"   Running: {health_status['running']}")
    print(f"   Subscriptions: {health_status['subscription_count']}")
    print(f"   Healthy: {health_status['healthy_count']}")
    print(f"   Events processed: {health_status['total_events_processed']}")
    print(f"   Events failed: {health_status['total_events_failed']}")
    print(f"   Uptime: {health_status['uptime_seconds']:.2f}s")

    # Also check readiness and liveness
    readiness = await manager.readiness_check()
    liveness = await manager.liveness_check()

    print(f"\n   Readiness: {'READY' if readiness.ready else 'NOT READY'}")
    print(f"   Liveness: {'ALIVE' if liveness.alive else 'NOT ALIVE'}")

    # =========================================================================
    # Step 7: Graceful shutdown
    # =========================================================================
    print("\nStep 7: Graceful shutdown...")

    await manager.stop()

    print("   Manager stopped gracefully")
    print(f"   Final inventory: {projection.total_items} items")
    print(f"   Errors captured: {len(errors_captured)}")

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 50)
    print("Exercise Complete!")
    print("=" * 50)
    print("""
Key production practices demonstrated:
1. Configured shutdown/drain timeouts for graceful termination
2. Used EVERY_BATCH checkpoint strategy for balanced durability
3. Enabled continue_on_error for resilient processing
4. Registered error callbacks for monitoring/alerting
5. Implemented health checks for operational visibility
6. Used proper async/await shutdown pattern

In a real production setup, you would also:
- Use PostgreSQL or SQLite for persistent storage
- Expose health endpoints via HTTP (FastAPI/Flask)
- Connect error callbacks to monitoring systems
- Use run_until_shutdown() for daemon mode
- Configure logging and metrics collection
""")


if __name__ == "__main__":
    asyncio.run(main())
