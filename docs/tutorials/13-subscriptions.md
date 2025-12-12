# Tutorial 13: Subscription Management with SubscriptionManager

**Difficulty:** Intermediate-Advanced
**Progress:** Tutorial 13 of 21 | Phase 3: Production Readiness

---

## Overview

`SubscriptionManager` coordinates catch-up (historical events from event store) and live events (from event bus) for projections, handling the seamless transition between them using a watermark approach to ensure gap-free, duplicate-free delivery.

```python
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

# Create manager with all dependencies
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
)

# Subscribe projection
await manager.subscribe(
    order_projection,
    config=SubscriptionConfig(start_from="checkpoint"),
)

# Run until shutdown signal
await manager.run_until_shutdown()
```

---

## Basic Setup

Required components: `EventStore` (historical events), `EventBus` (live events), `CheckpointRepository` (persistence).

```python
import asyncio
from uuid import uuid4

from eventsource import (
    InMemoryEventStore,
    InMemoryEventBus,
    InMemoryCheckpointRepository,
    DeclarativeProjection,
    DomainEvent,
    register_event,
    handles,
)
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig


# Define events
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_name: str
    total: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"


# Define projection
class OrderDashboardProjection(DeclarativeProjection):
    """Projection for order statistics."""

    def __init__(self):
        super().__init__()
        self.total_orders = 0
        self.total_revenue = 0.0
        self.shipped_orders = 0

    @handles(OrderCreated)
    async def _on_created(self, event: OrderCreated) -> None:
        self.total_orders += 1
        self.total_revenue += event.total

    @handles(OrderShipped)
    async def _on_shipped(self, event: OrderShipped) -> None:
        self.shipped_orders += 1

    async def _truncate_read_models(self) -> None:
        self.total_orders = 0
        self.total_revenue = 0.0
        self.shipped_orders = 0


async def main():
    # Create infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    # Create projection
    projection = OrderDashboardProjection()

    # Create subscription manager
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Subscribe projection
    subscription = await manager.subscribe(
        projection,
        name="order-dashboard",
    )

    print(f"Subscribed: {subscription.name}")

    # Start manager
    await manager.start()
    print("Manager started")

    # ... process events ...

    # Stop gracefully
    await manager.stop()
    print("Manager stopped")


asyncio.run(main())
```

---

## Subscribing Projections

```python
# Simple subscription (uses defaults)
subscription = await manager.subscribe(my_projection)

# With configuration
config = SubscriptionConfig(
    start_from="checkpoint",  # "checkpoint", "beginning", "end", or position number
    batch_size=100,
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    continue_on_error=True,
    processing_timeout=30.0,
)
subscription = await manager.subscribe(my_projection, config=config, name="order-projection")
```

---

## Lifecycle Management

```python
# Start/stop manually
await manager.start()
await manager.stop()

# Production daemon with signal handling
result = await manager.run_until_shutdown()  # Blocks until SIGTERM/SIGINT

# Context manager
async with SubscriptionManager(...) as manager:
    await manager.subscribe(my_projection)
    # Auto-starts and stops

# Check state
print(manager.is_running, manager.subscription_count, manager.subscription_names)

# Pause/resume
await manager.pause_subscription("order-projection")
await manager.resume_subscription("order-projection")
```

---

## Checkpoint Strategies

| Strategy | Durability | Performance | Replay on Crash | Use Case |
|----------|------------|-------------|-----------------|----------|
| `EVERY_EVENT` | Highest | Lowest | 0-1 events | Critical financial data |
| `EVERY_BATCH` (default) | High | Medium | Up to batch_size | Most production workloads |
| `PERIODIC` | Lower | Highest | Interval's events | High-throughput, replay-tolerant |

```python
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    batch_size=100,  # For EVERY_BATCH
    checkpoint_interval_seconds=5.0,  # For PERIODIC
)
```

---

## Error Handling

```python
from eventsource.subscriptions import ErrorInfo, ErrorSeverity, ErrorCategory

# Register error callbacks
async def log_errors(error_info: ErrorInfo):
    print(f"Error: {error_info.subscription_name} - {error_info.error_message}")

manager.on_error(log_errors)  # All errors
manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical)  # Critical only
manager.on_error_category(ErrorCategory.TRANSIENT, handle_transient)  # Network/timeout

# Continue or stop on error
config = SubscriptionConfig(continue_on_error=True)  # Default: True

# Get error stats
stats = manager.get_error_stats()
```

Categories: `TRANSIENT` (retryable), `PERMANENT`, `INFRASTRUCTURE`, `APPLICATION`
Severities: `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`

---

## Health Monitoring

```python
# Overall health
health = await manager.health_check()  # healthy, degraded, unhealthy, critical

# K8s-style probes
readiness = await manager.readiness_check()  # Ready to process?
liveness = await manager.liveness_check()    # Still alive?

# Per-subscription
sub_health = manager.get_subscription_health("order-projection")

# Configure thresholds
from eventsource.subscriptions import HealthCheckConfig
health_config = HealthCheckConfig(
    max_error_rate_per_minute=10.0,
    max_lag_events_warning=1000,
)
manager = SubscriptionManager(..., health_check_config=health_config)
```

---

## Complete Example

```python
"""
Tutorial 13: Subscription Management with SubscriptionManager

This example demonstrates coordinated event subscriptions.
Run with: python tutorial_13_subscriptions.py
"""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    InMemoryEventBus,
    AggregateRepository,
    DeclarativeProjection,
    handles,
    InMemoryCheckpointRepository,
)
from eventsource.subscriptions import (
    SubscriptionManager,
    SubscriptionConfig,
    CheckpointStrategy,
)


# =============================================================================
# Events
# =============================================================================

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_name: str
    total: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"


@register_event
class OrderCancelled(DomainEvent):
    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"
    reason: str


# =============================================================================
# Aggregate
# =============================================================================

class OrderState(BaseModel):
    order_id: UUID
    customer_name: str = ""
    total: float = 0.0
    status: str = "draft"


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_name=event.customer_name,
                total=event.total,
                status="created",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={"status": "shipped"})
        elif isinstance(event, OrderCancelled):
            if self._state:
                self._state = self._state.model_copy(update={"status": "cancelled"})

    def create(self, customer_name: str, total: float) -> None:
        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_name=customer_name,
            total=total,
            aggregate_version=self.get_next_version(),
        ))

    def ship(self) -> None:
        self.apply_event(OrderShipped(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))

    def cancel(self, reason: str) -> None:
        self.apply_event(OrderCancelled(
            aggregate_id=self.aggregate_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Projection
# =============================================================================

class OrderDashboardProjection(DeclarativeProjection):
    """Projection for order dashboard showing order statistics."""

    def __init__(self):
        super().__init__()
        self.total_orders = 0
        self.total_revenue = 0.0
        self.orders_by_status: dict[str, int] = {
            "created": 0,
            "shipped": 0,
            "cancelled": 0,
        }
        self._orders: dict[UUID, str] = {}  # Track order statuses

    @handles(OrderCreated)
    async def _on_created(self, event: OrderCreated) -> None:
        self.total_orders += 1
        self.total_revenue += event.total
        self.orders_by_status["created"] += 1
        self._orders[event.aggregate_id] = "created"

    @handles(OrderShipped)
    async def _on_shipped(self, event: OrderShipped) -> None:
        if event.aggregate_id in self._orders:
            old_status = self._orders[event.aggregate_id]
            self.orders_by_status[old_status] -= 1
            self.orders_by_status["shipped"] += 1
            self._orders[event.aggregate_id] = "shipped"

    @handles(OrderCancelled)
    async def _on_cancelled(self, event: OrderCancelled) -> None:
        if event.aggregate_id in self._orders:
            old_status = self._orders[event.aggregate_id]
            self.orders_by_status[old_status] -= 1
            self.orders_by_status["cancelled"] += 1
            self._orders[event.aggregate_id] = "cancelled"

    async def _truncate_read_models(self) -> None:
        self.total_orders = 0
        self.total_revenue = 0.0
        self.orders_by_status = {"created": 0, "shipped": 0, "cancelled": 0}
        self._orders.clear()

    def get_stats(self) -> dict:
        return {
            "total_orders": self.total_orders,
            "total_revenue": self.total_revenue,
            "by_status": self.orders_by_status.copy(),
        }


# =============================================================================
# Demo Functions
# =============================================================================

async def basic_subscription_example():
    """Demonstrate basic subscription management."""
    print("=== Basic Subscription Management ===\n")

    # Create infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    # Create projection
    projection = OrderDashboardProjection()

    # Create subscription manager
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Subscribe projection
    subscription = await manager.subscribe(
        projection,
        name="order-dashboard",
    )
    print(f"Subscribed: {subscription.name}")

    # Create aggregate repository for publishing events
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,
    )

    # Create some orders BEFORE starting (these need catch-up)
    for i in range(3):
        order = repo.create_new(uuid4())
        order.create(f"Customer {i+1}", (i+1) * 100.0)
        await repo.save(order)

    print(f"Created 3 orders before starting subscription")

    # Start subscription - catch-up phase begins
    await manager.start()
    print("Manager started - processing events")

    # Give catch-up time to process
    await asyncio.sleep(0.5)

    # Check projection state after catch-up
    stats = projection.get_stats()
    print(f"\nAfter catch-up:")
    print(f"  Total orders: {stats['total_orders']}")
    print(f"  Total revenue: ${stats['total_revenue']:.2f}")

    # Create more orders (live events)
    for i in range(2):
        order = repo.create_new(uuid4())
        order.create(f"Live Customer {i+1}", 50.0)
        await repo.save(order)

    await asyncio.sleep(0.2)

    stats = projection.get_stats()
    print(f"\nAfter live events:")
    print(f"  Total orders: {stats['total_orders']}")
    print(f"  Total revenue: ${stats['total_revenue']:.2f}")

    # Stop gracefully
    await manager.stop()
    print("\nManager stopped gracefully")


async def configured_subscription_example():
    """Demonstrate subscription with custom configuration."""
    print("\n=== Configured Subscription ===\n")

    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    projection = OrderDashboardProjection()

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        shutdown_timeout=10.0,  # 10 second shutdown timeout
        drain_timeout=5.0,      # 5 second drain timeout
    )

    # Subscribe with custom configuration
    config = SubscriptionConfig(
        # Start from beginning (ignore checkpoint)
        start_from="beginning",

        # Batch processing
        batch_size=50,           # Process 50 events per batch
        max_in_flight=500,       # Max 500 events being processed

        # Checkpoint every batch (balanced approach)
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,

        # Continue processing after errors (don't stop projection)
        continue_on_error=True,

        # Processing timeout
        processing_timeout=30.0,
    )

    await manager.subscribe(
        projection,
        config=config,
        name="configured-dashboard",
    )

    print("Subscription configured with:")
    print(f"  start_from: {config.start_from}")
    print(f"  batch_size: {config.batch_size}")
    print(f"  checkpoint_strategy: {config.checkpoint_strategy.value}")
    print(f"  continue_on_error: {config.continue_on_error}")

    await manager.start()
    await asyncio.sleep(0.2)
    await manager.stop()
    print("Example complete")


async def health_monitoring_example():
    """Demonstrate health check capabilities."""
    print("\n=== Health Monitoring ===\n")

    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    projection = OrderDashboardProjection()

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    await manager.subscribe(projection, name="health-demo")
    await manager.start()

    # Wait a moment for startup
    await asyncio.sleep(0.2)

    # Check overall health
    health = await manager.health_check()
    print(f"Manager Health:")
    print(f"  Status: {health.status}")
    print(f"  Running: {health.running}")
    print(f"  Subscription count: {health.subscription_count}")
    print(f"  Healthy: {health.healthy_count}")
    print(f"  Uptime: {health.uptime_seconds:.2f}s")

    # Check readiness
    readiness = await manager.readiness_check()
    print(f"\nReadiness Check:")
    print(f"  Ready: {readiness.ready}")
    print(f"  Reason: {readiness.reason}")

    # Check liveness
    liveness = await manager.liveness_check()
    print(f"\nLiveness Check:")
    print(f"  Alive: {liveness.alive}")
    print(f"  Reason: {liveness.reason}")

    # Check per-subscription health
    sub_health = manager.get_subscription_health("health-demo")
    if sub_health:
        print(f"\nSubscription Health (health-demo):")
        print(f"  Status: {sub_health.status}")
        print(f"  State: {sub_health.state}")
        print(f"  Events processed: {sub_health.events_processed}")

    await manager.stop()


async def error_handling_example():
    """Demonstrate error callback registration."""
    print("\n=== Error Handling ===\n")

    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    projection = OrderDashboardProjection()

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Register error callbacks
    errors_logged = []

    async def log_error(error_info):
        errors_logged.append(error_info)
        print(f"  [ERROR] {error_info.error_type}: {error_info.error_message}")

    async def alert_critical(error_info):
        print(f"  [CRITICAL ALERT] Would send pager for: {error_info.error_message}")

    manager.on_error(log_error)

    # Import and register for critical errors
    from eventsource.subscriptions import ErrorSeverity
    manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical)

    print("Error callbacks registered:")
    print("  - log_error: logs all errors")
    print("  - alert_critical: alerts on critical errors")

    await manager.subscribe(projection, name="error-demo")
    await manager.start()
    await asyncio.sleep(0.2)
    await manager.stop()

    print(f"\nTotal errors logged: {len(errors_logged)}")


async def main():
    await basic_subscription_example()
    await configured_subscription_example()
    await health_monitoring_example()
    await error_handling_example()

    print("\n=== All Examples Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
```

Run with: `python tutorial_13_subscriptions.py`

---

## Next Steps

Continue to [Tutorial 14: Optimizing with Aggregate Snapshotting](14-snapshotting.md) to learn about performance optimization for aggregates with many events.
