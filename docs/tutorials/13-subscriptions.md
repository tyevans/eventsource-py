# Tutorial 13: Subscription Management with SubscriptionManager

**Estimated Time:** 60-75 minutes
**Difficulty:** Intermediate-Advanced
**Progress:** Tutorial 13 of 21 | Phase 3: Production Readiness

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 12: Using SQLite](12-sqlite.md) (or Tutorial 11 for PostgreSQL)
- Understanding of projections from [Tutorial 6: Projections](06-projections.md)
- Understanding of checkpoints from [Tutorial 10: Checkpoints](10-checkpoints.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial introduces **subscription management**, the production-ready approach to coordinating catch-up and live event processing for projections.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain how SubscriptionManager coordinates catch-up and live events
2. Configure subscriptions with appropriate checkpoint strategies
3. Implement health checks for subscription monitoring
4. Handle errors and graceful shutdown properly
5. Run projections as daemon processes with signal handling

---

## What is SubscriptionManager?

The `SubscriptionManager` is the central coordinator for event subscriptions in eventsource-py. It bridges the gap between the **event store** (historical events) and the **event bus** (live events), providing a seamless subscription experience for projections.

### The Problem: Catch-Up and Live Events

Without a subscription manager, you have to handle a complex problem manually:

```
Timeline:
  t0: Application starts
  t1: Events 1-1000 exist in the event store (historical)
  t2: Projection starts reading from position 0
  t3: Events 1001, 1002 are published (live)
  t4: Projection reaches position 500
  t5: Events 1003, 1004 are published (live)
  t6: Projection reaches position 1000
  t7: How do we switch to live events without losing 1001-1004?
```

Managing this transition manually is error-prone:

- **Gap risk**: Missing events during the switchover
- **Duplicate risk**: Processing the same event twice
- **Race conditions**: Events arriving while catching up
- **Checkpoint management**: Tracking position across both phases

### The Solution: Unified Subscription

The `SubscriptionManager` handles this transition automatically using a **watermark approach**:

```
1. Get watermark (current max position in event store)
2. Subscribe to live events (buffering mode)
3. Catch up from checkpoint to watermark
4. Process buffered live events (filtering duplicates)
5. Switch to live mode
```

This ensures **gap-free** and **duplicate-free** event delivery.

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

## Creating a SubscriptionManager

### Required Dependencies

The SubscriptionManager requires three core components:

| Component | Purpose | Example |
|-----------|---------|---------|
| `EventStore` | Source of historical events | `PostgreSQLEventStore`, `SQLiteEventStore`, `InMemoryEventStore` |
| `EventBus` | Source of live events | `InMemoryEventBus`, `RedisEventBus` |
| `CheckpointRepository` | Checkpoint persistence | `PostgreSQLCheckpointRepository`, `InMemoryCheckpointRepository` |

### Basic Setup

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

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `event_store` | `EventStore` | Required | Source of historical events |
| `event_bus` | `EventBus` | Required | Source of live events |
| `checkpoint_repo` | `CheckpointRepository` | Required | Checkpoint storage |
| `shutdown_timeout` | `float` | `30.0` | Total graceful shutdown timeout (seconds) |
| `drain_timeout` | `float` | `10.0` | Time to drain in-flight events (seconds) |
| `dlq_repo` | `DLQRepository` | `None` | Dead letter queue for failed events |
| `error_handling_config` | `ErrorHandlingConfig` | `None` | Error handling configuration |
| `health_check_config` | `HealthCheckConfig` | `None` | Health check thresholds |
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing |

---

## Subscribing Projections

### Simple Subscription

The simplest way to subscribe a projection uses default configuration:

```python
# Subscribe with defaults
subscription = await manager.subscribe(my_projection)

# The subscription name defaults to the class name
print(subscription.name)  # "MyProjection"
```

You can also specify a custom name:

```python
subscription = await manager.subscribe(
    my_projection,
    name="order-summary-v2",
)
```

### Configured Subscription

For production use, configure subscriptions explicitly using `SubscriptionConfig`:

```python
from eventsource.subscriptions import SubscriptionConfig, CheckpointStrategy

config = SubscriptionConfig(
    # Where to start reading events
    start_from="checkpoint",  # Resume from last position

    # Batch processing settings
    batch_size=100,           # Events per batch during catch-up
    max_in_flight=1000,       # Maximum concurrent events

    # Checkpoint timing
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,

    # Error handling
    continue_on_error=True,   # Continue after errors (vs stop)

    # Timeouts
    processing_timeout=30.0,  # Max time per event (seconds)
)

subscription = await manager.subscribe(
    my_projection,
    config=config,
    name="order-projection",
)
```

### SubscriptionConfig Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `start_from` | `StartPosition` | `"checkpoint"` | Where to start reading |
| `batch_size` | `int` | `100` | Events per batch during catch-up |
| `max_in_flight` | `int` | `1000` | Maximum concurrent events |
| `backpressure_threshold` | `float` | `0.8` | Fraction (0-1) to signal backpressure |
| `checkpoint_strategy` | `CheckpointStrategy` | `EVERY_BATCH` | When to save checkpoints |
| `checkpoint_interval_seconds` | `float` | `5.0` | Interval for PERIODIC strategy |
| `processing_timeout` | `float` | `30.0` | Max seconds per event |
| `continue_on_error` | `bool` | `True` | Continue after errors |

### StartPosition Options

The `start_from` parameter controls where a subscription begins reading:

```python
from eventsource.subscriptions import SubscriptionConfig

# Resume from last checkpoint (default, recommended)
config = SubscriptionConfig(start_from="checkpoint")

# Start from the very beginning (for new projections)
config = SubscriptionConfig(start_from="beginning")

# Start from the end (live-only, skip historical events)
config = SubscriptionConfig(start_from="end")

# Start from a specific global position
config = SubscriptionConfig(start_from=1000)
```

**Best Practice:** Use `"checkpoint"` in production. If no checkpoint exists, it automatically starts from the beginning.

---

## Subscription Lifecycle

### Starting Subscriptions

The `start()` method begins event processing for all subscribed projections:

```python
# Start all subscriptions
results = await manager.start()

# Check for failures
for name, error in results.items():
    if error:
        print(f"Failed to start {name}: {error}")
    else:
        print(f"Started {name}")
```

You can also start specific subscriptions:

```python
# Start only specific subscriptions
results = await manager.start(subscription_names=["order-projection"])
```

### Checking Manager State

```python
# Is the manager running?
if manager.is_running:
    print("Manager is processing events")

# How many subscriptions?
print(f"Subscription count: {manager.subscription_count}")

# List subscription names
print(f"Subscriptions: {manager.subscription_names}")

# Get a specific subscription
sub = manager.get_subscription("order-projection")
if sub:
    print(f"State: {sub.state.value}")
    print(f"Position: {sub.last_processed_position}")
```

### Stopping Subscriptions

The `stop()` method gracefully shuts down subscriptions:

```python
# Stop all subscriptions (default 30 second timeout)
await manager.stop()

# Stop with custom timeout
await manager.stop(timeout=60.0)

# Stop specific subscriptions
await manager.stop(subscription_names=["order-projection"])
```

Graceful shutdown:
1. Stops accepting new events
2. Drains in-flight events
3. Saves final checkpoints
4. Closes connections

### Daemon Mode with Signal Handling

For production services, use `run_until_shutdown()`:

```python
async def main():
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    await manager.subscribe(my_projection)

    # Runs until SIGTERM or SIGINT
    result = await manager.run_until_shutdown()

    # Check shutdown result
    print(f"Shutdown phase: {result.phase.value}")
    print(f"Duration: {result.duration_seconds:.2f}s")
    print(f"Events drained: {result.events_drained}")
    print(f"Checkpoints saved: {result.checkpoints_saved}")

    if result.forced:
        print("Warning: Shutdown was forced due to timeout")


if __name__ == "__main__":
    asyncio.run(main())
```

This method:
1. Registers SIGTERM/SIGINT signal handlers
2. Starts all subscriptions
3. Blocks until a shutdown signal is received
4. Executes graceful shutdown
5. Returns detailed shutdown results

### Context Manager Usage

The SubscriptionManager supports async context manager:

```python
async def main():
    async with SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    ) as manager:
        await manager.subscribe(my_projection)
        # Manager automatically starts

        # ... your application logic ...

    # Manager automatically stops on exit
```

---

## Checkpoint Strategies

The checkpoint strategy determines when checkpoints are persisted, balancing durability against performance.

### CheckpointStrategy.EVERY_EVENT

Saves checkpoint after every single event:

```python
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
)
```

**Characteristics:**
- **Safest**: Maximum durability, minimal replay on restart
- **Slowest**: One checkpoint write per event
- **Use case**: Critical financial data, audit logs

### CheckpointStrategy.EVERY_BATCH (Default)

Saves checkpoint after each batch of events:

```python
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    batch_size=100,  # Checkpoint every 100 events
)
```

**Characteristics:**
- **Balanced**: Good durability with acceptable performance
- **Moderate**: One checkpoint write per batch
- **Use case**: Most production workloads (recommended default)

### CheckpointStrategy.PERIODIC

Saves checkpoint on a time interval:

```python
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.PERIODIC,
    checkpoint_interval_seconds=5.0,  # Every 5 seconds
)
```

**Characteristics:**
- **Fastest**: Fewest checkpoint writes
- **Riskiest**: May replay more events on restart
- **Use case**: High-throughput, replay-tolerant projections

### Choosing a Strategy

| Strategy | Durability | Performance | Replay on Crash |
|----------|------------|-------------|-----------------|
| EVERY_EVENT | Highest | Lowest | Minimal (0-1 events) |
| EVERY_BATCH | High | Medium | Up to batch_size events |
| PERIODIC | Lower | Highest | Up to interval worth of events |

---

## Error Handling

### Error Callbacks

Register callbacks to be notified when errors occur:

```python
from eventsource.subscriptions import ErrorInfo, ErrorSeverity

async def log_all_errors(error_info: ErrorInfo):
    """Called for all errors."""
    print(f"Error in {error_info.subscription_name}")
    print(f"  Event: {error_info.event_type}")
    print(f"  Error: {error_info.error_message}")
    print(f"  Severity: {error_info.classification.severity.value}")

async def alert_critical_errors(error_info: ErrorInfo):
    """Called only for critical errors."""
    await send_pagerduty_alert(
        message=f"Critical error in {error_info.subscription_name}",
        details=error_info.to_dict(),
    )

# Register callbacks on the manager
manager.on_error(log_all_errors)
manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical_errors)
```

### Error Categories and Severity

Errors are automatically classified:

```python
from eventsource.subscriptions import ErrorCategory, ErrorSeverity

# Register for specific error categories
async def handle_transient_errors(error_info: ErrorInfo):
    # Network issues, timeouts - typically retry
    pass

manager.on_error_category(ErrorCategory.TRANSIENT, handle_transient_errors)
```

**Error Categories:**
- `TRANSIENT`: Temporary failures (network, timeouts) - retryable
- `PERMANENT`: Data/validation errors - not retryable
- `INFRASTRUCTURE`: System failures (DB down)
- `APPLICATION`: Code bugs, business logic errors

**Error Severities:**
- `LOW`: Minor issues
- `MEDIUM`: Affects some processing
- `HIGH`: Significant, needs attention
- `CRITICAL`: Requires immediate action

### Continue on Error

The `continue_on_error` setting controls behavior after failures:

```python
# Continue processing after errors (default)
config = SubscriptionConfig(continue_on_error=True)

# Stop processing on first error
config = SubscriptionConfig(continue_on_error=False)
```

### Error Statistics

Get aggregate error statistics:

```python
stats = manager.get_error_stats()

print(f"Total errors: {stats['total_errors']}")
print(f"Events in DLQ: {stats['total_dlq_count']}")

for name, sub_stats in stats['subscriptions'].items():
    print(f"  {name}: {sub_stats['total_errors']} errors")
```

---

## Health Monitoring

### Health Check API

The SubscriptionManager provides comprehensive health checks:

```python
# Get overall health status
health = await manager.health_check()

print(f"Status: {health.status}")  # healthy, degraded, unhealthy, critical
print(f"Running: {health.running}")
print(f"Subscriptions: {health.subscription_count}")
print(f"Healthy: {health.healthy_count}")
print(f"Degraded: {health.degraded_count}")
print(f"Unhealthy: {health.unhealthy_count}")
print(f"Total events processed: {health.total_events_processed}")
print(f"Total lag: {health.total_lag_events}")
```

### Kubernetes-Style Probes

For container orchestration, use readiness and liveness probes:

```python
# Readiness probe - ready to accept work?
readiness = await manager.readiness_check()
if readiness.ready:
    print("Ready to process events")
else:
    print(f"Not ready: {readiness.reason}")

# Liveness probe - is the manager alive?
liveness = await manager.liveness_check()
if liveness.alive:
    print("Manager is alive and responsive")
else:
    print(f"Manager may be stuck: {liveness.reason}")
```

Use these in HTTP health endpoints:

```python
# FastAPI example
from fastapi import FastAPI, Response

app = FastAPI()

@app.get("/health/ready")
async def readiness():
    status = await manager.readiness_check()
    if status.ready:
        return {"status": "ready"}
    return Response(
        content={"status": "not_ready", "reason": status.reason},
        status_code=503,
    )

@app.get("/health/live")
async def liveness():
    status = await manager.liveness_check()
    if status.alive:
        return {"status": "alive"}
    return Response(
        content={"status": "not_alive", "reason": status.reason},
        status_code=503,
    )
```

### Per-Subscription Health

Get health status for individual subscriptions:

```python
sub_health = manager.get_subscription_health("order-projection")

if sub_health:
    print(f"Subscription: {sub_health.name}")
    print(f"Status: {sub_health.status}")
    print(f"State: {sub_health.state}")
    print(f"Events processed: {sub_health.events_processed}")
    print(f"Events failed: {sub_health.events_failed}")
    print(f"Lag: {sub_health.lag_events}")
    print(f"Error rate: {sub_health.error_rate}/min")
```

### Health Check Configuration

Configure health check thresholds:

```python
from eventsource.subscriptions import HealthCheckConfig

health_config = HealthCheckConfig(
    # Error thresholds
    max_error_rate_per_minute=10.0,
    max_errors_warning=10,
    max_errors_critical=100,

    # Lag thresholds
    max_lag_events_warning=1000,
    max_lag_events_critical=10000,

    # DLQ thresholds
    max_dlq_events_warning=10,
    max_dlq_events_critical=100,
)

manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    health_check_config=health_config,
)
```

---

## Pause and Resume

For maintenance operations, you can pause and resume subscriptions:

```python
# Pause a specific subscription
await manager.pause_subscription("order-projection")

# Check if paused
if "order-projection" in manager.paused_subscription_names:
    print("Subscription is paused")

# Resume
await manager.resume_subscription("order-projection")
```

Pause all subscriptions:

```python
# Pause all for maintenance
results = await manager.pause_all()
print(f"Paused: {sum(results.values())} subscriptions")

# Do maintenance work...

# Resume all
results = await manager.resume_all()
print(f"Resumed: {sum(results.values())} subscriptions")
```

---

## Complete Example

Here is a complete, runnable example demonstrating subscription management:

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

Save this as `tutorial_13_subscriptions.py` and run it:

```bash
python tutorial_13_subscriptions.py
```

---

## Exercises

### Exercise 1: Production Subscription Setup

**Objective:** Create a production-ready projection with subscription management.

**Time:** 25-30 minutes

**Requirements:**

1. Create an event store (SQLite or InMemory)
2. Create a SubscriptionManager with appropriate timeouts
3. Configure a projection subscription with:
   - Appropriate checkpoint strategy
   - Error handling with callbacks
   - Health monitoring
4. Implement a health check function
5. Demonstrate graceful shutdown

**Starter Code:**

```python
"""
Tutorial 13 - Exercise 1: Production Subscription Setup

Your task: Create a production-ready subscription setup with
health monitoring and graceful shutdown.
"""
import asyncio
from uuid import uuid4

from eventsource import (
    DomainEvent,
    register_event,
    DeclarativeProjection,
    handles,
    InMemoryEventStore,
    InMemoryEventBus,
    InMemoryCheckpointRepository,
)
from eventsource.subscriptions import (
    SubscriptionManager,
    SubscriptionConfig,
    CheckpointStrategy,
)


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


class InventoryProjection(DeclarativeProjection):
    """Tracks inventory levels."""

    def __init__(self):
        super().__init__()
        self.products: dict[str, int] = {}
        self.total_items = 0

    # TODO: Implement event handlers using @handles decorator

    async def _truncate_read_models(self) -> None:
        self.products.clear()
        self.total_items = 0


async def health_check(manager: SubscriptionManager) -> dict:
    """
    TODO: Implement health check that returns status dict with:
    - status: "healthy", "degraded", or "unhealthy"
    - subscription_count
    - total_events_processed
    """
    pass


async def main():
    print("=== Exercise 13-1: Production Subscription Setup ===\n")

    # TODO: Step 1 - Create infrastructure (event store, bus, checkpoint repo)

    # TODO: Step 2 - Create SubscriptionManager with appropriate settings

    # TODO: Step 3 - Create and configure the projection subscription

    # TODO: Step 4 - Set up error handling callbacks

    # TODO: Step 5 - Start the manager and create test events

    # TODO: Step 6 - Perform health check

    # TODO: Step 7 - Graceful shutdown


if __name__ == "__main__":
    asyncio.run(main())
```

**Hints:**

- Use `SubscriptionConfig` with `CheckpointStrategy.EVERY_BATCH`
- Register an error callback with `manager.on_error()`
- Use `manager.health_check()` for comprehensive health status
- Remember to await `manager.stop()` for graceful shutdown

The solution is available at: `docs/tutorials/exercises/solutions/13-1.py`

---

## Summary

In this tutorial, you learned:

- **SubscriptionManager** coordinates catch-up and live event subscriptions seamlessly
- **SubscriptionConfig** controls start position, batching, checkpoints, and error handling
- **Checkpoint strategies** balance durability vs performance (EVERY_EVENT, EVERY_BATCH, PERIODIC)
- **Health checks** enable monitoring with Kubernetes-style readiness and liveness probes
- **Error callbacks** allow custom handling and alerting for processing failures
- **Graceful shutdown** ensures no events are lost using `run_until_shutdown()`

---

## Key Takeaways

!!! note "Remember"
    - Always use SubscriptionManager in production for reliable event processing
    - Choose checkpoint strategy based on durability requirements
    - Monitor subscription health continuously with readiness/liveness probes
    - Handle shutdown gracefully to preserve checkpoints

!!! tip "Best Practice"
    Use `start_from="checkpoint"` in production. This automatically starts from the beginning if no checkpoint exists, and resumes from the last position otherwise.

!!! warning "Common Mistake"
    Do not forget to call `await manager.stop()` or use `run_until_shutdown()`. Abrupt termination may lose in-flight events and fail to save checkpoints.

---

## Next Steps

Continue to [Tutorial 14: Optimizing with Aggregate Snapshotting](14-snapshotting.md) to learn about performance optimization for aggregates with many events.

---

## Related Documentation

- [Subscriptions API Reference](../api/subscriptions.md) - Complete subscriptions API
- [Tutorial 6: Projections](06-projections.md) - Projection fundamentals
- [Tutorial 10: Checkpoints](10-checkpoints.md) - Checkpoint management
- [Tutorial 11: PostgreSQL](11-postgresql.md) - Production persistence
