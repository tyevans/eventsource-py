# Tutorial 13: Subscription Management - Coordinating Catch-Up and Live Events

**Difficulty:** Intermediate-Advanced

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- [Tutorial 6: Projections - Building Read Models](06-projections.md)
- [Tutorial 10: Checkpoints - Tracking Projection Progress](10-checkpoints.md)
- Python 3.10 or higher
- Understanding of async/await
- Understanding of projections and event subscribers

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Understand the subscription model and why it's essential for production projections
2. Use SubscriptionManager to coordinate catch-up and live event processing
3. Configure subscriptions with SubscriptionConfig for different scenarios
4. Understand the subscription lifecycle and state machine
5. Handle errors gracefully with error callbacks and DLQ support
6. Monitor subscription health with health checks and metrics
7. Implement graceful shutdown with signal handling
8. Pause and resume subscriptions for maintenance operations

---

## What is a Subscription?

A **subscription** is a managed connection between a projection (or other event subscriber) and the event stream. It coordinates two distinct phases:

1. **Catch-up phase**: Reading historical events from the event store
2. **Live phase**: Receiving real-time events from the event bus

The subscription automatically transitions from catch-up to live mode, ensuring your projection processes all events in order without gaps or duplicates.

### Why Subscriptions Matter

Without subscriptions, you'd need to manually:
- Track which events have been processed
- Coordinate between event store and event bus
- Handle the transition from historical to live events
- Manage checkpoints for resumability
- Implement error handling and retries
- Monitor health and lag

**SubscriptionManager** handles all of this for you.

---

## The Problem: Catch-Up vs Live Events

Consider a projection that tracks order statistics. When you deploy a new projection:

**Without subscriptions:**
```python
# Option 1: Only read historical events (misses new orders)
async for event in event_store.read_all():
    await projection.handle(event)
# Stops here - no live events!

# Option 2: Only subscribe to live events (misses history)
await event_bus.subscribe(projection)
# Starts here - no historical data!
```

You have a **gap** - events that arrive between finishing catch-up and subscribing to live events.

**With subscriptions:**
```python
manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
await manager.subscribe(projection)
await manager.start()
# Automatically handles catch-up, transition, and live events
# No gaps, no duplicates
```

---

## SubscriptionManager Overview

`SubscriptionManager` is the central coordinator for all subscriptions. It manages:

- **Registration**: Subscribe projections with custom configuration
- **Lifecycle**: Start, stop, restart subscriptions
- **Coordination**: Seamlessly transition from catch-up to live events
- **Checkpointing**: Automatic position tracking for resumability
- **Error handling**: Retry, circuit breaking, DLQ for failed events
- **Health monitoring**: Status, metrics, lag tracking
- **Graceful shutdown**: Signal handling, drain in-flight events

### Basic Architecture

```
SubscriptionManager
├── SubscriptionRegistry (tracks all subscriptions)
├── SubscriptionLifecycleManager (start/stop operations)
├── HealthCheckProvider (health monitoring)
├── PauseResumeController (pause/resume support)
└── ShutdownCoordinator (graceful shutdown)
```

---

## Basic Setup

Let's create a subscription for a simple order statistics projection:

```python
import asyncio
from uuid import uuid4

from eventsource import (
    InMemoryEventStore,
    InMemoryEventBus,
    InMemoryCheckpointRepository,
    DomainEvent,
    register_event,
    AggregateRepository,
    AggregateRoot,
)
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig
from pydantic import BaseModel


# Events
@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_name: str
    total: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


# Aggregate
class OrderState(BaseModel):
    order_id: str
    customer_name: str = ""
    total: float = 0.0
    status: str = "draft"


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=str(self.aggregate_id),
                customer_name=event.customer_name,
                total=event.total,
                status="placed",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={"status": "shipped"})

    def place(self, customer_name: str, total: float) -> None:
        event = OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_name=customer_name,
            total=total,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def ship(self, tracking_number: str) -> None:
        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# Projection
class OrderStatsProjection:
    """Simple projection tracking order statistics."""

    def __init__(self):
        self.total_orders = 0
        self.total_revenue = 0.0
        self.shipped_count = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which events to process."""
        return [OrderPlaced, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event."""
        if isinstance(event, OrderPlaced):
            self.total_orders += 1
            self.total_revenue += event.total
        elif isinstance(event, OrderShipped):
            self.shipped_count += 1

    def get_stats(self) -> dict:
        """Query method for read model."""
        return {
            "total_orders": self.total_orders,
            "total_revenue": self.total_revenue,
            "shipped_count": self.shipped_count,
        }


async def basic_subscription():
    # Create infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    # Create repository (publishes events to bus after save)
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,
    )

    # Create some historical orders BEFORE starting subscription
    print("Creating historical orders...")
    for i in range(3):
        order = repo.create_new(uuid4())
        order.place(f"Customer {i+1}", 100.0 * (i + 1))
        await repo.save(order)
    print(f"Created 3 historical orders")

    # Create projection
    projection = OrderStatsProjection()

    # Create subscription manager
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Subscribe projection
    subscription = await manager.subscribe(
        projection,
        name="order-stats",  # Unique name for checkpointing
    )
    print(f"Subscribed: {subscription.name}")

    # Start manager (begins catch-up)
    await manager.start()
    print("Manager started - catching up...")

    # Wait for catch-up to complete
    await asyncio.sleep(0.5)

    # Check stats after catch-up
    stats = projection.get_stats()
    print(f"After catch-up: {stats['total_orders']} orders, ${stats['total_revenue']:.2f}")

    # Create new order (live event)
    print("\nCreating live order...")
    order = repo.create_new(uuid4())
    order.place("Live Customer", 500.0)
    await repo.save(order)

    # Wait for live processing
    await asyncio.sleep(0.2)

    # Check updated stats
    stats = projection.get_stats()
    print(f"After live event: {stats['total_orders']} orders, ${stats['total_revenue']:.2f}")

    # Graceful shutdown
    await manager.stop()
    print("Manager stopped gracefully")


asyncio.run(basic_subscription())
```

**Expected output:**
```
Creating historical orders...
Created 3 historical orders
Subscribed: order-stats
Manager started - catching up...
After catch-up: 3 orders, $600.00

Creating live order...
After live event: 4 orders, $1100.00
Manager stopped gracefully
```

---

## Subscription Configuration

`SubscriptionConfig` controls how subscriptions behave:

```python
from eventsource.subscriptions import SubscriptionConfig, CheckpointStrategy

config = SubscriptionConfig(
    # Starting position
    start_from="checkpoint",  # "beginning", "end", "checkpoint", or int position

    # Batch processing
    batch_size=100,           # Events per batch during catch-up
    max_in_flight=1000,       # Max concurrent events being processed

    # Backpressure
    backpressure_threshold=0.8,  # Pause at 80% of max_in_flight

    # Checkpoint strategy
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    checkpoint_interval_seconds=5.0,  # For PERIODIC strategy

    # Timeouts
    processing_timeout=30.0,   # Max time to process one event
    shutdown_timeout=30.0,     # Max time for graceful shutdown

    # Error handling
    continue_on_error=True,    # Continue after DLQ'd events
    max_retries=5,             # Retry attempts before DLQ
    initial_retry_delay=1.0,   # Initial retry backoff
    max_retry_delay=60.0,      # Max retry backoff

    # Circuit breaker
    circuit_breaker_enabled=True,
    circuit_breaker_failure_threshold=5,
    circuit_breaker_recovery_timeout=30.0,

    # Filtering (optional)
    event_types=None,          # Filter to specific event types
    aggregate_types=None,      # Filter to specific aggregate types
    tenant_id=None,            # Filter to specific tenant (multi-tenancy)
)

await manager.subscribe(projection, config=config, name="order-stats")
```

### Start Position Options

| Option | Behavior | Use Case |
|--------|----------|----------|
| `"beginning"` | Start from position 0 | New projection, rebuild from scratch |
| `"end"` | Start from latest position | Live-only, skip history |
| `"checkpoint"` | Resume from saved checkpoint | Normal operation (default) |
| `123` (int) | Start from specific position | Replay from known point |

```python
# New projection - process all history
config = SubscriptionConfig(start_from="beginning")

# Live-only projection - only new events
config = SubscriptionConfig(start_from="end")

# Resume after restart (default)
config = SubscriptionConfig(start_from="checkpoint")

# Start from specific position
config = SubscriptionConfig(start_from=1000)
```

### Checkpoint Strategies

| Strategy | When Saved | Durability | Performance | Replay on Crash |
|----------|-----------|------------|-------------|-----------------|
| `EVERY_EVENT` | After each event | Highest | Slowest | 0-1 events |
| `EVERY_BATCH` | After each batch | High | Medium | Up to batch_size |
| `PERIODIC` | Every N seconds | Lower | Fastest | Interval's events |

```python
# Maximum durability (financial systems)
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
)

# Balanced approach (most production workloads)
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    batch_size=100,
)

# High throughput (analytics, replay-tolerant)
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.PERIODIC,
    checkpoint_interval_seconds=5.0,
)
```

---

## Subscription Lifecycle

Subscriptions follow a state machine with well-defined transitions:

### Subscription States

```python
from eventsource.subscriptions import SubscriptionState

# States:
SubscriptionState.STARTING      # Initializing, reading checkpoint
SubscriptionState.CATCHING_UP   # Reading historical events
SubscriptionState.LIVE          # Receiving real-time events
SubscriptionState.PAUSED        # Temporarily paused
SubscriptionState.STOPPED       # Cleanly shut down (terminal)
SubscriptionState.ERROR         # Failed with error
```

### State Transitions

```
STARTING → CATCHING_UP → LIVE → STOPPED
           ↓            ↓
           ↓            PAUSED
           ↓              ↓
           ERROR ←────────┘
```

**Valid transitions:**
- `STARTING` → `CATCHING_UP`, `LIVE`, `STOPPED`, `ERROR`
- `CATCHING_UP` → `LIVE`, `PAUSED`, `STOPPED`, `ERROR`
- `LIVE` → `CATCHING_UP` (fell behind), `PAUSED`, `STOPPED`, `ERROR`
- `PAUSED` → `CATCHING_UP`, `LIVE`, `STOPPED`, `ERROR`
- `STOPPED` → (terminal state)
- `ERROR` → `STARTING` (restart)

### Checking Subscription Status

```python
# Get status for one subscription
subscription = manager.get_subscription("order-stats")
if subscription:
    status = subscription.get_status()
    print(f"State: {status.state}")
    print(f"Position: {status.position}")
    print(f"Events processed: {status.events_processed}")
    print(f"Lag: {status.lag_events} events")

# Get status for all subscriptions
all_statuses = manager.get_all_statuses()
for name, status in all_statuses.items():
    print(f"{name}: {status.state} @ position {status.position}")
```

### Manual Start/Stop

```python
# Start all subscriptions
await manager.start()

# Start specific subscriptions only
await manager.start(subscription_names=["order-stats", "customer-stats"])

# Stop all subscriptions
await manager.stop(timeout=30.0)

# Stop specific subscriptions only
await manager.stop(subscription_names=["order-stats"])

# Check if running
if manager.is_running:
    print("Manager is active")
```

### Context Manager

```python
# Automatically start and stop
async with SubscriptionManager(event_store, event_bus, checkpoint_repo) as manager:
    await manager.subscribe(projection, name="order-stats")
    # Manager starts automatically here
    await asyncio.sleep(10)
# Manager stops automatically on exit
```

---

## Error Handling

SubscriptionManager provides comprehensive error handling with retries, circuit breaking, and dead letter queue (DLQ) support.

### Error Callbacks

Register callbacks to be notified of errors:

```python
from eventsource.subscriptions import ErrorInfo, ErrorSeverity, ErrorCategory

# Global error callback (all errors)
async def log_all_errors(error_info: ErrorInfo):
    print(f"[ERROR] {error_info.subscription_name}: {error_info.error_message}")
    print(f"  Event: {error_info.event_type} @ position {error_info.position}")
    print(f"  Severity: {error_info.severity}")
    print(f"  Category: {error_info.category}")

manager.on_error(log_all_errors)

# Severity-specific callbacks
async def alert_critical_errors(error_info: ErrorInfo):
    # Send page to on-call engineer
    await send_pager_alert(error_info)

manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical_errors)

# Category-specific callbacks
async def handle_transient_errors(error_info: ErrorInfo):
    # Transient errors are retried automatically
    print(f"Transient error, will retry: {error_info.error_message}")

manager.on_error_category(ErrorCategory.TRANSIENT, handle_transient_errors)
```

### Error Severities

| Severity | Description | Example |
|----------|-------------|---------|
| `LOW` | Minor issues, automatically recovered | Network timeout (retried successfully) |
| `MEDIUM` | Handled errors, may need attention | Event validation failure |
| `HIGH` | Serious errors affecting functionality | Database connection lost |
| `CRITICAL` | System-level failures | Circuit breaker open, DLQ full |

### Error Categories

| Category | Description | Retry Strategy |
|----------|-------------|----------------|
| `TRANSIENT` | Temporary failures | Retry with exponential backoff |
| `PERMANENT` | Unrecoverable errors | Send to DLQ after max retries |
| `INFRASTRUCTURE` | External system failures | Circuit breaker, retry |
| `APPLICATION` | Business logic errors | Send to DLQ, log for investigation |

### Retry Configuration

```python
config = SubscriptionConfig(
    # Retry settings
    max_retries=5,                  # Retry up to 5 times
    initial_retry_delay=1.0,        # Start with 1 second
    max_retry_delay=60.0,           # Cap at 60 seconds
    retry_exponential_base=2.0,     # Double delay each retry
    retry_jitter=0.1,               # Add ±10% randomization

    # Continue processing after DLQ'd events
    continue_on_error=True,
)
```

**Retry delays:**
- Attempt 1: 1.0s (±10%)
- Attempt 2: 2.0s (±10%)
- Attempt 3: 4.0s (±10%)
- Attempt 4: 8.0s (±10%)
- Attempt 5: 16.0s (±10%)
- After 5 attempts: Send to DLQ

### Circuit Breaker

Protects against cascading failures by stopping event processing when error rate exceeds threshold:

```python
config = SubscriptionConfig(
    # Circuit breaker
    circuit_breaker_enabled=True,
    circuit_breaker_failure_threshold=5,     # Open after 5 failures
    circuit_breaker_recovery_timeout=30.0,   # Try to close after 30s
)
```

**Circuit states:**
- **CLOSED**: Normal operation, events processed
- **OPEN**: Too many failures, reject events immediately
- **HALF_OPEN**: Testing if system recovered, allow one event

### Dead Letter Queue (DLQ)

Failed events (after retries) are sent to the DLQ for manual investigation:

```python
from eventsource import InMemoryDLQRepository

# Create DLQ repository
dlq_repo = InMemoryDLQRepository()

# Provide to manager
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    dlq_repo=dlq_repo,
)

# Query DLQ
failed_events = await dlq_repo.get_failed_events(
    subscription_name="order-stats",
    limit=10,
)

for failed in failed_events:
    print(f"Failed: {failed.event_type} - {failed.error_message}")
    print(f"  Attempts: {failed.retry_count}")
```

### Error Statistics

```python
# Get error stats for all subscriptions
stats = manager.get_error_stats()
print(f"Total errors: {stats['total_errors']}")
print(f"Total DLQ count: {stats['total_dlq_count']}")

for name, sub_stats in stats['subscriptions'].items():
    print(f"{name}:")
    print(f"  Errors: {sub_stats['total_errors']}")
    print(f"  DLQ: {sub_stats['dlq_count']}")
    print(f"  Retries: {sub_stats['retry_count']}")
```

---

## Health Monitoring

SubscriptionManager provides Kubernetes-style health checks and metrics.

### Health Check

```python
# Overall manager health
health = await manager.health_check()
print(f"Status: {health.status}")  # healthy, degraded, unhealthy, critical
print(f"Running: {health.running}")
print(f"Subscriptions: {health.subscription_count}")
print(f"Healthy: {health.healthy_count}")
print(f"Degraded: {health.degraded_count}")
print(f"Unhealthy: {health.unhealthy_count}")
print(f"Events processed: {health.events_processed}")
print(f"Events failed: {health.events_failed}")
print(f"Uptime: {health.uptime_seconds}s")

# Per-subscription details
for sub_health in health.subscriptions:
    print(f"{sub_health.name}: {sub_health.status}")
    print(f"  State: {sub_health.state}")
    print(f"  Processed: {sub_health.events_processed}")
    print(f"  Failed: {sub_health.events_failed}")
    print(f"  Lag: {sub_health.lag_events} events")
```

### Readiness Check (Kubernetes)

Determines if the manager should receive traffic:

```python
readiness = await manager.readiness_check()
if readiness.ready:
    print("Ready to receive traffic")
else:
    print(f"Not ready: {readiness.reason}")

# Readiness is True when:
# - Manager is running
# - No subscriptions in ERROR state
# - Not shutting down
```

### Liveness Check (Kubernetes)

Determines if the manager should be restarted:

```python
liveness = await manager.liveness_check()
if liveness.alive:
    print("Manager is alive")
else:
    print(f"Manager not alive: {liveness.reason}")
    # Kubernetes would restart the pod

# Liveness is True when:
# - Manager can respond to health checks
# - Internal lock is not deadlocked
# - Manager has not crashed
```

### Per-Subscription Health

```python
# Get health for specific subscription
sub_health = manager.get_subscription_health("order-stats")
if sub_health:
    print(f"Status: {sub_health.status}")
    print(f"State: {sub_health.state}")
    print(f"Events processed: {sub_health.events_processed}")
    print(f"Lag: {sub_health.lag_events}")
```

### Health Check Configuration

```python
from eventsource.subscriptions import HealthCheckConfig

health_config = HealthCheckConfig(
    # Lag thresholds (events behind)
    max_lag_events_warning=1000,   # Degraded if lag > 1000
    max_lag_events_critical=10000, # Unhealthy if lag > 10000

    # Error rate thresholds
    max_error_rate_per_minute=10.0,  # Degraded if > 10 errors/min
)

manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    health_check_config=health_config,
)
```

---

## Graceful Shutdown

SubscriptionManager supports graceful shutdown with signal handling for production deployments.

### Signal Handling (Daemon Mode)

```python
async def main():
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        shutdown_timeout=30.0,  # Max time for shutdown
        drain_timeout=10.0,     # Max time to drain in-flight events
    )

    # Subscribe projections
    await manager.subscribe(projection1, name="order-stats")
    await manager.subscribe(projection2, name="customer-stats")

    # Run until SIGTERM or SIGINT
    result = await manager.run_until_shutdown()

    # Shutdown result
    print(f"Shutdown phase: {result.phase}")
    print(f"Duration: {result.duration:.2f}s")
    print(f"Forced: {result.forced}")
    print(f"In-flight at shutdown: {result.events_in_flight}")
    print(f"Checkpoints saved: {result.checkpoints_saved}")

asyncio.run(main())
```

**Shutdown sequence:**
1. **Signal received** (SIGTERM or SIGINT)
2. **Stop accepting events**: Stop all coordinators
3. **Drain in-flight events**: Wait up to `drain_timeout` for events being processed
4. **Save checkpoints**: Persist final positions
5. **Cleanup**: Close connections, release resources

### Manual Shutdown Request

```python
# Programmatically trigger shutdown
manager.request_shutdown()

# Check if shutting down
if manager.is_shutting_down:
    print("Shutdown in progress")

# Get current shutdown phase
from eventsource.subscriptions import ShutdownPhase

phase = manager.shutdown_phase
if phase == ShutdownPhase.DRAINING:
    print("Draining in-flight events...")
elif phase == ShutdownPhase.CHECKPOINTING:
    print("Saving checkpoints...")
```

### Shutdown Phases

```python
ShutdownPhase.NONE          # Not shutting down
ShutdownPhase.INITIATED     # Shutdown requested
ShutdownPhase.STOPPING      # Stopping coordinators
ShutdownPhase.DRAINING      # Waiting for in-flight events
ShutdownPhase.CHECKPOINTING # Saving final checkpoints
ShutdownPhase.COMPLETED     # Shutdown complete
```

---

## Pause and Resume

Pause subscriptions for maintenance without losing position:

```python
# Pause specific subscription
await manager.pause_subscription("order-stats")
print("Subscription paused - events buffered")

# Do maintenance...
await perform_database_migration()

# Resume subscription
await manager.resume_subscription("order-stats")
print("Subscription resumed - processing buffered events")

# Pause all subscriptions
results = await manager.pause_all()
paused_count = sum(1 for r in results.values() if r)
print(f"Paused {paused_count} subscriptions")

# Resume all subscriptions
results = await manager.resume_all()
resumed_count = sum(1 for r in results.values() if r)
print(f"Resumed {resumed_count} subscriptions")

# Check paused subscriptions
paused = manager.paused_subscription_names
print(f"Currently paused: {paused}")
```

**Pause reasons:**

```python
from eventsource.subscriptions import PauseReason

# Manual pause
await manager.pause_subscription("order-stats", reason=PauseReason.MANUAL)

# Automatic pause (backpressure)
await manager.pause_subscription("order-stats", reason=PauseReason.BACKPRESSURE)

# Maintenance pause
await manager.pause_subscription("order-stats", reason=PauseReason.MAINTENANCE)
```

---

## Event Filtering

Filter events by type, aggregate type, or tenant:

```python
from uuid import UUID

# Filter by event types
config = SubscriptionConfig(
    event_types=(OrderPlaced, OrderShipped),  # Only these events
)

# Filter by aggregate types
config = SubscriptionConfig(
    aggregate_types=("Order", "Invoice"),  # Only these aggregates
)

# Filter by tenant (multi-tenancy)
tenant_id = UUID("12345678-1234-5678-1234-567812345678")
config = SubscriptionConfig(
    tenant_id=tenant_id,  # Only events for this tenant
)

# Combine filters
config = SubscriptionConfig(
    event_types=(OrderPlaced,),
    aggregate_types=("Order",),
    tenant_id=tenant_id,
)
```

---

## Multiple Subscriptions

Run multiple projections concurrently, each with independent configuration:

```python
# Create multiple projections
order_stats = OrderStatsProjection()
customer_stats = CustomerStatsProjection()
daily_revenue = DailyRevenueProjection()

# Subscribe with different configurations
await manager.subscribe(
    order_stats,
    config=SubscriptionConfig(start_from="beginning", batch_size=500),
    name="order-stats",
)

await manager.subscribe(
    customer_stats,
    config=SubscriptionConfig(start_from="beginning", batch_size=1000),
    name="customer-stats",
)

await manager.subscribe(
    daily_revenue,
    config=SubscriptionConfig(start_from="end"),  # Live-only
    name="daily-revenue",
)

# Start all subscriptions concurrently
await manager.start()

# Each subscription:
# - Has its own checkpoint
# - Processes at its own pace
# - Can be paused/resumed independently
# - Has independent error handling
```

---

## Complete Working Example

```python
"""
Tutorial 13: Subscription Management

Demonstrates comprehensive subscription management with catch-up,
live events, error handling, health monitoring, and graceful shutdown.

Run with: python tutorial_13_subscriptions.py
"""

import asyncio
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
from eventsource.subscriptions import (
    CheckpointStrategy,
    SubscriptionConfig,
    SubscriptionManager,
)


# =============================================================================
# Events
# =============================================================================


@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_id: UUID
    customer_name: str
    total: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


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
    customer_id: UUID | None = None
    customer_name: str = ""
    total: float = 0.0
    status: str = "draft"


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                customer_name=event.customer_name,
                total=event.total,
                status="placed",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={"status": "shipped"})
        elif isinstance(event, OrderCancelled):
            if self._state:
                self._state = self._state.model_copy(update={"status": "cancelled"})

    def place(self, customer_id: UUID, customer_name: str, total: float) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        event = OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            customer_name=customer_name,
            total=total,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "placed":
            raise ValueError("Order must be placed to ship")
        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def cancel(self, reason: str) -> None:
        if not self.state or self.state.status in ("shipped", "cancelled"):
            raise ValueError("Cannot cancel this order")
        event = OrderCancelled(
            aggregate_id=self.aggregate_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Projections
# =============================================================================


class OrderDashboardProjection:
    """Projection tracking order statistics."""

    def __init__(self):
        self.total_orders = 0
        self.total_revenue = 0.0
        self.orders_by_status: dict[str, int] = {
            "placed": 0,
            "shipped": 0,
            "cancelled": 0,
        }
        self._orders: dict[UUID, str] = {}  # Track order statuses

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self.total_orders += 1
            self.total_revenue += event.total
            self.orders_by_status["placed"] += 1
            self._orders[event.aggregate_id] = "placed"

        elif isinstance(event, OrderShipped):
            if event.aggregate_id in self._orders:
                old_status = self._orders[event.aggregate_id]
                self.orders_by_status[old_status] -= 1
                self.orders_by_status["shipped"] += 1
                self._orders[event.aggregate_id] = "shipped"

        elif isinstance(event, OrderCancelled):
            if event.aggregate_id in self._orders:
                old_status = self._orders[event.aggregate_id]
                self.orders_by_status[old_status] -= 1
                self.orders_by_status["cancelled"] += 1
                self._orders[event.aggregate_id] = "cancelled"

    def get_stats(self) -> dict:
        return {
            "total_orders": self.total_orders,
            "total_revenue": self.total_revenue,
            "by_status": self.orders_by_status.copy(),
        }


class CustomerStatsProjection:
    """Projection tracking customer statistics."""

    def __init__(self):
        from collections import defaultdict

        self.stats: dict[UUID, dict] = defaultdict(
            lambda: {
                "customer_id": None,
                "customer_name": "",
                "order_count": 0,
                "total_spent": 0.0,
            }
        )

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            stats = self.stats[event.customer_id]
            stats["customer_id"] = event.customer_id
            stats["customer_name"] = event.customer_name
            stats["order_count"] += 1
            stats["total_spent"] += event.total

    def get_customer_stats(self, customer_id: UUID) -> dict | None:
        if customer_id in self.stats:
            return dict(self.stats[customer_id])
        return None

    def get_top_customers(self, limit: int = 5) -> list[dict]:
        customers = list(self.stats.values())
        customers.sort(key=lambda x: x["total_spent"], reverse=True)
        return customers[:limit]


# =============================================================================
# Demo Functions
# =============================================================================


async def basic_example():
    """Demonstrate basic subscription setup and lifecycle."""
    print("=" * 60)
    print("Basic Subscription Example")
    print("=" * 60)

    # Setup infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,
    )

    # Create historical orders
    print("\n1. Creating historical orders...")
    customers = [
        (uuid4(), "Alice Johnson"),
        (uuid4(), "Bob Smith"),
        (uuid4(), "Carol Williams"),
    ]

    for customer_id, customer_name in customers:
        order = repo.create_new(uuid4())
        order.place(customer_id, customer_name, 100.0)
        await repo.save(order)
    print(f"   Created {len(customers)} orders")

    # Ship one order
    orders = await event_store.read_all()
    first_order_id = orders[0].aggregate_id
    order = await repo.load(first_order_id)
    order.ship("TRACK-001")
    await repo.save(order)

    # Create projection
    print("\n2. Creating projection...")
    projection = OrderDashboardProjection()

    # Create subscription manager
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Subscribe with configuration
    config = SubscriptionConfig(
        start_from="beginning",
        batch_size=100,
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    )

    await manager.subscribe(projection, config=config, name="order-dashboard")
    print("   Subscribed: order-dashboard")

    # Start manager (catch-up phase)
    print("\n3. Starting subscription (catch-up)...")
    await manager.start()
    await asyncio.sleep(0.5)  # Wait for catch-up

    # Check stats
    print("\n4. After catch-up:")
    stats = projection.get_stats()
    print(f"   Total orders: {stats['total_orders']}")
    print(f"   Total revenue: ${stats['total_revenue']:.2f}")
    print(f"   By status: {stats['by_status']}")

    # Create live event
    print("\n5. Creating live order...")
    order = repo.create_new(uuid4())
    order.place(uuid4(), "David Brown", 250.0)
    await repo.save(order)
    await asyncio.sleep(0.2)

    # Check updated stats
    print("\n6. After live event:")
    stats = projection.get_stats()
    print(f"   Total orders: {stats['total_orders']}")
    print(f"   Total revenue: ${stats['total_revenue']:.2f}")

    # Stop gracefully
    print("\n7. Stopping manager...")
    await manager.stop()
    print("   Manager stopped")


async def health_monitoring_example():
    """Demonstrate health monitoring capabilities."""
    print("\n" + "=" * 60)
    print("Health Monitoring Example")
    print("=" * 60)

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
    await asyncio.sleep(0.2)

    # Overall health
    print("\n1. Overall health check:")
    health = await manager.health_check()
    print(f"   Status: {health.status}")
    print(f"   Running: {health.running}")
    print(f"   Subscriptions: {health.subscription_count}")
    print(f"   Healthy: {health.healthy_count}")
    print(f"   Uptime: {health.uptime_seconds:.2f}s")

    # Readiness check
    print("\n2. Readiness check (Kubernetes):")
    readiness = await manager.readiness_check()
    print(f"   Ready: {readiness.ready}")
    print(f"   Reason: {readiness.reason}")

    # Liveness check
    print("\n3. Liveness check (Kubernetes):")
    liveness = await manager.liveness_check()
    print(f"   Alive: {liveness.alive}")
    print(f"   Reason: {liveness.reason}")

    # Per-subscription health
    print("\n4. Per-subscription health:")
    sub_health = manager.get_subscription_health("health-demo")
    if sub_health:
        print(f"   Name: {sub_health.name}")
        print(f"   Status: {sub_health.status}")
        print(f"   State: {sub_health.state}")
        print(f"   Events processed: {sub_health.events_processed}")

    await manager.stop()


async def error_handling_example():
    """Demonstrate error handling with callbacks."""
    print("\n" + "=" * 60)
    print("Error Handling Example")
    print("=" * 60)

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
        print(f"   [ERROR] {error_info.error_type}: {error_info.error_message}")

    async def alert_critical(error_info):
        print(f"   [CRITICAL] Would send alert for: {error_info.error_message}")

    manager.on_error(log_error)

    from eventsource.subscriptions import ErrorSeverity

    manager.on_error_severity(ErrorSeverity.CRITICAL, alert_critical)

    print("\n1. Error callbacks registered:")
    print("   - log_error: logs all errors")
    print("   - alert_critical: alerts on critical errors")

    await manager.subscribe(projection, name="error-demo")
    await manager.start()
    await asyncio.sleep(0.2)

    print(f"\n2. Total errors logged: {len(errors_logged)}")

    # Get error stats
    stats = manager.get_error_stats()
    print(f"   Total errors (all): {stats['total_errors']}")
    print(f"   Total DLQ count: {stats['total_dlq_count']}")

    await manager.stop()


async def multiple_subscriptions_example():
    """Demonstrate multiple concurrent subscriptions."""
    print("\n" + "=" * 60)
    print("Multiple Subscriptions Example")
    print("=" * 60)

    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,
    )

    # Create some orders
    print("\n1. Creating orders...")
    for i in range(5):
        order = repo.create_new(uuid4())
        order.place(uuid4(), f"Customer {i+1}", (i + 1) * 50.0)
        await repo.save(order)
    print("   Created 5 orders")

    # Create multiple projections
    order_dashboard = OrderDashboardProjection()
    customer_stats = CustomerStatsProjection()

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Subscribe with different configs
    print("\n2. Subscribing multiple projections...")
    await manager.subscribe(
        order_dashboard,
        config=SubscriptionConfig(start_from="beginning", batch_size=2),
        name="order-dashboard",
    )
    await manager.subscribe(
        customer_stats,
        config=SubscriptionConfig(start_from="beginning", batch_size=5),
        name="customer-stats",
    )
    print("   Subscribed 2 projections")

    # Start all
    print("\n3. Starting all subscriptions...")
    await manager.start()
    await asyncio.sleep(0.5)

    # Check results
    print("\n4. Results:")
    print(f"   Order Dashboard:")
    stats = order_dashboard.get_stats()
    print(f"     Total orders: {stats['total_orders']}")
    print(f"     Total revenue: ${stats['total_revenue']:.2f}")

    print(f"\n   Customer Stats:")
    top_customers = customer_stats.get_top_customers(limit=3)
    for customer in top_customers:
        print(
            f"     {customer['customer_name']}: "
            f"{customer['order_count']} orders, "
            f"${customer['total_spent']:.2f}"
        )

    # Check subscription statuses
    print("\n5. Subscription statuses:")
    statuses = manager.get_all_statuses()
    for name, status in statuses.items():
        print(f"   {name}:")
        print(f"     State: {status.state}")
        print(f"     Events processed: {status.events_processed}")

    await manager.stop()


async def main():
    """Run all examples."""
    await basic_example()
    await health_monitoring_example()
    await error_handling_example()
    await multiple_subscriptions_example()

    print("\n" + "=" * 60)
    print("All examples complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**
```
============================================================
Basic Subscription Example
============================================================

1. Creating historical orders...
   Created 3 orders

2. Creating projection...
   Subscribed: order-dashboard

3. Starting subscription (catch-up)...

4. After catch-up:
   Total orders: 3
   Total revenue: $300.00
   By status: {'placed': 2, 'shipped': 1, 'cancelled': 0}

5. Creating live order...

6. After live event:
   Total orders: 4
   Total revenue: $550.00

7. Stopping manager...
   Manager stopped

============================================================
Health Monitoring Example
============================================================

1. Overall health check:
   Status: healthy
   Running: True
   Subscriptions: 1
   Healthy: 1
   Uptime: 0.21s

2. Readiness check (Kubernetes):
   Ready: True
   Reason: Manager is ready

3. Liveness check (Kubernetes):
   Alive: True
   Reason: Manager is alive

4. Per-subscription health:
   Name: health-demo
   Status: healthy
   State: live
   Events processed: 0

============================================================
Error Handling Example
============================================================

1. Error callbacks registered:
   - log_error: logs all errors
   - alert_critical: alerts on critical errors

2. Total errors logged: 0
   Total errors (all): 0
   Total DLQ count: 0

============================================================
Multiple Subscriptions Example
============================================================

1. Creating orders...
   Created 5 orders

2. Subscribing multiple projections...
   Subscribed 2 projections

3. Starting all subscriptions...

4. Results:
   Order Dashboard:
     Total orders: 5
     Total revenue: $750.00

   Customer Stats:
     Customer 5: 1 orders, $250.00
     Customer 4: 1 orders, $200.00
     Customer 3: 1 orders, $150.00

5. Subscription statuses:
   order-dashboard:
     State: live
     Events processed: 5
   customer-stats:
     State: live
     Events processed: 5

============================================================
All examples complete!
============================================================
```

---

## Key Takeaways

1. **SubscriptionManager coordinates catch-up and live events**: Seamlessly transitions from historical to real-time processing without gaps
2. **Configuration controls behavior**: Use SubscriptionConfig to optimize for different scenarios
3. **State machine tracks lifecycle**: STARTING → CATCHING_UP → LIVE → STOPPED
4. **Automatic checkpointing**: Resumable processing with configurable strategies
5. **Comprehensive error handling**: Retries, circuit breaking, DLQ, error callbacks
6. **Health monitoring**: Kubernetes-style readiness/liveness checks, per-subscription health
7. **Graceful shutdown**: Signal handling, drain in-flight events, save checkpoints
8. **Pause/resume support**: Maintenance operations without losing position
9. **Event filtering**: By type, aggregate type, or tenant
10. **Multiple subscriptions**: Run projections concurrently with independent configuration

---

## Common Patterns

### Production Daemon

```python
async def run_production_service():
    manager = SubscriptionManager(
        event_store=postgres_event_store,
        event_bus=redis_event_bus,
        checkpoint_repo=postgres_checkpoint_repo,
        shutdown_timeout=60.0,
        drain_timeout=30.0,
    )

    # Subscribe all projections
    await manager.subscribe(order_projection, name="orders")
    await manager.subscribe(customer_projection, name="customers")
    await manager.subscribe(analytics_projection, name="analytics")

    # Run until signal
    result = await manager.run_until_shutdown()

    # Log shutdown details
    logger.info(f"Shutdown complete: {result.phase}, forced={result.forced}")

asyncio.run(run_production_service())
```

### Rebuild Projection

```python
# Reset projection
await checkpoint_repo.reset_checkpoint("order-stats")

# Subscribe from beginning
config = SubscriptionConfig(start_from="beginning")
await manager.subscribe(projection, config=config, name="order-stats")

# Start and catch up
await manager.start()
```

### Live-Only Subscription

```python
# Only receive new events
config = SubscriptionConfig(
    start_from="end",
    checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
)
await manager.subscribe(notification_handler, config=config, name="notifications")
```

---

## Next Steps

Now that you understand subscription management, you're ready to optimize aggregate performance with snapshotting.

Continue to [Tutorial 14: Optimizing with Aggregate Snapshotting](14-snapshotting.md) to learn about:
- Snapshot strategies for large aggregates
- Optimizing aggregate loading with snapshots
- Balancing snapshot frequency with storage costs
- Implementing custom snapshot repositories

For more examples, see:
- `examples/projection_example.py` - Complete projection workflow
- `examples/subscriptions/` - Various subscription patterns
- `tests/integration/subscriptions/` - Production patterns and edge cases
