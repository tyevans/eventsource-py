# Tutorial 10: Checkpoints - Tracking Projection Progress

**Difficulty:** Intermediate

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- [Tutorial 6: Projections - Building Read Models](06-projections.md)
- Python 3.10 or higher
- Understanding of async/await
- Understanding of projections and SubscriptionManager

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain what checkpoints are and why they're essential for projection reliability
2. Use the CheckpointRepository protocol and its implementations
3. Track event processing positions with `get_checkpoint()` and `update_checkpoint()`
4. Use global positions with `get_position()` and `save_position()`
5. Monitor projection lag with `get_lag_metrics()`
6. Reset projections for rebuilds with `reset_checkpoint()`
7. Understand how SubscriptionManager integrates checkpoints automatically
8. Choose the right checkpoint repository for your deployment

---

## What are Checkpoints?

**Checkpoints** are position markers that track which events a projection has already processed. They enable:

- **Resumable processing**: Restart from where you left off after crashes or deployments
- **Idempotency**: Avoid processing the same event twice
- **Lag monitoring**: See how far behind a projection is
- **Progress tracking**: Monitor event processing for health checks

### The Problem Without Checkpoints

Without checkpoints, projections would need to replay **all** events from the beginning on every restart:

```python
# Without checkpoints - always starts from position 0
async for stored_event in event_store.read_all():
    await projection.handle(stored_event.event)
# After processing 1 million events, process crashes...
# On restart: process all 1 million events AGAIN
```

This is inefficient and wasteful. Checkpoints solve this by remembering the last processed position.

### How Checkpoints Work

```python
# First run
# Process events 1-1000
await checkpoint_repo.save_position(
    subscription_id="OrderProjection",
    position=1000,
    event_id=last_event.event_id,
    event_type=last_event.event_type,
)
# Checkpoint saved: position 1000

# Process crashes or is restarted

# Second run
last_position = await checkpoint_repo.get_position("OrderProjection")
# Returns: 1000
# Resume from position 1001 (checkpoint + 1)
# Only process NEW events
```

---

## The CheckpointRepository Protocol

All checkpoint repositories implement the `CheckpointRepository` protocol:

```python
from eventsource.repositories.checkpoint import CheckpointRepository
from uuid import UUID

class CheckpointRepository(Protocol):
    """Protocol for checkpoint repositories."""

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """Get the last processed event ID for a projection."""
        ...

    async def update_checkpoint(
        self,
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """Update the checkpoint (idempotent UPSERT)."""
        ...

    async def get_position(self, subscription_id: str) -> int | None:
        """Get last processed global position."""
        ...

    async def save_position(
        self,
        subscription_id: str,
        position: int,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """Save checkpoint with global position (idempotent UPSERT)."""
        ...

    async def get_lag_metrics(
        self,
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None:
        """Get projection lag metrics."""
        ...

    async def reset_checkpoint(self, projection_name: str) -> None:
        """Reset the checkpoint for rebuilding."""
        ...

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """Get all projection checkpoints."""
        ...
```

**Key methods:**

| Method | Purpose |
|--------|---------|
| `get_checkpoint()` | Get last processed event ID (for event-based tracking) |
| `update_checkpoint()` | Save checkpoint by event ID |
| `get_position()` | Get last processed global position (for position-based tracking) |
| `save_position()` | Save checkpoint by global position |
| `get_lag_metrics()` | Calculate how far behind a projection is |
| `reset_checkpoint()` | Clear checkpoint to start from beginning |
| `get_all_checkpoints()` | List all projection checkpoints |

---

## Checkpoint Repository Implementations

eventsource-py provides three checkpoint repository implementations:

| Implementation | Use Case | Persistence | Multi-Process | Setup |
|----------------|----------|-------------|---------------|-------|
| **InMemoryCheckpointRepository** | Testing, development | No | No | None |
| **PostgreSQLCheckpointRepository** | Production | Yes (PostgreSQL) | Yes | Database required |
| **SQLiteCheckpointRepository** | Embedded apps | Yes (SQLite file) | No | File required |

---

## InMemoryCheckpointRepository

For testing and development, use `InMemoryCheckpointRepository`:

```python
from eventsource import InMemoryCheckpointRepository

# Create in-memory checkpoint repository
checkpoint_repo = InMemoryCheckpointRepository()

# All data is lost when process terminates
```

**Features:**
- No configuration needed
- Perfect for tests and tutorials
- All checkpoints lost on restart

---

## Basic Checkpoint Operations

### Saving Checkpoints by Event ID

```python
from uuid import uuid4
from eventsource import InMemoryCheckpointRepository

async def basic_checkpoint_example():
    checkpoint_repo = InMemoryCheckpointRepository()

    # Process an event
    event_id = uuid4()

    # Save checkpoint (UPSERT - safe to call multiple times)
    await checkpoint_repo.update_checkpoint(
        projection_name="OrderProjection",
        event_id=event_id,
        event_type="OrderPlaced",
    )

    # Retrieve checkpoint
    last_event_id = await checkpoint_repo.get_checkpoint("OrderProjection")
    print(f"Last processed: {last_event_id}")  # The event_id we saved
```

### Saving Checkpoints by Global Position

For streaming operations, track global positions:

```python
async def position_checkpoint_example():
    checkpoint_repo = InMemoryCheckpointRepository()

    # Process an event with global position
    await checkpoint_repo.save_position(
        subscription_id="OrderProjection",
        position=1000,  # Global position in event stream
        event_id=event_id,
        event_type="OrderPlaced",
    )

    # Retrieve position
    last_position = await checkpoint_repo.get_position("OrderProjection")
    print(f"Resume from: {last_position + 1}")  # Next position to process
```

**Position vs Event ID:**

- **Event ID**: Tracks specific event UUID (good for event-based processing)
- **Global Position**: Tracks numerical position in the stream (good for streaming)

Both are stored together in the same checkpoint record.

---

## CheckpointData Structure

Checkpoints store comprehensive metadata:

```python
from eventsource.repositories.checkpoint import CheckpointData

checkpoint_data = CheckpointData(
    projection_name="OrderProjection",
    last_event_id=uuid4(),
    last_event_type="OrderPlaced",
    last_processed_at=datetime.now(UTC),
    events_processed=1500,
    global_position=1500,
)
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `projection_name` | str | Name of the projection |
| `last_event_id` | UUID \| None | Last processed event ID |
| `last_event_type` | str \| None | Type of last processed event |
| `last_processed_at` | datetime \| None | When last event was processed |
| `events_processed` | int | Total event count (increments on each update) |
| `global_position` | int \| None | Last processed global position |

---

## Monitoring Projection Lag

Use `get_lag_metrics()` to monitor how far behind a projection is:

```python
from eventsource.repositories.checkpoint import LagMetrics

async def monitor_lag():
    checkpoint_repo = PostgreSQLCheckpointRepository(conn)

    # Get lag metrics
    metrics = await checkpoint_repo.get_lag_metrics(
        projection_name="OrderProjection",
        event_types=["OrderPlaced", "OrderShipped"],  # Filter to relevant events
    )

    if metrics:
        print(f"Projection: {metrics.projection_name}")
        print(f"Last event ID: {metrics.last_event_id}")
        print(f"Latest event ID: {metrics.latest_event_id}")
        print(f"Lag: {metrics.lag_seconds} seconds")
        print(f"Events processed: {metrics.events_processed}")
        print(f"Last processed at: {metrics.last_processed_at}")

        # Alert if lag is too high
        if metrics.lag_seconds > 60:
            print("WARNING: Projection is lagging behind!")
```

**LagMetrics fields:**

| Field | Type | Description |
|-------|------|-------------|
| `projection_name` | str | Name of the projection |
| `last_event_id` | str \| None | Last processed event ID |
| `latest_event_id` | str \| None | Latest relevant event in store |
| `lag_seconds` | float | Time lag in seconds (0.0 if up to date) |
| `events_processed` | int | Total events processed |
| `last_processed_at` | str \| None | ISO 8601 timestamp of last processing |

**Note:** `get_lag_metrics()` requires PostgreSQL or SQLite to query the event store. InMemoryCheckpointRepository returns placeholder metrics.

---

## Resetting Checkpoints for Rebuilds

When you need to rebuild a projection from scratch:

```python
async def rebuild_projection():
    checkpoint_repo = PostgreSQLCheckpointRepository(conn)
    projection = OrderProjection()

    # 1. Clear the read model
    await projection.truncate_read_models()

    # 2. Reset the checkpoint
    await checkpoint_repo.reset_checkpoint("OrderProjection")

    # 3. Restart from beginning
    # SubscriptionManager will start from position 0
    await manager.start()
```

**Warning:** Resetting a checkpoint while the projection is running will cause it to reprocess events. Always stop the projection before resetting.

---

## Integration with SubscriptionManager

The `SubscriptionManager` (from Tutorial 6) automatically manages checkpoints. You don't need to manually save checkpoints when using it.

### Automatic Checkpoint Management

```python
from eventsource import (
    InMemoryEventStore,
    InMemoryEventBus,
    InMemoryCheckpointRepository,
)
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

async def automatic_checkpoints():
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    # Create SubscriptionManager with checkpoint repo
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,  # Checkpoints managed automatically
    )

    # Subscribe projection
    projection = OrderProjection()
    config = SubscriptionConfig(
        start_from="beginning",  # Or "checkpoint" to resume
        batch_size=100,
    )

    await manager.subscribe(projection, config=config, name="OrderProjection")
    await manager.start()

    # SubscriptionManager automatically:
    # 1. Loads checkpoint on start
    # 2. Resumes from saved position
    # 3. Saves checkpoint after each event
    # 4. Saves final checkpoint on shutdown

    await manager.stop()  # Final checkpoint saved here
```

### Start Position Options

Control where to start processing using `SubscriptionConfig.start_from`:

| Option | Behavior |
|--------|----------|
| `"beginning"` | Start from position 0 (process all events) |
| `"end"` | Start from latest position (skip history) |
| `"checkpoint"` | Resume from saved checkpoint (default) |
| `123` (int) | Start from specific position number |

```python
# Start from beginning (rebuild)
config = SubscriptionConfig(start_from="beginning")

# Resume from checkpoint (normal operation)
config = SubscriptionConfig(start_from="checkpoint")

# Start from specific position
config = SubscriptionConfig(start_from=1000)

# Start from end (only new events)
config = SubscriptionConfig(start_from="end")
```

### Checkpoint Strategy

Configure when checkpoints are saved:

```python
from eventsource.subscriptions import CheckpointStrategy

config = SubscriptionConfig(
    start_from="checkpoint",
    checkpoint_strategy=CheckpointStrategy.AFTER_EACH_EVENT,  # Default
)

# Strategies:
# - AFTER_EACH_EVENT: Save after every event (safest, slowest)
# - AFTER_BATCH: Save after processing a batch (balanced)
# - PERIODIC: Save at time intervals (fastest, more risk)
```

---

## PostgreSQL Checkpoint Repository

For production, use `PostgreSQLCheckpointRepository`:

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource import PostgreSQLCheckpointRepository

# Create database connection
engine = create_async_engine(
    "postgresql+asyncpg://user:password@localhost/eventstore",
    echo=False,
)

session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,
)

# Create checkpoint repository
checkpoint_repo = PostgreSQLCheckpointRepository(engine)

# Use it
await checkpoint_repo.save_position(
    subscription_id="OrderProjection",
    position=1000,
    event_id=event_id,
    event_type="OrderPlaced",
)
```

**Features:**
- Full ACID compliance
- Multi-process safe (uses database locks)
- Lag metrics with event store queries
- Production-ready performance

**Database Schema:**

```sql
CREATE TABLE projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_event_id UUID,
    last_event_type TEXT,
    last_processed_at TIMESTAMPTZ,
    events_processed BIGINT DEFAULT 0,
    global_position BIGINT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

---

## SQLite Checkpoint Repository

For embedded applications:

```python
import aiosqlite
from eventsource.repositories.checkpoint import SQLiteCheckpointRepository

async def sqlite_checkpoints():
    # Open SQLite connection
    async with aiosqlite.connect("events.db") as db:
        checkpoint_repo = SQLiteCheckpointRepository(db)

        # Use it like any other checkpoint repository
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=1000,
            event_id=event_id,
            event_type="OrderPlaced",
        )
```

**Features:**
- Zero configuration
- Single file database
- Perfect for CLI tools, desktop apps, edge computing

**Limitations:**
- Single process only (no distributed deployments)
- Lower concurrency than PostgreSQL

---

## Multiple Projections with Checkpoints

Each projection tracks its own checkpoint:

```python
async def multiple_projections():
    checkpoint_repo = InMemoryCheckpointRepository()
    manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)

    # Create multiple projections
    order_list = OrderListProjection()
    customer_stats = CustomerStatsProjection()
    daily_revenue = DailyRevenueProjection()

    # Each has independent checkpoint
    config = SubscriptionConfig(start_from="checkpoint")

    await manager.subscribe(order_list, config=config, name="OrderList")
    await manager.subscribe(customer_stats, config=config, name="CustomerStats")
    await manager.subscribe(daily_revenue, config=config, name="DailyRevenue")

    await manager.start()

    # Each projection:
    # - Has its own checkpoint
    # - Processes at its own pace
    # - Can be reset independently

    # Check individual checkpoints
    order_checkpoint = await checkpoint_repo.get_checkpoint("OrderList")
    stats_checkpoint = await checkpoint_repo.get_checkpoint("CustomerStats")
    revenue_checkpoint = await checkpoint_repo.get_checkpoint("DailyRevenue")

    print(f"OrderList: {order_checkpoint}")
    print(f"CustomerStats: {stats_checkpoint}")
    print(f"DailyRevenue: {revenue_checkpoint}")
```

**Benefits:**
- **Independent progress**: Slow projections don't block fast ones
- **Selective rebuilds**: Rebuild one projection without affecting others
- **Targeted monitoring**: Track lag per projection

---

## Complete Working Example

```python
"""
Tutorial 10: Checkpoints
Run with: python tutorial_10_checkpoints.py
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
from eventsource.subscriptions import SubscriptionConfig, SubscriptionManager


# =============================================================================
# Events
# =============================================================================


@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: UUID
    customer_name: str
    total_amount: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str


# =============================================================================
# Aggregate
# =============================================================================


class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
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
                total_amount=event.total_amount,
                status="placed",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={"status": "shipped"})

    def place(self, customer_id: UUID, customer_name: str, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        event = OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            customer_name=customer_name,
            total_amount=total_amount,
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


# =============================================================================
# Projection
# =============================================================================


class OrderProjection:
    """Simple projection that tracks order counts."""

    def __init__(self):
        self.order_count = 0
        self.total_revenue = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self.order_count += 1
            self.total_revenue += event.total_amount

    def get_summary(self) -> dict:
        return {
            "order_count": self.order_count,
            "total_revenue": self.total_revenue,
        }


# =============================================================================
# Demo
# =============================================================================


async def main():
    print("=" * 60)
    print("Tutorial 10: Checkpoints")
    print("=" * 60)

    # Setup infrastructure
    print("\n1. Setting up infrastructure...")
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
    print("\n2. Creating historical orders...")
    for i in range(3):
        order_id = uuid4()
        order = repo.create_new(order_id)
        order.place(uuid4(), f"Customer {i+1}", 100.0 * (i + 1))
        await repo.save(order)
    print("   Created 3 orders")

    # Create projection and subscription manager
    print("\n3. Creating projection...")
    projection = OrderProjection()

    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    config = SubscriptionConfig(start_from="beginning")
    await manager.subscribe(projection, config=config, name="OrderProjection")

    # Start and catch up
    print("\n4. Starting SubscriptionManager (catching up)...")
    await manager.start()
    await asyncio.sleep(0.3)  # Wait for catch-up

    # Query projection
    print("\n5. Query projection after catch-up:")
    summary = projection.get_summary()
    print(f"   Orders: {summary['order_count']}")
    print(f"   Revenue: ${summary['total_revenue']:.2f}")

    # Check checkpoint
    print("\n6. Check checkpoint:")
    checkpoint = await checkpoint_repo.get_checkpoint("OrderProjection")
    position = await checkpoint_repo.get_position("OrderProjection")
    print(f"   Last event ID: {checkpoint}")
    print(f"   Last position: {position}")

    # Get all checkpoints
    all_checkpoints = await checkpoint_repo.get_all_checkpoints()
    if all_checkpoints:
        ckpt = all_checkpoints[0]
        print(f"   Events processed: {ckpt.events_processed}")
        print(f"   Last processed at: {ckpt.last_processed_at}")

    # Simulate restart
    print("\n7. Simulating restart...")
    await manager.stop()
    print("   Manager stopped")

    # Create new projection (simulates process restart)
    projection2 = OrderProjection()
    manager2 = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Subscribe with checkpoint resume
    config2 = SubscriptionConfig(start_from="checkpoint")
    await manager2.subscribe(projection2, config=config2, name="OrderProjection")

    await manager2.start()
    await asyncio.sleep(0.3)

    print("\n8. After restart (resumed from checkpoint):")
    summary2 = projection2.get_summary()
    print(f"   Orders: {summary2['order_count']}")
    print(f"   Revenue: ${summary2['total_revenue']:.2f}")

    # Create new order (live event)
    print("\n9. Creating new order (live event)...")
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.place(uuid4(), "New Customer", 250.0)
    await repo.save(order)
    await asyncio.sleep(0.2)

    print("\n10. After live event:")
    summary3 = projection2.get_summary()
    print(f"   Orders: {summary3['order_count']}")
    print(f"   Revenue: ${summary3['total_revenue']:.2f}")

    # Final checkpoint
    final_checkpoint = await checkpoint_repo.get_checkpoint("OrderProjection")
    final_position = await checkpoint_repo.get_position("OrderProjection")
    print(f"\n11. Final checkpoint:")
    print(f"   Last event ID: {final_checkpoint}")
    print(f"   Last position: {final_position}")

    # Cleanup
    await manager2.stop()

    print("\n" + "=" * 60)
    print("Tutorial complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
============================================================
Tutorial 10: Checkpoints
============================================================

1. Setting up infrastructure...

2. Creating historical orders...
   Created 3 orders

3. Creating projection...

4. Starting SubscriptionManager (catching up)...

5. Query projection after catch-up:
   Orders: 3
   Revenue: $600.00

6. Check checkpoint:
   Last event ID: [UUID]
   Last position: 3
   Events processed: 3
   Last processed at: [timestamp]

7. Simulating restart...
   Manager stopped

8. After restart (resumed from checkpoint):
   Orders: 0
   Revenue: $0.00

9. Creating new order (live event)...

10. After live event:
   Orders: 1
   Revenue: $250.00

11. Final checkpoint:
   Last event ID: [UUID]
   Last position: 4

============================================================
Tutorial complete!
============================================================
```

**Note:** After restart, the projection starts at checkpoint position 3. When it resumes, it only processes the new event (position 4), not the historical ones (positions 1-3).

---

## Checkpoint Patterns

### Health Check Based on Lag

```python
async def health_check():
    """Check if projection is healthy based on lag."""
    metrics = await checkpoint_repo.get_lag_metrics(
        "OrderProjection",
        event_types=["OrderPlaced", "OrderShipped"],
    )

    if metrics is None:
        return {"status": "error", "message": "No checkpoint found"}

    if metrics.lag_seconds > 300:  # 5 minutes
        return {
            "status": "unhealthy",
            "lag_seconds": metrics.lag_seconds,
            "message": "Projection is lagging behind",
        }

    if metrics.lag_seconds > 60:  # 1 minute
        return {
            "status": "degraded",
            "lag_seconds": metrics.lag_seconds,
            "message": "Projection has minor lag",
        }

    return {
        "status": "healthy",
        "lag_seconds": metrics.lag_seconds,
        "events_processed": metrics.events_processed,
    }
```

### Periodic Checkpoint Reporting

```python
import asyncio

async def checkpoint_reporter(checkpoint_repo: CheckpointRepository):
    """Report checkpoint status every 30 seconds."""
    while True:
        await asyncio.sleep(30)

        checkpoints = await checkpoint_repo.get_all_checkpoints()
        for ckpt in checkpoints:
            print(f"[{ckpt.projection_name}]")
            print(f"  Position: {ckpt.global_position}")
            print(f"  Events: {ckpt.events_processed}")
            print(f"  Last processed: {ckpt.last_processed_at}")
```

### Conditional Reset

```python
async def reset_if_needed(
    checkpoint_repo: CheckpointRepository,
    projection_name: str,
    max_age_hours: int = 24,
):
    """Reset checkpoint if it's too old."""
    checkpoints = await checkpoint_repo.get_all_checkpoints()

    for ckpt in checkpoints:
        if ckpt.projection_name != projection_name:
            continue

        if ckpt.last_processed_at:
            age = datetime.now(UTC) - ckpt.last_processed_at
            if age.total_seconds() > max_age_hours * 3600:
                print(f"Checkpoint is {age.total_seconds()/3600:.1f}h old, resetting")
                await checkpoint_repo.reset_checkpoint(projection_name)
```

---

## Checkpoint Repository Comparison

| Feature | InMemory | PostgreSQL | SQLite |
|---------|----------|------------|--------|
| **Persistence** | No | Yes | Yes |
| **Multi-process** | No | Yes | No |
| **Lag metrics** | Limited | Full | Full |
| **Setup** | None | Database + schema | File + schema |
| **Production** | No | Yes | Limited |
| **Best for** | Testing, development | Production apps | Embedded apps, CLI tools |
| **Concurrency** | Single process | Multi-process | Single process |

---

## Key Takeaways

1. **Checkpoints enable resumable processing**: Restart from where you left off after crashes
2. **Two tracking modes**: Event ID (UUID) or Global Position (integer)
3. **SubscriptionManager handles checkpoints automatically**: No manual checkpoint management needed
4. **Three implementations**: InMemory (testing), PostgreSQL (production), SQLite (embedded)
5. **Independent checkpoints**: Each projection tracks its own position
6. **Lag monitoring**: Use `get_lag_metrics()` for health checks
7. **Reset for rebuilds**: Use `reset_checkpoint()` to start from beginning
8. **UPSERT semantics**: Checkpoints are idempotent - safe to call multiple times

---

## Common Patterns

### Resume from Checkpoint

```python
config = SubscriptionConfig(start_from="checkpoint")
await manager.subscribe(projection, config=config, name="OrderProjection")
```

### Force Rebuild from Beginning

```python
await checkpoint_repo.reset_checkpoint("OrderProjection")
config = SubscriptionConfig(start_from="beginning")
await manager.subscribe(projection, config=config, name="OrderProjection")
```

### Monitor All Projections

```python
checkpoints = await checkpoint_repo.get_all_checkpoints()
for ckpt in checkpoints:
    print(f"{ckpt.projection_name}: {ckpt.events_processed} events")
```

---

## Next Steps

Now that you understand checkpoints and projection progress tracking, you're ready to learn about database deployment patterns.

Continue to **Tutorial 11: PostgreSQL Deployment** to learn about:
- Setting up PostgreSQL event stores in production
- Database schema migrations
- Connection pooling and performance tuning
- High availability and backup strategies

For more examples, see:
- `examples/projection_example.py` - Complete projection workflow with checkpoints
- `tests/integration/repositories/test_checkpoint.py` - Comprehensive checkpoint tests
