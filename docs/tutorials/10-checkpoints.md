# Tutorial 10: Checkpoint Management and Resumability

**Estimated Time:** 30-45 minutes
**Difficulty:** Intermediate
**Progress:** Tutorial 10 of 21 | Phase 2: Core Patterns

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 9: Error Handling and Recovery](09-error-handling.md)
- Understanding of projections from [Tutorial 6: Projections](06-projections.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial introduces **checkpoint management**, a critical pattern for building production-ready projections that can resume processing after restarts without losing progress or reprocessing events.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain why checkpoints are essential for production projections
2. Create projections with automatic checkpoint tracking using `CheckpointTrackingProjection`
3. Implement resumable event processing that survives restarts
4. Monitor projection lag using `LagMetrics` for health checks
5. Reset and rebuild projections from scratch using checkpoints

---

## What are Checkpoints?

A **checkpoint** is a marker that records which event a projection has processed most recently. Think of it like a bookmark in a book - it saves your place so you can continue from where you left off.

### The Problem: Lost Progress

Without checkpoints, projections have a significant problem. Consider this scenario:

```
Time 0:00  - Projection starts, processes events 1-100
Time 0:30  - Application crashes or restarts
Time 0:31  - Projection restarts... but from where?
```

Without knowing where it left off, the projection has two bad options:

1. **Start from the beginning**: Process all events again, duplicating work and potentially corrupting the read model (e.g., incrementing counters twice)
2. **Start from the end**: Skip events that occurred during the restart, leaving gaps in the read model

Neither option is acceptable for production systems.

### The Solution: Tracking Position

Checkpoints solve this by persistently recording the projection's position in the event stream:

```
Time 0:00  - Process event 1, checkpoint: event_id_1
Time 0:01  - Process event 2, checkpoint: event_id_2
...
Time 0:30  - Process event 100, checkpoint: event_id_100
Time 0:30  - Crash!
Time 0:31  - Restart, read checkpoint: event_id_100
Time 0:31  - Resume from event 101 (no duplicates, no gaps)
```

This is exactly how databases work with write-ahead logs - the same battle-tested approach applied to event processing.

### Checkpoint Data Structure

eventsource-py stores comprehensive checkpoint information:

```python
@dataclass(frozen=True)
class CheckpointData:
    projection_name: str           # Unique identifier for the projection
    last_event_id: UUID | None     # ID of last processed event
    last_event_type: str | None    # Type of last processed event
    last_processed_at: datetime | None  # When processing occurred
    events_processed: int          # Running count of processed events
    global_position: int | None    # Position in global event stream
```

This data enables:

- **Resumability**: Know exactly where to continue
- **Monitoring**: Track how many events have been processed
- **Debugging**: See when processing last occurred
- **Lag detection**: Compare against latest events

---

## CheckpointTrackingProjection

The `CheckpointTrackingProjection` base class provides automatic checkpoint management. You already saw this in Tutorial 6 as a parent of `DeclarativeProjection` - now we will use it directly and understand how it works.

### Projection Hierarchy

```
Projection                    # Abstract base (handle + reset)
    |
CheckpointTrackingProjection  # Adds checkpoint, retry, DLQ
    |
DeclarativeProjection         # Adds @handles decorator
    |
DatabaseProjection            # Adds database connection
```

All production projections should inherit from `CheckpointTrackingProjection` or one of its subclasses.

### Creating a Checkpoint-Aware Projection

Here is a projection that tracks task counts with automatic checkpoint support:

```python
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    register_event,
    CheckpointTrackingProjection,
    InMemoryCheckpointRepository,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


class TaskCounterProjection(CheckpointTrackingProjection):
    """
    Projection that counts tasks by status.

    Uses CheckpointTrackingProjection for automatic checkpoint management.
    """

    def __init__(self, checkpoint_repo=None):
        super().__init__(checkpoint_repo=checkpoint_repo)
        self.total_created = 0
        self.total_completed = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Event types this projection handles."""
        return [TaskCreated, TaskCompleted]

    async def _process_event(self, event: DomainEvent) -> None:
        """Process event and update counts."""
        if isinstance(event, TaskCreated):
            self.total_created += 1
        elif isinstance(event, TaskCompleted):
            self.total_completed += 1

    async def _truncate_read_models(self) -> None:
        """Clear counts for rebuild."""
        self.total_created = 0
        self.total_completed = 0
```

**Key Points:**

- `subscribed_to()`: Returns the event types this projection handles
- `_process_event()`: Your projection logic (called by `handle()`)
- `_truncate_read_models()`: Clears read model data for rebuilds
- Pass a `CheckpointRepository` to persist checkpoints

### How Automatic Checkpoint Tracking Works

When you call `handle(event)` on a `CheckpointTrackingProjection`:

```
handle(event)
    |
    v
1. Execute _process_event(event)  <-- Your projection logic
    |
    v
2. If successful, update checkpoint automatically
    |
    v
3. If failed, retry with backoff, then DLQ
```

The checkpoint is updated **after** successful processing, ensuring:

- No checkpoint update if processing fails (event will be reprocessed)
- Checkpoint reflects actual processed state
- Safe to restart at any point

---

## Working with Checkpoints

### Creating a Checkpoint Repository

eventsource-py provides multiple checkpoint repository implementations:

```python
from eventsource import (
    InMemoryCheckpointRepository,    # Development/testing
    PostgreSQLCheckpointRepository,  # Production (PostgreSQL)
    SQLiteCheckpointRepository,      # Lightweight/embedded
)
```

For development and testing, use `InMemoryCheckpointRepository`:

```python
# Checkpoints stored in memory (lost on restart)
checkpoint_repo = InMemoryCheckpointRepository()

# Create projection with checkpoint support
projection = TaskCounterProjection(checkpoint_repo=checkpoint_repo)
```

For production, use `PostgreSQLCheckpointRepository` (covered in Tutorial 11).

### Getting Current Position

After processing events, you can query the checkpoint:

```python
import asyncio

async def demo_checkpoint():
    checkpoint_repo = InMemoryCheckpointRepository()
    projection = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    # Process some events
    events = [
        TaskCreated(aggregate_id=uuid4(), title="Task 1", aggregate_version=1),
        TaskCreated(aggregate_id=uuid4(), title="Task 2", aggregate_version=1),
        TaskCreated(aggregate_id=uuid4(), title="Task 3", aggregate_version=1),
    ]

    for event in events:
        await projection.handle(event)

    # Query checkpoint
    checkpoint = await projection.get_checkpoint()
    print(f"Last processed event ID: {checkpoint}")
    print(f"Total tasks created: {projection.total_created}")

asyncio.run(demo_checkpoint())
```

Output:
```
Last processed event ID: 7a3b2c1d-...
Total tasks created: 3
```

### Resuming from Checkpoint

The real power of checkpoints is resuming after restarts. Here is the pattern:

```python
async def resumable_processing_demo():
    """Demonstrate resuming from checkpoint after restart."""

    # Shared checkpoint repository (persists across "restarts")
    # In production, this would be PostgreSQL/SQLite
    checkpoint_repo = InMemoryCheckpointRepository()

    # =========================================================
    # Phase 1: Initial processing (before restart)
    # =========================================================
    print("Phase 1: Initial processing")
    projection = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    # Process first batch of events
    task_ids = [uuid4() for _ in range(5)]
    events_batch1 = [
        TaskCreated(aggregate_id=task_ids[0], title="Task A", aggregate_version=1),
        TaskCreated(aggregate_id=task_ids[1], title="Task B", aggregate_version=1),
        TaskCompleted(aggregate_id=task_ids[0], aggregate_version=2),
    ]

    for event in events_batch1:
        await projection.handle(event)

    print(f"  Created: {projection.total_created}")
    print(f"  Completed: {projection.total_completed}")
    checkpoint_before = await projection.get_checkpoint()
    print(f"  Checkpoint saved: {checkpoint_before}")

    # =========================================================
    # Simulating restart (projection instance destroyed)
    # =========================================================
    print("\n--- Simulating application restart ---\n")
    del projection

    # =========================================================
    # Phase 2: Resume after restart
    # =========================================================
    print("Phase 2: After restart")

    # Create new projection instance with SAME checkpoint repository
    projection = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    # In-memory state is gone (counters reset to 0)
    print(f"  In-memory state lost: created={projection.total_created}")

    # But checkpoint is preserved!
    checkpoint_after = await projection.get_checkpoint()
    print(f"  Checkpoint preserved: {checkpoint_after}")

    # Now we can query the event store for events AFTER the checkpoint
    # and resume processing without duplicates
    print("\n  Resuming with new events...")
    events_batch2 = [
        TaskCreated(aggregate_id=task_ids[2], title="Task C", aggregate_version=1),
        TaskCompleted(aggregate_id=task_ids[1], aggregate_version=2),
    ]

    for event in events_batch2:
        await projection.handle(event)

    print(f"  New events processed: created={projection.total_created}")


asyncio.run(resumable_processing_demo())
```

Output:
```
Phase 1: Initial processing
  Created: 2
  Completed: 1
  Checkpoint saved: 3f8a2b1c-...

--- Simulating application restart ---

Phase 2: After restart
  In-memory state lost: created=0
  Checkpoint preserved: 3f8a2b1c-...

  Resuming with new events...
  New events processed: created=1
```

**Important Note:** In this example, the in-memory counters were lost on restart. For production systems with persistent read models (database-backed projections), the read model state is also preserved. For in-memory projections, you would need to replay all events on startup to rebuild state.

---

## Monitoring Lag

Production systems need to monitor how far behind projections are. **Lag metrics** tell you the difference between what has been processed and what is available.

### LagMetrics Data Structure

```python
@dataclass(frozen=True)
class LagMetrics:
    projection_name: str           # Which projection
    last_event_id: str | None      # Last processed event
    latest_event_id: str | None    # Latest event in store
    lag_seconds: float             # Time difference
    events_processed: int          # Total processed count
    last_processed_at: str | None  # ISO timestamp
```

### Getting Lag Metrics

```python
async def monitoring_demo():
    checkpoint_repo = InMemoryCheckpointRepository()
    projection = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    # Process some events
    for i in range(5):
        event = TaskCreated(
            aggregate_id=uuid4(),
            title=f"Task {i+1}",
            aggregate_version=1,
        )
        await projection.handle(event)

    # Get lag metrics
    metrics = await projection.get_lag_metrics()

    if metrics:
        print(f"Projection: {metrics['projection_name']}")
        print(f"Events processed: {metrics['events_processed']}")
        print(f"Last processed at: {metrics['last_processed_at']}")
        print(f"Lag (seconds): {metrics['lag_seconds']}")
    else:
        print("No checkpoint data yet")


asyncio.run(monitoring_demo())
```

Output:
```
Projection: TaskCounterProjection
Events processed: 5
Last processed at: 2024-01-15T10:30:45.123456+00:00
Lag (seconds): 0.0
```

### Using Lag for Health Checks

Lag metrics are essential for production monitoring:

```python
async def health_check(projection) -> dict:
    """
    Health check endpoint for monitoring systems.

    Returns health status based on projection lag.
    """
    metrics = await projection.get_lag_metrics()

    if metrics is None:
        return {
            "status": "unknown",
            "message": "No checkpoint data available",
        }

    # Define thresholds
    WARNING_THRESHOLD = 30.0   # seconds
    CRITICAL_THRESHOLD = 300.0  # 5 minutes

    lag = metrics["lag_seconds"]

    if lag < WARNING_THRESHOLD:
        status = "healthy"
    elif lag < CRITICAL_THRESHOLD:
        status = "degraded"
    else:
        status = "unhealthy"

    return {
        "status": status,
        "projection": metrics["projection_name"],
        "lag_seconds": lag,
        "events_processed": metrics["events_processed"],
        "last_processed_at": metrics["last_processed_at"],
    }


# Example usage in a web framework
async def health_endpoint():
    health = await health_check(projection)
    status_code = 200 if health["status"] == "healthy" else 503
    return health, status_code
```

---

## Rebuilding Projections

Sometimes you need to rebuild a projection from scratch:

- **Bug fix**: Corrected projection logic needs to reprocess all events
- **Schema change**: Added new fields to the read model
- **Data corruption**: Read model got into an inconsistent state
- **New projection**: Deploying a new projection to an existing system

### The Reset Operation

The `reset()` method clears both the checkpoint and the read model:

```python
async def rebuild_demo():
    checkpoint_repo = InMemoryCheckpointRepository()
    projection = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    # Build up some state
    events = [
        TaskCreated(aggregate_id=uuid4(), title=f"Task {i}", aggregate_version=1)
        for i in range(5)
    ]

    for event in events:
        await projection.handle(event)

    print("Before reset:")
    print(f"  Count: {projection.total_created}")
    checkpoint = await projection.get_checkpoint()
    print(f"  Checkpoint: {checkpoint}")

    # Reset clears everything
    await projection.reset()

    print("\nAfter reset:")
    print(f"  Count: {projection.total_created}")
    checkpoint = await projection.get_checkpoint()
    print(f"  Checkpoint: {checkpoint}")

    # Rebuild by replaying all events
    print("\nRebuilding...")
    for event in events:
        await projection.handle(event)

    print("\nAfter rebuild:")
    print(f"  Count: {projection.total_created}")


asyncio.run(rebuild_demo())
```

Output:
```
Before reset:
  Count: 5
  Checkpoint: a1b2c3d4-...

After reset:
  Count: 0
  Checkpoint: None

Rebuilding...

After rebuild:
  Count: 5
```

### Production Rebuild Workflow

In production, a rebuild typically follows this pattern:

```python
async def rebuild_projection(
    projection: CheckpointTrackingProjection,
    event_store,
):
    """
    Rebuild a projection by replaying all events.

    Args:
        projection: The projection to rebuild
        event_store: Event store to read events from
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.warning(
        "Starting rebuild for projection %s",
        projection.projection_name,
    )

    # Step 1: Reset checkpoint and read model
    await projection.reset()

    # Step 2: Read all events from the store
    stream = await event_store.read_all()

    # Step 3: Filter and replay relevant events
    subscribed_types = set(projection.subscribed_to())
    processed = 0

    for stored_event in stream.events:
        if type(stored_event.event) in subscribed_types:
            await projection.handle(stored_event.event)
            processed += 1

            # Log progress periodically
            if processed % 1000 == 0:
                logger.info("Rebuild progress: %d events processed", processed)

    logger.info(
        "Rebuild complete for %s: %d events processed",
        projection.projection_name,
        processed,
    )
```

**Important Considerations:**

1. **Downtime**: Read model may be inconsistent during rebuild
2. **Duration**: Can take hours for large event stores
3. **Resource usage**: Monitor memory and CPU during replay
4. **Idempotency**: Handlers must handle replay correctly

---

## Best Practices

### 1. Always Use Checkpoint Tracking in Production

```python
# GOOD: Production-ready projection
class OrderProjection(CheckpointTrackingProjection):
    def __init__(self, checkpoint_repo):
        # Always inject the checkpoint repository
        super().__init__(checkpoint_repo=checkpoint_repo)

# BAD: No checkpoint support
class OrderProjection(Projection):
    # Cannot resume after restart!
    pass
```

### 2. Make Handlers Idempotent

Handlers may process the same event multiple times during rebuilds or retries:

```python
@handles(OrderCreated)
async def _on_order_created(self, event: OrderCreated) -> None:
    # GOOD: Using aggregate_id as key ensures idempotency
    self.orders[event.aggregate_id] = {
        "id": event.aggregate_id,
        "total": event.total,
    }

    # BAD: Would duplicate on replay
    # self.total_revenue += event.total
```

### 3. Store Checkpoint Repository Reference

Share the same checkpoint repository across projections:

```python
# Application startup
checkpoint_repo = PostgreSQLCheckpointRepository(engine)

# All projections share the same repository
order_projection = OrderProjection(checkpoint_repo=checkpoint_repo)
inventory_projection = InventoryProjection(checkpoint_repo=checkpoint_repo)
notification_projection = NotificationProjection(checkpoint_repo=checkpoint_repo)
```

### 4. Monitor Lag Continuously

Set up monitoring dashboards and alerts:

```python
# Prometheus metrics example
async def collect_projection_metrics():
    for projection in projections:
        metrics = await projection.get_lag_metrics()
        if metrics:
            projection_lag_seconds.labels(
                projection=metrics["projection_name"]
            ).set(metrics["lag_seconds"])

            projection_events_total.labels(
                projection=metrics["projection_name"]
            ).set(metrics["events_processed"])
```

### 5. Plan for Rebuilds

Design your system to support projection rebuilds:

- Document the rebuild procedure
- Test rebuilds in staging environments
- Consider using feature flags to disable projections during rebuild
- Monitor rebuild progress

---

## Complete Example

Here is a complete, runnable example demonstrating checkpoint management:

```python
"""
Tutorial 10: Checkpoint Management and Resumability

This example demonstrates checkpoint tracking for resumable projections.
Run with: python tutorial_10_checkpoints.py
"""
import asyncio
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    register_event,
    CheckpointTrackingProjection,
    InMemoryCheckpointRepository,
)


# =============================================================================
# Events
# =============================================================================

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


# =============================================================================
# Checkpoint-Tracking Projection
# =============================================================================

class TaskCounterProjection(CheckpointTrackingProjection):
    """
    Projection that counts tasks by status.

    Uses CheckpointTrackingProjection for automatic checkpoint management.
    """

    def __init__(self, checkpoint_repo=None):
        super().__init__(checkpoint_repo=checkpoint_repo)
        self.total_created = 0
        self.total_completed = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Event types this projection handles."""
        return [TaskCreated, TaskCompleted]

    async def _process_event(self, event: DomainEvent) -> None:
        """Process event and update counts."""
        if isinstance(event, TaskCreated):
            self.total_created += 1
        elif isinstance(event, TaskCompleted):
            self.total_completed += 1

    async def _truncate_read_models(self) -> None:
        """Clear counts for rebuild."""
        self.total_created = 0
        self.total_completed = 0


# =============================================================================
# Demo Functions
# =============================================================================

async def basic_checkpoint_demo():
    """Demonstrate basic checkpoint tracking."""
    print("=== Basic Checkpoint Demo ===\n")

    # Create shared checkpoint repository
    checkpoint_repo = InMemoryCheckpointRepository()

    # Create projection with checkpoint support
    projection = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    # Process some events
    events = [
        TaskCreated(aggregate_id=uuid4(), title="Task 1", aggregate_version=1),
        TaskCreated(aggregate_id=uuid4(), title="Task 2", aggregate_version=1),
        TaskCreated(aggregate_id=uuid4(), title="Task 3", aggregate_version=1),
    ]

    for event in events:
        await projection.handle(event)

    # Check counts
    print(f"Tasks created: {projection.total_created}")
    print(f"Tasks completed: {projection.total_completed}")

    # Check checkpoint
    checkpoint = await projection.get_checkpoint()
    print(f"\nCheckpoint (last event ID): {checkpoint}")


async def resumable_processing_demo():
    """Demonstrate resuming from checkpoint after restart."""
    print("\n=== Resumable Processing Demo ===\n")

    # Shared checkpoint repository (persists across "restarts")
    checkpoint_repo = InMemoryCheckpointRepository()

    # Phase 1: Initial processing
    print("Phase 1: Initial processing")
    projection1 = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    task_ids = [uuid4() for _ in range(5)]
    events_batch1 = [
        TaskCreated(aggregate_id=task_ids[0], title="Task A", aggregate_version=1),
        TaskCreated(aggregate_id=task_ids[1], title="Task B", aggregate_version=1),
        TaskCompleted(aggregate_id=task_ids[0], aggregate_version=2),
    ]

    for event in events_batch1:
        await projection1.handle(event)

    print(f"  Created: {projection1.total_created}, Completed: {projection1.total_completed}")
    checkpoint1 = await projection1.get_checkpoint()
    print(f"  Checkpoint: {checkpoint1}")

    # Simulate restart (projection instance destroyed)
    print("\n  [Simulating restart...]")
    del projection1

    # Phase 2: Resume after restart
    print("\nPhase 2: After restart")
    projection2 = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    # Note: counts reset to 0 (in-memory state lost)
    print(f"  Initial counts: {projection2.total_created}, {projection2.total_completed}")

    # But checkpoint is preserved!
    checkpoint2 = await projection2.get_checkpoint()
    print(f"  Checkpoint preserved: {checkpoint2}")

    # In real scenario, we would query event store for events after checkpoint
    # For now, just process new events
    events_batch2 = [
        TaskCreated(aggregate_id=task_ids[2], title="Task C", aggregate_version=1),
        TaskCompleted(aggregate_id=task_ids[1], aggregate_version=2),
    ]

    for event in events_batch2:
        await projection2.handle(event)

    print(f"  After new events: {projection2.total_created}, {projection2.total_completed}")


async def lag_metrics_demo():
    """Demonstrate lag monitoring."""
    print("\n=== Lag Metrics Demo ===\n")

    checkpoint_repo = InMemoryCheckpointRepository()
    projection = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    # Process a few events
    for i in range(3):
        event = TaskCreated(
            aggregate_id=uuid4(),
            title=f"Task {i+1}",
            aggregate_version=1,
        )
        await projection.handle(event)

    # Get lag metrics
    metrics = await projection.get_lag_metrics()

    if metrics:
        print(f"Projection: {metrics['projection_name']}")
        print(f"Last event ID: {metrics['last_event_id']}")
        print(f"Events processed: {metrics['events_processed']}")
        print(f"Last processed at: {metrics['last_processed_at']}")
        print(f"Lag (seconds): {metrics['lag_seconds']}")
    else:
        print("No checkpoint data yet")


async def reset_and_rebuild_demo():
    """Demonstrate projection reset for rebuilding."""
    print("\n=== Reset and Rebuild Demo ===\n")

    checkpoint_repo = InMemoryCheckpointRepository()
    projection = TaskCounterProjection(checkpoint_repo=checkpoint_repo)

    # Build up some state
    events = [
        TaskCreated(aggregate_id=uuid4(), title=f"Task {i}", aggregate_version=1)
        for i in range(5)
    ]
    for event in events:
        await projection.handle(event)

    print("Before reset:")
    print(f"  Count: {projection.total_created}")
    checkpoint = await projection.get_checkpoint()
    print(f"  Checkpoint: {checkpoint}")

    # Reset for rebuild
    await projection.reset()

    print("\nAfter reset:")
    print(f"  Count: {projection.total_created}")
    checkpoint = await projection.get_checkpoint()
    print(f"  Checkpoint: {checkpoint}")

    # Now we can replay all events from the beginning
    print("\nRebuilding by replaying all events...")
    for event in events:
        await projection.handle(event)

    print("After rebuild:")
    print(f"  Count: {projection.total_created}")


async def main():
    await basic_checkpoint_demo()
    await resumable_processing_demo()
    await lag_metrics_demo()
    await reset_and_rebuild_demo()


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_10_checkpoints.py` and run it:

```bash
python tutorial_10_checkpoints.py
```

---

## Exercises

### Exercise 1: Resumable Order Revenue Projection

**Objective:** Create a projection that tracks order revenue and can resume after restarts.

**Time:** 15-20 minutes

**Requirements:**

1. Create a `CheckpointTrackingProjection` for order revenue
2. Track total revenue and order count
3. Process some events and record the checkpoint
4. Simulate a "restart" by creating a new projection instance
5. Verify the checkpoint was preserved
6. Continue processing new events

**Starter Code:**

```python
"""
Tutorial 10 - Exercise 1: Resumable Order Revenue Projection

Your task: Create a projection that tracks order revenue and
demonstrates checkpoint-based resumability.
"""
import asyncio
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    register_event,
    CheckpointTrackingProjection,
    InMemoryCheckpointRepository,
)


@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    total: float


class OrderRevenueProjection(CheckpointTrackingProjection):
    """
    Tracks total revenue from orders.

    TODO: Implement the following:
    1. Initialize total_revenue and order_count in __init__
    2. Implement subscribed_to() to return [OrderPlaced]
    3. Implement _process_event() to update revenue and count
    4. Implement _truncate_read_models() to reset counters
    """

    def __init__(self, checkpoint_repo=None):
        super().__init__(checkpoint_repo=checkpoint_repo)
        # TODO: Initialize counters

    def subscribed_to(self) -> list[type[DomainEvent]]:
        # TODO: Return event types
        pass

    async def _process_event(self, event: DomainEvent) -> None:
        # TODO: Update revenue and count
        pass

    async def _truncate_read_models(self) -> None:
        # TODO: Reset counters
        pass


async def main():
    print("=== Resumable Order Revenue Exercise ===\n")

    # Persistent checkpoint storage (simulated)
    checkpoint_repo = InMemoryCheckpointRepository()

    # All events (simulating event store)
    all_events = [
        OrderPlaced(aggregate_id=uuid4(), total=100.00, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=250.00, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=75.50, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=300.00, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=150.00, aggregate_version=1),
    ]

    # TODO: Step 1 - Create projection and process first 3 events
    # TODO: Step 2 - Print revenue and checkpoint
    # TODO: Step 3 - Delete projection (simulate restart)
    # TODO: Step 4 - Create new projection instance
    # TODO: Step 5 - Verify checkpoint preserved
    # TODO: Step 6 - Process remaining events


if __name__ == "__main__":
    asyncio.run(main())
```

**Hints:**

- Use `get_checkpoint()` to verify the checkpoint is preserved after restart
- Remember that in-memory state (counters) will be lost on restart
- The checkpoint repository persists across restarts

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 10 - Exercise 1 Solution: Resumable Order Revenue Projection
"""
import asyncio
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    register_event,
    CheckpointTrackingProjection,
    InMemoryCheckpointRepository,
)


@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    total: float


class OrderRevenueProjection(CheckpointTrackingProjection):
    """Tracks total revenue from orders."""

    def __init__(self, checkpoint_repo=None):
        super().__init__(checkpoint_repo=checkpoint_repo)
        self.total_revenue = 0.0
        self.order_count = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced]

    async def _process_event(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self.total_revenue += event.total
            self.order_count += 1

    async def _truncate_read_models(self) -> None:
        self.total_revenue = 0.0
        self.order_count = 0


async def main():
    print("=== Resumable Order Revenue Exercise ===\n")

    # Persistent checkpoint storage (simulated)
    checkpoint_repo = InMemoryCheckpointRepository()

    # All events (simulating event store)
    all_events = [
        OrderPlaced(aggregate_id=uuid4(), total=100.00, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=250.00, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=75.50, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=300.00, aggregate_version=1),
        OrderPlaced(aggregate_id=uuid4(), total=150.00, aggregate_version=1),
    ]

    # Step 1: Create projection and process first 3 events
    print("Step 1: Initial processing (first 3 events)")
    projection = OrderRevenueProjection(checkpoint_repo=checkpoint_repo)

    for event in all_events[:3]:
        await projection.handle(event)

    print(f"  Revenue: ${projection.total_revenue:.2f}")
    print(f"  Orders: {projection.order_count}")

    # Step 2: Record checkpoint position
    checkpoint = await projection.get_checkpoint()
    print(f"\nStep 2: Checkpoint recorded")
    print(f"  Last event ID: {checkpoint}")

    # Step 3: Simulate restart
    print(f"\nStep 3: Simulating restart (clearing in-memory state)")
    del projection

    # Step 4: Create new projection instance
    print(f"\nStep 4: Creating new projection instance")
    projection2 = OrderRevenueProjection(checkpoint_repo=checkpoint_repo)

    # Step 5: Verify checkpoint preserved
    resumed_checkpoint = await projection2.get_checkpoint()
    print(f"  Checkpoint preserved: {resumed_checkpoint}")
    print(f"  In-memory state reset: revenue=${projection2.total_revenue:.2f}")

    # Step 6: Process remaining events
    print(f"\nStep 5: Resuming from checkpoint (remaining events)")
    for event in all_events[3:]:
        await projection2.handle(event)

    print(f"  Revenue (new events only): ${projection2.total_revenue:.2f}")
    print(f"  Orders (new events only): {projection2.order_count}")

    # Note about production usage
    print(f"\nNote: In production, use database-backed projections")
    print(f"for persistent read model state between restarts.")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

The solution file is also available at: `docs/tutorials/exercises/solutions/10-1.py`

---

## Summary

In this tutorial, you learned:

- **Checkpoints** track projection progress for resumable event processing
- **CheckpointTrackingProjection** handles checkpoint updates automatically after each event
- **CheckpointRepository** persists checkpoints (in-memory for dev, PostgreSQL for production)
- **Lag metrics** enable health monitoring of projection processing
- **Reset** allows projection rebuilding from scratch when needed

---

## Phase 2 Complete!

Congratulations! You have completed **Phase 2: Core Patterns**. You now have a solid understanding of the essential patterns for building production event-sourced applications.

### Phase 2 Summary

| Tutorial | Topic | Key Takeaway |
|----------|-------|--------------|
| 6 | Projections | Read models optimized for queries using CQRS |
| 7 | Event Bus | Distributing events to multiple handlers |
| 8 | Testing | Given-When-Then patterns for event sourcing |
| 9 | Error Handling | Retry patterns and recovery strategies |
| 10 | Checkpoints | Resumable, monitorable projection processing |

### What You Can Build Now

With these patterns, you can build:

- **Reliable projections** that survive restarts
- **Monitored systems** with lag detection and health checks
- **Rebuildable read models** for bug fixes and schema changes
- **Production-ready** event processing pipelines

### Next: Production Infrastructure

Phase 3 covers the production infrastructure you need to deploy your event-sourced application. You will learn about:

- PostgreSQL for production event storage
- SQLite for development and testing
- Subscription management for catch-up processing
- Snapshotting for performance optimization
- Production deployment patterns

---

## Key Takeaways

!!! note "Remember"
    - Checkpoints make projections production-ready by enabling resumability
    - Always use checkpoint tracking in production environments
    - Monitor lag to detect processing issues before they become critical
    - Design handlers to be idempotent for safe replay during rebuilds

!!! tip "Best Practice"
    Store checkpoints in a durable store (PostgreSQL) that survives application restarts. Never rely on in-memory checkpoints in production.

!!! warning "Common Mistake"
    Do not forget to implement `_truncate_read_models()`. Without it, `reset()` will clear the checkpoint but leave stale data in your read model.

---

## Next Steps

Continue to [Tutorial 11: Using PostgreSQL Event Store](11-postgresql.md) to learn how to set up production-grade persistence for your events and checkpoints.

---

## Related Documentation

- [Projections API Reference](../api/projections.md) - Complete projections API
- [Tutorial 6: Projections](06-projections.md) - Projection fundamentals
- [Tutorial 13: Subscription Management](13-subscriptions.md) - Advanced event processing
