# Building Read Models with Projections

Projections consume domain events and build read-optimized views of your data. They are the "query" side in CQRS (Command Query Responsibility Segregation) architecture, transforming append-only event streams into queryable data structures.

## Overview

In event sourcing, the event store is optimized for writes (appending immutable events). However, querying the raw event stream for every read operation is inefficient. Projections solve this by:

1. **Subscribing** to domain events
2. **Transforming** events into read-optimized views
3. **Storing** results in queryable data structures (databases, caches, etc.)
4. **Tracking progress** via checkpoints for resumable processing

```
Event Store                    Projections                    Read Models
+---------------+             +------------------+            +----------------+
|               |   events    |                  |   writes   |                |
| OrderCreated  | ----------> | OrderProjection  | ---------> | orders table   |
| OrderShipped  |             |                  |            |                |
| OrderCancelled|             | StatsProjection  | ---------> | metrics table  |
|     ...       |             |                  |            |                |
+---------------+             +------------------+            +----------------+
```

---

## Projection Types

The eventsource library provides several projection base classes:

| Class | Use Case |
|-------|----------|
| `Projection` | Simple async projection (implement `handle()` and `reset()`) |
| `SyncProjection` | Synchronous projection for non-async contexts |
| `CheckpointTrackingProjection` | Adds checkpoint, retry, and DLQ support |
| `DeclarativeProjection` | Uses `@handles` decorator for cleaner routing |

For production use, **`DeclarativeProjection`** is recommended as it combines declarative event routing with checkpoint tracking and error handling.

---

## Quick Start: DeclarativeProjection

The simplest way to create a projection is using `DeclarativeProjection` with the `@handles` decorator:

```python
from uuid import UUID
from eventsource import DomainEvent, register_event
from eventsource.projections import DeclarativeProjection, handles
from eventsource.repositories import InMemoryCheckpointRepository, InMemoryDLQRepository


# Define your events
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    total_amount: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


# Create a projection
class OrderSummaryProjection(DeclarativeProjection):
    """Builds a summary view of orders."""

    def __init__(self):
        super().__init__(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
        )
        # In-memory read model (use a database in production)
        self.orders: dict[UUID, dict] = {}

    @handles(OrderCreated)
    async def _on_order_created(self, event: OrderCreated) -> None:
        """Create order entry in read model."""
        self.orders[event.aggregate_id] = {
            "order_id": event.aggregate_id,
            "customer_id": event.customer_id,
            "total_amount": event.total_amount,
            "status": "created",
            "created_at": event.occurred_at,
        }

    @handles(OrderShipped)
    async def _on_order_shipped(self, event: OrderShipped) -> None:
        """Update order status to shipped."""
        if event.aggregate_id in self.orders:
            self.orders[event.aggregate_id].update({
                "status": "shipped",
                "tracking_number": event.tracking_number,
                "shipped_at": event.occurred_at,
            })

    async def _truncate_read_models(self) -> None:
        """Clear all data for rebuild."""
        self.orders.clear()

    # Query methods for the read model
    def get_order(self, order_id: UUID) -> dict | None:
        return self.orders.get(order_id)

    def get_orders_by_status(self, status: str) -> list[dict]:
        return [o for o in self.orders.values() if o["status"] == status]
```

### Using the Projection

```python
import asyncio

async def main():
    projection = OrderSummaryProjection()

    # Process events (typically from event bus subscription)
    event1 = OrderCreated(
        aggregate_id=uuid4(),
        customer_id=uuid4(),
        total_amount=99.99,
        aggregate_version=1,
    )
    await projection.handle(event1)

    # Query the read model
    order = projection.get_order(event1.aggregate_id)
    print(f"Order status: {order['status']}")  # Output: Order status: created

asyncio.run(main())
```

---

## The @handles Decorator

The `@handles` decorator marks methods as event handlers for specific event types. This creates a clean, declarative mapping between events and handler methods.

### Basic Usage

```python
from eventsource.projections import handles

@handles(OrderCreated)
async def _handle_order_created(self, event: OrderCreated) -> None:
    # Handler code here
    pass
```

### Handler Signature Requirements

Handlers **must be async functions** and accept either 1 or 2 parameters:

```python
# Single parameter: event only
@handles(OrderCreated)
async def _handle_created(self, event: OrderCreated) -> None:
    await self._save_order(event)

# Two parameters: connection and event
@handles(OrderCreated)
async def _handle_created(self, conn, event: OrderCreated) -> None:
    # conn is passed by _process_event; None by default
    await conn.execute("INSERT INTO orders ...")
```

### Automatic Subscription Discovery

`DeclarativeProjection` automatically generates `subscribed_to()` from the `@handles` decorators:

```python
projection = OrderSummaryProjection()

# Auto-generated from @handles decorators
print(projection.subscribed_to())
# Output: [<class 'OrderCreated'>, <class 'OrderShipped'>]
```

### Handler Validation

At initialization, `DeclarativeProjection` validates handlers:

- **Non-async handlers raise ValueError:**
  ```python
  @handles(OrderCreated)
  def _handle_created(self, event):  # Missing 'async'
      pass
  # Raises: ValueError: Handler _handle_created must be async
  ```

- **Wrong parameter count raises ValueError:**
  ```python
  @handles(OrderCreated)
  async def _handle_created(self, a, b, c):  # Too many params
      pass
  # Raises: ValueError: Handler must accept 1 or 2 parameters
  ```

---

## Checkpoint Tracking

Checkpoints track which events have been processed, enabling:

- **Resumable processing**: After restart, continue from last checkpoint
- **Exactly-once semantics**: Avoid reprocessing events
- **Lag monitoring**: Measure how far behind a projection is

### How Checkpoints Work

1. Event arrives at projection
2. Handler processes the event
3. On success, checkpoint is updated with event ID
4. On failure, retry logic kicks in (see [Error Handling](#error-handling))

```python
# After processing events, check the checkpoint
checkpoint = await projection.get_checkpoint()
print(f"Last processed event: {checkpoint}")
# Output: Last processed event: 550e8400-e29b-41d4-a716-446655440000
```

### Checkpoint Repositories

Two implementations are provided:

| Implementation | Use Case |
|----------------|----------|
| `InMemoryCheckpointRepository` | Testing, development |
| `PostgreSQLCheckpointRepository` | Production |

```python
from eventsource.repositories import (
    InMemoryCheckpointRepository,
    PostgreSQLCheckpointRepository,
)

# For testing
checkpoint_repo = InMemoryCheckpointRepository()

# For production
checkpoint_repo = PostgreSQLCheckpointRepository(engine)
```

### Lag Metrics

Monitor projection health with lag metrics:

```python
metrics = await projection.get_lag_metrics()
if metrics:
    print(f"Projection: {metrics['projection_name']}")
    print(f"Last event ID: {metrics['last_event_id']}")
    print(f"Events processed: {metrics['events_processed']}")
    print(f"Lag (seconds): {metrics['lag_seconds']}")
    print(f"Last processed at: {metrics['last_processed_at']}")
```

---

## Error Handling

Projections use a **retry-with-dead-letter-queue (DLQ)** strategy for robust error handling. For detailed rationale, see [ADR-0004: Projection Error Handling](../adrs/0004-projection-error-handling.md).

### Retry Behavior

When event processing fails:

1. **First failure**: Log error, wait 1 second, retry
2. **Second failure**: Log error, wait 2 seconds, retry
3. **Third failure**: Log error, send to DLQ, re-raise exception

```
Event arrives
    |
    v
Process (attempt 1) --[success]--> Update checkpoint --> Done
    |
    +--[failure]--> Wait 1s --> Process (attempt 2) --[success]--> Done
                                     |
                                     +--[failure]--> Wait 2s --> Process (attempt 3)
                                                                      |
                                                                      +--[success]--> Done
                                                                      |
                                                                      +--[failure]--> DLQ --> Re-raise
```

### Configuring Retry Behavior

Override class attributes to customize retry behavior:

```python
class HighReliabilityProjection(DeclarativeProjection):
    # Increase retries for transient failures
    MAX_RETRIES = 5
    RETRY_BACKOFF_BASE = 3  # Backoff: 3s, 9s, 27s, 81s

    @handles(OrderCreated)
    async def _handle_created(self, event: OrderCreated) -> None:
        await self._call_external_service(event)  # May have transient failures
```

### Dead Letter Queue (DLQ)

Events that fail all retries are preserved in the DLQ with full context:

```python
from eventsource.repositories import InMemoryDLQRepository

# Access DLQ through the projection
dlq_repo = projection._dlq_repo

# Get failed events
failed_events = await dlq_repo.get_failed_events(
    projection_name="OrderSummaryProjection",
    status="failed",
    limit=100,
)

for entry in failed_events:
    print(f"Event ID: {entry['event_id']}")
    print(f"Event type: {entry['event_type']}")
    print(f"Error: {entry['error_message']}")
    print(f"Retry count: {entry['retry_count']}")
    print(f"First failed: {entry['first_failed_at']}")
    print(f"---")

# Mark as resolved after manual intervention
await dlq_repo.mark_resolved(dlq_id=entry['id'], resolved_by="admin")

# Get DLQ statistics
stats = await dlq_repo.get_failure_stats()
print(f"Total failed: {stats['total_failed']}")
print(f"Affected projections: {stats['affected_projections']}")
```

### Best Practice: Let Errors Propagate

Don't catch exceptions in handlers; let them propagate to trigger retry/DLQ:

```python
# GOOD: Let CheckpointTrackingProjection handle errors
@handles(OrderCreated)
async def _handle_created(self, event: OrderCreated) -> None:
    await self._db.execute(...)  # May raise - that's OK

# BAD: Swallowing errors prevents retry/DLQ
@handles(OrderCreated)
async def _handle_created(self, event: OrderCreated) -> None:
    try:
        await self._db.execute(...)
    except Exception:
        pass  # Error is lost, checkpoint still updated!
```

---

## Rebuilding Projections

Projections can be rebuilt from scratch by replaying all events. This is useful when:

- Bug fixes require reprocessing events
- Adding new fields to read models
- Recovering from data corruption

### Basic Rebuild

```python
async def rebuild_projection(projection, event_store):
    """Rebuild a projection from scratch."""
    # Step 1: Reset checkpoint and clear read model
    await projection.reset()

    # Step 2: Replay all relevant events
    async for stored_event in event_store.read_all():
        event = stored_event.event
        # Only process events this projection handles
        if type(event) in projection.subscribed_to():
            await projection.handle(event)

    print(f"Rebuild complete. Checkpoint: {await projection.get_checkpoint()}")
```

### Using ProjectionCoordinator for Rebuilds

The `ProjectionCoordinator` provides rebuild utilities:

```python
from eventsource.projections import ProjectionCoordinator, ProjectionRegistry

# Create registry and coordinator
registry = ProjectionRegistry()
registry.register_projection(order_summary_projection)
registry.register_projection(customer_stats_projection)

coordinator = ProjectionCoordinator(registry=registry)

# Rebuild all projections
events = [e async for e in event_store.read_all()]
await coordinator.rebuild_all(events=[e.event for e in events])

# Rebuild single projection
await coordinator.rebuild_projection(
    projection=order_summary_projection,
    events=[e.event for e in events if type(e.event) in order_summary_projection.subscribed_to()],
)
```

### Production Rebuild Considerations

For large event stores, consider:

1. **Batching**: Process events in batches to avoid memory issues
2. **Progress logging**: Log progress for long-running rebuilds
3. **Downtime**: Consider read model availability during rebuilds

```python
async def rebuild_with_progress(projection, event_store, batch_size=1000):
    """Rebuild with batching and progress logging."""
    await projection.reset()

    count = 0
    async for stored_event in event_store.read_all():
        if type(stored_event.event) in projection.subscribed_to():
            await projection.handle(stored_event.event)
            count += 1

            if count % batch_size == 0:
                print(f"Processed {count} events...")

    print(f"Rebuild complete: {count} events processed")
```

---

## Projection with Database Persistence

For production use, projections typically write to a database. Here's a complete example:

```python
from uuid import UUID
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from eventsource import DomainEvent, register_event
from eventsource.projections import DeclarativeProjection, handles
from eventsource.repositories import PostgreSQLCheckpointRepository, PostgreSQLDLQRepository


@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    customer_name: str
    total_amount: float


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


class OrderDashboardProjection(DeclarativeProjection):
    """
    Production projection that writes to PostgreSQL.

    Read model tables:
    - orders: Current order state
    - daily_revenue: Aggregated daily metrics
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory
        super().__init__(
            checkpoint_repo=PostgreSQLCheckpointRepository(session_factory),
            dlq_repo=PostgreSQLDLQRepository(session_factory),
        )

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        async with self._session_factory() as session:
            # Insert order record (idempotent with ON CONFLICT)
            await session.execute(
                text("""
                    INSERT INTO orders (
                        id, customer_id, customer_name, total_amount,
                        status, created_at
                    ) VALUES (
                        :id, :customer_id, :customer_name, :total_amount,
                        'created', :created_at
                    )
                    ON CONFLICT (id) DO NOTHING
                """),
                {
                    "id": event.aggregate_id,
                    "customer_id": event.customer_id,
                    "customer_name": event.customer_name,
                    "total_amount": event.total_amount,
                    "created_at": event.occurred_at,
                }
            )

            # Update daily revenue metrics
            await session.execute(
                text("""
                    INSERT INTO daily_revenue (date, order_count, total_revenue)
                    VALUES (DATE(:date), 1, :amount)
                    ON CONFLICT (date) DO UPDATE SET
                        order_count = daily_revenue.order_count + 1,
                        total_revenue = daily_revenue.total_revenue + :amount
                """),
                {"date": event.occurred_at, "amount": event.total_amount}
            )

            await session.commit()

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        async with self._session_factory() as session:
            await session.execute(
                text("""
                    UPDATE orders
                    SET status = 'shipped',
                        tracking_number = :tracking,
                        shipped_at = :shipped_at
                    WHERE id = :id
                """),
                {
                    "id": event.aggregate_id,
                    "tracking": event.tracking_number,
                    "shipped_at": event.occurred_at,
                }
            )
            await session.commit()

    @handles(OrderCancelled)
    async def _handle_order_cancelled(self, event: OrderCancelled) -> None:
        async with self._session_factory() as session:
            # Update order status
            await session.execute(
                text("""
                    UPDATE orders
                    SET status = 'cancelled',
                        cancellation_reason = :reason,
                        cancelled_at = :cancelled_at
                    WHERE id = :id
                """),
                {
                    "id": event.aggregate_id,
                    "reason": event.reason,
                    "cancelled_at": event.occurred_at,
                }
            )

            # Subtract from daily revenue (for the original order date)
            # Note: This is simplified; real implementation would track order date
            await session.commit()

    async def _truncate_read_models(self) -> None:
        """Clear all read model tables for rebuild."""
        async with self._session_factory() as session:
            await session.execute(text("TRUNCATE TABLE orders, daily_revenue"))
            await session.commit()


# Usage
async def setup_projection():
    engine = create_async_engine("postgresql+asyncpg://localhost/mydb")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    projection = OrderDashboardProjection(session_factory)
    return projection
```

---

## Integrating with Event Bus

Projections typically subscribe to an event bus for real-time updates:

```python
from eventsource import InMemoryEventBus, InMemoryEventStore, AggregateRepository

async def setup_system():
    # Infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()

    # Projections
    order_summary = OrderSummaryProjection()
    customer_stats = CustomerStatsProjection()

    # Subscribe projections to event bus
    # subscribe_all sends all events to the projection
    event_bus.subscribe_all(order_summary)
    event_bus.subscribe_all(customer_stats)

    # Repository publishes events to bus after saving
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,  # Events go here after save
    )

    return repo, order_summary, customer_stats


async def demo():
    repo, order_summary, customer_stats = await setup_system()

    # Create an order - events automatically flow to projections
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(customer_id=uuid4(), total_amount=150.00)
    await repo.save(order)  # Publishes OrderCreated to event bus

    # Projections are now updated
    print(f"Orders in projection: {len(order_summary.orders)}")
```

---

## Best Practices Summary

### Design Guidelines

1. **Make handlers idempotent**: Use UPSERT patterns (`ON CONFLICT DO ...`)
2. **Let errors propagate**: Don't catch exceptions; let retry/DLQ handle them
3. **Keep projections focused**: One projection per read model / query pattern
4. **Use transactions**: Update read models atomically

### Performance Tips

1. **Batch operations**: For bulk updates, accumulate and batch writes
2. **Index read models**: Add appropriate indexes for query patterns
3. **Monitor lag**: Set up alerts for projection lag metrics

### Operational Procedures

1. **Monitor DLQ**: Check for failed events regularly
2. **Document rebuild process**: Know how to rebuild each projection
3. **Test rebuilds**: Verify rebuild procedures in staging

---

## Complete Example

See the full working example in `examples/projection_example.py`, which demonstrates:

- Multiple projections (`OrderListProjection`, `CustomerStatsProjection`, `DailyRevenueProjection`)
- Event bus integration
- Checkpoint tracking
- Projection rebuilds

Run it with:
```bash
python -m examples.projection_example
```

---

## See Also

- [Projections API Reference](../api/projections.md) - Complete API documentation
- [ADR-0004: Projection Error Handling](../adrs/0004-projection-error-handling.md) - Error handling design rationale
- [ADR-0001: Async-First Design](../adrs/0001-async-first-design.md) - Why projections are async
- [Testing Patterns](./testing.md) - Testing projections
