# Building Read Models with Projections

Projections transform append-only event streams into queryable read models. They are the "query" side in CQRS (Command Query Responsibility Segregation) architecture.

```
Event Store                    Projections                    Read Models
+---------------+             +------------------+            +----------------+
|               |   events    |                  |   writes   |                |
| OrderCreated  | ----------> | OrderProjection  | ---------> | orders table   |
| OrderShipped  |             |                  |            |                |
| OrderCancelled|             | StatsProjection  | ---------> | metrics table  |
+---------------+             +------------------+            +----------------+
```

---

## Projection Class Hierarchy

| Class | Use Case |
|-------|----------|
| `Projection` | Simple async projection (implement `handle()` and `reset()`) |
| `SyncProjection` | Synchronous projection for non-async contexts |
| `CheckpointTrackingProjection` | Adds checkpoint, retry, and DLQ support |
| `DeclarativeProjection` | Uses `@handles` decorator (in-memory projections) |
| `DatabaseProjection` | `@handles` with database connection injection |

**Recommendation**: Use `DeclarativeProjection` for in-memory/simple projections, `DatabaseProjection` for database-backed read models.

---

## Quick Start: DeclarativeProjection

```python
from uuid import UUID, uuid4
from eventsource import DomainEvent, register_event
from eventsource.projections import DeclarativeProjection, handles


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


class OrderSummaryProjection(DeclarativeProjection):
    """In-memory projection for order summaries."""

    def __init__(self):
        super().__init__()  # Uses in-memory checkpoint/DLQ by default
        self.orders: dict[UUID, dict] = {}

    @handles(OrderCreated)
    async def _on_created(self, event: OrderCreated) -> None:
        self.orders[event.aggregate_id] = {
            "order_id": event.aggregate_id,
            "customer_id": event.customer_id,
            "total_amount": event.total_amount,
            "status": "created",
        }

    @handles(OrderShipped)
    async def _on_shipped(self, event: OrderShipped) -> None:
        if event.aggregate_id in self.orders:
            self.orders[event.aggregate_id]["status"] = "shipped"
            self.orders[event.aggregate_id]["tracking"] = event.tracking_number

    async def _truncate_read_models(self) -> None:
        self.orders.clear()

    # Query methods
    def get_order(self, order_id: UUID) -> dict | None:
        return self.orders.get(order_id)


# Usage
async def main():
    projection = OrderSummaryProjection()

    event = OrderCreated(
        aggregate_id=uuid4(),
        customer_id=uuid4(),
        total_amount=99.99,
        aggregate_version=1,
    )
    await projection.handle(event)

    order = projection.get_order(event.aggregate_id)
    print(f"Status: {order['status']}")  # Output: Status: created
```

---

## The @handles Decorator

The `@handles` decorator marks methods as event handlers for specific event types.

```python
from eventsource.projections import handles

# DeclarativeProjection: single parameter (event only)
@handles(OrderCreated)
async def _handle_created(self, event: OrderCreated) -> None:
    await self._save_order(event)

# DatabaseProjection: two parameters (connection + event)
@handles(OrderCreated)
async def _handle_created(self, conn, event: OrderCreated) -> None:
    await conn.execute(text("INSERT INTO orders ..."))
```

**Key behaviors:**
- Auto-generates `subscribed_to()` from decorated methods
- Validates handlers at initialization (must be async, correct param count)
- Works with both `DeclarativeProjection` and `DatabaseProjection`

---

## Checkpoint Tracking

Checkpoints track processed events, enabling resumable processing and lag monitoring.

```python
# Get last processed event
checkpoint = await projection.get_checkpoint()
print(f"Last processed: {checkpoint}")

# Get lag metrics
metrics = await projection.get_lag_metrics()
if metrics:
    print(f"Lag: {metrics['lag_seconds']} seconds")
    print(f"Events processed: {metrics['events_processed']}")
```

**Repository implementations:**
- `InMemoryCheckpointRepository` - Testing/development
- `PostgreSQLCheckpointRepository` - Production

---

## Error Handling

Projections use a **retry-with-dead-letter-queue (DLQ)** strategy. See [ADR-0004](../adrs/0004-projection-error-handling.md) for design rationale.

```
Event --> Process --> [success] --> Checkpoint --> Done
              |
              +--[failure]--> Backoff --> Retry (up to max_retries)
                                              |
                                              +--[exhausted]--> DLQ --> Re-raise
```

### Configuring Retry Behavior

```python
from eventsource.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.subscriptions.retry import RetryConfig

# Custom retry policy
policy = ExponentialBackoffRetryPolicy(
    config=RetryConfig(max_retries=5, initial_delay=1.0)
)

projection = MyProjection(retry_policy=policy)
```

### Dead Letter Queue

Failed events are preserved with full context for debugging:

```python
# Get failed events
failed = await projection._dlq_repo.get_failed_events(
    projection_name="OrderSummaryProjection",
    limit=100,
)

for entry in failed:
    print(f"Event: {entry.event_id}, Error: {entry.error_message}")

# Mark as resolved after manual fix
await projection._dlq_repo.mark_resolved(dlq_id=entry.id, resolved_by="admin")
```

### Best Practice: Let Errors Propagate

```python
# GOOD: Let retry/DLQ handle errors
@handles(OrderCreated)
async def _handle_created(self, event: OrderCreated) -> None:
    await self._db.execute(...)  # May raise - that's OK

# BAD: Swallowing errors prevents retry/DLQ
@handles(OrderCreated)
async def _handle_created(self, event: OrderCreated) -> None:
    try:
        await self._db.execute(...)
    except Exception:
        pass  # Error is lost!
```

---

## Rebuilding Projections

Projections can be rebuilt by replaying events (useful for bug fixes, schema changes, or recovery).

```python
async def rebuild_projection(projection, event_store):
    """Rebuild a projection from scratch."""
    await projection.reset()  # Clear checkpoint + read model

    async for stored_event in event_store.read_all():
        if type(stored_event.event) in projection.subscribed_to():
            await projection.handle(stored_event.event)

    print(f"Rebuild complete: {await projection.get_checkpoint()}")
```

### Using ProjectionCoordinator

```python
from eventsource.projections import ProjectionCoordinator, ProjectionRegistry

registry = ProjectionRegistry()
registry.register_projection(order_projection)

coordinator = ProjectionCoordinator(registry=registry)

# Rebuild all projections
await coordinator.rebuild_all(all_events)

# Rebuild single projection
await coordinator.rebuild_projection(order_projection, relevant_events)

# Catch up without reset
await coordinator.catchup(order_projection, missed_events)
```

---

## DatabaseProjection (Production Pattern)

For production database-backed projections, use `DatabaseProjection` which provides automatic transaction handling:

```python
from uuid import UUID
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from eventsource import DomainEvent, register_event
from eventsource.projections import DatabaseProjection, handles
from eventsource.repositories import PostgreSQLCheckpointRepository, PostgreSQLDLQRepository


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


class OrderDashboardProjection(DatabaseProjection):
    """
    Production projection with automatic transaction handling.

    Handler operations share the same database transaction,
    committed on success, rolled back on error.
    """

    @handles(OrderCreated)
    async def _handle_order_created(self, conn, event: OrderCreated) -> None:
        # Insert order (idempotent with ON CONFLICT)
        await conn.execute(
            text("""
                INSERT INTO orders (id, customer_id, total_amount, status, created_at)
                VALUES (:id, :customer_id, :total_amount, 'created', :created_at)
                ON CONFLICT (id) DO NOTHING
            """),
            {
                "id": str(event.aggregate_id),
                "customer_id": str(event.customer_id),
                "total_amount": event.total_amount,
                "created_at": event.occurred_at,
            }
        )

        # Update daily metrics (same transaction)
        await conn.execute(
            text("""
                INSERT INTO daily_revenue (date, order_count, total_revenue)
                VALUES (DATE(:date), 1, :amount)
                ON CONFLICT (date) DO UPDATE SET
                    order_count = daily_revenue.order_count + 1,
                    total_revenue = daily_revenue.total_revenue + :amount
            """),
            {"date": event.occurred_at, "amount": event.total_amount}
        )

    @handles(OrderShipped)
    async def _handle_order_shipped(self, conn, event: OrderShipped) -> None:
        await conn.execute(
            text("""
                UPDATE orders SET status = 'shipped', tracking_number = :tracking
                WHERE id = :id
            """),
            {"id": str(event.aggregate_id), "tracking": event.tracking_number}
        )


# Usage
engine = create_async_engine("postgresql+asyncpg://localhost/mydb")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

projection = OrderDashboardProjection(
    session_factory=session_factory,
    checkpoint_repo=PostgreSQLCheckpointRepository(session_factory),
    dlq_repo=PostgreSQLDLQRepository(session_factory),
)
```

---

## Integrating with SubscriptionManager

For production use, `SubscriptionManager` coordinates catch-up (historical) and live (real-time) event delivery:

```python
from eventsource import InMemoryEventBus, InMemoryEventStore, InMemoryCheckpointRepository
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

# Setup
event_store = InMemoryEventStore()
event_bus = InMemoryEventBus()
checkpoint_repo = InMemoryCheckpointRepository()

manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
)

# Subscribe projections
config = SubscriptionConfig(start_from="beginning", batch_size=100)
await manager.subscribe(order_projection, config=config, name="Orders")
await manager.subscribe(stats_projection, config=config, name="Stats")

# Start (catch-up then live)
await manager.start()

# ... process events ...

# Graceful shutdown
await manager.stop()
```

**Why SubscriptionManager?**

| Feature | Direct EventBus | SubscriptionManager |
|---------|-----------------|---------------------|
| Live events | Yes | Yes |
| Historical catch-up | No | Yes |
| Checkpoint tracking | No | Yes |
| Resumable after restart | No | Yes |

---

## Best Practices

1. **Idempotent handlers**: Use `ON CONFLICT DO NOTHING/UPDATE` patterns
2. **Let errors propagate**: Don't catch exceptions; retry/DLQ handles them
3. **One projection per read model**: Keep projections focused
4. **Use transactions**: `DatabaseProjection` handles this automatically
5. **Monitor DLQ**: Check for failed events regularly
6. **Test rebuilds**: Verify rebuild procedures work correctly

---

## Example Code

See `examples/projection_example.py` for a complete working example:

```bash
python -m examples.projection_example
```

---

## See Also

- [Projections API Reference](../api/projections.md) - Complete API documentation
- [ADR-0004: Projection Error Handling](../adrs/0004-projection-error-handling.md) - Error handling rationale
