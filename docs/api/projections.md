# Projections API Reference

This document covers the projection system for building read models from event streams.

## Overview

Projections consume domain events and build read models optimized for specific query patterns. They provide the query side in CQRS architecture.

```python
from eventsource.projections import (
    # Base classes
    Projection,
    SyncProjection,
    EventHandlerBase,
    CheckpointTrackingProjection,
    DeclarativeProjection,

    # Decorators
    handles,

    # Coordinator
    ProjectionCoordinator,
)
```

---

## Projection Base Classes

### Projection

Simple abstract base class for async projections.

```python
from abc import ABC, abstractmethod
from eventsource.events.base import DomainEvent

class Projection(ABC):
    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """Handle a domain event."""
        pass

    @abstractmethod
    async def reset(self) -> None:
        """Reset the projection (clear all read model data)."""
        pass
```

### SyncProjection

Synchronous version for non-async contexts:

```python
class SyncProjection(ABC):
    @abstractmethod
    def handle(self, event: DomainEvent) -> None:
        pass

    @abstractmethod
    def reset(self) -> None:
        pass
```

---

## CheckpointTrackingProjection

Base class with automatic checkpoint tracking, retry logic, and dead letter queue support.

### Features

- Automatic checkpoint management
- Idempotent event processing
- Retry with exponential backoff
- Dead letter queue for permanent failures
- Lag monitoring support

### Configuration

```python
class MyProjection(CheckpointTrackingProjection):
    # Retry configuration (class attributes)
    MAX_RETRIES: int = 3           # Number of retry attempts
    RETRY_BACKOFF_BASE: int = 2    # Base for exponential backoff (seconds)
```

### Constructor

```python
from eventsource.repositories import (
    CheckpointRepository,
    DLQRepository,
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
)

projection = MyProjection(
    checkpoint_repo=PostgreSQLCheckpointRepository(session_factory),
    dlq_repo=PostgreSQLDLQRepository(session_factory),
)

# Or with defaults (in-memory)
projection = MyProjection()
```

### Implementation

```python
from eventsource.projections import CheckpointTrackingProjection
from eventsource.events.base import DomainEvent

class OrderSummaryProjection(CheckpointTrackingProjection):
    """Projection that builds order summaries."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return event types this projection handles."""
        return [OrderCreated, OrderShipped, OrderCancelled]

    async def _process_event(self, event: DomainEvent) -> None:
        """Process event and update read model."""
        if isinstance(event, OrderCreated):
            await self._create_summary(event)
        elif isinstance(event, OrderShipped):
            await self._update_status(event, "shipped")
        elif isinstance(event, OrderCancelled):
            await self._update_status(event, "cancelled")

    async def _truncate_read_models(self) -> None:
        """Clear read model tables for reset."""
        # Implement table truncation
        pass

    async def _create_summary(self, event: OrderCreated) -> None:
        # Insert into read model
        pass

    async def _update_status(self, event: DomainEvent, status: str) -> None:
        # Update read model
        pass
```

### Methods

| Method | Description |
|--------|-------------|
| `handle(event)` | Handle event with retry logic and DLQ fallback |
| `subscribed_to()` | Return list of event types to handle (abstract) |
| `_process_event(event)` | Actual projection logic (abstract) |
| `_truncate_read_models()` | Table truncation for reset (override) |
| `get_checkpoint()` | Get last processed event ID |
| `get_lag_metrics()` | Get projection lag information |
| `reset()` | Clear checkpoint and read model data |

### Lag Metrics

```python
metrics = await projection.get_lag_metrics()
if metrics:
    print(f"Projection: {metrics['projection_name']}")
    print(f"Lag: {metrics['lag_seconds']} seconds")
    print(f"Events processed: {metrics['events_processed']}")
```

---

## DeclarativeProjection

Projection using `@handles` decorator for cleaner event routing.

### Usage

```python
from eventsource.projections import DeclarativeProjection, handles

class OrderProjection(DeclarativeProjection):
    """Declarative order projection with @handles decorators."""

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        """Handle OrderCreated event."""
        await self._db.execute(
            "INSERT INTO orders (id, customer_id, status) VALUES ($1, $2, $3)",
            event.aggregate_id,
            event.customer_id,
            "created",
        )

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        """Handle OrderShipped event."""
        await self._db.execute(
            "UPDATE orders SET status = $1 WHERE id = $2",
            "shipped",
            event.aggregate_id,
        )

    @handles(OrderCancelled)
    async def _handle_order_cancelled(self, event: OrderCancelled) -> None:
        """Handle OrderCancelled event."""
        await self._db.execute(
            "UPDATE orders SET status = $1 WHERE id = $2",
            "cancelled",
            event.aggregate_id,
        )

    async def _truncate_read_models(self) -> None:
        """Clear read model tables."""
        await self._db.execute("TRUNCATE TABLE orders")
```

### Handler Signature

Handlers must be async and accept 1 or 2 parameters:

```python
# Single parameter (event only)
@handles(OrderCreated)
async def _handle_created(self, event: OrderCreated) -> None:
    pass

# Two parameters (connection + event)
@handles(OrderCreated)
async def _handle_created(self, conn, event: OrderCreated) -> None:
    # conn is passed as None by default; override _process_event for custom behavior
    pass
```

### Benefits

- Auto-generates `subscribed_to()` from decorators
- No large if/elif chains
- Handler methods are self-documenting
- Type annotations validated at initialization

---

## Checkpoint and DLQ Repositories

### CheckpointRepository

Tracks projection progress:

```python
from eventsource.repositories import (
    CheckpointRepository,
    PostgreSQLCheckpointRepository,
    InMemoryCheckpointRepository,
    CheckpointData,
    LagMetrics,
)

# PostgreSQL implementation
checkpoint_repo = PostgreSQLCheckpointRepository(session_factory)

# Update checkpoint
await checkpoint_repo.update_checkpoint(
    projection_name="OrderSummaryProjection",
    event_id=event.event_id,
    event_type=event.event_type,
)

# Get checkpoint
event_id = await checkpoint_repo.get_checkpoint("OrderSummaryProjection")

# Get detailed checkpoint data
data: CheckpointData = await checkpoint_repo.get_checkpoint_data("OrderSummaryProjection")

# Get lag metrics
metrics: LagMetrics = await checkpoint_repo.get_lag_metrics(
    "OrderSummaryProjection",
    event_types=["OrderCreated", "OrderShipped"],
)
```

### DLQRepository

Manages failed events:

```python
from eventsource.repositories import (
    DLQRepository,
    PostgreSQLDLQRepository,
    InMemoryDLQRepository,
    DLQEntry,
    DLQStats,
)

dlq_repo = PostgreSQLDLQRepository(session_factory)

# Add failed event
await dlq_repo.add_failed_event(
    event_id=event.event_id,
    projection_name="OrderSummaryProjection",
    event_type=event.event_type,
    event_data=event.model_dump(mode="json"),
    error=error,
    retry_count=3,
)

# Get failed events
entries = await dlq_repo.get_failed_events(
    projection_name="OrderSummaryProjection",
    limit=10,
)

# Mark as resolved
await dlq_repo.mark_resolved(entry_id)

# Get stats
stats: DLQStats = await dlq_repo.get_stats("OrderSummaryProjection")
print(f"Pending: {stats.pending_count}")
print(f"Resolved: {stats.resolved_count}")
```

---

## ProjectionCoordinator

Coordinates multiple projections (optional utility class).

```python
from eventsource.projections import ProjectionCoordinator

coordinator = ProjectionCoordinator()

# Register projections
coordinator.register(order_summary_projection)
coordinator.register(customer_stats_projection)

# Process event to all projections
await coordinator.handle(event)

# Reset all projections
await coordinator.reset_all()

# Get all lag metrics
metrics = await coordinator.get_all_metrics()
```

---

## Complete Example

```python
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from eventsource.projections import DeclarativeProjection, handles
from eventsource.repositories import (
    PostgreSQLCheckpointRepository,
    PostgreSQLDLQRepository,
)

# Events
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    total: float

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


class OrderDashboardProjection(DeclarativeProjection):
    """
    Builds a dashboard read model for orders.

    Maintains:
    - orders table: current order state
    - order_metrics table: aggregated statistics
    """

    def __init__(
        self,
        session_factory,
        checkpoint_repo=None,
        dlq_repo=None,
    ):
        self._session_factory = session_factory
        super().__init__(
            checkpoint_repo=checkpoint_repo or PostgreSQLCheckpointRepository(session_factory),
            dlq_repo=dlq_repo or PostgreSQLDLQRepository(session_factory),
        )

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        async with self._session_factory() as session:
            await session.execute(
                text("""
                    INSERT INTO orders (id, customer_id, total, status, created_at)
                    VALUES (:id, :customer_id, :total, 'created', :created_at)
                """),
                {
                    "id": event.aggregate_id,
                    "customer_id": event.customer_id,
                    "total": event.total,
                    "created_at": event.occurred_at,
                }
            )
            await session.execute(
                text("""
                    INSERT INTO order_metrics (date, orders_count, total_revenue)
                    VALUES (DATE(:date), 1, :total)
                    ON CONFLICT (date)
                    DO UPDATE SET
                        orders_count = order_metrics.orders_count + 1,
                        total_revenue = order_metrics.total_revenue + :total
                """),
                {"date": event.occurred_at, "total": event.total}
            )
            await session.commit()

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        async with self._session_factory() as session:
            await session.execute(
                text("""
                    UPDATE orders
                    SET status = 'shipped', tracking_number = :tracking, shipped_at = :shipped_at
                    WHERE id = :id
                """),
                {
                    "id": event.aggregate_id,
                    "tracking": event.tracking_number,
                    "shipped_at": event.occurred_at,
                }
            )
            await session.commit()

    async def _truncate_read_models(self) -> None:
        async with self._session_factory() as session:
            await session.execute(text("TRUNCATE TABLE orders, order_metrics"))
            await session.commit()


# Usage
async def main():
    projection = OrderDashboardProjection(session_factory)

    # Process events
    async for stored_event in event_store.read_all():
        await projection.handle(stored_event.event)

    # Check progress
    metrics = await projection.get_lag_metrics()
    if metrics:
        print(f"Lag: {metrics['lag_seconds']} seconds")
```

---

## Best Practices

### Idempotency

Design handlers to be idempotent:

```python
@handles(OrderCreated)
async def _handle_order_created(self, event: OrderCreated) -> None:
    # Use UPSERT instead of INSERT
    await session.execute(
        text("""
            INSERT INTO orders (id, customer_id, status)
            VALUES (:id, :customer_id, 'created')
            ON CONFLICT (id) DO NOTHING
        """),
        {"id": event.aggregate_id, "customer_id": event.customer_id}
    )
```

### Error Handling

Let errors propagate to trigger DLQ:

```python
@handles(OrderCreated)
async def _handle_order_created(self, event: OrderCreated) -> None:
    # Don't catch exceptions - let CheckpointTrackingProjection handle retry/DLQ
    await self._create_order(event)  # May raise
```

### Transactional Consistency

Update read model atomically:

```python
@handles(OrderCreated)
async def _handle_order_created(self, event: OrderCreated) -> None:
    async with self._session_factory() as session:
        async with session.begin():  # Transaction
            await session.execute(...)  # Insert order
            await session.execute(...)  # Update metrics
        # Commits on context exit
```

### Projection Rebuild

Support full rebuilds:

```python
async def rebuild_projection():
    """Rebuild projection from scratch."""
    await projection.reset()  # Clear checkpoint + data

    async for stored_event in event_store.read_all():
        if type(stored_event.event) in projection.subscribed_to():
            await projection.handle(stored_event.event)
```
