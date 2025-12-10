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
    DatabaseProjection,

    # Decorators
    handles,
    get_handled_event_type,
    is_event_handler,

    # Coordinators and registries
    ProjectionCoordinator,
    ProjectionRegistry,
    SubscriberRegistry,

    # Protocols
    EventHandler,
    SyncEventHandler,
    EventSubscriber,
    AsyncEventHandler,
)

# Retry policies (for custom retry configuration)
from eventsource.projections.retry import (
    RetryPolicy,
    ExponentialBackoffRetryPolicy,
    NoRetryPolicy,
    FilteredRetryPolicy,
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
- Configurable retry policies with exponential backoff
- Dead letter queue for permanent failures
- Lag monitoring support
- Optional OpenTelemetry tracing (disabled by default)

### Constructor

```python
from eventsource.repositories import (
    PostgreSQLCheckpointRepository,
    PostgreSQLDLQRepository,
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
)
from eventsource.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.subscriptions.retry import RetryConfig

# Production usage with PostgreSQL repositories
projection = MyProjection(
    checkpoint_repo=PostgreSQLCheckpointRepository(session_factory),
    dlq_repo=PostgreSQLDLQRepository(session_factory),
    retry_policy=ExponentialBackoffRetryPolicy(),  # Optional: uses defaults
    enable_tracing=False,  # Set True for OpenTelemetry tracing
)

# Development/testing with in-memory defaults
projection = MyProjection()
```

### Configuration: Retry Policies (Recommended)

Use `retry_policy` parameter for configurable retry behavior:

```python
from eventsource.projections.retry import (
    ExponentialBackoffRetryPolicy,
    NoRetryPolicy,
    FilteredRetryPolicy,
)
from eventsource.subscriptions.retry import RetryConfig

# Custom retry configuration
policy = ExponentialBackoffRetryPolicy(
    config=RetryConfig(
        max_retries=5,        # Number of retry attempts (not including initial)
        initial_delay=1.0,    # First retry delay in seconds
        max_delay=60.0,       # Maximum delay cap
        exponential_base=2.0, # Backoff multiplier
        jitter=0.1,           # Random jitter factor (0.0 for deterministic)
    )
)

projection = MyProjection(retry_policy=policy)
```

### Configuration: Class Attributes (Deprecated)

Legacy configuration via class attributes still works for backward compatibility:

```python
class MyProjection(CheckpointTrackingProjection):
    # DEPRECATED: Use retry_policy parameter instead
    MAX_RETRIES: int = 3           # Total attempts (converted to max_retries-1)
    RETRY_BACKOFF_BASE: int = 2    # Initial delay in seconds
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

Projection using `@handles` decorator for cleaner event routing. Extends `CheckpointTrackingProjection` with automatic handler discovery.

### Usage

```python
from eventsource.projections import DeclarativeProjection, handles
from eventsource.repositories import InMemoryCheckpointRepository, InMemoryDLQRepository

class OrderProjection(DeclarativeProjection):
    """Declarative order projection with @handles decorators."""

    def __init__(self):
        super().__init__(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=InMemoryDLQRepository(),
        )
        self.orders: dict = {}  # In-memory read model

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        """Handle OrderCreated event."""
        self.orders[event.aggregate_id] = {
            "id": event.aggregate_id,
            "customer_id": event.customer_id,
            "status": "created",
        }

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        """Handle OrderShipped event."""
        if event.aggregate_id in self.orders:
            self.orders[event.aggregate_id]["status"] = "shipped"

    async def _truncate_read_models(self) -> None:
        """Clear read model for rebuild."""
        self.orders.clear()
```

### Handler Signature

Handlers must be async and accept 1 parameter (the event):

```python
@handles(OrderCreated)
async def _handle_created(self, event: OrderCreated) -> None:
    # Process event and update read model
    pass
```

For database operations, use `DatabaseProjection` which provides connection injection.

### Unregistered Event Handling

Control behavior when an event has no registered handler:

```python
class StrictProjection(DeclarativeProjection):
    # Options: "ignore" (default), "warn", "error"
    unregistered_event_handling = "error"

    @handles(OrderCreated)
    async def _handle_created(self, event: OrderCreated) -> None:
        pass

# With "ignore": Unhandled events are silently skipped
# With "warn": Logs a warning for unhandled events
# With "error": Raises UnhandledEventError for unhandled events
```

### Benefits

- Auto-generates `subscribed_to()` from `@handles` decorators
- No large if/elif chains for event routing
- Handler methods are self-documenting
- Handler signatures validated at initialization
- Supports configurable unregistered event handling

---

## DatabaseProjection

Projection with database connection support for handlers. Extends `DeclarativeProjection` to inject database connections into handler methods.

### Usage

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import text
from eventsource.projections import DatabaseProjection, handles
from eventsource.repositories import PostgreSQLCheckpointRepository, PostgreSQLDLQRepository

class OrderDatabaseProjection(DatabaseProjection):
    """Projection that writes to PostgreSQL."""

    @handles(OrderCreated)
    async def _handle_order_created(self, conn, event: OrderCreated) -> None:
        """Handle OrderCreated with database connection."""
        await conn.execute(
            text("""
                INSERT INTO orders (id, customer_id, status)
                VALUES (:id, :customer_id, 'created')
                ON CONFLICT (id) DO NOTHING
            """),
            {"id": str(event.aggregate_id), "customer_id": str(event.customer_id)}
        )

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        """Single-param handler (no database needed)."""
        print(f"Order shipped: {event.tracking_number}")

    async def _truncate_read_models(self) -> None:
        """Clear read model tables for rebuild."""
        # Note: Uses _current_connection set during handle()
        pass

# Usage
engine = create_async_engine("postgresql+asyncpg://localhost/mydb")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

projection = OrderDatabaseProjection(
    session_factory=session_factory,
    checkpoint_repo=PostgreSQLCheckpointRepository(session_factory),
    dlq_repo=PostgreSQLDLQRepository(session_factory),
)
```

### Handler Signatures

`DatabaseProjection` supports two handler signatures:

```python
# Two parameters: connection + event (for database operations)
@handles(OrderCreated)
async def _handle_created(self, conn, event: OrderCreated) -> None:
    await conn.execute(text("INSERT INTO ..."))

# Single parameter: event only (no database access needed)
@handles(OrderShipped)
async def _handle_shipped(self, event: OrderShipped) -> None:
    # No database operations
    await send_notification(event)
```

### Transaction Handling

`DatabaseProjection.handle()` wraps all operations in a database transaction:
- Handler SQL operations are transactional
- On success, transaction is committed
- On error, transaction is rolled back automatically

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

## Retry Policies

Configurable retry behavior for event processing failures.

### ExponentialBackoffRetryPolicy (Default)

```python
from eventsource.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.subscriptions.retry import RetryConfig

# Default settings (matches original behavior)
policy = ExponentialBackoffRetryPolicy()
# max_retries=3, initial_delay=2.0, exponential_base=2.0
# Backoff progression: 2s, 4s, 8s

# Custom configuration
policy = ExponentialBackoffRetryPolicy(
    config=RetryConfig(
        max_retries=5,
        initial_delay=1.0,
        max_delay=60.0,
        exponential_base=2.0,
        jitter=0.1,
    )
)
```

### NoRetryPolicy

For fail-fast scenarios or when retries are handled elsewhere:

```python
from eventsource.projections.retry import NoRetryPolicy

policy = NoRetryPolicy()
# Immediately sends to DLQ on first failure
```

### FilteredRetryPolicy

Only retry specific exception types:

```python
from eventsource.projections.retry import FilteredRetryPolicy, ExponentialBackoffRetryPolicy

# Only retry transient failures
policy = FilteredRetryPolicy(
    base_policy=ExponentialBackoffRetryPolicy(),
    retryable_exceptions=(ConnectionError, TimeoutError, OSError),
)
# ValueError, KeyError, etc. go directly to DLQ without retry
```

---

## ProjectionRegistry

Registry for managing multiple projections with concurrent dispatch.

```python
from eventsource.projections import ProjectionRegistry

registry = ProjectionRegistry(enable_tracing=False)

# Register projections
registry.register_projection(order_summary_projection)
registry.register_projection(customer_stats_projection)

# Register event handlers
registry.register_handler(notification_handler)

# Dispatch event to all projections concurrently
await registry.dispatch(event)

# Dispatch multiple events (sequential, maintains ordering)
await registry.dispatch_many(events)

# Reset all projections
await registry.reset_all()

# Query registered items
print(f"Projections: {registry.get_projection_count()}")
print(f"Handlers: {registry.get_handler_count()}")
```

---

## ProjectionCoordinator

Coordinates event distribution from event store to projections with batching and rebuild support.

```python
from eventsource.projections import ProjectionCoordinator, ProjectionRegistry

registry = ProjectionRegistry()
registry.register_projection(order_projection)
registry.register_projection(stats_projection)

coordinator = ProjectionCoordinator(
    registry=registry,
    batch_size=100,
    poll_interval_seconds=1.0,
    enable_tracing=False,
)

# Dispatch events
events_processed = await coordinator.dispatch_events(events)

# Rebuild all projections from scratch
await coordinator.rebuild_all(all_events)

# Rebuild single projection
await coordinator.rebuild_projection(order_projection, relevant_events)

# Catch up a lagging projection (without reset)
await coordinator.catchup(order_projection, missed_events)

# Health check
health = await coordinator.health_check()
print(f"Status: {health['status']}")
print(f"Projections: {health['projections']}")
```

---

## SubscriberRegistry

Registry for `EventSubscriber` instances with event-type filtering.

```python
from eventsource.projections import SubscriberRegistry

registry = SubscriberRegistry()

# Register subscribers (must implement subscribed_to() and handle())
registry.register(order_projection)
registry.register(customer_stats_projection)

# Get subscribers for specific event type
subscribers = registry.get_subscribers_for(OrderCreated)

# Dispatch to only interested subscribers
await registry.dispatch(event)
await registry.dispatch_many(events)

# Unregister
registry.unregister(order_projection)
```

---

## Complete Example

```python
from uuid import UUID
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from eventsource import DomainEvent, register_event
from eventsource.projections import DatabaseProjection, handles
from eventsource.repositories import PostgreSQLCheckpointRepository, PostgreSQLDLQRepository


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


class OrderDashboardProjection(DatabaseProjection):
    """
    Builds a dashboard read model for orders.

    Uses DatabaseProjection for automatic transaction handling.

    Maintains:
    - orders table: current order state
    - order_metrics table: aggregated statistics
    """

    @handles(OrderCreated)
    async def _handle_order_created(self, conn, event: OrderCreated) -> None:
        """Insert order and update metrics (both in same transaction)."""
        # Insert order (idempotent with ON CONFLICT)
        await conn.execute(
            text("""
                INSERT INTO orders (id, customer_id, total, status, created_at)
                VALUES (:id, :customer_id, :total, 'created', :created_at)
                ON CONFLICT (id) DO NOTHING
            """),
            {
                "id": str(event.aggregate_id),
                "customer_id": str(event.customer_id),
                "total": event.total,
                "created_at": event.occurred_at,
            }
        )

        # Update daily metrics
        await conn.execute(
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

    @handles(OrderShipped)
    async def _handle_order_shipped(self, conn, event: OrderShipped) -> None:
        """Update order status to shipped."""
        await conn.execute(
            text("""
                UPDATE orders
                SET status = 'shipped', tracking_number = :tracking, shipped_at = :shipped_at
                WHERE id = :id
            """),
            {
                "id": str(event.aggregate_id),
                "tracking": event.tracking_number,
                "shipped_at": event.occurred_at,
            }
        )


# Usage
async def main():
    # Setup database
    engine = create_async_engine("postgresql+asyncpg://localhost/mydb")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Create projection with PostgreSQL repositories
    projection = OrderDashboardProjection(
        session_factory=session_factory,
        checkpoint_repo=PostgreSQLCheckpointRepository(session_factory),
        dlq_repo=PostgreSQLDLQRepository(session_factory),
    )

    # Process events
    async for stored_event in event_store.read_all():
        await projection.handle(stored_event.event)

    # Check progress
    checkpoint = await projection.get_checkpoint()
    print(f"Last processed: {checkpoint}")

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
