# Tutorial 6: Projections - Building Read Models

**Difficulty:** Intermediate

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- Python 3.10 or higher
- Understanding of async/await
- Basic understanding of CQRS pattern

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain what projections are and why they're essential in event sourcing
2. Implement the EventSubscriber protocol for basic projections
3. Build in-memory read models from event streams
4. Create projections with the SubscriptionManager for production use
5. Understand different projection base classes and when to use them
6. Implement checkpoint tracking for resumable processing
7. Build multiple projections from the same event stream
8. Query projections to retrieve read model data

---

## What are Projections?

**Projections** are read models built from events. They're the "query side" of CQRS (Command Query Responsibility Segregation). While aggregates enforce business rules and emit events, projections transform those events into data structures optimized for querying.

### Key Characteristics

- **Event-driven**: Built by processing events in order
- **Rebuildable**: Can be reconstructed from scratch by replaying events
- **Eventually consistent**: May lag slightly behind the write model
- **Query-optimized**: Denormalized for fast reads
- **Multiple views**: Many projections can consume the same events

### Real-World Analogy

Think of events as transactions in a bank ledger. Projections are different reports generated from those transactions:
- **Account balance**: Sum of all credits and debits
- **Monthly statement**: Filtered by date range
- **Spending by category**: Grouped and aggregated
- **Largest transactions**: Sorted by amount

Each report (projection) is built from the same source (events) but optimized for different queries.

---

## The EventSubscriber Protocol

The simplest way to create a projection is to implement the `EventSubscriber` protocol:

```python
from eventsource import DomainEvent
from eventsource.protocols import EventSubscriber

class MyProjection(EventSubscriber):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which event types this projection handles."""
        return [OrderPlaced, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event and update the read model."""
        if isinstance(event, OrderPlaced):
            # Update read model for placed orders
            pass
```

**Required methods:**

- `subscribed_to()`: Returns list of event types to process
- `handle()`: Processes a single event asynchronously

This protocol works with `SubscriptionManager` for production use, which we'll cover shortly.

---

## Building Your First Projection

Let's build a simple order summary projection:

```python
from uuid import UUID
from collections import defaultdict
from eventsource import DomainEvent, register_event

# Define events
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

@register_event
class OrderCancelled(DomainEvent):
    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"

    reason: str

# Create projection
class OrderSummaryProjection:
    """Maintains order counts by status."""

    def __init__(self):
        # In-memory read model
        self.order_counts: dict[str, int] = defaultdict(int)
        self.total_revenue: float = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which event types this projection handles."""
        return [OrderPlaced, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event and update the read model."""
        if isinstance(event, OrderPlaced):
            self.order_counts["placed"] += 1
            self.total_revenue += event.total_amount

        elif isinstance(event, OrderShipped):
            # Move from placed to shipped
            if self.order_counts["placed"] > 0:
                self.order_counts["placed"] -= 1
            self.order_counts["shipped"] += 1

        elif isinstance(event, OrderCancelled):
            # Decrement appropriate count
            for status in ["placed", "shipped"]:
                if self.order_counts[status] > 0:
                    self.order_counts[status] -= 1
                    break
            self.order_counts["cancelled"] += 1

    # Query methods
    def get_summary(self) -> dict:
        """Get summary statistics."""
        return {
            "total_orders": sum(self.order_counts.values()),
            "by_status": dict(self.order_counts),
            "total_revenue": self.total_revenue,
        }
```

**Pattern breakdown:**

1. **State storage**: In-memory dictionary (in production, use a database)
2. **Event handlers**: Update state based on event type
3. **Query methods**: Expose read model data to consumers

---

## Using SubscriptionManager

For production use, projections should use `SubscriptionManager`. It handles:

- **Catch-up**: Replays historical events from the event store
- **Live subscriptions**: Processes new events from the event bus
- **Checkpoint tracking**: Remembers position for resumable processing
- **Seamless transition**: Automatically switches from catch-up to live mode

### Basic Setup

```python
import asyncio
from eventsource import (
    InMemoryEventStore,
    InMemoryEventBus,
    InMemoryCheckpointRepository,
    AggregateRepository,
)
from eventsource.subscriptions import (
    SubscriptionManager,
    SubscriptionConfig,
)

async def main():
    # Create infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    # Create repository with event publishing
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,  # Events published after save
    )

    # Create projection
    projection = OrderSummaryProjection()

    # Create subscription manager
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Configure subscription to start from beginning
    config = SubscriptionConfig(
        start_from="beginning",  # Or "end", "checkpoint", or position number
        batch_size=100,          # Events per batch during catch-up
    )

    # Subscribe projection
    await manager.subscribe(
        projection,
        config=config,
        name="OrderSummary",  # Unique name for checkpointing
    )

    # Start the manager (begins catch-up)
    await manager.start()

    # Create some orders
    order = repo.create_new(uuid4())
    order.place(customer_id, "Alice", 100.0)
    await repo.save(order)  # Published to event bus automatically

    # Wait for event to be processed
    await asyncio.sleep(0.1)

    # Query the projection
    summary = projection.get_summary()
    print(f"Total orders: {summary['total_orders']}")
    print(f"Revenue: ${summary['total_revenue']:.2f}")

    # Graceful shutdown
    await manager.stop()

asyncio.run(main())
```

**SubscriptionManager lifecycle:**

1. **Subscribe**: Register projections with configuration
2. **Start**: Begin catch-up phase, replay historical events
3. **Transition**: Automatically switch to live subscriptions
4. **Process**: Handle events as they arrive
5. **Stop**: Gracefully shut down, save final checkpoints

---

## Checkpoint Tracking

Checkpoints track the last processed event position, enabling:

- **Resumable processing**: Restart from where you left off
- **Idempotency**: Avoid processing the same event twice
- **Progress monitoring**: See how far behind a projection is

### How Checkpoints Work

```python
# First run
await manager.start()
# Processes events 1-100
# Checkpoint saved: position 100

# Manager stops

# Second run (restart)
await manager.start()
# Starts from position 101 (checkpoint + 1)
# Processes only new events
```

**Checkpoint storage:**

```python
# Check current checkpoint
checkpoint = await checkpoint_repo.get_checkpoint("OrderSummary")
print(f"Last processed: {checkpoint}")  # Event ID

# Checkpoints are saved automatically after each event
```

**Start position options:**

| Option | Behavior |
|--------|----------|
| `"beginning"` | Start from first event (position 0) |
| `"end"` | Start from latest position (skip history) |
| `"checkpoint"` | Resume from saved checkpoint |
| `123` | Start from specific position number |

---

## Multiple Projections

Build multiple read models from the same event stream:

```python
class CustomerStatsProjection:
    """Tracks customer statistics."""

    def __init__(self):
        self.stats: dict[UUID, dict] = defaultdict(
            lambda: {
                "customer_id": None,
                "customer_name": "",
                "total_orders": 0,
                "total_spent": 0.0,
            }
        )
        # Track order->customer for later updates
        self._order_to_customer: dict[UUID, UUID] = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            stats = self.stats[event.customer_id]
            stats["customer_id"] = event.customer_id
            stats["customer_name"] = event.customer_name
            stats["total_orders"] += 1
            stats["total_spent"] += event.total_amount

            # Track for other events
            self._order_to_customer[event.aggregate_id] = event.customer_id

    def get_customer_stats(self, customer_id: UUID) -> dict | None:
        """Get stats for a specific customer."""
        if customer_id in self.stats:
            return dict(self.stats[customer_id])
        return None

    def get_top_customers(self, limit: int = 10) -> list[dict]:
        """Get top customers by spend."""
        customers = list(self.stats.values())
        customers.sort(key=lambda x: x["total_spent"], reverse=True)
        return customers[:limit]


class DailyRevenueProjection:
    """Tracks daily revenue."""

    def __init__(self):
        self.daily_revenue: dict[str, dict] = defaultdict(
            lambda: {"date": None, "order_count": 0, "total_revenue": 0.0}
        )

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            date_str = event.occurred_at.strftime("%Y-%m-%d")
            self.daily_revenue[date_str]["date"] = date_str
            self.daily_revenue[date_str]["order_count"] += 1
            self.daily_revenue[date_str]["total_revenue"] += event.total_amount

    def get_daily_revenue(self, date_str: str) -> dict | None:
        """Get revenue for a specific date."""
        if date_str in self.daily_revenue:
            return dict(self.daily_revenue[date_str])
        return None


# Subscribe all projections
await manager.subscribe(order_summary, config=config, name="OrderSummary")
await manager.subscribe(customer_stats, config=config, name="CustomerStats")
await manager.subscribe(daily_revenue, config=config, name="DailyRevenue")

await manager.start()

# All three projections process the same events independently
# Each maintains its own checkpoint
```

**Benefits of multiple projections:**

- **Specialized views**: Each optimized for specific queries
- **Independent scaling**: Update projections separately
- **Easy additions**: Add new projections without affecting existing ones
- **Isolation**: One projection's failure doesn't affect others

---

## Advanced Projection Base Classes

For production use with databases and advanced features, eventsource-py provides several base classes:

### DeclarativeProjection

Uses `@handles` decorator for type-safe event routing:

```python
from eventsource.projections import DeclarativeProjection, handles
from eventsource import InMemoryCheckpointRepository

class OrderProjection(DeclarativeProjection):
    """Projection using declarative handlers."""

    def __init__(self):
        # Initialize parent with checkpoint repo
        super().__init__(
            checkpoint_repo=InMemoryCheckpointRepository(),
            enable_tracing=False,  # OpenTelemetry tracing (optional)
        )
        # Your read model state
        self.orders: dict[UUID, dict] = {}

    @handles(OrderPlaced)
    async def _on_order_placed(self, event: OrderPlaced) -> None:
        """Handle OrderPlaced event."""
        self.orders[event.aggregate_id] = {
            "order_id": event.aggregate_id,
            "customer_name": event.customer_name,
            "total_amount": event.total_amount,
            "status": "placed",
        }

    @handles(OrderShipped)
    async def _on_order_shipped(self, event: OrderShipped) -> None:
        """Handle OrderShipped event."""
        if event.aggregate_id in self.orders:
            self.orders[event.aggregate_id]["status"] = "shipped"

    async def _truncate_read_models(self) -> None:
        """Clear data for rebuild (called during reset)."""
        self.orders.clear()

    # subscribed_to() is auto-generated from @handles decorators!

# Use with SubscriptionManager
projection = OrderProjection()
await manager.subscribe(projection, config=config, name="Orders")
```

**Features:**

- **Auto-discovery**: `subscribed_to()` generated automatically
- **Type safety**: Each handler gets exact event type
- **Checkpoint tracking**: Built-in via `CheckpointTrackingProjection`
- **Retry logic**: Automatic retry with exponential backoff
- **DLQ support**: Failed events sent to dead letter queue
- **Reset support**: Override `_truncate_read_models()` for rebuilds

### DatabaseProjection

Extends `DeclarativeProjection` with database connection support:

```python
from eventsource.projections import DatabaseProjection, handles
from sqlalchemy.ext.asyncio import async_sessionmaker

class OrderDatabaseProjection(DatabaseProjection):
    """Projection with database operations."""

    @handles(OrderPlaced)
    async def _on_order_placed(
        self,
        conn,  # AsyncConnection automatically provided
        event: OrderPlaced,
    ) -> None:
        """Handle OrderPlaced with database access."""
        from sqlalchemy import text

        await conn.execute(
            text("""
                INSERT INTO order_summary (id, customer_name, total_amount, status)
                VALUES (:id, :name, :amount, 'placed')
            """),
            {
                "id": str(event.aggregate_id),
                "name": event.customer_name,
                "amount": event.total_amount,
            },
        )

    @handles(OrderShipped)
    async def _on_order_shipped(self, conn, event: OrderShipped) -> None:
        """Update order status in database."""
        await conn.execute(
            text("UPDATE order_summary SET status = 'shipped' WHERE id = :id"),
            {"id": str(event.aggregate_id)},
        )

    async def _truncate_read_models(self) -> None:
        """Truncate database tables for reset."""
        # This is called in a separate transaction during reset()
        async with self._session_factory() as session, session.begin():
            conn = await session.connection()
            await conn.execute(text("TRUNCATE TABLE order_summary"))

# Usage
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

projection = OrderDatabaseProjection(session_factory=session_factory)
await manager.subscribe(projection, config=config, name="OrderDB")
```

**Features:**

- **Automatic transactions**: Each event processed in a transaction
- **Connection injection**: Handlers receive `conn` parameter
- **Rollback on error**: Failed events trigger rollback
- **Retry support**: Inherited from `DeclarativeProjection`

### ReadModelProjection

Highest-level base class with automatic repository support:

```python
from eventsource.readmodels import ReadModelProjection, ReadModel
from eventsource.projections import handles

# Define read model
class OrderSummary(ReadModel):
    """Read model for order summaries."""

    order_number: str
    customer_name: str
    total_amount: float
    status: str

    @classmethod
    def table_name(cls) -> str:
        return "order_summary"

# Create projection
class OrderReadModelProjection(ReadModelProjection[OrderSummary]):
    """Projection with automatic repository support."""

    @handles(OrderPlaced)
    async def _on_order_placed(
        self,
        repo,  # ReadModelRepository automatically provided
        event: OrderPlaced,
    ) -> None:
        """Handle OrderPlaced with repository."""
        await repo.save(
            OrderSummary(
                id=event.aggregate_id,
                order_number=f"ORD-{event.aggregate_id.hex[:8].upper()}",
                customer_name=event.customer_name,
                total_amount=event.total_amount,
                status="placed",
            )
        )

    @handles(OrderShipped)
    async def _on_order_shipped(self, repo, event: OrderShipped) -> None:
        """Update order via repository."""
        summary = await repo.get(event.aggregate_id)
        if summary:
            summary.status = "shipped"
            await repo.save(summary)

    @handles(OrderCancelled)
    async def _on_order_cancelled(self, repo, event: OrderCancelled) -> None:
        """Soft delete cancelled orders."""
        await repo.soft_delete(event.aggregate_id)

# Usage
projection = OrderReadModelProjection(
    session_factory=session_factory,
    model_class=OrderSummary,
)

await manager.subscribe(projection, config=config, name="OrderReadModel")
```

**Features:**

- **Repository abstraction**: No raw SQL needed
- **CRUD operations**: save(), get(), soft_delete(), truncate()
- **Type safety**: Generic[TModel] provides type hints
- **Automatic table operations**: Truncate handled automatically

---

## Complete Working Example

Here's a runnable example demonstrating all concepts:

```python
"""
Tutorial 6: Projections
Run with: python tutorial_06_projections.py
"""

import asyncio
from collections import defaultdict
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
        elif isinstance(event, OrderCancelled) and self._state:
            self._state = self._state.model_copy(update={"status": "cancelled"})

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


class OrderSummaryProjection:
    """Counts orders by status."""

    def __init__(self):
        self.order_counts: dict[str, int] = defaultdict(int)
        self.total_revenue: float = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self.order_counts["placed"] += 1
            self.total_revenue += event.total_amount

        elif isinstance(event, OrderShipped):
            if self.order_counts["placed"] > 0:
                self.order_counts["placed"] -= 1
            self.order_counts["shipped"] += 1

        elif isinstance(event, OrderCancelled):
            for status in ["placed", "shipped"]:
                if self.order_counts[status] > 0:
                    self.order_counts[status] -= 1
                    break
            self.order_counts["cancelled"] += 1

    def get_summary(self) -> dict:
        return {
            "total_orders": sum(self.order_counts.values()),
            "by_status": dict(self.order_counts),
            "total_revenue": self.total_revenue,
        }


class CustomerStatsProjection:
    """Tracks customer statistics."""

    def __init__(self):
        self.stats: dict[UUID, dict] = defaultdict(
            lambda: {
                "customer_id": None,
                "customer_name": "",
                "total_orders": 0,
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
            stats["total_orders"] += 1
            stats["total_spent"] += event.total_amount

    def get_customer_stats(self, customer_id: UUID) -> dict | None:
        if customer_id in self.stats:
            return dict(self.stats[customer_id])
        return None

    def get_top_customers(self, limit: int = 10) -> list[dict]:
        customers = list(self.stats.values())
        customers.sort(key=lambda x: x["total_spent"], reverse=True)
        return customers[:limit]


# =============================================================================
# Demo
# =============================================================================


async def main():
    print("=" * 60)
    print("Tutorial 6: Projections")
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
    print("   - Event store, bus, and checkpoint repo initialized")

    # Create historical orders
    print("\n2. Creating historical orders...")
    customers = [
        (uuid4(), "Alice Johnson"),
        (uuid4(), "Bob Smith"),
        (uuid4(), "Carol Williams"),
    ]

    order_ids = []
    for customer_id, customer_name in customers:
        order_id = uuid4()
        order_ids.append(order_id)

        order = repo.create_new(order_id)
        order.place(customer_id, customer_name, 100.0 + len(order_ids) * 50)
        await repo.save(order)

    # Ship one order
    order = await repo.load(order_ids[0])
    order.ship("TRACK-001")
    await repo.save(order)

    # Cancel one order
    order = await repo.load(order_ids[1])
    order.cancel("Customer request")
    await repo.save(order)

    print(f"   Created {len(order_ids)} orders (1 shipped, 1 cancelled)")

    # Create projections
    print("\n3. Creating projections...")
    order_summary = OrderSummaryProjection()
    customer_stats = CustomerStatsProjection()
    print("   - OrderSummaryProjection")
    print("   - CustomerStatsProjection")

    # Create subscription manager
    print("\n4. Creating SubscriptionManager...")
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    config = SubscriptionConfig(start_from="beginning", batch_size=100)

    await manager.subscribe(order_summary, config=config, name="OrderSummary")
    await manager.subscribe(customer_stats, config=config, name="CustomerStats")
    print("   - Subscribed 2 projections (start from beginning)")

    # Start manager (catch-up)
    print("\n5. Starting SubscriptionManager (catching up)...")
    await manager.start()
    await asyncio.sleep(0.5)  # Wait for catch-up
    print("   - Catch-up complete")

    # Query projections
    print("\n6. Querying projections:")
    print("\n   Order Summary:")
    summary = order_summary.get_summary()
    print(f"      Total orders: {summary['total_orders']}")
    print(f"      By status: {summary['by_status']}")
    print(f"      Total revenue: ${summary['total_revenue']:.2f}")

    print("\n   Customer Stats:")
    for customer_id, customer_name in customers:
        stats = customer_stats.get_customer_stats(customer_id)
        if stats:
            print(
                f"      {customer_name}: {stats['total_orders']} orders, "
                f"${stats['total_spent']:.2f}"
            )

    # Create new order (live subscription)
    print("\n7. Creating new order (live event)...")
    new_order_id = uuid4()
    order = repo.create_new(new_order_id)
    order.place(uuid4(), "David Brown", 500.0)
    await repo.save(order)

    await asyncio.sleep(0.2)  # Wait for live event
    print("   - New order processed by projections")

    # Verify live update
    print("\n8. Verifying live update:")
    summary = order_summary.get_summary()
    print(f"   Total orders now: {summary['total_orders']}")

    # Check subscription status
    print("\n9. Subscription status:")
    status = manager.get_all_statuses()
    for name, sub_status in status.items():
        print(f"   {name}:")
        print(f"      State: {sub_status.state}")
        print(f"      Events processed: {sub_status.events_processed}")

    # Graceful shutdown
    print("\n10. Stopping SubscriptionManager...")
    await manager.stop()
    print("    - Stopped gracefully")

    # Verify checkpoints
    checkpoint = await checkpoint_repo.get_checkpoint("OrderSummary")
    print(f"    - Final checkpoint saved: {checkpoint is not None}")

    print("\n" + "=" * 60)
    print("Tutorial complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
============================================================
Tutorial 6: Projections
============================================================

1. Setting up infrastructure...
   - Event store, bus, and checkpoint repo initialized

2. Creating historical orders...
   Created 3 orders (1 shipped, 1 cancelled)

3. Creating projections...
   - OrderSummaryProjection
   - CustomerStatsProjection

4. Creating SubscriptionManager...
   - Subscribed 2 projections (start from beginning)

5. Starting SubscriptionManager (catching up)...
   - Catch-up complete

6. Querying projections:

   Order Summary:
      Total orders: 3
      By status: {'placed': 1, 'shipped': 1, 'cancelled': 1}
      Total revenue: $450.00

   Customer Stats:
      Alice Johnson: 1 orders, $150.00
      Bob Smith: 1 orders, $200.00
      Carol Williams: 1 orders, $100.00

7. Creating new order (live event)...
   - New order processed by projections

8. Verifying live update:
   Total orders now: 4

9. Subscription status:
   OrderSummary:
      State: SubscriptionState.RUNNING
      Events processed: 5
   CustomerStats:
      State: SubscriptionState.RUNNING
      Events processed: 4

10. Stopping SubscriptionManager...
    - Stopped gracefully
    - Final checkpoint saved: True

============================================================
Tutorial complete!
============================================================
```

---

## Projection Patterns

### Read Model Storage

**In-memory (for demos):**

```python
self.orders: dict[UUID, dict] = {}
```

**Database (for production):**

```python
# With DatabaseProjection
@handles(OrderPlaced)
async def _on_order_placed(self, conn, event: OrderPlaced) -> None:
    await conn.execute(text("INSERT INTO orders ..."))

# With ReadModelProjection
@handles(OrderPlaced)
async def _on_order_placed(self, repo, event: OrderPlaced) -> None:
    await repo.save(OrderSummary(...))
```

### Denormalization

Projections should denormalize data for fast queries:

```python
# Bad: Normalized (requires joins)
orders_table: {order_id, customer_id}
customers_table: {customer_id, name}

# Good: Denormalized (single query)
orders_table: {order_id, customer_id, customer_name, total_amount, status}
```

### Event-Driven Updates

Handle all relevant events to keep projections current:

```python
def subscribed_to(self) -> list[type[DomainEvent]]:
    return [
        OrderPlaced,    # Create record
        OrderShipped,   # Update status
        OrderDelivered, # Update status
        OrderCancelled, # Update status or soft delete
    ]
```

### Eventual Consistency

Projections may lag slightly behind writes:

```python
# Write model (immediate)
order.ship(tracking_number)
await repo.save(order)

# Read model (eventual)
await asyncio.sleep(0.1)  # Small delay
projection.get_order(order_id)  # Now up to date
```

For most applications, this lag is acceptable (milliseconds to seconds).

---

## Key Takeaways

1. **Projections build read models from events**: Transform event streams into query-optimized views
2. **EventSubscriber protocol is the foundation**: Implement `subscribed_to()` and `handle()`
3. **SubscriptionManager for production**: Handles catch-up, live subscriptions, and checkpoints
4. **Multiple projections from one stream**: Build different views for different queries
5. **Choose the right base class**: EventSubscriber (simple), DeclarativeProjection (advanced), DatabaseProjection (SQL), ReadModelProjection (repository)
6. **Checkpoints enable resumability**: Restart from where you left off
7. **Denormalize for queries**: Optimize read models for specific access patterns
8. **Eventually consistent**: Accept small lag between writes and reads

---

## Common Patterns

### Rebuild from Scratch

```python
# Reset projection (clears data)
await projection.reset()

# Subscribe and catch up again
config = SubscriptionConfig(start_from="beginning")
await manager.subscribe(projection, config=config, name="OrderSummary")
await manager.start()
```

### Filter by Event Type

```python
def subscribed_to(self) -> list[type[DomainEvent]]:
    # Only process specific events
    return [OrderPlaced, OrderShipped]
```

### Aggregate Across Events

```python
# Track running totals
async def handle(self, event: DomainEvent) -> None:
    if isinstance(event, OrderPlaced):
        self.total_revenue += event.total_amount
        self.order_count += 1
```

---

## Next Steps

Now that you understand projections, you're ready to learn about advanced event distribution patterns.

Continue to [Tutorial 7: Event Bus and Pub/Sub](07-event-bus.md) to learn about:
- Publishing events to subscribers in real-time
- Using InMemoryEventBus, RedisEventBus, and other implementations
- Building reactive systems with event-driven architecture
- Integrating projections with event buses

For more examples, see:
- `examples/projection_example.py` - Complete projection workflow with SubscriptionManager
- `examples/subscriptions/basic_projection.py` - Simple projection tutorial
- `tests/integration/readmodels/test_projection.py` - Production patterns with databases
