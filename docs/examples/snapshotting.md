# Snapshotting Example

This example demonstrates aggregate snapshotting for performance optimization in an event-sourced application.

## Overview

We'll extend the basic order system to use snapshotting, showing:

- Configuring snapshot stores
- Automatic snapshot creation at thresholds
- Manual snapshot creation
- Schema versioning for state evolution
- Background snapshot mode

## Complete Code

```python
"""
Complete snapshotting example with an Order aggregate.

This example demonstrates:
1. Configuring snapshotting on a repository
2. Automatic snapshot creation at event thresholds
3. Manual snapshot creation
4. Loading aggregates from snapshots
5. Schema versioning for state evolution
"""

import asyncio
from datetime import datetime, UTC
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)
from eventsource.snapshots import InMemorySnapshotStore, Snapshot


# =============================================================================
# Events
# =============================================================================


@register_event
class OrderCreated(DomainEvent):
    """Event: Order was created."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    customer_email: str


@register_event
class OrderItemAdded(DomainEvent):
    """Event: Item was added to order."""

    event_type: str = "OrderItemAdded"
    aggregate_type: str = "Order"
    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float


@register_event
class OrderSubmitted(DomainEvent):
    """Event: Order was submitted for processing."""

    event_type: str = "OrderSubmitted"
    aggregate_type: str = "Order"
    total_amount: float


@register_event
class OrderItemRemoved(DomainEvent):
    """Event: Item was removed from order."""

    event_type: str = "OrderItemRemoved"
    aggregate_type: str = "Order"
    product_id: UUID


# =============================================================================
# State
# =============================================================================


class OrderItem(BaseModel):
    """Order line item."""

    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float

    @property
    def total(self) -> float:
        return self.quantity * self.unit_price


class OrderState(BaseModel):
    """State of an Order aggregate."""

    order_id: UUID
    customer_id: UUID | None = None
    customer_email: str = ""
    items: list[OrderItem] = []
    status: str = "draft"
    total_amount: float = 0.0

    @property
    def calculated_total(self) -> float:
        return sum(item.total for item in self.items)

    @property
    def item_count(self) -> int:
        return len(self.items)


# =============================================================================
# Aggregate
# =============================================================================


class OrderAggregate(AggregateRoot[OrderState]):
    """
    Event-sourced Order aggregate with snapshotting support.

    The schema_version attribute tracks compatibility between
    the state model and stored snapshots.
    """

    aggregate_type = "Order"
    schema_version = 1  # Increment when OrderState structure changes

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                customer_email=event.customer_email,
                status="created",
            )
        elif isinstance(event, OrderItemAdded):
            if self._state:
                new_item = OrderItem(
                    product_id=event.product_id,
                    product_name=event.product_name,
                    quantity=event.quantity,
                    unit_price=event.unit_price,
                )
                self._state = self._state.model_copy(
                    update={"items": [*self._state.items, new_item]}
                )
        elif isinstance(event, OrderItemRemoved):
            if self._state:
                updated_items = [
                    item
                    for item in self._state.items
                    if item.product_id != event.product_id
                ]
                self._state = self._state.model_copy(update={"items": updated_items})
        elif isinstance(event, OrderSubmitted):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "submitted",
                        "total_amount": event.total_amount,
                    }
                )

    # Command methods

    def create(self, customer_id: UUID, customer_email: str) -> None:
        """Create the order."""
        if self.version > 0:
            raise ValueError("Order already exists")

        self.apply_event(
            OrderCreated(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                customer_email=customer_email,
                aggregate_version=self.get_next_version(),
            )
        )

    def add_item(
        self,
        product_id: UUID,
        product_name: str,
        quantity: int,
        unit_price: float,
    ) -> None:
        """Add an item to the order."""
        if not self.state or self.state.status != "created":
            raise ValueError("Can only add items to created orders")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")

        self.apply_event(
            OrderItemAdded(
                aggregate_id=self.aggregate_id,
                product_id=product_id,
                product_name=product_name,
                quantity=quantity,
                unit_price=unit_price,
                aggregate_version=self.get_next_version(),
            )
        )

    def remove_item(self, product_id: UUID) -> None:
        """Remove an item from the order."""
        if not self.state or self.state.status != "created":
            raise ValueError("Can only remove items from created orders")

        if not any(item.product_id == product_id for item in self.state.items):
            raise ValueError("Item not found in order")

        self.apply_event(
            OrderItemRemoved(
                aggregate_id=self.aggregate_id,
                product_id=product_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def submit(self) -> None:
        """Submit the order for processing."""
        if not self.state or self.state.status != "created":
            raise ValueError("Order must be in created status")
        if not self.state.items:
            raise ValueError("Cannot submit empty order")

        self.apply_event(
            OrderSubmitted(
                aggregate_id=self.aggregate_id,
                total_amount=self.state.calculated_total,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Examples
# =============================================================================


async def basic_snapshotting_example():
    """
    Basic example: Enable snapshotting with automatic threshold.

    Shows how to configure a repository with snapshot support and
    let it automatically create snapshots at the configured threshold.
    """
    print("\n" + "=" * 60)
    print("BASIC SNAPSHOTTING EXAMPLE")
    print("=" * 60)

    # Create stores
    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Configure repository with snapshotting
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        snapshot_store=snapshot_store,
        snapshot_threshold=5,  # Snapshot every 5 events (low for demo)
        snapshot_mode="sync",
    )

    # Create order
    order_id = uuid4()
    customer_id = uuid4()

    order = repo.create_new(order_id)
    order.create(customer_id, "customer@example.com")

    # Add items (reaching threshold at event 5)
    products = [
        ("Widget A", 2, 19.99),
        ("Widget B", 1, 49.99),
        ("Widget C", 3, 9.99),
        ("Widget D", 1, 29.99),
    ]

    for name, qty, price in products:
        order.add_item(uuid4(), name, qty, price)

    await repo.save(order)

    # Check if snapshot was created
    snapshot = await snapshot_store.get_snapshot(order_id, "Order")
    print(f"Order version after save: {order.version}")
    print(f"Snapshot exists: {snapshot is not None}")
    if snapshot:
        print(f"Snapshot version: {snapshot.version}")
        print(f"Snapshot state keys: {list(snapshot.state.keys())}")

    # Load order - will use snapshot
    print("\nLoading order from snapshot...")
    loaded = await repo.load(order_id)
    print(f"Loaded order version: {loaded.version}")
    print(f"Loaded order items: {loaded.state.item_count}")
    print(f"Loaded order status: {loaded.state.status}")


async def manual_snapshot_example():
    """
    Manual snapshot example: Create snapshots at specific points.

    Shows how to use manual snapshot mode for precise control
    over when snapshots are taken.
    """
    print("\n" + "=" * 60)
    print("MANUAL SNAPSHOT EXAMPLE")
    print("=" * 60)

    # Create stores
    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Configure repository with manual snapshot mode
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        snapshot_store=snapshot_store,
        snapshot_mode="manual",  # No automatic snapshots
    )

    # Create and populate order
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(uuid4(), "customer@example.com")

    for i in range(10):
        order.add_item(uuid4(), f"Product {i}", 1, 10.0 + i)

    await repo.save(order)

    # No snapshot created automatically
    snapshot = await snapshot_store.get_snapshot(order_id, "Order")
    print(f"Snapshot after save: {snapshot is not None}")

    # Manually create snapshot at a business milestone
    print("\nCreating manual snapshot...")
    snapshot = await repo.create_snapshot(order)
    print(f"Manual snapshot created at version: {snapshot.version}")

    # Submit order (another business milestone)
    order.submit()
    await repo.save(order)

    # Create another snapshot after submission
    snapshot = await repo.create_snapshot(order)
    print(f"Post-submission snapshot at version: {snapshot.version}")


async def background_snapshot_example():
    """
    Background snapshot example: Non-blocking snapshot creation.

    Shows how to use background mode for high-throughput scenarios
    where snapshot creation shouldn't block the save operation.
    """
    print("\n" + "=" * 60)
    print("BACKGROUND SNAPSHOT EXAMPLE")
    print("=" * 60)

    # Create stores
    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Configure repository with background snapshot mode
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        snapshot_store=snapshot_store,
        snapshot_threshold=5,
        snapshot_mode="background",  # Non-blocking snapshots
    )

    # Create order with enough events to trigger snapshot
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(uuid4(), "customer@example.com")

    for i in range(4):
        order.add_item(uuid4(), f"Product {i}", 1, 10.0)

    # Save returns immediately, snapshot created in background
    print("Saving order (snapshot will be created in background)...")
    await repo.save(order)
    print(f"Save completed. Pending snapshots: {repo.pending_snapshot_count}")

    # Wait for background snapshots to complete
    count = await repo.await_pending_snapshots()
    print(f"Waited for {count} background snapshot(s)")

    # Verify snapshot exists
    snapshot = await snapshot_store.get_snapshot(order_id, "Order")
    print(f"Snapshot exists: {snapshot is not None}")


async def schema_versioning_example():
    """
    Schema versioning example: Handling state model evolution.

    Shows how schema versioning invalidates incompatible snapshots
    and triggers full event replay.
    """
    print("\n" + "=" * 60)
    print("SCHEMA VERSIONING EXAMPLE")
    print("=" * 60)

    # Create stores
    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Repository with schema_version=1
    repo_v1 = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,  # schema_version=1
        aggregate_type="Order",
        snapshot_store=snapshot_store,
        snapshot_threshold=3,
    )

    # Create order and trigger snapshot
    order_id = uuid4()
    order = repo_v1.create_new(order_id)
    order.create(uuid4(), "customer@example.com")
    order.add_item(uuid4(), "Product", 1, 10.0)
    order.add_item(uuid4(), "Product 2", 2, 20.0)
    await repo_v1.save(order)

    snapshot = await snapshot_store.get_snapshot(order_id, "Order")
    print(f"Snapshot created with schema_version: {snapshot.schema_version}")

    # Simulate schema version change by creating new aggregate class
    class OrderAggregateV2(OrderAggregate):
        schema_version = 2  # Incremented

    # Repository with schema_version=2
    repo_v2 = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregateV2,
        aggregate_type="Order",
        snapshot_store=snapshot_store,
        snapshot_threshold=3,
    )

    # Load with v2 - snapshot will be invalidated
    print("\nLoading with schema_version=2...")
    loaded = await repo_v2.load(order_id)
    print(f"Loaded successfully via full event replay")
    print(f"Loaded version: {loaded.version}")
    print(f"Loaded items: {loaded.state.item_count}")


async def performance_comparison_example():
    """
    Performance comparison: Snapshot vs full replay.

    Shows the performance difference between loading with
    and without snapshots.
    """
    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON EXAMPLE")
    print("=" * 60)

    import time

    # Create stores
    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Repository with snapshotting
    repo_with_snapshot = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        snapshot_store=snapshot_store,
        snapshot_threshold=50,
    )

    # Repository without snapshotting (same event store)
    repo_without_snapshot = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        # No snapshot_store
    )

    # Create order with many events
    order_id = uuid4()
    order = repo_with_snapshot.create_new(order_id)
    order.create(uuid4(), "customer@example.com")

    num_items = 200
    print(f"Creating order with {num_items} item events...")

    for i in range(num_items):
        order.add_item(uuid4(), f"Product {i}", 1, 10.0 + i)

    await repo_with_snapshot.save(order)
    print(f"Order saved. Version: {order.version}")

    # Check snapshot
    snapshot = await snapshot_store.get_snapshot(order_id, "Order")
    print(f"Snapshot at version: {snapshot.version if snapshot else 'None'}")

    # Load without snapshot
    print("\nLoading WITHOUT snapshot...")
    start = time.perf_counter()
    for _ in range(10):
        await repo_without_snapshot.load(order_id)
    time_without = (time.perf_counter() - start) / 10
    print(f"Average load time: {time_without * 1000:.2f}ms")

    # Load with snapshot
    print("\nLoading WITH snapshot...")
    start = time.perf_counter()
    for _ in range(10):
        await repo_with_snapshot.load(order_id)
    time_with = (time.perf_counter() - start) / 10
    print(f"Average load time: {time_with * 1000:.2f}ms")

    # Calculate improvement
    if time_without > 0:
        improvement = ((time_without - time_with) / time_without) * 100
        print(f"\nPerformance improvement: {improvement:.1f}%")


async def snapshot_inspection_example():
    """
    Snapshot inspection: Examining snapshot contents.

    Shows how to inspect snapshot data for debugging
    and understanding what's being stored.
    """
    print("\n" + "=" * 60)
    print("SNAPSHOT INSPECTION EXAMPLE")
    print("=" * 60)

    # Create stores
    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Configure repository
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        snapshot_store=snapshot_store,
        snapshot_threshold=3,
    )

    # Create order
    order_id = uuid4()
    customer_id = uuid4()

    order = repo.create_new(order_id)
    order.create(customer_id, "customer@example.com")
    order.add_item(uuid4(), "Premium Widget", 2, 99.99)
    order.add_item(uuid4(), "Basic Widget", 5, 19.99)

    await repo.save(order)

    # Get and inspect snapshot
    snapshot = await snapshot_store.get_snapshot(order_id, "Order")

    print("Snapshot Details:")
    print(f"  Aggregate ID: {snapshot.aggregate_id}")
    print(f"  Aggregate Type: {snapshot.aggregate_type}")
    print(f"  Version: {snapshot.version}")
    print(f"  Schema Version: {snapshot.schema_version}")
    print(f"  Created At: {snapshot.created_at}")
    print(f"\nSnapshot State:")
    for key, value in snapshot.state.items():
        if isinstance(value, list):
            print(f"  {key}: [{len(value)} items]")
            for i, item in enumerate(value[:2]):  # Show first 2
                print(f"    [{i}]: {item}")
            if len(value) > 2:
                print(f"    ... and {len(value) - 2} more")
        else:
            print(f"  {key}: {value}")


async def main():
    """Run all examples."""
    await basic_snapshotting_example()
    await manual_snapshot_example()
    await background_snapshot_example()
    await schema_versioning_example()
    await performance_comparison_example()
    await snapshot_inspection_example()

    print("\n" + "=" * 60)
    print("ALL EXAMPLES COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Running the Example

Save the code as `snapshotting_example.py` and run:

```bash
python snapshotting_example.py
```

Expected output:

```
============================================================
BASIC SNAPSHOTTING EXAMPLE
============================================================
Order version after save: 5
Snapshot exists: True
Snapshot version: 5
Snapshot state keys: ['order_id', 'customer_id', 'customer_email', 'items', 'status', 'total_amount']

Loading order from snapshot...
Loaded order version: 5
Loaded order items: 4
Loaded order status: created

============================================================
MANUAL SNAPSHOT EXAMPLE
============================================================
Snapshot after save: False

Creating manual snapshot...
Manual snapshot created at version: 11
Post-submission snapshot at version: 12

============================================================
BACKGROUND SNAPSHOT EXAMPLE
============================================================
Saving order (snapshot will be created in background)...
Save completed. Pending snapshots: 1
Waited for 1 background snapshot(s)
Snapshot exists: True

============================================================
SCHEMA VERSIONING EXAMPLE
============================================================
Snapshot created with schema_version: 1

Loading with schema_version=2...
Loaded successfully via full event replay
Loaded version: 3
Loaded items: 2

============================================================
PERFORMANCE COMPARISON EXAMPLE
============================================================
Creating order with 200 item events...
Order saved. Version: 201
Snapshot at version: 200

Loading WITHOUT snapshot...
Average load time: 15.23ms

Loading WITH snapshot...
Average load time: 1.45ms

Performance improvement: 90.5%

============================================================
SNAPSHOT INSPECTION EXAMPLE
============================================================
Snapshot Details:
  Aggregate ID: 550e8400-e29b-41d4-a716-446655440000
  Aggregate Type: Order
  Version: 3
  Schema Version: 1
  Created At: 2024-01-15 10:30:45.123456+00:00

Snapshot State:
  order_id: 550e8400-e29b-41d4-a716-446655440000
  customer_id: 661f9511-f30c-52e5-b827-557766551111
  customer_email: customer@example.com
  items: [2 items]
    [0]: {'product_id': '...', 'product_name': 'Premium Widget', ...}
    [1]: {'product_id': '...', 'product_name': 'Basic Widget', ...}
  status: created
  total_amount: 0.0

============================================================
ALL EXAMPLES COMPLETED
============================================================
```

---

## Key Concepts Demonstrated

### 1. Automatic Snapshotting

Configure `snapshot_threshold` to automatically create snapshots:

```python
repo = AggregateRepository(
    ...,
    snapshot_threshold=100,  # Snapshot every 100 events
)
```

### 2. Manual Snapshotting

Use `snapshot_mode="manual"` for explicit control:

```python
repo = AggregateRepository(
    ...,
    snapshot_mode="manual",
)

# Create snapshot at business milestones
await repo.create_snapshot(order)
```

### 3. Background Snapshotting

Use `snapshot_mode="background"` for non-blocking saves:

```python
repo = AggregateRepository(
    ...,
    snapshot_mode="background",
)

# Wait for background tasks in tests
await repo.await_pending_snapshots()
```

### 4. Schema Versioning

Increment `schema_version` when state structure changes:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 2  # Invalidates snapshots with schema_version=1
```

---

## Production Example

For production with PostgreSQL:

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource import AggregateRepository, PostgreSQLEventStore
from eventsource.snapshots import PostgreSQLSnapshotStore

# Database setup
engine = create_async_engine(
    "postgresql+asyncpg://user:pass@localhost/mydb",
    pool_size=20,
    max_overflow=10,
)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

# Create stores
event_store = PostgreSQLEventStore(session_factory)
snapshot_store = PostgreSQLSnapshotStore(session_factory)

# Production repository configuration
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
    snapshot_mode="background",  # Non-blocking for production
)
```

---

## See Also

- [Snapshotting Guide](../guides/snapshotting.md) - Complete usage guide
- [Migration Guide](../guides/snapshotting-migration.md) - Adding to existing projects
- [API Reference](../api/snapshots.md) - Detailed API documentation
- [Basic Order Example](./basic-order.md) - Event sourcing fundamentals
