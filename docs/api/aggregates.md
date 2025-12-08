# Aggregates API Reference

This document covers the aggregate pattern implementation, including `AggregateRoot`, `DeclarativeAggregate`, and `AggregateRepository`.

## Overview

Aggregates are consistency boundaries in event sourcing. They maintain their state by applying events and emit new events when commands are executed.

```python
from eventsource import (
    AggregateRoot,
    DeclarativeAggregate,
    AggregateRepository,
    handles,
)
```

---

## AggregateRoot

The base class for event-sourced aggregates.

### Class Definition

```python
class AggregateRoot(Generic[TState], ABC):
    """Base class for event-sourced aggregate roots."""

    aggregate_type: str = "Unknown"  # Override in subclasses
    schema_version: int = 1  # Increment when state structure changes (for snapshotting)

    def __init__(self, aggregate_id: UUID) -> None: ...

    @property
    def aggregate_id(self) -> UUID: ...

    @property
    def version(self) -> int: ...

    @property
    def state(self) -> TState | None: ...

    @property
    def uncommitted_events(self) -> list[DomainEvent]: ...

    @property
    def has_uncommitted_events(self) -> bool: ...

    def apply_event(self, event: DomainEvent, is_new: bool = True) -> None: ...

    @abstractmethod
    def _apply(self, event: DomainEvent) -> None: ...

    @abstractmethod
    def _get_initial_state(self) -> TState: ...

    def mark_events_as_committed(self) -> None: ...

    def load_from_history(self, events: list[DomainEvent]) -> None: ...

    def get_next_version(self) -> int: ...

    def clear_uncommitted_events(self) -> list[DomainEvent]: ...
```

### Creating an Aggregate

#### 1. Define State Model

```python
from pydantic import BaseModel
from uuid import UUID

class OrderState(BaseModel):
    """State of an Order aggregate."""
    order_id: UUID
    customer_id: UUID | None = None
    status: str = "draft"
    items: list[dict] = []
    total: float = 0.0
```

#### 2. Define Events

```python
from eventsource import DomainEvent, register_event

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID

@register_event
class ItemAdded(DomainEvent):
    event_type: str = "ItemAdded"
    aggregate_type: str = "Order"
    product_id: UUID
    quantity: int
    price: float

@register_event
class OrderSubmitted(DomainEvent):
    event_type: str = "OrderSubmitted"
    aggregate_type: str = "Order"
```

#### 3. Implement Aggregate

```python
from eventsource import AggregateRoot

class OrderAggregate(AggregateRoot[OrderState]):
    """Event-sourced Order aggregate."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        """Return initial state for new aggregates."""
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        """Apply event to update state."""
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                status="created",
            )
        elif isinstance(event, ItemAdded):
            if self._state:
                new_items = [*self._state.items, {
                    "product_id": str(event.product_id),
                    "quantity": event.quantity,
                    "price": event.price,
                }]
                new_total = self._state.total + (event.quantity * event.price)
                self._state = self._state.model_copy(update={
                    "items": new_items,
                    "total": new_total,
                })
        elif isinstance(event, OrderSubmitted):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": "submitted",
                })

    # Command methods
    def create(self, customer_id: UUID) -> None:
        """Command: Create the order."""
        if self.version > 0:
            raise ValueError("Order already created")

        event = OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def add_item(self, product_id: UUID, quantity: int, price: float) -> None:
        """Command: Add item to order."""
        if not self.state or self.state.status != "created":
            raise ValueError("Order must be created before adding items")

        event = ItemAdded(
            aggregate_id=self.aggregate_id,
            product_id=product_id,
            quantity=quantity,
            price=price,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def submit(self) -> None:
        """Command: Submit the order."""
        if not self.state or self.state.status != "created":
            raise ValueError("Order must be created before submitting")
        if not self.state.items:
            raise ValueError("Cannot submit empty order")

        event = OrderSubmitted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)
```

### Key Methods

#### `apply_event(event, is_new=True)`

Apply an event to the aggregate:

- `is_new=True`: New event - will be tracked for persistence
- `is_new=False`: Historical event - only updates state

```python
# New event (generated by command)
aggregate.apply_event(order_created, is_new=True)

# Historical event (loaded from store)
aggregate.apply_event(stored_event, is_new=False)
```

#### `load_from_history(events)`

Reconstitute aggregate from event history:

```python
aggregate = OrderAggregate(order_id)
aggregate.load_from_history(stream.events)
print(f"Loaded with version {aggregate.version}")
```

#### `get_next_version()`

Get version number for new events:

```python
event = OrderCreated(
    aggregate_id=self.aggregate_id,
    aggregate_version=self.get_next_version(),
    ...
)
```

#### `mark_events_as_committed()`

Clear uncommitted events after save:

```python
# Called by repository after successful save
aggregate.mark_events_as_committed()
assert not aggregate.has_uncommitted_events
```

---

## DeclarativeAggregate

Alternative to `AggregateRoot` using decorators for event handlers.

### Usage

```python
from eventsource import DeclarativeAggregate, handles

class OrderAggregate(DeclarativeAggregate[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    @handles(OrderCreated)
    def _on_order_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=self.aggregate_id,
            customer_id=event.customer_id,
            status="created",
        )

    @handles(ItemAdded)
    def _on_item_added(self, event: ItemAdded) -> None:
        if self._state:
            # Update state...
            pass

    @handles(OrderSubmitted)
    def _on_order_submitted(self, event: OrderSubmitted) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "submitted"})

    # Command methods remain the same
    def create(self, customer_id: UUID) -> None:
        ...
```

### Benefits

- No large `if/elif` chains in `_apply()`
- Handler methods are self-documenting
- Easy to add new event handlers
- Forward compatible (unknown events are ignored)

---

## AggregateRepository

Repository pattern for loading and saving aggregates.

### Setup

```python
from eventsource import AggregateRepository, InMemoryEventStore

event_store = InMemoryEventStore()
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=event_bus,  # Optional
)
```

### Constructor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `event_store` | `EventStore` | Required | Event store for persistence |
| `aggregate_factory` | `type[TAggregate]` | Required | Class to instantiate |
| `aggregate_type` | `str` | Required | Type name for events |
| `event_publisher` | `EventPublisher \| None` | `None` | Optional event broadcaster |
| `snapshot_store` | `SnapshotStore \| None` | `None` | Optional snapshot store for state caching |
| `snapshot_threshold` | `int \| None` | `None` | Events between automatic snapshots |
| `snapshot_mode` | `"sync" \| "background" \| "manual"` | `"sync"` | When to create snapshots |

### Snapshot-Enabled Repository

```python
from eventsource import AggregateRepository, PostgreSQLEventStore
from eventsource.snapshots import PostgreSQLSnapshotStore

repo = AggregateRepository(
    event_store=PostgreSQLEventStore(session_factory),
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=PostgreSQLSnapshotStore(session_factory),
    snapshot_threshold=100,  # Snapshot every 100 events
    snapshot_mode="background",  # Non-blocking snapshot creation
)
```

### Methods

#### `load(aggregate_id)`

Load aggregate from history:

```python
order = await repo.load(order_id)
print(f"Order status: {order.state.status}")
```

Raises `AggregateNotFoundError` if no events exist.

#### `load_or_create(aggregate_id)`

Load existing or create new:

```python
order = await repo.load_or_create(order_id)
if order.version == 0:
    order.create(customer_id=customer_id)
```

#### `save(aggregate)`

Save uncommitted events:

```python
order.add_item(product_id, quantity=2, price=19.99)
await repo.save(order)

# Events are now committed
assert not order.has_uncommitted_events
```

#### `exists(aggregate_id)`

Check if aggregate exists:

```python
if await repo.exists(order_id):
    order = await repo.load(order_id)
```

#### `get_version(aggregate_id)`

Get current version:

```python
version = await repo.get_version(order_id)
```

#### `create_new(aggregate_id)`

Create new aggregate instance (in memory only):

```python
order = repo.create_new(uuid4())
order.create(customer_id=customer_id)
await repo.save(order)
```

#### `create_snapshot(aggregate)`

Manually create a snapshot:

```python
order = await repo.load(order_id)
snapshot = await repo.create_snapshot(order)
print(f"Created snapshot at version {snapshot.version}")
```

**Note:** Requires `snapshot_store` to be configured.

#### `await_pending_snapshots()`

Wait for background snapshot tasks (useful for testing):

```python
await repo.save(aggregate)  # Triggers background snapshot
count = await repo.await_pending_snapshots()
print(f"Waited for {count} background snapshots")
```

### Snapshot Properties

| Property | Type | Description |
|----------|------|-------------|
| `snapshot_store` | `SnapshotStore \| None` | The configured snapshot store |
| `snapshot_threshold` | `int \| None` | Events between snapshots |
| `snapshot_mode` | `str` | Current snapshot mode |
| `has_snapshot_support` | `bool` | Whether snapshotting is enabled |
| `pending_snapshot_count` | `int` | Background tasks not yet complete |

### Complete Example

```python
import asyncio
from uuid import uuid4
from eventsource import AggregateRepository, InMemoryEventStore

async def main():
    # Setup
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

    # Create order
    order_id = uuid4()
    customer_id = uuid4()

    order = repo.create_new(order_id)
    order.create(customer_id=customer_id)
    order.add_item(uuid4(), quantity=2, price=29.99)
    order.add_item(uuid4(), quantity=1, price=49.99)
    order.submit()

    await repo.save(order)

    # Reload and verify
    loaded = await repo.load(order_id)
    print(f"Status: {loaded.state.status}")
    print(f"Items: {len(loaded.state.items)}")
    print(f"Total: ${loaded.state.total:.2f}")
    print(f"Version: {loaded.version}")

asyncio.run(main())
```

---

## Error Handling

### AggregateNotFoundError

```python
from eventsource import AggregateNotFoundError

try:
    order = await repo.load(order_id)
except AggregateNotFoundError as e:
    print(f"Not found: {e.aggregate_id}")
    print(f"Type: {e.aggregate_type}")
```

### OptimisticLockError

Concurrent modification detected during save:

```python
from eventsource import OptimisticLockError

async def update_order(order_id: UUID, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            order = await repo.load(order_id)
            order.submit()
            await repo.save(order)
            return order
        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise
            # Retry with fresh state

raise RuntimeError("Max retries exceeded")
```

---

## Best Practices

### Command Validation

Validate commands before emitting events:

```python
def submit(self) -> None:
    # Validate state
    if not self.state:
        raise ValueError("Order not created")
    if self.state.status != "created":
        raise ValueError(f"Cannot submit order in {self.state.status} status")
    if not self.state.items:
        raise ValueError("Cannot submit empty order")

    # Emit event
    event = OrderSubmitted(...)
    self.apply_event(event)
```

### Immutable State

Use Pydantic's `model_copy()` for state updates:

```python
def _apply(self, event: DomainEvent) -> None:
    if isinstance(event, ItemAdded) and self._state:
        # Create new state object
        self._state = self._state.model_copy(update={
            "items": [*self._state.items, new_item],
            "total": self._state.total + item_total,
        })
```

### Separate Commands from Queries

Aggregates should only be used for writes:

```python
# Good: Load aggregate, execute command, save
order = await repo.load(order_id)
order.submit()
await repo.save(order)

# Bad: Using aggregate for queries
# Use projections/read models instead
```

### Keep Aggregates Small

Break large aggregates into smaller ones:

```python
# Instead of one huge OrderAggregate with all details,
# consider separate aggregates:
# - OrderAggregate (order lifecycle)
# - OrderLineItemAggregate (individual items)
# - ShippingAggregate (shipping details)
```

---

## See Also

- [Snapshots API Reference](./snapshots.md) - Complete snapshot store documentation
- [Snapshotting Guide](../guides/snapshotting.md) - How to enable and configure snapshotting
- [Snapshotting Migration Guide](../guides/snapshotting-migration.md) - Adding snapshotting to existing projects
- [Snapshotting Example](../examples/snapshotting.md) - Working code examples
- [Event Stores API](./stores.md) - Event store implementations
