# Aggregates API Reference

Aggregates are consistency boundaries in event sourcing. They encapsulate domain logic, maintain state by applying events, and enforce business invariants.

```python
from eventsource import (
    AggregateRoot,
    DeclarativeAggregate,
    AggregateRepository,
    handles,  # The @handles decorator for declarative event handling
)
```

---

## AggregateRoot

The base class for event-sourced aggregates.

### Class Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `aggregate_type` | `str` | `"Unknown"` | Type identifier for this aggregate (override in subclasses) |
| `schema_version` | `int` | `1` | Increment when state structure changes (invalidates old snapshots) |
| `validate_versions` | `bool` | `True` | When `True`, raises `EventVersionError` on version mismatch |

### Instance Properties

| Property | Type | Description |
|----------|------|-------------|
| `aggregate_id` | `UUID` | Unique identifier for this aggregate instance |
| `version` | `int` | Current version (number of events applied) |
| `state` | `TState \| None` | Current state (`None` for new aggregates) |
| `uncommitted_events` | `list[DomainEvent]` | Events waiting to be persisted |
| `has_uncommitted_events` | `bool` | Whether there are events to save |

### Key Methods

| Method | Description |
|--------|-------------|
| `apply_event(event, is_new=True)` | Apply event to aggregate; tracks for persistence if `is_new=True` |
| `_apply(event)` | **Abstract.** Update state based on event (implement in subclass) |
| `_get_initial_state()` | **Abstract.** Return initial state for new aggregates |
| `_raise_event(event)` | Convenience alias for `apply_event(event, is_new=True)` |
| `load_from_history(events)` | Reconstitute state by replaying historical events |
| `get_next_version()` | Get version number for the next event (`version + 1`) |
| `mark_events_as_committed()` | Clear uncommitted events after successful save |
| `clear_uncommitted_events()` | Clear and return uncommitted events |

### Creating an Aggregate

Building an aggregate requires three components: state model, events, and the aggregate class itself.

#### Step 1: Define the State Model

Use Pydantic for validation and serialization:

```python
from pydantic import BaseModel
from uuid import UUID

class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    status: str = "draft"
    items: list[dict] = []
    total: float = 0.0
```

#### Step 2: Define Domain Events

Events capture what happened. Register them for serialization:

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

#### Step 3: Implement the Aggregate

```python
from eventsource import AggregateRoot

class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        """Route events to state changes."""
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                status="created",
            )
        elif isinstance(event, ItemAdded):
            if self._state:
                self._state = self._state.model_copy(update={
                    "items": [*self._state.items, {
                        "product_id": str(event.product_id),
                        "quantity": event.quantity,
                        "price": event.price,
                    }],
                    "total": self._state.total + (event.quantity * event.price),
                })
        elif isinstance(event, OrderSubmitted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "submitted"})

    # Commands validate business rules then emit events
    def create(self, customer_id: UUID) -> None:
        if self.version > 0:
            raise ValueError("Order already created")
        self._raise_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            aggregate_version=self.get_next_version(),
        ))

    def add_item(self, product_id: UUID, quantity: int, price: float) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Order must be created before adding items")
        self._raise_event(ItemAdded(
            aggregate_id=self.aggregate_id,
            product_id=product_id,
            quantity=quantity,
            price=price,
            aggregate_version=self.get_next_version(),
        ))

    def submit(self) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Order must be created before submitting")
        if not self.state.items:
            raise ValueError("Cannot submit empty order")
        self._raise_event(OrderSubmitted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))
```

### Version Validation

By default, aggregates validate that events arrive in sequence. Disable for special cases:

```python
class LenientAggregate(AggregateRoot[MyState]):
    validate_versions = False  # Log warnings instead of raising errors
```

When `validate_versions=True` (default) and a version mismatch occurs, `EventVersionError` is raised.

---

## DeclarativeAggregate

Use `@handles` decorators instead of `if/elif` chains. Cleaner and more maintainable for aggregates with many event types.

### Basic Usage

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
            self._state = self._state.model_copy(update={
                "items": [*self._state.items, {"product_id": str(event.product_id)}],
            })

    @handles(OrderSubmitted)
    def _on_order_submitted(self, event: OrderSubmitted) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "submitted"})

    # Commands remain the same as AggregateRoot
    def create(self, customer_id: UUID) -> None:
        ...
```

### Unregistered Event Handling

Control behavior when an event has no `@handles` decorator:

| Mode | Behavior |
|------|----------|
| `"ignore"` | Silently skip (default, forward-compatible) |
| `"warn"` | Log a warning |
| `"error"` | Raise `UnhandledEventError` |

```python
class StrictOrderAggregate(DeclarativeAggregate[OrderState]):
    unregistered_event_handling = "error"  # Fail fast on missing handlers

    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        ...
    # If OrderShipped arrives without a handler, raises UnhandledEventError
```

### Benefits

- No `if/elif` chains in `_apply()`
- Self-documenting handler methods
- Easy to add new event types
- Forward-compatible by default (ignores unknown events)

---

## AggregateRepository

Abstracts aggregate persistence: loading from event history and saving new events.

> **For comprehensive usage patterns, see the [Repository Pattern Guide](../guides/repository-pattern.md).**

### Constructor

```python
from eventsource import AggregateRepository, InMemoryEventStore

repo = AggregateRepository(
    event_store=InMemoryEventStore(),
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=event_bus,       # Optional: broadcast events after save
    snapshot_store=snapshot_store,   # Optional: enable snapshotting
    snapshot_threshold=100,          # Optional: auto-snapshot every N events
    snapshot_mode="sync",            # "sync" | "background" | "manual"
    enable_tracing=True,             # Optional: OpenTelemetry support
)
```

### Core Methods

```python
# Load existing aggregate (raises AggregateNotFoundError if missing)
order = await repo.load(order_id)

# Load or create new if not found
order = await repo.load_or_create(order_id)

# Create new (in-memory only, not persisted)
order = repo.create_new(uuid4())

# Save uncommitted events (uses optimistic locking)
await repo.save(order)

# Check existence without loading
if await repo.exists(order_id):
    ...

# Get current version
version = await repo.get_version(order_id)
```

### Snapshot Configuration

Snapshots speed up loading for aggregates with many events:

```python
from eventsource import AggregateRepository, PostgreSQLEventStore
from eventsource.snapshots import PostgreSQLSnapshotStore

repo = AggregateRepository(
    event_store=PostgreSQLEventStore(session_factory),
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=PostgreSQLSnapshotStore(session_factory),
    snapshot_threshold=100,      # Auto-snapshot every 100 events
    snapshot_mode="background",  # Non-blocking
)
```

| Mode | Behavior |
|------|----------|
| `"sync"` | Create snapshot immediately after save (blocking) |
| `"background"` | Create snapshot asynchronously |
| `"manual"` | Only via explicit `create_snapshot()` call |

```python
# Manual snapshot creation
snapshot = await repo.create_snapshot(order)

# Wait for background snapshots (useful in tests)
count = await repo.await_pending_snapshots()
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `aggregate_type` | `str` | The aggregate type this repository manages |
| `event_store` | `EventStore` | The configured event store |
| `event_publisher` | `EventPublisher \| None` | The event bus, if configured |
| `snapshot_store` | `SnapshotStore \| None` | The snapshot store, if configured |
| `snapshot_threshold` | `int \| None` | Events between auto-snapshots |
| `snapshot_mode` | `Literal["sync", "background", "manual"]` | When snapshots are created |
| `has_snapshot_support` | `bool` | `True` if snapshot_store is configured |
| `pending_snapshot_count` | `int` | Number of pending background snapshots |

### Complete Example

```python
import asyncio
from uuid import uuid4
from eventsource import AggregateRepository, InMemoryEventStore

async def main():
    repo = AggregateRepository(
        event_store=InMemoryEventStore(),
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

    # Create and persist
    order = repo.create_new(uuid4())
    order.create(customer_id=uuid4())
    order.add_item(uuid4(), quantity=2, price=29.99)
    order.submit()
    await repo.save(order)

    # Reload from event store
    loaded = await repo.load(order.aggregate_id)
    print(f"Status: {loaded.state.status}, Version: {loaded.version}")

asyncio.run(main())
```

---

## Error Handling

### AggregateNotFoundError

Raised when loading an aggregate with no events:

```python
from eventsource import AggregateNotFoundError

try:
    order = await repo.load(order_id)
except AggregateNotFoundError as e:
    print(f"Aggregate {e.aggregate_id} of type {e.aggregate_type} not found")
```

### OptimisticLockError

Raised on concurrent modification. Handle with retry:

```python
from eventsource import OptimisticLockError

async def update_with_retry(order_id: UUID, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            order = await repo.load(order_id)
            order.submit()
            await repo.save(order)
            return order
        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise
            # Reload fresh state and retry
```

### EventVersionError

Raised when event versions are out of sequence (if `validate_versions=True`):

```python
from eventsource import EventVersionError

try:
    aggregate.apply_event(event)
except EventVersionError as e:
    print(f"Expected v{e.expected_version}, got v{e.actual_version}")
```

### UnhandledEventError

Raised by `DeclarativeAggregate` when `unregistered_event_handling="error"` and no handler exists:

```python
from eventsource.exceptions import UnhandledEventError

try:
    aggregate.apply_event(unknown_event)
except UnhandledEventError as e:
    print(f"No handler for {e.event_type} in {e.handler_class}")
```

---

## Aggregate Lifecycle

```
                    +------------------+
                    |   Create New     |
                    |  repo.create_new |
                    +--------+---------+
                             |
                             v
+------------------+    +----+----+
|  Load from Store |    | version |
| repo.load(id)    |--->|   = 0   |
+------------------+    +----+----+
                             |
                    Execute Commands
                   (emit & apply events)
                             |
                             v
                    +--------+--------+
                    | uncommitted     |
                    | events > 0      |
                    +--------+--------+
                             |
                        repo.save()
                             |
                             v
                    +--------+--------+
                    | Events persisted|
                    | version updated |
                    +--------+--------+
                             |
                   (Reload to continue)
```

---

## Best Practices

### Validate Before Emitting

Commands should validate business rules before raising events:

```python
def submit(self) -> None:
    if not self.state:
        raise ValueError("Order not created")
    if self.state.status != "created":
        raise ValueError(f"Cannot submit {self.state.status} order")
    if not self.state.items:
        raise ValueError("Cannot submit empty order")

    self._raise_event(OrderSubmitted(...))
```

### Immutable State Updates

Use `model_copy()` to create new state objects:

```python
def _apply(self, event: DomainEvent) -> None:
    if isinstance(event, ItemAdded) and self._state:
        self._state = self._state.model_copy(update={
            "items": [*self._state.items, new_item],
            "total": self._state.total + item_total,
        })
```

### Aggregates Are for Writes

Use aggregates only for commands. For queries, use projections:

```python
# Correct: Load, command, save
order = await repo.load(order_id)
order.submit()
await repo.save(order)

# Wrong: Using aggregates for reads
# Use projections/read models instead
```

### Keep Aggregates Small

Large aggregates are slow to load and prone to conflicts. Split by consistency boundary:

```
Instead of one giant OrderAggregate:
  - OrderAggregate (lifecycle)
  - InventoryAggregate (stock levels)
  - ShippingAggregate (delivery)
```

---

## Schema Versioning

When your state model changes, increment `schema_version` to invalidate old snapshots:

```python
class OrderAggregate(AggregateRoot[OrderStateV2]):
    aggregate_type = "Order"
    schema_version = 2  # Was 1, incremented after OrderState changed

    def _get_initial_state(self) -> OrderStateV2:
        return OrderStateV2(order_id=self.aggregate_id)
```

Mismatched versions trigger full event replay instead of using stale snapshots.

---

## See Also

- [Repository Pattern Guide](../guides/repository-pattern.md) - Comprehensive repository usage guide
- [Snapshots API Reference](./snapshots.md) - Snapshot store documentation
- [Snapshotting Guide](../guides/snapshotting.md) - Enable and configure snapshots
- [Events API Reference](./events.md) - Domain event documentation
- [Event Stores API Reference](./stores.md) - Event store implementations
- [ADR-0003: Optimistic Locking](../adrs/0003-optimistic-locking.md) - Concurrency control design
- [Basic Order Example](../examples/basic-order.md) - Complete working example
