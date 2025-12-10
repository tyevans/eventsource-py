# Repository Pattern Guide

The Repository Pattern is a cornerstone of event sourcing, providing a clean abstraction between your domain aggregates and the underlying event store. This guide covers everything you need to know about using `AggregateRepository` effectively.

## What is a Repository?

In event sourcing, a repository:

1. **Loads aggregates** by replaying their event history
2. **Saves aggregates** by appending new events to the store
3. **Coordinates** with snapshots, event buses, and concurrency control
4. **Abstracts** storage details from your domain logic

```
+-------------------+          +-------------------+          +-------------------+
|   Application     |   load   |   Repository      |  events  |   Event Store     |
|   (Commands)      |--------->|                   |--------->|                   |
|                   |<---------|                   |<---------|                   |
|                   |   save   |                   |          |                   |
+-------------------+          +--------+----------+          +-------------------+
                                        |
                                        | (optional)
                                        v
                               +-------------------+
                               | Snapshot Store    |
                               | Event Bus         |
                               +-------------------+
```

---

## Basic Usage

### Creating a Repository

```python
from eventsource import AggregateRepository, InMemoryEventStore

event_store = InMemoryEventStore()

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
)
```

**Required parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `event_store` | `EventStore` | Where events are persisted |
| `aggregate_factory` | `type[TAggregate]` | Class to instantiate when loading |
| `aggregate_type` | `str` | Type identifier (e.g., "Order") |

### Core Operations

```python
from uuid import uuid4

# Create a new aggregate (in-memory only)
order = repo.create_new(uuid4())

# Apply commands
order.create(customer_id=uuid4())
order.add_item(product_id=uuid4(), quantity=2, price=29.99)

# Persist to event store
await repo.save(order)

# Load an existing aggregate
loaded = await repo.load(order.aggregate_id)

# Check if aggregate exists
exists = await repo.exists(order.aggregate_id)

# Get current version without loading full state
version = await repo.get_version(order.aggregate_id)
```

---

## Loading Aggregates

### `load(aggregate_id)` - Standard Load

Loads an aggregate by replaying all its events. Raises `AggregateNotFoundError` if the aggregate doesn't exist.

```python
from eventsource import AggregateNotFoundError

try:
    order = await repo.load(order_id)
    print(f"Loaded order at version {order.version}")
except AggregateNotFoundError as e:
    print(f"Order {e.aggregate_id} not found")
```

**What happens internally:**

1. Fetch all events for the aggregate from the event store
2. Create a new aggregate instance using `aggregate_factory`
3. Replay events via `load_from_history()`
4. Return the hydrated aggregate

### `load_or_create(aggregate_id)` - Idempotent Load

Loads existing or creates new. Useful when you don't know if an aggregate exists.

```python
order = await repo.load_or_create(order_id)

if order.version == 0:
    # New aggregate - needs initialization
    order.create(customer_id=customer_id)
else:
    # Existing aggregate - ready to use
    order.add_item(...)

await repo.save(order)
```

### `get_or_raise(aggregate_id)` - Explicit Intent

Alias for `load()`. Use when your code explicitly expects the aggregate to exist:

```python
# Clear intent: we expect this order to exist
order = await repo.get_or_raise(order_id)
order.ship(tracking_number)
await repo.save(order)
```

---

## Saving Aggregates

The `save()` method persists all uncommitted events atomically:

```python
order = repo.create_new(uuid4())
order.create(customer_id=uuid4())
order.add_item(product_id=uuid4(), quantity=1, price=10.00)
order.add_item(product_id=uuid4(), quantity=2, price=15.00)

# All three events saved atomically
await repo.save(order)

# After save, no uncommitted events remain
assert not order.has_uncommitted_events
assert order.version == 3
```

**Save behavior:**

1. If no uncommitted events, returns immediately (no-op)
2. Calculates expected version for optimistic locking
3. Appends events to event store atomically
4. Marks events as committed on the aggregate
5. Publishes events to event bus (if configured)
6. Creates snapshot if threshold met (if configured)

---

## Optimistic Concurrency Control

The repository uses version-based optimistic locking to prevent lost updates:

```python
from eventsource import OptimisticLockError

# Two processes load the same aggregate
order_a = await repo.load(order_id)  # version 5
order_b = await repo.load(order_id)  # version 5

# Both modify it
order_a.add_item(product_a, 1, 10.00)  # expects version 5
order_b.add_item(product_b, 2, 20.00)  # expects version 5

# First save succeeds
await repo.save(order_a)  # now version 6

# Second save fails - version conflict
try:
    await repo.save(order_b)  # expected 5, actual 6
except OptimisticLockError as e:
    print(f"Conflict! Expected v{e.expected_version}, actual v{e.actual_version}")
```

### Recommended Retry Pattern

```python
async def update_with_retry(
    repo: AggregateRepository,
    aggregate_id: UUID,
    command: Callable,
    max_retries: int = 3,
):
    """Execute command with automatic retry on conflict."""
    for attempt in range(max_retries):
        try:
            aggregate = await repo.load(aggregate_id)
            command(aggregate)
            await repo.save(aggregate)
            return aggregate
        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise
            # Loop continues with fresh load
    raise RuntimeError("Unreachable")

# Usage
order = await update_with_retry(
    repo, order_id,
    lambda o: o.add_item(product_id, 1, 10.00)
)
```

---

## Event Publishing Integration

Connect a repository to an event bus to automatically publish events after save:

```python
from eventsource import InMemoryEventBus

event_bus = InMemoryEventBus()

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=event_bus,  # Enable auto-publishing
)

# When you save, events are published to the bus
order = repo.create_new(uuid4())
order.create(customer_id=uuid4())
await repo.save(order)  # OrderCreated published to event_bus
```

**Publishing happens after successful persistence**, ensuring events are never published without being stored.

### With Projections

```python
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
)

# Subscribe projection
await manager.subscribe(order_projection, config=SubscriptionConfig())
await manager.start()

# Events flow: Aggregate -> Repository -> Event Store -> Event Bus -> Projection
```

---

## Snapshot Integration

For aggregates with many events, snapshots dramatically improve load performance:

```python
from eventsource.snapshots import InMemorySnapshotStore

snapshot_store = InMemorySnapshotStore()

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,   # Auto-snapshot every 100 events
    snapshot_mode="sync",     # Create synchronously after save
)
```

### Snapshot Modes

| Mode | Behavior | Best For |
|------|----------|----------|
| `"sync"` | Create snapshot immediately after save | Most applications |
| `"background"` | Create snapshot asynchronously | High-throughput scenarios |
| `"manual"` | Only via explicit `create_snapshot()` | Custom snapshotting strategies |

### Manual Snapshot Creation

```python
# Create snapshot at a business milestone
order = await repo.load(order_id)
order.complete()
await repo.save(order)

# Explicitly create snapshot after completion
snapshot = await repo.create_snapshot(order)
print(f"Snapshot at version {snapshot.version}")
```

### How Loading Works with Snapshots

```
1. Check for snapshot
   |
   v
2. Snapshot exists? ----No----> Get all events (version 0)
   |                                    |
   Yes                                  v
   |                            Apply all events
   v                                    |
3. Schema version matches? --No-->     |
   |                                    |
   Yes                                  |
   v                                    |
4. Restore from snapshot               |
   |                                    |
   v                                    |
5. Get events since snapshot           |
   |                                    |
   v                                    |
6. Apply recent events                 |
   |                                    |
   +----------> Return aggregate <------+
```

For full snapshot documentation, see the [Snapshotting Guide](./snapshotting.md).

---

## Observability

The repository supports OpenTelemetry tracing out of the box:

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    enable_tracing=True,  # Default: True
)
```

**Traced operations:**

| Span Name | Attributes |
|-----------|------------|
| `eventsource.repository.load` | `aggregate_id`, `aggregate_type`, `version`, `snapshot.used` |
| `eventsource.repository.save` | `aggregate_id`, `aggregate_type`, `event_count`, `version` |
| `eventsource.repository.exists` | `aggregate_id`, `aggregate_type`, `exists` |
| `eventsource.repository.create_snapshot` | `aggregate_id`, `aggregate_type`, `version` |

To disable tracing (e.g., in tests):

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    enable_tracing=False,
)
```

---

## Repository Properties

Access repository configuration via properties:

```python
repo.aggregate_type     # "Order"
repo.event_store        # The configured event store
repo.event_publisher    # The event bus (or None)
repo.snapshot_store     # The snapshot store (or None)
repo.snapshot_threshold # Events between auto-snapshots (or None)
repo.snapshot_mode      # "sync" | "background" | "manual"
repo.has_snapshot_support  # True if snapshot_store is configured
```

---

## Complete Example

```python
import asyncio
from uuid import UUID, uuid4
from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    AggregateRepository,
    InMemoryEventStore,
    InMemoryEventBus,
    OptimisticLockError,
    AggregateNotFoundError,
)
from eventsource.snapshots import InMemorySnapshotStore


# Events
@register_event
class AccountOpened(DomainEvent):
    event_type: str = "AccountOpened"
    aggregate_type: str = "Account"
    owner_name: str
    initial_balance: float = 0.0


@register_event
class MoneyDeposited(DomainEvent):
    event_type: str = "MoneyDeposited"
    aggregate_type: str = "Account"
    amount: float


@register_event
class MoneyWithdrawn(DomainEvent):
    event_type: str = "MoneyWithdrawn"
    aggregate_type: str = "Account"
    amount: float


# State
class AccountState(BaseModel):
    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0


# Aggregate
class AccountAggregate(AggregateRoot[AccountState]):
    aggregate_type = "Account"
    schema_version = 1  # For snapshot compatibility

    def _get_initial_state(self) -> AccountState:
        return AccountState(account_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountOpened):
            self._state = AccountState(
                account_id=self.aggregate_id,
                owner_name=event.owner_name,
                balance=event.initial_balance,
            )
        elif isinstance(event, MoneyDeposited) and self._state:
            self._state = self._state.model_copy(
                update={"balance": self._state.balance + event.amount}
            )
        elif isinstance(event, MoneyWithdrawn) and self._state:
            self._state = self._state.model_copy(
                update={"balance": self._state.balance - event.amount}
            )

    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        if self.version > 0:
            raise ValueError("Account already opened")
        self.apply_event(AccountOpened(
            aggregate_id=self.aggregate_id,
            owner_name=owner_name,
            initial_balance=initial_balance,
            aggregate_version=self.get_next_version(),
        ))

    def deposit(self, amount: float) -> None:
        if not self.state:
            raise ValueError("Account not opened")
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")
        self.apply_event(MoneyDeposited(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        ))

    def withdraw(self, amount: float) -> None:
        if not self.state:
            raise ValueError("Account not opened")
        if amount <= 0:
            raise ValueError("Withdrawal amount must be positive")
        if self.state.balance < amount:
            raise ValueError("Insufficient funds")
        self.apply_event(MoneyWithdrawn(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        ))


async def main():
    # Setup infrastructure
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    snapshot_store = InMemorySnapshotStore()

    # Create repository with full features
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        event_publisher=event_bus,
        snapshot_store=snapshot_store,
        snapshot_threshold=50,
        snapshot_mode="sync",
    )

    # Create a new account
    account_id = uuid4()
    account = repo.create_new(account_id)
    account.open(owner_name="Alice Smith", initial_balance=100.0)
    await repo.save(account)
    print(f"Account opened: {account.state.owner_name}")

    # Deposit money
    account = await repo.load(account_id)
    account.deposit(250.0)
    await repo.save(account)
    print(f"After deposit: ${account.state.balance}")

    # Withdraw money
    account = await repo.load(account_id)
    account.withdraw(75.0)
    await repo.save(account)
    print(f"After withdrawal: ${account.state.balance}")

    # Check version
    version = await repo.get_version(account_id)
    print(f"Account version: {version}")

    # Try to overdraw (should fail)
    account = await repo.load(account_id)
    try:
        account.withdraw(500.0)
    except ValueError as e:
        print(f"Withdrawal rejected: {e}")

    # Final state
    final = await repo.load(account_id)
    print(f"Final balance: ${final.state.balance}")


asyncio.run(main())
```

**Output:**

```
Account opened: Alice Smith
After deposit: $350.0
After withdrawal: $275.0
Account version: 3
Withdrawal rejected: Insufficient funds
Final balance: $275.0
```

---

## Best Practices

### 1. One Repository Per Aggregate Type

```python
# Good: Separate repositories
order_repo = AggregateRepository(store, OrderAggregate, "Order")
account_repo = AggregateRepository(store, AccountAggregate, "Account")

# Bad: Trying to reuse repositories for different types
```

### 2. Load-Modify-Save Pattern

Always reload aggregates before modifications in separate operations:

```python
# Good: Load fresh before each operation
order = await repo.load(order_id)
order.add_item(...)
await repo.save(order)

order = await repo.load(order_id)
order.ship(...)
await repo.save(order)

# Bad: Keeping aggregates in memory across requests
```

### 3. Use Transactions Wisely

The repository doesn't manage database transactions. If you need transactional guarantees across multiple operations, handle this at the application layer:

```python
async with session.begin():
    order = await order_repo.load(order_id)
    order.complete()
    await order_repo.save(order)
    # Other database operations...
```

### 4. Configure Snapshots for High-Event Aggregates

```python
# If aggregates typically have >50 events, enable snapshotting
repo = AggregateRepository(
    ...,
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
)
```

### 5. Use Event Publishing for Decoupling

```python
# Connect event bus to enable projections and handlers
repo = AggregateRepository(
    ...,
    event_publisher=event_bus,
)
```

---

## See Also

- [Aggregates API Reference](../api/aggregates.md) - Detailed API documentation
- [Event Stores API Reference](../api/stores.md) - Event store implementations
- [Snapshotting Guide](./snapshotting.md) - Snapshot configuration and usage
- [ADR-0003: Optimistic Locking](../adrs/0003-optimistic-locking.md) - Concurrency control design
- [Getting Started Guide](../getting-started.md) - Initial setup tutorial
