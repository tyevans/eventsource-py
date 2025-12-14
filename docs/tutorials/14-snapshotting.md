# Tutorial 14: Optimizing with Aggregate Snapshotting

**Difficulty:** Intermediate-Advanced | **Progress:** Tutorial 14 of 21 | **Phase:** 3: Production Readiness

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- [Tutorial 13: Subscription Management](13-subscriptions.md)
- Python 3.10 or higher
- Understanding of aggregates and repositories
- Understanding of async/await

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain when and why to use snapshots for performance optimization
2. Understand the snapshot data model and storage interface
3. Configure repositories with snapshot stores
4. Choose between snapshot modes (sync, background, manual)
5. Implement snapshot strategies for different scenarios
6. Handle schema versioning for aggregate state evolution
7. Use all three snapshot store implementations (InMemory, PostgreSQL, SQLite)
8. Monitor snapshot performance and manage snapshot lifecycle

---

## What are Snapshots?

**Snapshots** are point-in-time captures of aggregate state. They're a performance optimization that allows aggregates to be loaded quickly by skipping event replay for events that occurred before the snapshot.

### The Problem: Event Replay Performance

As aggregates accumulate events over time, loading them becomes slower:

| Event Count | Without Snapshot | With Snapshot (every 100) | Improvement |
|-------------|------------------|---------------------------|-------------|
| 100         | ~50ms            | ~15ms                     | 3x faster   |
| 1,000       | ~500ms           | ~20ms                     | 25x faster  |
| 10,000      | ~5s              | ~60ms                     | 83x faster  |
| 100,000     | ~60s             | ~100ms                    | 600x faster |

**Why does event replay get slow?**

Each event requires:
1. Deserialization from JSON
2. Pydantic model validation
3. Calling `_apply()` to update state
4. Creating a new state object (immutable updates)

Multiply this by thousands of events, and loading a single aggregate can take seconds.

### The Solution: Snapshot + Partial Replay

Instead of replaying all events:

1. **Load snapshot**: Deserialize state directly (one operation)
2. **Replay new events**: Only events since the snapshot
3. **Result**: Current state with minimal overhead

```
Without snapshot:
Events 1-10,000 → Replay all 10,000 → Current state (5 seconds)

With snapshot at event 9,900:
Snapshot → Load state → Replay events 9,901-10,000 → Current state (60ms)
```

---

## When to Use Snapshots

**Good candidates for snapshotting:**

- **Long-lived aggregates**: Orders, users, accounts that accumulate many events
- **High-load aggregates**: Frequently accessed aggregates (hot paths)
- **Event-heavy workflows**: Aggregates with hundreds of state transitions
- **Performance-critical reads**: When aggregate load time impacts user experience

**When snapshots may not help:**

- **Short-lived aggregates**: Most aggregates have < 20 events
- **Rarely accessed aggregates**: Snapshot overhead > benefit
- **Simple state**: If event replay is already fast (< 10ms)
- **Write-heavy, read-light**: If you rarely load aggregates

**Rule of thumb:** Start without snapshots. Add them when you measure load times > 100ms.

---

## The Snapshot Data Model

Snapshots are immutable data structures with these fields:

```python
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

@dataclass(frozen=True)
class Snapshot:
    aggregate_id: UUID          # Which aggregate instance
    aggregate_type: str         # Type name (e.g., "Order")
    version: int                # Aggregate version when snapshot was taken
    state: dict[str, Any]       # Serialized state (JSON-compatible)
    schema_version: int         # Schema version for compatibility
    created_at: datetime        # When snapshot was created
```

**Key properties:**

- **One snapshot per aggregate**: Latest snapshot replaces older ones (upsert semantics)
- **Version tracking**: Snapshot captures state at specific aggregate version
- **Schema versioning**: Detects incompatible state changes
- **Immutable**: Snapshots never change after creation
- **Rebuildable**: Can be safely deleted and regenerated from events

---

## Snapshot Store Interface

All snapshot stores implement the `SnapshotStore` protocol:

```python
from abc import ABC, abstractmethod
from eventsource.snapshots import Snapshot, SnapshotStore

class SnapshotStore(ABC):
    @abstractmethod
    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save or update a snapshot (upsert semantics)."""
        pass

    @abstractmethod
    async def get_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> Snapshot | None:
        """Get the latest snapshot, or None if not found."""
        pass

    @abstractmethod
    async def delete_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """Delete a snapshot. Returns True if deleted, False if none existed."""
        pass

    async def snapshot_exists(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """Check if a snapshot exists."""
        pass

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """Delete snapshots for all aggregates of a type."""
        pass
```

---

## Snapshot Store Implementations

### InMemorySnapshotStore

**Use for:** Development, testing, and local experiments.

```python
from eventsource.snapshots import InMemorySnapshotStore

snapshot_store = InMemorySnapshotStore()
```

**Features:**
- Fast in-memory dictionary storage
- Thread-safe with asyncio.Lock
- `clear()` method for test cleanup
- `snapshot_count` property for diagnostics

**Limitations:**
- Data lost on process restart
- Single-process only (no sharing across workers)
- Not suitable for production

**Example:**
```python
from eventsource.snapshots import InMemorySnapshotStore, Snapshot
from datetime import datetime, UTC
from uuid import uuid4

store = InMemorySnapshotStore()

# Save snapshot
snapshot = Snapshot(
    aggregate_id=uuid4(),
    aggregate_type="Order",
    version=100,
    state={"status": "shipped", "total": 99.99},
    schema_version=1,
    created_at=datetime.now(UTC),
)
await store.save_snapshot(snapshot)

# Retrieve snapshot
loaded = await store.get_snapshot(snapshot.aggregate_id, "Order")
assert loaded == snapshot

# Test cleanup
await store.clear()
```

---

### PostgreSQLSnapshotStore

**Use for:** Production deployments with PostgreSQL.

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource.snapshots import PostgreSQLSnapshotStore

# Create async engine
engine = create_async_engine(
    "postgresql+asyncpg://user:pass@localhost:5432/mydb",
    pool_size=10,
    max_overflow=20,
)

# Create session factory
session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,  # Recommended for performance
)

# Create snapshot store
snapshot_store = PostgreSQLSnapshotStore(session_factory)
```

**Features:**
- ACID guarantees for snapshot persistence
- Upsert semantics via `INSERT ... ON CONFLICT DO UPDATE`
- Efficient single-row lookups
- Bulk delete by aggregate type
- Multi-process safe
- OpenTelemetry tracing support

**Database Schema:**

You need to create the snapshots table before using this store:

```sql
CREATE TABLE snapshots (
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    state JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (aggregate_id, aggregate_type)
);

-- Index for bulk operations
CREATE INDEX idx_snapshots_type_schema
ON snapshots(aggregate_type, schema_version);
```

**Example:**
```python
from eventsource.snapshots import PostgreSQLSnapshotStore

# Production configuration
snapshot_store = PostgreSQLSnapshotStore(
    session_factory,
    enable_tracing=True,  # OpenTelemetry tracing
)

# Use with repository
repo = AggregateRepository(
    event_store=PostgreSQLEventStore(session_factory),
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
)
```

---

### SQLiteSnapshotStore

**Use for:** Embedded apps, desktop applications, edge computing, single-instance deployments.

**Installation:**
```bash
pip install eventsource-py[sqlite]
```

**Usage:**
```python
from eventsource.snapshots import SQLiteSnapshotStore

# File-based database
snapshot_store = SQLiteSnapshotStore("./snapshots.db")

# In-memory database (testing)
snapshot_store = SQLiteSnapshotStore(":memory:")
```

**Features:**
- No external database server required
- Async operations via aiosqlite
- File-based persistence
- Upsert via `INSERT OR REPLACE`
- Lightweight and portable

**Database Schema:**

```sql
CREATE TABLE snapshots (
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    state TEXT NOT NULL,  -- JSON as TEXT
    created_at TEXT NOT NULL,  -- ISO format timestamp
    PRIMARY KEY (aggregate_id, aggregate_type)
);

CREATE INDEX idx_snapshots_type_schema
ON snapshots(aggregate_type, schema_version);
```

**Limitations:**
- Single-writer (slower concurrent writes than PostgreSQL)
- Not recommended for high-concurrency workloads
- Less mature than PostgreSQL for production

**Example:**
```python
from eventsource.snapshots import SQLiteSnapshotStore

snapshot_store = SQLiteSnapshotStore("./data/snapshots.db")

repo = AggregateRepository(
    event_store=SQLiteEventStore("./data/events.db"),
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=50,
)
```

---

## Repository Configuration with Snapshots

Enable snapshots by providing a `snapshot_store` to your repository:

```python
from eventsource import AggregateRepository, InMemoryEventStore
from eventsource.snapshots import InMemorySnapshotStore

event_store = InMemoryEventStore()
snapshot_store = InMemorySnapshotStore()

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,  # Create snapshot every 100 events
    snapshot_mode="sync",    # "sync" | "background" | "manual"
)
```

**Configuration parameters:**

- **`snapshot_store`**: The snapshot store implementation (required for snapshots)
- **`snapshot_threshold`**: Number of events between automatic snapshots
- **`snapshot_mode`**: When and how to create snapshots

---

## Snapshot Modes

Snapshots can be created in three different modes, each suited for different scenarios:

### Sync Mode (Default)

**When to use:** Development, testing, low-throughput scenarios.

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
    snapshot_mode="sync",  # Blocks until snapshot is written
)
```

**Behavior:**
- Snapshot created immediately after save
- Blocks until snapshot is persisted
- Adds latency to save operations (typically 5-20ms)
- Guaranteed snapshot exists after `save()` returns

**Advantages:**
- Simple and predictable
- No background tasks to manage
- Snapshots always up-to-date

**Disadvantages:**
- Increases save latency
- Can slow down high-throughput writes

---

### Background Mode

**When to use:** Production, high-throughput scenarios, when save latency is critical.

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
    snapshot_mode="background",  # Non-blocking
)
```

**Behavior:**
- Snapshot created in background task
- `save()` returns immediately (no blocking)
- Background task runs asynchronously
- Errors logged but don't fail save operation

**Advantages:**
- Zero impact on save latency
- Best for high-throughput scenarios
- Snapshots created asynchronously

**Disadvantages:**
- Snapshot may not exist immediately after save
- Need to await pending tasks in tests

**Testing with background mode:**
```python
# In tests, wait for background tasks
await repo.await_pending_snapshots()

# Or access the strategy directly
if isinstance(repo._snapshot_strategy, BackgroundSnapshotStrategy):
    count = await repo._snapshot_strategy.await_pending()
    print(f"Awaited {count} background snapshots")
```

---

### Manual Mode

**When to use:** Custom snapshot timing, business milestone snapshots, full control.

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_mode="manual",  # No automatic snapshots
)
```

**Behavior:**
- No automatic snapshot creation
- Snapshots only created via explicit `create_snapshot()` call
- Threshold parameter ignored

**Use cases:**
- Create snapshots at business milestones (e.g., order shipped, payment completed)
- Batch snapshot creation during off-peak hours
- Fine-grained control over snapshot timing

**Example:**
```python
# Load aggregate
order = await repo.load(order_id)

# Execute business logic
order.ship()
await repo.save(order)

# Create snapshot at important milestone
if order.state.status == "shipped":
    snapshot = await repo.create_snapshot(order)
    print(f"Created snapshot at version {snapshot.version}")
```

---

## Snapshot Threshold Guidelines

The threshold determines how often snapshots are created. Choose based on your workload:

**General guidelines:**

- **Low traffic (< 10 commands/day)**: threshold = 500-1000
  - Aggregates change infrequently
  - Snapshot overhead not worth it for small counts

- **Medium traffic (10-100 commands/day)**: threshold = 50-100
  - Balance between snapshot frequency and overhead
  - Good default for most applications

- **High traffic (> 100 commands/day)**: threshold = 20-50
  - Frequent snapshots keep load times low
  - Snapshot overhead amortized over many loads

**Tuning tips:**

1. **Measure first**: Use logging to track aggregate load times
2. **Start conservative**: Begin with threshold = 100
3. **Monitor**: Track snapshot creation frequency vs aggregate loads
4. **Adjust**: Increase threshold if too many snapshots, decrease if loads are slow

**Example monitoring:**
```python
import logging

logger = logging.getLogger(__name__)

# Before load
start = time.perf_counter()
order = await repo.load(order_id)
duration = time.perf_counter() - start

logger.info(
    "Loaded Order/%s at v%d in %.2fms (snapshot_at=v%d)",
    order_id,
    order.version,
    duration * 1000,
    order.version // 100 * 100,  # Last snapshot version
)
```

---

## Schema Versioning

When aggregate state changes incompatibly, you need to handle schema versioning:

### Defining Schema Version

```python
from eventsource import AggregateRoot
from pydantic import BaseModel

class OrderState(BaseModel):
    order_id: UUID
    status: str
    total: float
    # In v2, we added shipping_address field

class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
    schema_version = 2  # Increment when state model changes incompatibly
```

**When to increment:**

- Adding required fields without defaults
- Removing fields
- Renaming fields
- Changing field types
- Restructuring nested objects

**When NOT to increment:**

- Adding optional fields with defaults
- Adding new event types
- Changing business logic (not state structure)

### Schema Version Mismatch Handling

When loading an aggregate:

1. **Load snapshot**: Check `snapshot.schema_version`
2. **Compare**: Against `aggregate_class.schema_version`
3. **Mismatch**: Ignore snapshot, replay all events
4. **Match**: Use snapshot, replay events since snapshot

```python
# Repository automatically handles this
order = await repo.load(order_id)
# If schema version doesn't match, full replay occurs transparently
```

**Example scenario:**

```python
# Old schema (v1)
class OrderState(BaseModel):
    order_id: UUID
    status: str
    total: float

# Snapshot exists at version 100 with schema_version=1

# New schema (v2) - added required field
class OrderState(BaseModel):
    order_id: UUID
    status: str
    total: float
    shipping_address: str  # New required field

class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 2  # Incremented

# Next load:
order = await repo.load(order_id)
# 1. Finds snapshot with schema_version=1
# 2. Detects mismatch (expected 2, got 1)
# 3. Ignores snapshot, replays all 100 events
# 4. On next save (version 101), creates new snapshot with schema_version=2
```

### Cleaning Up Old Snapshots

After a schema migration, clean up old snapshots:

```python
# Delete all snapshots with schema_version < 2
count = await snapshot_store.delete_snapshots_by_type(
    aggregate_type="Order",
    schema_version_below=2,
)
print(f"Deleted {count} old snapshots")
```

**When to clean up:**

- After verifying the migration works
- During off-peak hours (reduces database load)
- As part of deployment automation

---

## How Snapshots Work Internally

Understanding the snapshot lifecycle helps debug issues and optimize performance:

### Snapshot-Aware Loading

When you call `repo.load(aggregate_id)` with snapshots enabled:

```python
async def load(self, aggregate_id: UUID) -> Aggregate:
    # 1. Try to load snapshot
    snapshot = await snapshot_store.get_snapshot(aggregate_id, "Order")

    if snapshot:
        # 2. Validate schema version
        if snapshot.schema_version != OrderAggregate.schema_version:
            # Schema mismatch - ignore snapshot
            logger.warning("Schema version mismatch, using full replay")
            snapshot = None

    # 3. Determine event range to load
    if snapshot:
        from_version = snapshot.version + 1  # Events after snapshot
        aggregate = OrderAggregate(aggregate_id)
        aggregate._restore_from_snapshot(snapshot)  # Restore state
    else:
        from_version = 0  # All events
        aggregate = OrderAggregate(aggregate_id)

    # 4. Load events
    stream = await event_store.get_events(
        aggregate_id,
        "Order",
        from_version=from_version,
    )

    # 5. Replay events
    aggregate.load_from_history(stream.events)

    return aggregate
```

### Snapshot Creation on Save

When you call `repo.save(aggregate)` with snapshots enabled:

```python
async def save(self, aggregate: Aggregate) -> None:
    # 1. Save events to event store
    await event_store.append_events(
        aggregate.aggregate_id,
        "Order",
        aggregate.uncommitted_events,
        expected_version=aggregate.version - len(aggregate.uncommitted_events),
    )

    # 2. Mark events as committed
    aggregate.mark_events_as_committed()

    # 3. Publish events (if event_publisher configured)
    if event_publisher:
        await event_publisher.publish_batch(committed_events)

    # 4. Check if snapshot should be created
    if snapshot_strategy.should_snapshot(aggregate, events_since_snapshot):
        # Mode determines execution
        if snapshot_mode == "sync":
            # Block until complete
            await snapshot_strategy.execute_snapshot(aggregate, snapshot_store, "Order")
        elif snapshot_mode == "background":
            # Fire and forget
            asyncio.create_task(
                snapshot_strategy.execute_snapshot(aggregate, snapshot_store, "Order")
            )
        # elif snapshot_mode == "manual": do nothing
```

---

## Complete Working Example

```python
"""
Tutorial 14: Optimizing with Aggregate Snapshotting

This example demonstrates aggregate snapshotting for performance optimization.
Run with: python tutorial_14_snapshotting.py
"""

import asyncio
import time
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)
from eventsource.snapshots import InMemorySnapshotStore


# =============================================================================
# Events
# =============================================================================


@register_event
class AccountOpened(DomainEvent):
    event_type: str = "AccountOpened"
    aggregate_type: str = "Account"

    owner_name: str
    initial_balance: float


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


# =============================================================================
# State & Aggregate
# =============================================================================


class AccountState(BaseModel):
    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0
    transaction_count: int = 0


class AccountAggregate(AggregateRoot[AccountState]):
    aggregate_type = "Account"
    schema_version = 1  # Schema version for snapshot compatibility

    def _get_initial_state(self) -> AccountState:
        return AccountState(account_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountOpened):
            self._state = AccountState(
                account_id=self.aggregate_id,
                owner_name=event.owner_name,
                balance=event.initial_balance,
                transaction_count=0,
            )
        elif isinstance(event, MoneyDeposited):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "balance": self._state.balance + event.amount,
                        "transaction_count": self._state.transaction_count + 1,
                    }
                )
        elif isinstance(event, MoneyWithdrawn):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "balance": self._state.balance - event.amount,
                        "transaction_count": self._state.transaction_count + 1,
                    }
                )

    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        """Open a new account."""
        self.apply_event(
            AccountOpened(
                aggregate_id=self.aggregate_id,
                owner_name=owner_name,
                initial_balance=initial_balance,
                aggregate_version=self.get_next_version(),
            )
        )

    def deposit(self, amount: float) -> None:
        """Deposit money."""
        self.apply_event(
            MoneyDeposited(
                aggregate_id=self.aggregate_id,
                amount=amount,
                aggregate_version=self.get_next_version(),
            )
        )

    def withdraw(self, amount: float) -> None:
        """Withdraw money."""
        self.apply_event(
            MoneyWithdrawn(
                aggregate_id=self.aggregate_id,
                amount=amount,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Demo Functions
# =============================================================================


async def performance_comparison():
    """Compare load times with and without snapshots."""
    print("=" * 70)
    print("Performance Comparison: With vs Without Snapshots")
    print("=" * 70)

    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Repository WITHOUT snapshots
    repo_no_snap = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
    )

    # Repository WITH snapshots
    repo_with_snap = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=50,
        snapshot_mode="sync",
    )

    # Create account with 200 transactions
    print("\n1. Creating account with 200 transactions...")
    account_id = uuid4()
    account = repo_no_snap.create_new(account_id)
    account.open("Performance Test Account", 1000.0)
    await repo_no_snap.save(account)

    for i in range(200):
        account = await repo_no_snap.load(account_id)
        if i % 2 == 0:
            account.deposit(10.0)
        else:
            account.withdraw(5.0)
        await repo_no_snap.save(account)

    print(f"   Account version: {account.version}")
    print(f"   Final balance: ${account.state.balance:.2f}")
    print(f"   Transaction count: {account.state.transaction_count}")

    # Benchmark WITHOUT snapshots
    print("\n2. Benchmarking WITHOUT snapshots (10 loads)...")
    start = time.perf_counter()
    for _ in range(10):
        await repo_no_snap.load(account_id)
    no_snap_time = (time.perf_counter() - start) / 10

    # Create snapshot and benchmark WITH snapshots
    print("\n3. Creating snapshot and benchmarking WITH snapshots...")
    loaded = await repo_with_snap.load(account_id)
    await repo_with_snap.create_snapshot(loaded)
    print(f"   Created snapshot at version {loaded.version}")

    start = time.perf_counter()
    for _ in range(10):
        await repo_with_snap.load(account_id)
    with_snap_time = (time.perf_counter() - start) / 10

    # Results
    print("\n4. Results:")
    print(f"   WITHOUT snapshot: {no_snap_time * 1000:.2f}ms per load")
    print(f"   WITH snapshot:    {with_snap_time * 1000:.2f}ms per load")
    print(f"   Improvement:      {(no_snap_time / with_snap_time):.1f}x faster")
    print(f"   Time saved:       {(no_snap_time - with_snap_time) * 1000:.2f}ms per load")


async def snapshot_modes_demo():
    """Demonstrate different snapshot creation modes."""
    print("\n" + "=" * 70)
    print("Snapshot Modes: Sync, Background, Manual")
    print("=" * 70)

    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # --- Sync Mode ---
    print("\n1. SYNC MODE (blocks until snapshot is written)")
    repo_sync = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=10,
        snapshot_mode="sync",
    )

    account_id_1 = uuid4()
    account = repo_sync.create_new(account_id_1)
    account.open("Sync Mode Account", 100.0)
    for _ in range(15):
        account.deposit(1.0)

    print("   Saving account (15 events, threshold=10)...")
    await repo_sync.save(account)

    snapshot = await snapshot_store.get_snapshot(account_id_1, "Account")
    if snapshot:
        print(f"   Snapshot created at version {snapshot.version}")
        print(f"   Snapshot exists immediately after save: True")

    # --- Background Mode ---
    print("\n2. BACKGROUND MODE (async snapshot creation)")
    repo_bg = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=10,
        snapshot_mode="background",
    )

    account_id_2 = uuid4()
    account2 = repo_bg.create_new(account_id_2)
    account2.open("Background Mode Account", 100.0)
    for _ in range(15):
        account2.deposit(1.0)

    print("   Saving account (15 events, threshold=10)...")
    await repo_bg.save(account2)

    # Check immediately
    snapshot_immediate = await snapshot_store.get_snapshot(account_id_2, "Account")
    print(f"   Snapshot exists immediately after save: {snapshot_immediate is not None}")

    # Wait for background tasks
    await repo_bg.await_pending_snapshots()
    snapshot_after = await snapshot_store.get_snapshot(account_id_2, "Account")
    print(f"   Snapshot exists after awaiting tasks: {snapshot_after is not None}")
    if snapshot_after:
        print(f"   Snapshot created at version {snapshot_after.version}")

    # --- Manual Mode ---
    print("\n3. MANUAL MODE (explicit snapshot creation)")
    repo_manual = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_mode="manual",  # No automatic snapshots
    )

    account_id_3 = uuid4()
    account3 = repo_manual.create_new(account_id_3)
    account3.open("Manual Mode Account", 100.0)
    for _ in range(20):
        account3.deposit(1.0)

    print("   Saving account (20 events, manual mode)...")
    await repo_manual.save(account3)

    snapshot_auto = await snapshot_store.get_snapshot(account_id_3, "Account")
    print(f"   Automatic snapshot created: {snapshot_auto is not None}")

    print("   Creating snapshot manually...")
    snapshot_manual = await repo_manual.create_snapshot(account3)
    print(f"   Manual snapshot created at version {snapshot_manual.version}")


async def schema_versioning_demo():
    """Demonstrate schema versioning and migration."""
    print("\n" + "=" * 70)
    print("Schema Versioning: Handling State Evolution")
    print("=" * 70)

    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Original aggregate with schema_version=1
    class AccountV1(AggregateRoot[AccountState]):
        aggregate_type = "Account"
        schema_version = 1

        def _get_initial_state(self) -> AccountState:
            return AccountState(account_id=self.aggregate_id)

        def _apply(self, event: DomainEvent) -> None:
            if isinstance(event, AccountOpened):
                self._state = AccountState(
                    account_id=self.aggregate_id,
                    owner_name=event.owner_name,
                    balance=event.initial_balance,
                    transaction_count=0,
                )
            elif isinstance(event, MoneyDeposited):
                if self._state:
                    self._state = self._state.model_copy(
                        update={
                            "balance": self._state.balance + event.amount,
                            "transaction_count": self._state.transaction_count + 1,
                        }
                    )

        def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
            self.apply_event(
                AccountOpened(
                    aggregate_id=self.aggregate_id,
                    owner_name=owner_name,
                    initial_balance=initial_balance,
                    aggregate_version=self.get_next_version(),
                )
            )

        def deposit(self, amount: float) -> None:
            self.apply_event(
                MoneyDeposited(
                    aggregate_id=self.aggregate_id,
                    amount=amount,
                    aggregate_version=self.get_next_version(),
                )
            )

    # Create account with v1
    print("\n1. Creating account with schema_version=1...")
    repo_v1 = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountV1,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=10,
        snapshot_mode="sync",
    )

    account_id = uuid4()
    account = repo_v1.create_new(account_id)
    account.open("Schema Test Account", 500.0)
    for _ in range(15):
        account.deposit(10.0)
    await repo_v1.save(account)

    snapshot_v1 = await snapshot_store.get_snapshot(account_id, "Account")
    print(f"   Created snapshot at version {snapshot_v1.version}")
    print(f"   Snapshot schema_version: {snapshot_v1.schema_version}")

    # Simulate schema migration to v2
    print("\n2. Migrating to schema_version=2...")

    class AccountV2(AggregateRoot[AccountState]):
        aggregate_type = "Account"
        schema_version = 2  # Incremented

        def _get_initial_state(self) -> AccountState:
            return AccountState(account_id=self.aggregate_id)

        def _apply(self, event: DomainEvent) -> None:
            if isinstance(event, AccountOpened):
                self._state = AccountState(
                    account_id=self.aggregate_id,
                    owner_name=event.owner_name,
                    balance=event.initial_balance,
                    transaction_count=0,
                )
            elif isinstance(event, MoneyDeposited):
                if self._state:
                    self._state = self._state.model_copy(
                        update={
                            "balance": self._state.balance + event.amount,
                            "transaction_count": self._state.transaction_count + 1,
                        }
                    )

        def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
            self.apply_event(
                AccountOpened(
                    aggregate_id=self.aggregate_id,
                    owner_name=owner_name,
                    initial_balance=initial_balance,
                    aggregate_version=self.get_next_version(),
                )
            )

        def deposit(self, amount: float) -> None:
            self.apply_event(
                MoneyDeposited(
                    aggregate_id=self.aggregate_id,
                    amount=amount,
                    aggregate_version=self.get_next_version(),
                )
            )

    repo_v2 = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountV2,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=10,
        snapshot_mode="sync",
    )

    print("   Loading account with v2 aggregate (snapshot v1 exists)...")
    account_v2 = await repo_v2.load(account_id)
    print(f"   Account loaded successfully (full event replay)")
    print(f"   Account version: {account_v2.version}")
    print(f"   Account balance: ${account_v2.state.balance:.2f}")

    print("\n3. Saving with v2 creates new snapshot...")
    account_v2.deposit(50.0)
    await repo_v2.save(account_v2)

    snapshot_v2 = await snapshot_store.get_snapshot(account_id, "Account")
    print(f"   New snapshot at version {snapshot_v2.version}")
    print(f"   New snapshot schema_version: {snapshot_v2.schema_version}")

    print("\n4. Cleaning up old snapshots...")
    count = await snapshot_store.delete_snapshots_by_type(
        aggregate_type="Account",
        schema_version_below=2,
    )
    print(f"   Deleted {count} snapshots with schema_version < 2")


async def main():
    """Run all demonstrations."""
    await performance_comparison()
    await snapshot_modes_demo()
    await schema_versioning_demo()

    print("\n" + "=" * 70)
    print("Tutorial 14 Complete!")
    print("=" * 70)
    print("\nKey Takeaways:")
    print("  1. Snapshots dramatically improve load times for event-heavy aggregates")
    print("  2. Choose snapshot mode based on your workload (sync/background/manual)")
    print("  3. Tune snapshot_threshold to balance frequency and overhead")
    print("  4. Use schema_version to handle aggregate state evolution")
    print("  5. Snapshots are optional - events remain the source of truth")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Performance Tuning

### Measuring Snapshot Impact

Track metrics to validate snapshot effectiveness:

```python
import time
import logging

logger = logging.getLogger(__name__)

async def measure_load_time(repo, aggregate_id):
    start = time.perf_counter()
    aggregate = await repo.load(aggregate_id)
    duration = time.perf_counter() - start

    logger.info(
        "Loaded %s/%s v%d in %.2fms",
        aggregate.aggregate_type,
        aggregate_id,
        aggregate.version,
        duration * 1000,
    )

    return aggregate, duration
```

### Monitoring Snapshot Health

Track snapshot effectiveness:

```python
# Get snapshot stats
snapshot = await snapshot_store.get_snapshot(aggregate_id, "Order")
if snapshot:
    events_since_snapshot = aggregate.version - snapshot.version
    snapshot_age = datetime.now(UTC) - snapshot.created_at

    logger.info(
        "Snapshot health: age=%s, events_since=%d",
        snapshot_age,
        events_since_snapshot,
    )

    # Alert if snapshot is stale
    if events_since_snapshot > snapshot_threshold * 2:
        logger.warning("Snapshot is stale, consider recreating")
```

### Database Maintenance

For PostgreSQL and SQLite snapshot stores:

```sql
-- Check snapshot table size
SELECT
    pg_size_pretty(pg_total_relation_size('snapshots')) as total_size,
    COUNT(*) as snapshot_count
FROM snapshots;

-- Find largest snapshots
SELECT
    aggregate_type,
    aggregate_id,
    version,
    pg_size_pretty(length(state::text)::bigint) as state_size,
    created_at
FROM snapshots
ORDER BY length(state::text) DESC
LIMIT 10;

-- Clean up old schema versions
DELETE FROM snapshots
WHERE aggregate_type = 'Order'
  AND schema_version < 2;
```

---

## Common Pitfalls and Solutions

### Pitfall 1: Over-Snapshotting

**Problem:** Creating snapshots too frequently wastes storage and CPU.

```python
# BAD: Snapshot every event
repo = AggregateRepository(
    snapshot_threshold=1,  # Too frequent!
)
```

**Solution:** Start with threshold=100, tune based on metrics.

```python
# GOOD: Balanced threshold
repo = AggregateRepository(
    snapshot_threshold=100,  # Reasonable default
)
```

---

### Pitfall 2: Not Handling Schema Migrations

**Problem:** Changing state structure without incrementing schema_version.

```python
# BAD: Changed state but didn't increment version
class OrderState(BaseModel):
    order_id: UUID
    status: str
    new_field: str  # Added without version increment!

class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 1  # Should be 2!
```

**Solution:** Always increment schema_version for incompatible changes.

```python
# GOOD: Increment schema version
class OrderState(BaseModel):
    order_id: UUID
    status: str
    new_field: str = "default"  # Optional field with default

class OrderAggregate(AggregateRoot[OrderState]):
    schema_version = 2  # Incremented
```

---

### Pitfall 3: Forgetting to Await Background Snapshots in Tests

**Problem:** Tests fail because snapshots aren't created yet.

```python
# BAD: Test fails intermittently
async def test_snapshot_created():
    repo = AggregateRepository(snapshot_mode="background")
    account = repo.create_new(account_id)
    # ... create 100 events ...
    await repo.save(account)

    snapshot = await snapshot_store.get_snapshot(account_id, "Account")
    assert snapshot is not None  # Fails! Background task not done
```

**Solution:** Await pending snapshots in tests.

```python
# GOOD: Wait for background tasks
async def test_snapshot_created():
    repo = AggregateRepository(snapshot_mode="background")
    account = repo.create_new(account_id)
    # ... create 100 events ...
    await repo.save(account)

    await repo.await_pending_snapshots()  # Wait for background tasks

    snapshot = await snapshot_store.get_snapshot(account_id, "Account")
    assert snapshot is not None  # Passes
```

---

### Pitfall 4: Snapshotting Before Aggregate Has Meaningful State

**Problem:** Creating snapshots of empty or nearly empty aggregates.

```python
# BAD: Snapshot at version 1
repo = AggregateRepository(
    snapshot_threshold=1,  # Creates snapshot immediately
)

account = repo.create_new(account_id)
account.open("New Account", 0.0)  # Version 1
await repo.save(account)
# Snapshot created, but aggregate only has 1 event - no benefit!
```

**Solution:** Set threshold high enough to avoid snapshots on new aggregates.

```python
# GOOD: Threshold ensures meaningful snapshots
repo = AggregateRepository(
    snapshot_threshold=50,  # Only snapshot after 50 events
)
```

---

## Key Takeaways

1. **Snapshots are optimizations**: Events remain the source of truth. Snapshots can be deleted and recreated.

2. **Start without snapshots**: Add them when you measure load times > 100ms.

3. **Choose the right mode**:
   - Sync for development and low-throughput
   - Background for production high-throughput
   - Manual for business milestone snapshots

4. **Tune the threshold**: Balance snapshot frequency (overhead) vs load performance.

5. **Handle schema versioning**: Increment `schema_version` for incompatible state changes.

6. **Monitor and maintain**: Track snapshot effectiveness and clean up old snapshots.

7. **Use the right store**:
   - InMemory for testing
   - PostgreSQL for production
   - SQLite for embedded apps

8. **Snapshots fail gracefully**: Schema mismatches and errors fall back to full event replay.

---

## Next Steps

Continue to [Tutorial 15: Production Deployment Guide](15-production.md) to learn about:

- Deploying event-sourced applications to production
- Infrastructure patterns and best practices
- Monitoring and observability strategies
- Scaling event stores and projections
- High availability and disaster recovery
- Performance optimization techniques

For snapshot examples and advanced patterns, see:
- Source code: `src/eventsource/snapshots/`
- Tests: `tests/unit/test_snapshots.py`
- Production examples: `examples/advanced_usage.py`
