# Tutorial 14: Optimizing with Aggregate Snapshotting

**Estimated Time:** 60-75 minutes
**Difficulty:** Intermediate-Advanced
**Progress:** Tutorial 14 of 21 | Phase 3: Production Readiness

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 13: Subscription Management](13-subscriptions.md)
- Understanding of aggregates from [Tutorial 3: First Aggregate](03-first-aggregate.md)
- Understanding of repositories from [Tutorial 5: Repositories](05-repositories.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial introduces **aggregate snapshotting**, a critical performance optimization for aggregates that accumulate many events over time.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain why snapshotting dramatically improves aggregate loading performance
2. Configure a repository with automatic snapshot creation
3. Choose appropriate snapshot thresholds and modes for different scenarios
4. Implement schema versioning for safe snapshot evolution
5. Use different snapshot stores for development, testing, and production

---

## The Problem: Event Replay Gets Slow

In event sourcing, aggregates are reconstituted by replaying their entire event history. This is conceptually simple and provides a complete audit trail, but it creates a performance challenge: as aggregates accumulate events, loading them becomes increasingly expensive.

Consider a bank account aggregate that records every transaction:

```python
# An account with 10,000 transactions...
account = await repo.load(account_id)
# ^ This replays 10,000 events to calculate the current balance!
```

The math is straightforward but concerning:

| Event Count | Approximate Load Time |
|-------------|----------------------|
| 100 | ~50ms |
| 1,000 | ~500ms |
| 10,000 | ~5 seconds |
| 100,000 | ~60 seconds |

For high-traffic aggregates like user accounts, order histories, or IoT sensor data, this becomes a serious bottleneck.

---

## The Solution: Snapshots

A **snapshot** is a point-in-time capture of an aggregate's state. Instead of replaying all events from the beginning, the system can:

1. Load the snapshot (single database read)
2. Replay only events that occurred after the snapshot was taken

```
Without Snapshot:
[Event 1] -> [Event 2] -> ... -> [Event 10,000] -> Current State
                    (replay all 10,000 events)

With Snapshot at Event 9,900:
[Snapshot @ 9,900] -> [Event 9,901] -> ... -> [Event 10,000] -> Current State
                            (replay only 100 events!)
```

**Performance improvement with snapshots:**

| Event Count | Without Snapshot | With Snapshot | Improvement |
|-------------|-----------------|---------------|-------------|
| 100 | ~50ms | ~15ms | 3x faster |
| 1,000 | ~500ms | ~20ms | 25x faster |
| 10,000 | ~5s | ~60ms | 83x faster |
| 100,000 | ~60s | ~100ms | 600x faster |

### Key Principle: Snapshots Are Optimizations Only

Snapshots are **not** the source of truth - events remain the authoritative record. Snapshots can be safely deleted and regenerated from events at any time. They exist purely to improve performance.

---

## The Snapshot Data Structure

A snapshot captures everything needed to restore an aggregate to a known state:

```python
from eventsource.snapshots import Snapshot
from datetime import datetime, UTC
from uuid import uuid4

# Example snapshot structure
snapshot = Snapshot(
    aggregate_id=uuid4(),            # Which aggregate this belongs to
    aggregate_type="Account",         # Type identifier
    version=100,                      # Aggregate version when snapshot was taken
    state={"balance": 1500.00, ...},  # Serialized aggregate state
    schema_version=1,                 # State schema version (for evolution)
    created_at=datetime.now(UTC),     # When the snapshot was created
)
```

| Field | Purpose |
|-------|---------|
| `aggregate_id` | Links snapshot to the specific aggregate instance |
| `aggregate_type` | Type name for routing to correct aggregate class |
| `version` | Version at snapshot time; events with version > this are replayed |
| `state` | JSON-serialized aggregate state (from Pydantic `model_dump`) |
| `schema_version` | Detects incompatible state model changes |
| `created_at` | Timestamp for monitoring and debugging |

---

## Snapshot Stores

The library provides three snapshot store implementations for different environments:

### InMemorySnapshotStore

Best for: **Testing and development**

```python
from eventsource.snapshots import InMemorySnapshotStore

snapshot_store = InMemorySnapshotStore()
```

**Characteristics:**
- No database setup required
- Very fast operations
- Data lost on process restart
- Easy to clear between tests

### PostgreSQLSnapshotStore

Best for: **Production deployments**

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource.snapshots import PostgreSQLSnapshotStore

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

snapshot_store = PostgreSQLSnapshotStore(session_factory)
```

**Characteristics:**
- Production-ready persistence
- Supports distributed deployments
- ACID-compliant storage
- Integrated OpenTelemetry tracing

### SQLiteSnapshotStore

Best for: **Embedded applications and single-instance deployments**

```python
from eventsource.snapshots import SQLiteSnapshotStore

# File-based database
snapshot_store = SQLiteSnapshotStore("./snapshots.db")

# In-memory database (for testing)
snapshot_store = SQLiteSnapshotStore(":memory:")
```

**Note:** SQLiteSnapshotStore requires the `aiosqlite` package. Install with:
```bash
pip install eventsource-py[sqlite]
```

**Characteristics:**
- File-based persistence
- No external database server required
- Single-writer limitation (not ideal for concurrent writes)
- Good for desktop apps, CLI tools, edge computing

---

## Configuring the Repository

To enable snapshotting, provide a `snapshot_store` when creating your repository:

### Basic Configuration

```python
from eventsource import AggregateRepository, InMemoryEventStore
from eventsource.snapshots import InMemorySnapshotStore

event_store = InMemoryEventStore()
snapshot_store = InMemorySnapshotStore()

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=AccountAggregate,
    aggregate_type="Account",
    # Snapshot configuration
    snapshot_store=snapshot_store,
    snapshot_threshold=100,  # Create snapshot every 100 events
)
```

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `snapshot_store` | `SnapshotStore` | `None` | Storage backend for snapshots |
| `snapshot_threshold` | `int` | `None` | Events between automatic snapshots |
| `snapshot_mode` | `str` | `"sync"` | When to create snapshots |

### Choosing a Threshold

The `snapshot_threshold` determines how often snapshots are created:

| Scenario | Suggested Threshold | Rationale |
|----------|-------------------|-----------|
| Low event rate (~10/day) | 500 | Events replay quickly anyway |
| Medium event rate (~100/day) | 100 | Balance storage vs. speed |
| High event rate (~1000/day) | 50 | Frequent snapshots reduce replay |
| Very high rate (IoT sensors) | 20 | Minimize replay at all costs |

**Rule of thumb:** Start with 100 and adjust based on observed load times.

---

## Snapshot Modes

The `snapshot_mode` parameter controls **when** snapshots are created:

### Sync Mode (Default)

Snapshots are created immediately after save, blocking until complete:

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=AccountAggregate,
    aggregate_type="Account",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
    snapshot_mode="sync",  # Default - blocks until snapshot created
)
```

**Behavior:**
- `save()` completes only after snapshot is written
- Guarantees snapshot availability for next load
- Adds latency to save operations (~10-50ms)

**Best for:** Most applications, lower throughput scenarios

### Background Mode

Snapshots are created asynchronously in a background task:

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=AccountAggregate,
    aggregate_type="Account",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,
    snapshot_mode="background",  # Non-blocking
)
```

**Behavior:**
- `save()` returns immediately after events are persisted
- Snapshot created in fire-and-forget background task
- Small chance of stale snapshot on immediate re-load

**Best for:** High-throughput scenarios, latency-sensitive operations

```python
# In tests, wait for background snapshots to complete
await repo.await_pending_snapshots()
```

### Manual Mode

Snapshots are only created via explicit `create_snapshot()` calls:

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=AccountAggregate,
    aggregate_type="Account",
    snapshot_store=snapshot_store,
    snapshot_mode="manual",  # No automatic snapshots
)

# Manually create snapshot at business milestone
account = await repo.load(account_id)
account.complete_verification()
await repo.save(account)

# Explicitly create snapshot after verification
snapshot = await repo.create_snapshot(account)
print(f"Snapshot created at version {snapshot.version}")
```

**Best for:**
- Creating snapshots at specific business milestones
- Complex snapshotting strategies
- Pre-warming frequently accessed aggregates

---

## Schema Versioning

When your aggregate's state model changes, existing snapshots may become incompatible. Schema versioning handles this gracefully.

### Adding Schema Version to Aggregates

Add the `schema_version` class attribute to your aggregate:

```python
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent, register_event
from uuid import UUID

class AccountState(BaseModel):
    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0

class AccountAggregate(AggregateRoot[AccountState]):
    aggregate_type = "Account"
    schema_version = 1  # Increment when state model changes incompatibly

    def _get_initial_state(self) -> AccountState:
        return AccountState(account_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        # ... event handling ...
        pass
```

### When to Increment Schema Version

**DO increment** when:
- Adding required fields without defaults
- Removing fields
- Changing field types incompatibly
- Renaming fields

**DON'T increment** when:
- Adding optional fields with defaults
- Adding new methods
- Changing validation rules only

### What Happens on Schema Mismatch

When a snapshot's schema version doesn't match the aggregate's expected version:

1. The snapshot is ignored (logged as info)
2. Full event replay is performed
3. A new snapshot is created at current schema version (if threshold is met)

```python
# Version 1: Original state
class AccountState(BaseModel):
    account_id: UUID
    balance: float

class AccountAggregate(AggregateRoot[AccountState]):
    schema_version = 1

# Version 2: Added required field
class AccountState(BaseModel):
    account_id: UUID
    balance: float
    currency: str  # New required field - old snapshots can't deserialize!

class AccountAggregate(AggregateRoot[AccountState]):
    schema_version = 2  # MUST increment
```

### Bulk Snapshot Invalidation

After schema changes, proactively clean up old snapshots:

```python
# Delete all Account snapshots with schema_version < 2
count = await snapshot_store.delete_snapshots_by_type(
    aggregate_type="Account",
    schema_version_below=2,
)
print(f"Invalidated {count} old snapshots")
```

---

## How Loading Works

When you call `repo.load(aggregate_id)`, the repository follows this sequence:

```
1. Check for snapshot in snapshot store
   |
   v
2. Snapshot exists? ----No----> Get all events (from_version=0)
   |                                    |
   Yes                                  v
   |                            Apply all events
   v                                    |
3. Schema version matches? --No-->     |
   |                                    |
   Yes                                  |
   |                                    |
   v                                    |
4. Restore state from snapshot         |
   |                                    |
   v                                    |
5. Get events since snapshot           |
   (from_version=snapshot.version)     |
   |                                    |
   v                                    |
6. Apply recent events                 |
   |                                    |
   +----------> Return hydrated aggregate <---+
```

This sequence ensures:
- Snapshots are used when available and valid
- Schema mismatches fall back gracefully
- Deserialization errors fall back to full replay
- The aggregate always ends up in the correct state

---

## Complete Example

Here is a complete, runnable example demonstrating aggregate snapshotting:

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
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
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
# State and Aggregate
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
                self._state = self._state.model_copy(update={
                    "balance": self._state.balance + event.amount,
                    "transaction_count": self._state.transaction_count + 1,
                })
        elif isinstance(event, MoneyWithdrawn):
            if self._state:
                self._state = self._state.model_copy(update={
                    "balance": self._state.balance - event.amount,
                    "transaction_count": self._state.transaction_count + 1,
                })

    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        self.apply_event(AccountOpened(
            aggregate_id=self.aggregate_id,
            owner_name=owner_name,
            initial_balance=initial_balance,
            aggregate_version=self.get_next_version(),
        ))

    def deposit(self, amount: float) -> None:
        if amount <= 0:
            raise ValueError("Deposit must be positive")
        self.apply_event(MoneyDeposited(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        ))

    def withdraw(self, amount: float) -> None:
        if amount <= 0:
            raise ValueError("Withdrawal must be positive")
        if self.state and self.state.balance < amount:
            raise ValueError("Insufficient funds")
        self.apply_event(MoneyWithdrawn(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Demo Functions
# =============================================================================

async def performance_comparison():
    """Compare load times with and without snapshots."""
    print("=== Performance Comparison ===\n")

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
        snapshot_threshold=50,  # Snapshot every 50 events
        snapshot_mode="sync",
    )

    # Create account with many transactions
    account_id = uuid4()
    account = repo_no_snap.create_new(account_id)
    account.open("Performance Test", 1000.0)
    await repo_no_snap.save(account)

    # Add many transactions
    num_transactions = 200
    print(f"Creating {num_transactions} transactions...")

    for i in range(num_transactions):
        account = await repo_no_snap.load(account_id)
        if i % 2 == 0:
            account.deposit(10.0)
        else:
            account.withdraw(5.0)
        await repo_no_snap.save(account)

    print(f"Account now has {account.version} events")
    print(f"Final balance: ${account.state.balance:.2f}")
    print(f"Transaction count: {account.state.transaction_count}")
    print()

    # Measure load time WITHOUT snapshot
    start = time.perf_counter()
    iterations = 10
    for _ in range(iterations):
        loaded = await repo_no_snap.load(account_id)
    no_snap_time = (time.perf_counter() - start) / iterations

    print(f"Load time WITHOUT snapshot: {no_snap_time*1000:.2f}ms")

    # Create a snapshot using the snapshot-enabled repository
    loaded = await repo_with_snap.load(account_id)
    await repo_with_snap.create_snapshot(loaded)
    print("Snapshot created!")

    # Measure load time WITH snapshot
    start = time.perf_counter()
    for _ in range(iterations):
        loaded = await repo_with_snap.load(account_id)
    with_snap_time = (time.perf_counter() - start) / iterations

    print(f"Load time WITH snapshot: {with_snap_time*1000:.2f}ms")

    if with_snap_time > 0:
        print(f"Improvement: {(no_snap_time/with_snap_time):.1f}x faster")


async def automatic_snapshot_demo():
    """Demonstrate automatic snapshot creation at threshold."""
    print("\n=== Automatic Snapshot Creation ===\n")

    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=10,  # Low threshold for demo
        snapshot_mode="sync",
    )

    account_id = uuid4()
    account = repo.create_new(account_id)
    account.open("Auto Snapshot Demo", 100.0)
    await repo.save(account)

    print(f"Starting with account at version {account.version}")

    # Add transactions, watching for automatic snapshot
    for i in range(25):
        account = await repo.load(account_id)
        account.deposit(1.0)
        await repo.save(account)

        # Check if snapshot exists
        snapshot = await snapshot_store.get_snapshot(account_id, "Account")
        if snapshot and account.version == snapshot.version:
            print(f"  Version {account.version}: Snapshot created!")
        elif account.version % 5 == 0:
            print(f"  Version {account.version}: No snapshot yet")

    # Final check
    snapshot = await snapshot_store.get_snapshot(account_id, "Account")
    if snapshot:
        print(f"\nFinal snapshot at version {snapshot.version}")
        print(f"Account current version: {account.version}")


async def snapshot_modes_demo():
    """Demonstrate different snapshot creation modes."""
    print("\n=== Snapshot Modes ===\n")

    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Mode: sync (default) - snapshot created immediately after save
    print("Mode: sync")
    repo_sync = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=10,
        snapshot_mode="sync",
    )

    account_id = uuid4()
    account = repo_sync.create_new(account_id)
    account.open("Sync Mode Test", 100.0)

    # Create enough events to trigger snapshot
    for i in range(15):
        account.deposit(1.0)
    await repo_sync.save(account)

    snapshot = await snapshot_store.get_snapshot(account_id, "Account")
    if snapshot:
        print(f"  Snapshot created at version {snapshot.version}")
    print()

    # Mode: background - snapshot created asynchronously
    print("Mode: background")
    repo_background = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=10,
        snapshot_mode="background",
    )

    account_id2 = uuid4()
    account2 = repo_background.create_new(account_id2)
    account2.open("Background Mode Test", 100.0)
    for i in range(15):
        account2.deposit(1.0)
    await repo_background.save(account2)

    # Wait for background snapshot to complete
    count = await repo_background.await_pending_snapshots()
    print(f"  Awaited {count} background snapshot(s)")

    snapshot2 = await snapshot_store.get_snapshot(account_id2, "Account")
    if snapshot2:
        print(f"  Snapshot created at version {snapshot2.version}")
    print()

    # Mode: manual - only create snapshots explicitly
    print("Mode: manual")
    repo_manual = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_mode="manual",
    )

    account_id3 = uuid4()
    account3 = repo_manual.create_new(account_id3)
    account3.open("Manual Mode Test", 100.0)
    for i in range(20):
        account3.deposit(1.0)
    await repo_manual.save(account3)

    # No automatic snapshot
    snapshot3 = await snapshot_store.get_snapshot(account_id3, "Account")
    print(f"  Auto snapshot exists: {snapshot3 is not None}")

    # Manually create snapshot
    account3 = await repo_manual.load(account_id3)
    await repo_manual.create_snapshot(account3)
    snapshot3 = await snapshot_store.get_snapshot(account_id3, "Account")
    print(f"  After manual create: {snapshot3 is not None}")
    if snapshot3:
        print(f"  Snapshot at version {snapshot3.version}")


async def schema_versioning_demo():
    """Demonstrate schema versioning for snapshots."""
    print("\n=== Schema Versioning ===\n")

    # This demonstrates what happens when schema_version changes

    # Simulate a V1 aggregate (original schema)
    class AccountStateV1(BaseModel):
        account_id: UUID
        owner_name: str = ""
        balance: float = 0.0

    class AccountAggregateV1(AggregateRoot[AccountStateV1]):
        aggregate_type = "Account"
        schema_version = 1

        def _get_initial_state(self) -> AccountStateV1:
            return AccountStateV1(account_id=self.aggregate_id)

        def _apply(self, event: DomainEvent) -> None:
            if isinstance(event, AccountOpened):
                self._state = AccountStateV1(
                    account_id=self.aggregate_id,
                    owner_name=event.owner_name,
                    balance=event.initial_balance,
                )
            elif isinstance(event, MoneyDeposited):
                if self._state:
                    self._state = self._state.model_copy(
                        update={"balance": self._state.balance + event.amount}
                    )

    # Create V1 snapshot
    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    repo_v1 = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregateV1,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=5,
        snapshot_mode="sync",
    )

    account_id = uuid4()
    account = repo_v1.create_new(account_id)
    account.apply_event(AccountOpened(
        aggregate_id=account_id,
        owner_name="Schema Test",
        initial_balance=100.0,
        aggregate_version=account.get_next_version(),
    ))
    for _ in range(10):
        account.apply_event(MoneyDeposited(
            aggregate_id=account_id,
            amount=10.0,
            aggregate_version=account.get_next_version(),
        ))
    await repo_v1.save(account)

    snapshot_v1 = await snapshot_store.get_snapshot(account_id, "Account")
    print(f"V1 Snapshot created at version {snapshot_v1.version}")
    print(f"  schema_version: {snapshot_v1.schema_version}")
    print(f"  state keys: {list(snapshot_v1.state.keys())}")
    print()

    # Now simulate upgrading to V2 (added transaction_count field)
    class AccountStateV2(BaseModel):
        account_id: UUID
        owner_name: str = ""
        balance: float = 0.0
        transaction_count: int = 0  # New field!

    class AccountAggregateV2(AggregateRoot[AccountStateV2]):
        aggregate_type = "Account"
        schema_version = 2  # Incremented!

        def _get_initial_state(self) -> AccountStateV2:
            return AccountStateV2(account_id=self.aggregate_id)

        def _apply(self, event: DomainEvent) -> None:
            if isinstance(event, AccountOpened):
                self._state = AccountStateV2(
                    account_id=self.aggregate_id,
                    owner_name=event.owner_name,
                    balance=event.initial_balance,
                    transaction_count=0,
                )
            elif isinstance(event, MoneyDeposited):
                if self._state:
                    self._state = self._state.model_copy(update={
                        "balance": self._state.balance + event.amount,
                        "transaction_count": self._state.transaction_count + 1,
                    })

    # Load with V2 aggregate - V1 snapshot will be ignored
    repo_v2 = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregateV2,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=5,
        snapshot_mode="sync",
    )

    print("Loading account with V2 aggregate (V1 snapshot should be ignored)...")
    loaded = await repo_v2.load(account_id)
    print(f"  Loaded successfully!")
    print(f"  Version: {loaded.version}")
    print(f"  Balance: ${loaded.state.balance:.2f}")
    print(f"  Transaction count: {loaded.state.transaction_count}")
    print("  (Schema mismatch caused full event replay)")


async def main():
    await performance_comparison()
    await automatic_snapshot_demo()
    await snapshot_modes_demo()
    await schema_versioning_demo()

    print("\n=== All Examples Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_14_snapshotting.py` and run it:

```bash
python tutorial_14_snapshotting.py
```

---

## Exercises

### Exercise 1: Add Snapshotting to an Existing Aggregate

**Objective:** Add snapshotting to an aggregate and verify the performance improvement.

**Time:** 20-25 minutes

**Requirements:**

1. Create an aggregate with many events (500+)
2. Measure load time without snapshots
3. Add snapshot support to the repository
4. Create a snapshot
5. Measure load time with the snapshot
6. Verify the performance improvement

**Starter Code:**

```python
"""
Tutorial 14 - Exercise 1: Add Snapshotting to Aggregate

Your task: Add snapshotting to a counter aggregate and measure
the performance improvement.
"""
import asyncio
import time
from uuid import uuid4

from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
)
from eventsource.snapshots import InMemorySnapshotStore


@register_event
class CounterIncremented(DomainEvent):
    event_type: str = "CounterIncremented"
    aggregate_type: str = "Counter"


class CounterState(BaseModel):
    counter_id: str
    value: int = 0


class CounterAggregate(AggregateRoot[CounterState]):
    aggregate_type = "Counter"
    # TODO: Add schema_version

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            if self._state is None:
                self._state = CounterState(counter_id=str(self.aggregate_id), value=1)
            else:
                self._state = self._state.model_copy(update={
                    "value": self._state.value + 1
                })

    def increment(self) -> None:
        self.apply_event(CounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


async def main():
    print("=== Exercise 14-1: Add Snapshotting ===\n")

    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Step 1: Create repository WITHOUT snapshots
    # TODO: Create repo_no_snap

    # Step 2: Create aggregate with 500 events
    counter_id = uuid4()
    # TODO: Create counter and add 500 increments

    # Step 3: Measure load time WITHOUT snapshots
    # TODO: Time loading the aggregate 20 times, calculate average

    # Step 4: Create repository WITH snapshots
    # TODO: Create repo_with_snap with snapshot_store and threshold

    # Step 5: Create a snapshot
    # TODO: Load aggregate and create snapshot

    # Step 6: Measure load time WITH snapshot
    # TODO: Time loading with snapshots, calculate improvement


if __name__ == "__main__":
    asyncio.run(main())
```

**Hints:**

- Add `schema_version = 1` to the aggregate class
- Use `repo.create_snapshot(aggregate)` to manually create a snapshot
- Use `time.perf_counter()` for accurate timing
- Loop 20 times and divide by 20 for average load time

The solution is available at: `docs/tutorials/exercises/solutions/14-1.py`

---

## Summary

In this tutorial, you learned:

- **Why snapshotting matters:** Aggregate loading slows dramatically as events accumulate
- **How snapshots work:** They capture point-in-time state, allowing partial event replay
- **Snapshot stores:** InMemorySnapshotStore for testing, PostgreSQLSnapshotStore for production, SQLiteSnapshotStore for embedded use
- **Configuration options:** snapshot_threshold controls frequency, snapshot_mode controls timing
- **Schema versioning:** Increment schema_version when state models change incompatibly
- **Loading sequence:** Repository automatically uses valid snapshots and falls back gracefully

---

## Key Takeaways

!!! note "Remember"
    - Snapshot every 50-100 events is a good starting point
    - Always set `schema_version` on aggregates that use snapshots
    - Snapshots are optimizations only - events remain the source of truth
    - Test snapshot loading before deploying to production

!!! tip "Best Practice"
    Start without snapshotting, then add it when you observe performance issues with aggregate loading. Premature optimization adds complexity without benefit.

!!! warning "Common Mistake"
    Do not forget to increment `schema_version` when making breaking changes to your state model. Old snapshots will cause deserialization errors if the schema changes incompatibly.

---

## Next Steps

Continue to [Tutorial 15: Production Deployment Guide](15-production.md) to learn about deploying event-sourced applications to production, including database setup, monitoring, and operational best practices.

---

## Related Documentation

- [Snapshotting Guide](../guides/snapshotting.md) - Comprehensive snapshotting reference
- [Snapshotting Migration Guide](../guides/snapshotting-migration.md) - Adding snapshots to existing systems
- [Tutorial 5: Repositories](05-repositories.md) - Repository pattern fundamentals
- [Tutorial 3: First Aggregate](03-first-aggregate.md) - Aggregate basics
