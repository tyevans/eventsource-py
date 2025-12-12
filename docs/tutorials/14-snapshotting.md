# Tutorial 14: Optimizing with Aggregate Snapshotting

**Difficulty:** Intermediate-Advanced | **Progress:** Tutorial 14 of 21 | Phase 3: Production Readiness

## Prerequisites

- Completed [Tutorial 13: Subscription Management](13-subscriptions.md)
- Understanding of aggregates and repositories
- Python 3.11+ with eventsource-py installed

Snapshotting is a performance optimization for aggregates that accumulate many events. Instead of replaying all events, snapshots capture point-in-time state and replay only recent events.

## The Problem

Event replay gets slow as aggregates grow:

| Event Count | Without Snapshot | With Snapshot | Improvement |
|-------------|-----------------|---------------|-------------|
| 100 | ~50ms | ~15ms | 3x faster |
| 1,000 | ~500ms | ~20ms | 25x faster |
| 10,000 | ~5s | ~60ms | 83x faster |
| 100,000 | ~60s | ~100ms | 600x faster |

## The Solution

Snapshots capture point-in-time state. Load the snapshot, then replay only events after it. Snapshots are optimizations only - events remain the source of truth.

## Snapshot Stores

```python
from eventsource.snapshots import InMemorySnapshotStore, PostgreSQLSnapshotStore, SQLiteSnapshotStore

# Testing/development
snapshot_store = InMemorySnapshotStore()

# Production (requires SQLAlchemy async engine)
snapshot_store = PostgreSQLSnapshotStore(session_factory)

# Embedded apps (requires: pip install eventsource-py[sqlite])
snapshot_store = SQLiteSnapshotStore("./snapshots.db")
```

## Repository Configuration

```python
from eventsource import AggregateRepository, InMemoryEventStore
from eventsource.snapshots import InMemorySnapshotStore

event_store = InMemoryEventStore()
snapshot_store = InMemorySnapshotStore()

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=AccountAggregate,
    aggregate_type="Account",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,  # Create snapshot every 100 events
    snapshot_mode="sync",    # "sync", "background", or "manual"
)
```

**Threshold guidance:** Start with 100. Use 500 for low-traffic aggregates, 50-100 for medium traffic, 20-50 for high traffic.

## Snapshot Modes

```python
# Sync (default): blocks until snapshot written
repo = AggregateRepository(..., snapshot_mode="sync")

# Background: non-blocking, for high-throughput
repo = AggregateRepository(..., snapshot_mode="background")
await repo.await_pending_snapshots()  # In tests

# Manual: explicit control
repo = AggregateRepository(..., snapshot_mode="manual")
await repo.create_snapshot(account)
```

## Schema Versioning

```python
class AccountAggregate(AggregateRoot[AccountState]):
    schema_version = 1  # Increment on incompatible state changes
```

Increment when adding required fields, removing/renaming fields, or changing types. Don't increment for optional fields with defaults.

On mismatch, snapshot is ignored and full replay occurs. Clean up old snapshots:

```python
await snapshot_store.delete_snapshots_by_type("Account", schema_version_below=2)
```

## Complete Example

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
        self.apply_event(MoneyDeposited(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        ))

    def withdraw(self, amount: float) -> None:
        self.apply_event(MoneyWithdrawn(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        ))


async def performance_comparison():
    """Compare load times with and without snapshots."""
    print("=== Performance Comparison ===\n")

    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    repo_no_snap = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
    )

    repo_with_snap = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=50,
        snapshot_mode="sync",
    )

    # Create account with 200 transactions
    account_id = uuid4()
    account = repo_no_snap.create_new(account_id)
    account.open("Performance Test", 1000.0)
    await repo_no_snap.save(account)

    for i in range(200):
        account = await repo_no_snap.load(account_id)
        account.deposit(10.0) if i % 2 == 0 else account.withdraw(5.0)
        await repo_no_snap.save(account)

    print(f"Account has {account.version} events, balance: ${account.state.balance:.2f}\n")

    # Benchmark without snapshot
    start = time.perf_counter()
    for _ in range(10):
        await repo_no_snap.load(account_id)
    no_snap_time = (time.perf_counter() - start) / 10

    # Create snapshot and benchmark
    loaded = await repo_with_snap.load(account_id)
    await repo_with_snap.create_snapshot(loaded)

    start = time.perf_counter()
    for _ in range(10):
        await repo_with_snap.load(account_id)
    with_snap_time = (time.perf_counter() - start) / 10

    print(f"Load time WITHOUT snapshot: {no_snap_time*1000:.2f}ms")
    print(f"Load time WITH snapshot: {with_snap_time*1000:.2f}ms")
    print(f"Improvement: {(no_snap_time/with_snap_time):.1f}x faster")


async def snapshot_modes_demo():
    """Demonstrate different snapshot creation modes."""
    print("\n=== Snapshot Modes ===\n")

    event_store = InMemoryEventStore()
    snapshot_store = InMemorySnapshotStore()

    # Sync mode
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
    for _ in range(15):
        account.deposit(1.0)
    await repo_sync.save(account)

    snapshot = await snapshot_store.get_snapshot(account_id, "Account")
    print(f"  Snapshot at version {snapshot.version}\n")

    # Background mode
    print("Mode: background")
    repo_bg = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
        snapshot_store=snapshot_store,
        snapshot_threshold=10,
        snapshot_mode="background",
    )

    account_id2 = uuid4()
    account2 = repo_bg.create_new(account_id2)
    account2.open("Background Mode Test", 100.0)
    for _ in range(15):
        account2.deposit(1.0)
    await repo_bg.save(account2)

    await repo_bg.await_pending_snapshots()
    snapshot2 = await snapshot_store.get_snapshot(account_id2, "Account")
    print(f"  Snapshot at version {snapshot2.version}\n")

    # Manual mode
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
    for _ in range(20):
        account3.deposit(1.0)
    await repo_manual.save(account3)

    print(f"  Auto snapshot: {await snapshot_store.get_snapshot(account_id3, 'Account') is not None}")

    await repo_manual.create_snapshot(account3)
    snapshot3 = await snapshot_store.get_snapshot(account_id3, "Account")
    print(f"  After manual create: snapshot at version {snapshot3.version}")



async def main():
    await performance_comparison()
    await snapshot_modes_demo()
    print("\n=== All Examples Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
```

## Next Steps

Continue to [Tutorial 15: Production Deployment Guide](15-production.md).
