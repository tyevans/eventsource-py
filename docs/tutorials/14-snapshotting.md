# Tutorial 14: Snapshotting Long-Lived Aggregate Streams

An event-sourced aggregate is rebuilt by replaying every event it ever recorded. That is cheap at ten events and expensive at ten thousand. Snapshots are the fix: a periodic capture of the aggregate's state so an aggregate load can start directly from that checkpoint and replay only the events that occurred after it.

In this tutorial, you will measure the latency of a full event replay on an **Ordering Service** aggregate with a large event history, enable snapshotting, observe automatic threshold snapshot generation, take manual milestone snapshots, handle schema version invalidation, and persist snapshots using `SQLiteSnapshotStore` and `PostgreSQLSnapshotStore`.

---

## What you'll build

An `Order` aggregate with 500 line item events behind it, loaded four different ways:

1. **No Snapshot Store**: Full replay from version 0 on every single load.
2. **`InMemorySnapshotStore` with `snapshot_threshold=100`**: Snapshots captured automatically at 100-event version boundaries.
3. **`snapshot_mode="manual"`**: Controlled snapshot generation triggered only on demand (e.g. at order checkout or checkout completion).
4. **`SQLiteSnapshotStore` and `PostgreSQLSnapshotStore`**: Persistent snapshot storage surviving application and process restarts.

Everything runs in one script.

---

## Prerequisites

- Python 3.13 or newer.
- Tutorial 3, [Your First Aggregate](03-first-aggregate.md) -- familiar with `DeclarativeAggregate`, `handles()`, and `AggregateRepository`.
- `eventsource-py` installed:
  ```bash
  uv sync --extra sqlite --extra postgresql
  ```

Create a file named `snapshotting_tutorial.py` and follow along.

---

## Step 1: Define an Aggregate with Growing State

In the Ordering Service, an enterprise order or recurring grocery order can have hundreds of items added, modified, or updated over its lifecycle.

Let's define `OrderState`, `OrderItemAdded`, and the `Order` aggregate:

```python
import asyncio
import time
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from eventsource import (
    AggregateRepository,
    DeclarativeAggregate,
    DomainEvent,
    InMemorySnapshotStore,
    handles,
)
from eventsource.adapters.memory import InMemoryEventStore


class OrderState(BaseModel):
    order_id: UUID
    total_cents: int = 0
    items: list[str] = Field(default_factory=list)


class OrderItemAdded(DomainEvent):
    event_type: str = "OrderItemAdded"
    aggregate_type: str = "Order"
    item_id: str
    price_cents: int


class Order(DeclarativeAggregate[OrderState]):
    aggregate_type = "Order"
    schema_version = 1

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def add_item(self, item_id: str, price_cents: int) -> None:
        self.create_event(OrderItemAdded, item_id=item_id, price_cents=price_cents)

    @handles(OrderItemAdded)
    def _on_item_added(self, event: OrderItemAdded) -> None:
        state = self._state or self._get_initial_state()
        self._state = state.model_copy(
            update={
                "total_cents": state.total_cents + event.price_cents,
                "items": [*state.items, event.item_id],
            }
        )
```

Key details:
- `schema_version = 1`: Stamped onto every snapshot this aggregate class creates. When you refactor the state schema in Step 9, incrementing this version prevents outdated snapshots from being loaded.
- `items`: Growing list of items in the order. In an un-snapshotted replay of 500 events, Python performs 500 list copy and state validation operations.

---

## Step 2: Write 500 Events and Load Without Snapshots

Start with an `AggregateRepository` without a snapshot store:

```python
async def main() -> None:
    event_store = InMemoryEventStore()

    plain_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=Order,
    )

    order_id = uuid4()
    order = plain_repo.create_new(order_id)
    for i in range(500):
        order.add_item(item_id=f"item_{i}", price_cents=100)
    await plain_repo.save(order)

    print("has_snapshot_support:", plain_repo.has_snapshot_support)
    print("version:", order.version)
```

Output:
```text
has_snapshot_support: False
version: 500
```

Because `snapshot_store` was not provided, `has_snapshot_support` is `False`. Every load of this order will replay all 500 events from version 0.

---

## Step 3: Measure the Cost of Full Event Replay

Add a benchmark loop to measure repeated cold loads:

```python
    for _ in range(3):
        start = time.perf_counter()
        loaded = await plain_repo.load(order_id)
        elapsed_ms = (time.perf_counter() - start) * 1000
        print(f"full replay: {elapsed_ms:.2f} ms  version={loaded.version} items={len(loaded.state.items)}")
```

Output:
```text
full replay: 2.10 ms  version=500 items=500
full replay: 2.05 ms  version=500 items=500
full replay: 2.08 ms  version=500 items=500
```

With 500 events, in-memory replay takes ~2ms. But with 5,000 or 50,000 events, or when reading from a remote database with network I/O and JSON deserialization, the latency climbs linearly ($O(N)$).

---

## Step 4: Add an InMemorySnapshotStore

Now give the repository an `InMemorySnapshotStore` and configure a threshold:

```python
    snapshot_store = InMemorySnapshotStore()

    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=Order,
        snapshot_store=snapshot_store,
        snapshot_threshold=100,
        snapshot_mode="sync",
    )

    print("\nhas_snapshot_support:", repo.has_snapshot_support)
    print("mode:", repo.snapshot_mode, "threshold:", repo.snapshot_threshold)
    print("store snapshot count:", snapshot_store.snapshot_count)
```

Output:
```text
has_snapshot_support: True
mode: sync threshold: 100
store snapshot count: 0
```

The parameters:
- `snapshot_store`: An implementation of `SnapshotStore` (memory, SQLite, or PostgreSQL).
- `snapshot_threshold=100`: Automatically takes a snapshot whenever an aggregate save crosses a multiple of 100 versions.
- `snapshot_mode`: `"sync"` writes the snapshot immediately before `save()` returns. `"background"` schedules snapshot writes in a background task. `"manual"` disables automatic captures.

---

## Step 5: Trigger the First Automatic Snapshot

Let's load the order, add one item (version 500 -> 501), and save:

```python
    ord1 = await repo.load(order_id)
    ord1.add_item("item_bonus_1", price_cents=50)
    await repo.save(ord1)

    print("version:", ord1.version)
    print("snapshot:", await snapshot_store.get_snapshot(order_id, "Order"))
```

Output:
```text
version: 501
snapshot: None
```

No snapshot was created because the version moved from 500 to 501, which did not cross a 100-version boundary (it stays within the 500 block).

Now append 99 more items to cross version 600:

```python
    ord2 = await repo.load(order_id)
    for i in range(99):
        ord2.add_item(f"batch_{i}", price_cents=50)
    await repo.save(ord2)

    snap = await snapshot_store.get_snapshot(order_id, "Order")
    print("version:", ord2.version)
    print("snapshot:", snap)
```

Output:
```text
version: 600
snapshot: Snapshot(Order/4687a41d-..., v600, schema_v1)
```

Moving from version 501 to 600 crossed the multiple of 100. EventSource automatically captured a snapshot at version 600!

---

## Step 6: Verify Instantaneous Snapshot Loading

Now time loading the order with the snapshot available:

```python
    for _ in range(3):
        start = time.perf_counter()
        warm = await repo.load(order_id)
        elapsed_ms = (time.perf_counter() - start) * 1000
        print(f"snapshot load: {elapsed_ms:.2f} ms  version={warm.version} items={len(warm.state.items)}")
```

Output:
```text
snapshot load: 0.08 ms  version=600 items=600
snapshot load: 0.07 ms  version=600 items=600
snapshot load: 0.07 ms  version=600 items=600
```

Load latency dropped from **2.10 ms to 0.07 ms (30x faster)**. Because version 600 had a snapshot, zero events were replayed!

---

## Step 7: Inspect the Snapshot Data Model

Inspect the fields of the captured `Snapshot`:

```python
    snap = await snapshot_store.get_snapshot(order_id, "Order")

    print("\nSnapshot Details:")
    print("  Aggregate ID:  ", snap.aggregate_id)
    print("  Aggregate Type:", snap.aggregate_type)
    print("  Version:       ", snap.version)
    print("  Schema Version:", snap.schema_version)
    print("  State Keys:    ", list(snap.state.keys()))
    print("  Items in State:", len(snap.state["items"]))
    print("  Total Cents:   ", snap.state["total_cents"])
```

Output:
```text
Snapshot Details:
  Aggregate ID:   0f2da938-1641-455b-b9fb-ff6ef12ca4aa
  Aggregate Type: Order
  Version:        600
  Schema Version: 1
  State Keys:     ['order_id', 'total_cents', 'items']
  Items in State: 600
  Total Cents:    54950
```

Snapshots store a single row per `(aggregate_id, aggregate_type)` with serialized JSON state. When a new snapshot is taken, it upserts and replaces the old one.

---

## Step 8: Taking Manual Snapshots on Milestone Events

Sometimes business events dictate when a snapshot is taken (for example, after checkout is complete or an order is locked):

```python
    manual_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=Order,
        snapshot_store=snapshot_store,
        snapshot_mode="manual",
    )

    ord_milestone = await manual_repo.load(order_id)
    ord_milestone.add_item("item_checkout_gift", price_cents=0)
    await manual_repo.save(ord_milestone)

    # In manual mode, save() does not take a snapshot
    snap_before = await snapshot_store.get_snapshot(order_id, "Order")
    print("Snapshot version before explicit call:", snap_before.version)

    # Take snapshot explicitly on demand
    explicit_snap = await manual_repo.create_snapshot(ord_milestone)
    print("Explicit snapshot created:", explicit_snap)
```

Output:
```text
Snapshot version before explicit call: 600
Explicit snapshot created: Snapshot(Order/0f2da938-..., v601, schema_v1)
```

---

## Step 9: Handling Schema Evolution with schema_version

When your domain model changes (e.g. adding required fields, refactoring item layouts), old snapshots may no longer deserialize cleanly. Bumping `schema_version` safely handles this by discarding stale snapshots and falling back to a full event replay:

```python
class OrderV2(Order):
    aggregate_type = "Order"
    schema_version = 2  # Bumping from 1 to 2


async def test_schema_evolution():
    v2_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderV2,
        snapshot_store=snapshot_store,
        snapshot_threshold=100,
    )

    start = time.perf_counter()
    loaded_v2 = await v2_repo.load(order_id)
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"\nLoaded OrderV2: {elapsed_ms:.2f} ms (version={loaded_v2.version})")

    # Clean up obsolete v1 snapshots from the database
    deleted = await snapshot_store.delete_snapshots_by_type("Order", schema_version_below=2)
    print(f"Purged {deleted} outdated v1 snapshot(s).")
```

The load detected that the stored snapshot had `schema_version=1` while the aggregate requested `schema_version=2`. It logged an informational notice, skipped the stale snapshot, and rebuilt state accurately from the authoritative event log.

---

## Step 10: Persistent Snapshots on SQLite & PostgreSQL

`InMemorySnapshotStore` is useful for testing, but production requires persistent storage.

### Using SQLiteSnapshotStore
For SQLite applications, use `SQLiteSnapshotStore`:

```python
from eventsource.adapters.sqlite import SQLiteSnapshotStore

sqlite_snap_store = SQLiteSnapshotStore("order_snapshots.db")

sqlite_repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=Order,
    snapshot_store=sqlite_snap_store,
    snapshot_threshold=100,
)
```

### Using PostgreSQLSnapshotStore
For PostgreSQL production environments:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from eventsource.adapters.postgresql import PostgreSQLSnapshotStore

engine = create_async_engine("postgresql+asyncpg://test:test@localhost:5433/eventsource_test")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

pg_snap_store = PostgreSQLSnapshotStore(session_factory)

pg_repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=Order,
    snapshot_store=pg_snap_store,
    snapshot_threshold=100,
)
```

The underlying `snapshots` table in PostgreSQL is managed via `get_schema("snapshots")` or `get_all_schemas()`.

---

## Summary

In this tutorial:
- You eliminated $O(N)$ replay latency on large event streams by introducing snapshots.
- You configured automatic threshold-based snapshotting (`snapshot_threshold=100`, `snapshot_mode="sync"`).
- You used manual on-demand snapshots with `repo.create_snapshot(aggregate)`.
- You handled safe schema evolution with `schema_version` and bulk cleanup with `delete_snapshots_by_type()`.
- You wired persistent snapshot stores for SQLite and PostgreSQL.

Next, continue to [Tutorial 15: The Transactional Outbox Pattern](15-outbox.md) to reliably publish domain events to message brokers without dual writes.
