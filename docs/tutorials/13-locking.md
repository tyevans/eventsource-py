# Tutorial 13: Distributed Concurrency with PostgreSQL Advisory Locks

In Tutorials 1 through 5, you relied on **optimistic concurrency control** (`ExpectedVersion` and `OptimisticLockError`). When two requests attempt to append to the same event stream concurrently, the database unique constraint (`uq_events_aggregate_version`) ensures that only one write succeeds, while the other raises `OptimisticLockError`.

Optimistic concurrency is fast and sufficient when conflicts are rare and operations are purely internal to the database. However, real-world systems frequently encounter scenarios where optimistic concurrency alone is insufficient:

1. **Non-Transactional Side Effects**: If a command handler charges a credit card via Stripe or reserves stock in a third-party warehouse *before* appending events, an `OptimisticLockError` on the append leaves the external side effect stranded.
2. **Thundering Herds**: When hundreds of concurrent requests contend for the same aggregate (e.g., flash sales, ticket releases), repeatedly loading, calculating, and failing on append burns CPU and database connections.
3. **Cross-Process Coordination**: Background maintenance, live migrations (cutover), or scheduled reconciliation tasks often require single-writer execution across multiple container replicas.

In this tutorial, you will learn how to serialize critical command execution across distributed processes using **PostgreSQL Advisory Locks** with `PostgreSQLLockManager` (also available as `PostgreSQLAdvisoryLock`).

---

## What are PostgreSQL Advisory Locks?

PostgreSQL advisory locks are application-level cooperative locks managed directly by the PostgreSQL server:

- **Ultra-lightweight**: Stored purely in PostgreSQL shared memory (the lock table), not on disk. They produce no table bloat, generate no WAL, and never lock database rows or tables.
- **Connection-Bound & Crash-Safe**: Advisory locks are tied to the database session. If an application worker crashes, runs out of memory, or disconnects from the network, PostgreSQL immediately releases all advisory locks held by that session.
- **Cross-Process & Cross-Host**: Any process connected to the same PostgreSQL instance or cluster participates in the mutual exclusion, regardless of whether workers run on Kubernetes pods, Celery workers, or serverless functions.
- **Deterministic 64-bit Integer Hashing**: PostgreSQL advisory lock functions (`pg_advisory_lock`, `pg_try_advisory_lock`, `pg_advisory_unlock`) take 64-bit integers (`bigint`). EventSource's `PostgreSQLLockManager` transparently converts arbitrary string keys (e.g., `"order:f47ac10b-58cc-4372-a567-0e02b2c3d479"`) into 63-bit signed integer hashes.

---

## Learning Objectives

By the end of this tutorial, you will:
- Understand when to pair optimistic concurrency with distributed pessimistic locks.
- Configure and instantiate `PostgreSQLLockManager` using SQLAlchemy async session factories.
- Serialize concurrent commands on the **Ordering Service** using async context managers (`async with lock_manager.acquire(...)`).
- Handle lock contention gracefully with timeouts and retry intervals (`LockAcquisitionError`).
- Use non-blocking acquisition with `try_acquire()` and explicit releases.
- Understand session scoping, re-entrancy boundaries, and deadlock prevention.
- Coordinate multiple background workers safely.

---

## Prerequisites

1. **Python 3.13+** with `eventsource-py` and the `postgresql` extra installed:
   ```bash
   uv sync --extra postgresql
   ```
2. **PostgreSQL 15** running via Docker Compose (from the repository root):
   ```bash
   docker compose -f docker-compose.test.yml up -d postgres
   ```
   PostgreSQL is accessible at `postgresql+asyncpg://test:test@localhost:5433/eventsource_test`.

---

## The Ordering Problem: Payment vs Cancellation Race

Let's model an ordering scenario where distributed locking is essential.

An `Order` aggregate starts in state `CREATED`. Two actions can happen:
1. **Pay**: Charges customer via external payment processor, then appends `OrderPaid`.
2. **Cancel**: Verifies the order is not yet paid, releases warehouse hold, then appends `OrderCancelled`.

```mermaid
sequenceDiagram
    participant Worker 1 (Pay)
    participant Lock as PostgreSQL Advisory Lock
    participant Stripe as Payment Gateway
    participant DB as PostgreSQLEventStore
    participant Worker 2 (Cancel)

    Worker 1->>Lock: acquire("order:123")
    activate Lock
    Note over Lock: Lock granted to Worker 1

    Worker 2->>Lock: acquire("order:123", timeout=2.0)
    Note over Worker 2: Blocked / Waiting...

    Worker 1->>Stripe: Charge $99.00
    Stripe-->>Worker 1: Payment Succeeded
    Worker 1->>DB: append(OrderPaid)
    Worker 1->>Lock: release()
    deactivate Lock

    Note over Lock: Lock granted to Worker 2
    activate Lock
    Worker 2->>DB: read_stream("order:123")
    Note over Worker 2: Sees OrderPaid!<br/>Rejects cancellation
    Worker 2->>Lock: release()
    deactivate Lock
```

Without the lock, Worker 2 could cancel the order while Worker 1 is in the middle of talking to Stripe, resulting in a customer being charged for a cancelled order.

---

## Step 1: Defining the Ordering Domain Events

Create a new file named `locking_tutorial.py`. First, define the domain events for our ordering service:

```python
import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4
from pydantic import BaseModel, Field

from eventsource import DomainEvent, ExpectedVersion
from eventsource.adapters.postgresql import (
    PostgreSQLEventStore,
    PostgreSQLLockManager,
    PostgreSQLAdvisoryLock,  # Alias for PostgreSQLLockManager
)
from eventsource.ports.exceptions import LockAcquisitionError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# --- Domain Events ---

class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: str
    total_amount: int  # in cents

class OrderPaid(DomainEvent):
    aggregate_type: str = "Order"
    transaction_id: str
    paid_at: str

class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"
    reason: str
    cancelled_at: str
```

---

## Step 2: Setting up Engine and LockManager

`PostgreSQLLockManager` requires an `async_sessionmaker[AsyncSession]` because PostgreSQL advisory locks are session-bound. The lock manager dedicates a session to each active lock for the duration of its hold:

```python
DATABASE_URL = "postgresql+asyncpg://test:test@localhost:5433/eventsource_test"

engine = create_async_engine(DATABASE_URL, echo=False)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

# Instantiate the lock manager
lock_manager = PostgreSQLLockManager(
    session_factory,
    holder_id="order-service-worker-1",
    enable_tracing=False,
)
```

> [!NOTE]
> `holder_id` is an optional diagnostic string. It helps identify which worker instance acquired a lock when inspecting lock telemetry or debug logs.

---

## Step 3: Acquiring Locks with Async Context Manager

The recommended and safest way to acquire a distributed lock is via `async with lock_manager.acquire(key)`:

```python
async def demo_basic_lock() -> None:
    order_id = uuid4()
    lock_key = f"order:{order_id}"

    print(f"Acquiring lock for {lock_key}...")
    async with lock_manager.acquire(lock_key) as lock_info:
        print(f"Lock successfully acquired!")
        print(f"  Key: {lock_info.key}")
        print(f"  PostgreSQL Lock ID: {lock_info.lock_id}")
        print(f"  Acquired at: {lock_info.acquired_at}")
        print(f"  Holder: {lock_info.holder_id}")
        print(f"  Is held? {await lock_manager.is_held(lock_key)}")

        # Perform protected work here...
        await asyncio.sleep(0.1)

    # Automatically released upon exiting the block
    print(f"Lock released. Is held? {await lock_manager.is_held(lock_key)}")
```

Even if an unexpected error occurs inside the `async with` block, the lock is guaranteed to be released cleanly in PostgreSQL.

---

## Step 4: Handling Contention with Timeouts

When multiple workers compete for the same order, a worker can specify a `timeout` (in seconds) and a `retry_interval`:

- `timeout=None` (default): Blocks indefinitely until the lock is acquired.
- `timeout=2.0`: Retries every `retry_interval` (default 0.1s) until acquired. If 2.0 seconds elapse without securing the lock, raises `LockAcquisitionError`.

Let's simulate two workers racing to process the same order:

```python
async def simulate_worker_payment(order_id: UUID, store: PostgreSQLEventStore) -> None:
    lock_key = f"order:{order_id}"
    print(f"[Worker 1 - Pay] Waiting for lock on {lock_key}...")

    async with lock_manager.acquire(lock_key, timeout=5.0):
        print(f"[Worker 1 - Pay] Lock acquired. Calling payment gateway...")
        # Simulate non-transactional external API call (e.g., Stripe)
        await asyncio.sleep(0.5)

        event = OrderPaid(
            aggregate_id=order_id,
            aggregate_version=2,
            transaction_id=f"tx_{uuid4().hex[:8]}",
            paid_at=datetime.now(UTC).isoformat(),
        )
        await store.append(
            stream_id=str(order_id),
            events=[event],
            expected_version=ExpectedVersion.EXACT(1),
        )
        print(f"[Worker 1 - Pay] Order {order_id} marked as PAID. Releasing lock.")


async def simulate_worker_cancellation(order_id: UUID, store: PostgreSQLEventStore) -> None:
    lock_key = f"order:{order_id}"
    # Wait a fraction of a second so Worker 1 acquires first
    await asyncio.sleep(0.05)
    print(f"[Worker 2 - Cancel] Attempting to acquire lock on {lock_key} (timeout 1.0s)...")

    try:
        async with lock_manager.acquire(lock_key, timeout=1.0):
            print(f"[Worker 2 - Cancel] Lock acquired! Checking order stream...")
            stream = await store.read_stream(str(order_id))
            event_types = [e.event_type for e in stream]

            if "OrderPaid" in event_types:
                print(f"[Worker 2 - Cancel] Cannot cancel: Order {order_id} has already been PAID!")
            else:
                event = OrderCancelled(
                    aggregate_id=order_id,
                    aggregate_version=len(stream) + 1,
                    reason="Customer changed mind",
                    cancelled_at=datetime.now(UTC).isoformat(),
                )
                await store.append(
                    stream_id=str(order_id),
                    events=[event],
                    expected_version=ExpectedVersion.EXACT(len(stream)),
                )
                print(f"[Worker 2 - Cancel] Order {order_id} cancelled.")
    except LockAcquisitionError as err:
        print(f"[Worker 2 - Cancel] Contention timeout! Could not acquire lock: {err.reason}")
```

Notice what happened:
Worker 2 did not crash or corrupt state. Because Worker 2 waited for the lock, it observed that Worker 1 had successfully appended `OrderPaid`, preventing an illegal cancellation!

---

## Step 5: Non-Blocking Acquisition with `try_acquire`

In high-throughput HTTP endpoints or websocket handlers, you may want to fail fast immediately if another process is working on the order, rather than waiting. Use `try_acquire()`:

```python
async def demo_try_acquire(order_id: UUID) -> None:
    lock_key = f"order:{order_id}"

    # Try acquiring without blocking
    lock_info = await lock_manager.try_acquire(lock_key)
    if lock_info is None:
        print(f"Could not acquire {lock_key}: another worker is currently holding it.")
        return

    try:
        print(f"Immediately acquired lock {lock_info.key}")
        # Execute exclusive operation...
    finally:
        # When using try_acquire, you must explicitly release!
        await lock_manager.release(lock_key)
        print("Explicitly released lock.")
```

---

## Step 6: Session Scoping, Re-entrancy, and Deadlock Prevention

PostgreSQL advisory locks have specific architectural rules that every engineer must understand:

### 1. Connection Isolation & Re-entrancy
In PostgreSQL, `pg_advisory_lock` is re-entrant **within the same database session**. If session A calls `pg_advisory_lock(42)` twice, it succeeds both times and must unlock twice.

However, `PostgreSQLLockManager` assigns a **dedicated database session** to each active lock to ensure isolation between distinct lock operations. Therefore:
- Acquiring the **same key** nested inside an already active `acquire()` block on the same manager will attempt to lock on a *different* database session, causing a self-deadlock or timeout.
- **Rule**: Do not nest lock acquisitions on the same key. Structure your command handlers so that locks are acquired once at the command boundary.

### 2. Lock Ordering for Multiple Resources
If a business workflow touches two aggregates (for example, transferring credit between `Order A` and `Order B`), acquiring locks in opposite orders across concurrent workers can cause a circular deadlock:

- Worker 1: Locks `order:A`, waits for `order:B`.
- Worker 2: Locks `order:B`, waits for `order:A`.

To prevent deadlocks, sort your lock keys lexicographically before acquiring them:

```python
async def lock_multiple_orders(order_id_1: UUID, order_id_2: UUID):
    keys = sorted([f"order:{order_id_1}", f"order:{order_id_2}"])

    async with lock_manager.acquire(keys[0]):
        async with lock_manager.acquire(keys[1]):
            # Safely operate across both orders with zero deadlock risk
            pass
```

---

## Step 7: Complete Working Simulation

Let's combine everything into a runnable script. This script initializes the PostgreSQL schema, seeds an initial order, runs concurrent racing workers, and verifies the resulting event stream:

```python
import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from eventsource import DomainEvent, ExpectedVersion
from eventsource.adapters.postgresql import (
    PostgreSQLEventStore,
    PostgreSQLLockManager,
)
from eventsource.adapters.sql.schemas import get_all_schemas
from eventsource.ports.exceptions import LockAcquisitionError


class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: str
    total_cents: int


class OrderPaid(DomainEvent):
    aggregate_type: str = "Order"
    transaction_id: str
    paid_at: str


class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"
    reason: str
    cancelled_at: str


async def main() -> None:
    db_url = "postgresql+asyncpg://test:test@localhost:5433/eventsource_test"
    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # 1. Ensure schemas exist
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        await raw_conn.driver_connection.execute(get_all_schemas())

    event_store = PostgreSQLEventStore(engine=engine)
    lock_mgr_1 = PostgreSQLLockManager(session_factory, holder_id="worker-pay")
    lock_mgr_2 = PostgreSQLLockManager(session_factory, holder_id="worker-cancel")

    order_id = uuid4()
    stream_id = str(order_id)
    lock_key = f"order:{order_id}"

    # 2. Place initial order
    placed_event = OrderPlaced(
        aggregate_id=order_id,
        aggregate_version=1,
        customer_id="cust_12345",
        total_cents=4999,
    )
    await event_store.append(
        stream_id=stream_id,
        events=[placed_event],
        expected_version=ExpectedVersion.NO_STREAM,
    )
    print(f"Created order {order_id} with OrderPlaced event.")

    # 3. Define concurrent worker tasks
    async def worker_payment():
        print("[Worker Pay] Requesting lock...")
        async with lock_mgr_1.acquire(lock_key, timeout=3.0) as info:
            print(f"[Worker Pay] Got lock (id={info.lock_id}). Charging payment...")
            await asyncio.sleep(0.4)  # Simulate payment processing

            paid_event = OrderPaid(
                aggregate_id=order_id,
                aggregate_version=2,
                transaction_id="tx_pay_9981",
                paid_at=datetime.now(UTC).isoformat(),
            )
            await event_store.append(
                stream_id=stream_id,
                events=[paid_event],
                expected_version=ExpectedVersion.EXACT(1),
            )
            print("[Worker Pay] OrderPaid appended successfully.")

    async def worker_cancellation():
        await asyncio.sleep(0.05)  # Start slightly after worker_payment
        print("[Worker Cancel] Requesting lock with 2.0s timeout...")
        try:
            async with lock_mgr_2.acquire(lock_key, timeout=2.0):
                print("[Worker Cancel] Got lock! Inspecting stream state...")
                events = await event_store.read_stream(stream_id)
                has_paid = any(isinstance(e, OrderPaid) or e.event_type == "OrderPaid" for e in events)

                if has_paid:
                    print("[Worker Cancel] Rejected: Order is already paid.")
                else:
                    cancel_event = OrderCancelled(
                        aggregate_id=order_id,
                        aggregate_version=len(events) + 1,
                        reason="Customer request",
                        cancelled_at=datetime.now(UTC).isoformat(),
                    )
                    await event_store.append(
                        stream_id=stream_id,
                        events=[cancel_event],
                        expected_version=ExpectedVersion.EXACT(len(events)),
                    )
                    print("[Worker Cancel] Order cancelled.")
        except LockAcquisitionError as e:
            print(f"[Worker Cancel] Timed out waiting for lock: {e}")

    # 4. Run both workers concurrently
    print("\n--- Starting Concurrent Execution ---")
    await asyncio.gather(worker_payment(), worker_cancellation())

    # 5. Verify final stream contents
    print("\n--- Final Stream State ---")
    final_stream = await event_store.read_stream(stream_id)
    for idx, evt in enumerate(final_stream, 1):
        print(f"  {idx}. {evt.event_type} (version {evt.aggregate_version})")

    # 6. Clean up
    await lock_mgr_1.release_all()
    await lock_mgr_2.release_all()
    await engine.dispose()
    print("\nAll locks released and connections closed.")


if __name__ == "__main__":
    asyncio.run(main())
```

Run the script:
```bash
uv run python locking_tutorial.py
```

Expected output:
```text
Created order 2197be04-f8b1-4f81-80bb-6975be6e25dc with OrderPlaced event.

--- Starting Concurrent Execution ---
[Worker Pay] Requesting lock...
[Worker Pay] Got lock (id=529182390192381203). Charging payment...
[Worker Cancel] Requesting lock with 2.0s timeout...
[Worker Pay] OrderPaid appended successfully.
[Worker Cancel] Got lock! Inspecting stream state...
[Worker Cancel] Rejected: Order is already paid.

--- Final Stream State ---
  1. OrderPlaced (version 1)
  2. OrderPaid (version 2)

All locks released and connections closed.
```

---

## Lifecycle & Graceful Shutdown

During application shutdown, ensure any lingering locks held by the process are freed:

```python
# Number of locks held by this manager instance
count = lock_manager.held_lock_count
print(f"Currently holding {count} locks")

# Bulk release all held locks
released = await lock_manager.release_all()
print(f"Cleanly released {released} locks on shutdown.")
```

> [!TIP]
> For unit tests that do not have access to a PostgreSQL database, use `InMemoryLockManager` from `eventsource.adapters.memory`. It implements the identical `LockManager` and `DistributedLock` protocols using Python `asyncio.Condition` primitives.

---

## Summary

In this tutorial, you learned:
- **Why distributed locking matters in event sourcing**: Optimistic concurrency detects database write collisions, but pessimistic advisory locks protect non-transactional side effects and prevent thundering herds.
- **PostgreSQL Advisory Locks**: Lightweight, memory-resident, session-bound, and automatically released on process crash or connection loss.
- **Using `PostgreSQLLockManager`**: Clean `async with lock_mgr.acquire(key, timeout=...)` syntax, non-blocking `try_acquire()`, deterministic 63-bit integer hashing, and bulk `release_all()`.
- **Concurrency Best Practices**: Keep critical sections short, do not nest acquisitions on the same key across separate sessions, and sort keys lexicographically to eliminate circular deadlocks.

In the next tutorial, [Tutorial 14: Snapshotting](14-snapshotting.md), you will learn how to optimize read performance for long-lived aggregate streams.
