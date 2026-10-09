# Tutorial 5: Optimistic Concurrency Control

In high-concurrency systems, multiple users, background workers, or API requests frequently attempt
to modify the same business entity at the exact same time.

In traditional relational databases, developers often prevent conflicts using **pessimistic locking**
(`SELECT FOR UPDATE`). However, holding database locks across network boundaries or application
processing degrades throughput, creates contention, and risks deadlocks.

In event sourcing, streams are protected through **Optimistic Concurrency Control (OCC)**. Instead
of locking an entity while deciding what to do, workers compute state changes optimistically. When
writing events back to the event store, they declare:

> *"Only append these events if the stream is still at the version I originally read."*

If another worker committed an event in the interim, the version check fails, the write is aborted,
and the store raises an `OptimisticLockError`.

In this tutorial, you will explore `ExpectedVersion`, simulate two concurrent requests competing to
ship and cancel the same order, observe `OptimisticLockError`, and implement the canonical
**reload-and-retry** pattern.

---

## Prerequisites

Before beginning, ensure you have:

- Completed [Tutorial 3: Your First Aggregate](03-first-aggregate.md) and [Tutorial 4: The Aggregate Repository](04-repository.md).
- Python 3.13+ with `eventsource-py` installed.

---

## The Four Concurrency Modes: `ExpectedVersion`

When appending events via `store.append(stream, events, expected=...)`, you pass an `ExpectedVersion`
specifying your concurrency expectation.

`ExpectedVersion` supports four distinct strategies:

| Strategy | Usage | Description |
| --- | --- | --- |
| `ExpectedVersion.exact(v)` | `ExpectedVersion.exact(2)` | Requires the stream to be at **exactly** version `v`. Used by `AggregateRepository` on every save. |
| `ExpectedVersion.no_stream()` | `ExpectedVersion.no_stream()` | Requires the stream to **not exist yet** (version 0). Guarantees that aggregate creation doesn't overwrite an existing entity with the same ID. |
| `ExpectedVersion.stream_exists()` | `ExpectedVersion.stream_exists()` | Requires the stream to exist (version $\ge 1$). Appends to an existing stream without asserting an exact version count. |
| `ExpectedVersion.any_()` | `ExpectedVersion.any_()` | Ignores the stream version entirely. Blindly appends events regardless of current state. Useful for fire-and-forget telemetry streams. |

Let's see how the event store validates each expectation:

```python
from uuid import uuid4
from eventsource import ExpectedVersion, InMemoryEventStore, OptimisticLockError, StreamId

store = InMemoryEventStore()
stream = StreamId(aggregate_id=uuid4(), category="Order")

# 1. no_stream: succeeds on empty stream
await store.append(stream, [OrderCreated(...)], ExpectedVersion.no_stream())

# 2. no_stream: fails if called again because the stream now exists
try:
    await store.append(stream, [OrderCreated(...)], ExpectedVersion.no_stream())
except OptimisticLockError as err:
    print(f"Collision: Expected {err.expected_version}, but actual version is {err.actual_version}")

# 3. exact: fails if version doesn't match
try:
    # Expecting version 5, but stream is currently at version 1
    await store.append(stream, [OrderShipped(...)], ExpectedVersion.exact(5))
except OptimisticLockError as err:
    print(f"Version mismatch: Expected {err.expected_version}, got {err.actual_version}")
```

---

## The Anatomy of a Race Condition

Consider a common e-commerce scenario:

1. An order has been placed (`OrderCreated`, version 1).
2. **Worker A (Warehouse Fulfillment)** receives a webhook to ship the order. It loads the order at version 1.
3. **Worker B (Customer Support)** receives a customer request to cancel the order. It also loads the order at version 1.
4. Both workers run domain validation concurrently in memory against version 1. Both commands are deemed valid!
5. **Worker A** commits first: appends `OrderShipped` with `ExpectedVersion.exact(1)`. Stream advances to version 2.
6. **Worker B** now tries to commit `OrderCancelled` with `ExpectedVersion.exact(1)`.

```
Timeline:

Worker A (Fulfill)          Worker B (Cancel)           Event Store (Stream v1)
      |                           |                               |
      |---- Load Order (v1) ----->|                               |
      |<--- State: Created -------|                               |
      |                           |---- Load Order (v1) --------->|
      |                           |<--- State: Created -----------|
      |                                                           |
   [Execute ShipOrder]         [Execute CancelOrder]              |
      |                                                           |
      |--- Save (Expected: 1) ----------------------------------->|
      |<-- Append OK (Now v2) ------------------------------------|
                                  |
                                  |--- Save (Expected: 1) ------->|
                                  |<-- OptimisticLockError! ------|
```

Without optimistic concurrency control, Worker B's write would have silently succeeded, leaving the
order in an impossible business state: **both shipped and cancelled**.

With OCC, Worker B's save is rejected with an `OptimisticLockError`.

---

## Inspecting `OptimisticLockError`

When `OptimisticLockError` is raised, it carries metadata that lets you diagnose the conflict:

```python
try:
    await repo.save(worker_b_order)
except OptimisticLockError as error:
    print(f"Aggregate ID:     {error.aggregate_id}")
    print(f"Expected Version: {error.expected_version}")
    print(f"Actual Version:   {error.actual_version}")
```

- `aggregate_id`: The UUID of the conflicting entity.
- `expected_version`: The version your in-memory aggregate started from before applying new events.
- `actual_version`: The true version currently stored in the event store.

---

## The Reload-and-Retry Pattern

How should application services handle an `OptimisticLockError`?

In many cases, the right solution is to **reload the aggregate from the repository and re-attempt the command**.

When you reload the aggregate:
1. All newly committed events are replayed into state.
2. The domain's `decide()` function re-evaluates the command against the **updated** state.
3. If the command is still valid under the new state, it is executed and saved.
4. If the new state invalidates the command (for example, attempting to cancel an order that has already shipped), `decide()` raises `CommandRejectedError`, cleanly halting the operation with a business rule refusal.

Here is the standard retry helper pattern:

```python
async def execute_with_retry(
    repo: AggregateRepository[OrderAggregate],
    order_id: UUID,
    command: OrderCommand,
    max_retries: int = 3,
) -> OrderAggregate:
    for attempt in range(max_retries):
        order = await repo.load(order_id)
        order.execute(command)  # May raise CommandRejectedError
        try:
            await repo.save(order)
            return order
        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise
            # Optional: await asyncio.sleep(0.05 * (2 ** attempt))
    raise RuntimeError("Exhausted retries")
```

---

## Complete Runnable Example

The following script simulates two concurrent workers colliding on the same order, demonstrates
`OptimisticLockError`, and shows how the reload-and-retry pattern prevents invalid state transitions.

Save this script as `optimistic_concurrency_demo.py`:

```python
import asyncio
from decimal import Decimal
from uuid import UUID, uuid4
from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    CommandRejectedError,
    DeciderAggregate,
    DomainCommand,
    DomainEvent,
    ExpectedVersion,
    InMemoryEventStore,
    OptimisticLockError,
    StreamId,
    register_event,
)


# --- 1. Domain Model ---
class OrderState(BaseModel):
    customer_id: UUID | None = None
    total: Decimal = Decimal("0")
    status: str = "pending"
    tracking_number: str | None = None
    cancellation_reason: str | None = None


@register_event
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: UUID
    total: Decimal


@register_event
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str


@register_event
class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"
    reason: str


class CreateOrder(DomainCommand):
    order_id: UUID
    customer_id: UUID
    total: Decimal


class ShipOrder(DomainCommand):
    order_id: UUID
    tracking_number: str


class CancelOrder(DomainCommand):
    order_id: UUID
    reason: str


OrderCommand = CreateOrder | ShipOrder | CancelOrder


class OrderAggregate(DeciderAggregate[OrderState, OrderCommand]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state() -> OrderState:
        return OrderState()

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        match event:
            case OrderCreated(customer_id=cid, total=tot):
                return state.model_copy(
                    update={"customer_id": cid, "total": tot, "status": "created"}
                )
            case OrderShipped(tracking_number=trk):
                return state.model_copy(
                    update={"tracking_number": trk, "status": "shipped"}
                )
            case OrderCancelled(reason=r):
                return state.model_copy(
                    update={"cancellation_reason": r, "status": "cancelled"}
                )
            case _:
                return state

    @staticmethod
    def decide(command: OrderCommand, state: OrderState) -> list[DomainEvent]:
        match command, state:
            case CreateOrder(order_id=oid, customer_id=cid, total=tot), OrderState(status="pending"):
                return [OrderCreated(aggregate_id=oid, customer_id=cid, total=tot)]
            case ShipOrder(order_id=oid, tracking_number=trk), OrderState(status="created"):
                return [OrderShipped(aggregate_id=oid, tracking_number=trk)]
            case ShipOrder(), OrderState(status="shipped"):
                raise CommandRejectedError("Order is already shipped", command=command)
            case ShipOrder(), OrderState(status="cancelled"):
                raise CommandRejectedError("Cannot ship a cancelled order", command=command)
            case CancelOrder(order_id=oid, reason=r), OrderState(status="created"):
                return [OrderCancelled(aggregate_id=oid, reason=r)]
            case CancelOrder(), OrderState(status="shipped"):
                raise CommandRejectedError("Cannot cancel an order that has already shipped", command=command)
            case CancelOrder(), OrderState(status="cancelled"):
                raise CommandRejectedError("Order is already cancelled", command=command)
            case _:
                raise CommandRejectedError(f"Action not permitted in state '{state.status}'", command=command)


# --- 2. Concurrency Simulation ---
async def main() -> None:
    store = InMemoryEventStore()
    repo = AggregateRepository(event_store=store, aggregate_factory=OrderAggregate)

    # Step A: Create and save an initial order (Version 1)
    order_id = uuid4()
    order = OrderAggregate(order_id)
    order.execute(CreateOrder(order_id=order_id, customer_id=uuid4(), total=Decimal("249.00")))
    await repo.save(order)
    print(f"Order created: {order_id} (Version: {order.version})\n")

    # Step B: Two concurrent workers load the order at Version 1
    worker_shipping = await repo.load(order_id)
    worker_support = await repo.load(order_id)

    print("Simulating concurrent read:")
    print(f"  Worker Shipping sees version: {worker_shipping.version}")
    print(f"  Worker Support sees version:  {worker_support.version}\n")

    # Step C: Both workers execute their domain commands in memory
    worker_shipping.execute(ShipOrder(order_id=order_id, tracking_number="UPS-CONCURRENT-01"))
    worker_support.execute(CancelOrder(order_id=order_id, reason="Customer called to cancel"))

    # Step D: Worker Shipping commits first -> Succeeds
    print("Worker Shipping committing...")
    await repo.save(worker_shipping)
    print(f"  Successfully saved! Stream is now at Version {worker_shipping.version}\n")

    # Step E: Worker Support attempts to commit -> Collision!
    print("Worker Support committing (still expects Version 1)...")
    try:
        await repo.save(worker_support)
        print("  Error: Save should not have succeeded!")
    except OptimisticLockError as err:
        print("  Caught expected OptimisticLockError:")
        print(f"    Target Aggregate: {err.aggregate_id}")
        print(f"    Expected Version: {err.expected_version}")
        print(f"    Actual Version:   {err.actual_version}\n")

    # Step F: Worker Support executes the reload-and-retry pattern
    print("Executing Reload-and-Retry for Worker Support:")
    # 1. Reload the latest state from the repository
    fresh_order = await repo.load(order_id)
    print(f"  Reloaded order state: status='{fresh_order.state.status}', version={fresh_order.version}")

    # 2. Re-attempt the CancelOrder command
    try:
        fresh_order.execute(CancelOrder(order_id=order_id, reason="Customer called to cancel"))
        await repo.save(fresh_order)
    except CommandRejectedError as rejection:
        print(f"  Domain rejected command: '{rejection}'")
        print("  -> Business invariant protected! Shipped orders cannot be cancelled.")


if __name__ == "__main__":
    asyncio.run(main())
```

Run the script:

```bash
python3 optimistic_concurrency_demo.py
```

Output:
```text
Order created: aee6ea4e-1b32-47ee-bda1-77fffe19a3bb (Version: 1)

Simulating concurrent read:
  Worker Shipping sees version: 1
  Worker Support sees version:  1

Worker Shipping committing...
  Successfully saved! Stream is now at Version 2

Worker Support committing (still expects Version 1)...
  Caught expected OptimisticLockError:
    Target Aggregate: aee6ea4e-1b32-47ee-bda1-77fffe19a3bb
    Expected Version: 1
    Actual Version:   2

Executing Reload-and-Retry for Worker Support:
  Reloaded order state: status='shipped', version=2
  Domain rejected command: 'Cannot cancel an order that has already shipped'
  -> Business invariant protected! Shipped orders cannot be cancelled.
```

---

## Key Takeaways

- **Locks are optimistic**: No locks are held in the database during business logic evaluation. Conflicts are detected strictly at write time.
- **`ExpectedVersion.exact(v)`**: Guarantees that appends succeed if and only if no concurrent writes slipped in.
- **`OptimisticLockError` prevents state corruption**: Concurrent conflicting commands cannot both succeed.
- **Reload-and-retry is the standard protocol**: Reloading fetches the latest events and allows pure `decide()` logic to re-verify business rules.

---

## What You've Accomplished in Phase 1

Congratulations! You have completed **Phase 1: Domain Modeling & Core Event Sourcing**:

1. **[01. Getting Started](01-getting-started.md)**: Installed `eventsource-py`, met the ordering domain, appended to and read from an in-memory store.
2. **[02. Your First Domain Event](02-first-event.md)**: Mastered immutable event modeling, Pydantic schemas, causation, and correlation chains.
3. **[03. Your First Aggregate](03-first-aggregate.md)**: Built pure `decide()` and `evolve()` decider functions inside `DeciderAggregate`.
4. **[04. The Aggregate Repository](04-repository.md)**: Bound aggregates to storage, replaying histories and publishing committed events.
5. **[05. Optimistic Concurrency](05-optimistic-concurrency.md)**: Protected streams against concurrent writes and implemented retry workflows.

---

## Next Steps

Now that you have a fully functional write model in memory, it is time to build the **read side**.
In Phase 2, you will learn how to consume event streams asynchronously and build high-performance query models:

👉 **[Tutorial 6: Projections & Read Models](06-projections.md)**
