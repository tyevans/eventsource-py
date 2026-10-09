# Tutorial 4: The Aggregate Repository

In [Tutorial 3: Your First Aggregate](03-first-aggregate.md), you created a complete
`OrderAggregate` using the decider pattern. You executed commands, observed state transitions,
and saw new events accumulate in `uncommitted_events`. But that aggregate existed only in memory:
when the script exited, its history was lost.

To build real applications, you need to persist those uncommitted events into an event store and
reload aggregates across distinct HTTP requests, background jobs, or user sessions.

You could theoretically call `store.append()` and `store.read_stream()` directly from your API
handlers or application services. In practice, doing so leads to brittle, repetitive code.

In this tutorial, you will learn why the **Repository pattern** is the canonical application boundary
for event-sourced aggregates. You will configure an `AggregateRepository`, load and save orders,
observe how historical events are folded into state, and publish newly committed events to an event bus.

---

## Why an Aggregate Repository?

In Clean Architecture and Domain-Driven Design (DDD), your domain layer contains pure business logic
(`OrderAggregate`, `OrderState`, `decide`, `evolve`), while your application layer coordinates
use cases.

If your application services interacted directly with the raw `EventStore` port, every service
would have to repeat several mechanical steps:

1. **Stream ID mapping**: Formatting the aggregate ID and category into a `StreamId`.
2. **Stream replay**: Fetching storage envelopes, unpacking domain events, instantiating the aggregate class, and calling `load_from_history()`.
3. **Concurrency bookkeeping**: Manually calculating `expected_version = aggregate.version - len(uncommitted_events)` to pass to `store.append()`.
4. **Lifecycle state management**: Calling `aggregate.mark_events_as_committed()` after a successful write, ensuring uncommitted events aren't re-saved.
5. **Event dispatch coordination**: Publishing committed events to subscribers (projections, notification handlers, external message brokers) without leaking phantom events if the database append fails.

`AggregateRepository[TAggregate]` encapsulates all five responsibilities into two clean, high-level async methods:

```python
# Load reconstituted aggregate from history
order = await repository.load(order_id)

# Execute domain command
order.execute(ShipOrder(order_id=order_id, tracking_number="TRK-100"))

# Atomically persist uncommitted events and publish to bus
await repository.save(order)
```

Your application code never touches event envelopes or stream offsets directly—it works exclusively
with domain models and commands.

---

## Prerequisites

Before beginning, ensure you have:

- Completed [Tutorial 1: Getting Started](01-getting-started.md) and [Tutorial 3: Your First Aggregate](03-first-aggregate.md).
- Python 3.13+ with `eventsource-py` installed.

---

## The Ordering Domain Model

Here is the `Order` decider aggregate from Tutorial 3. We'll declare the state, events,
commands, and decider functions in our script:

```python
from decimal import Decimal
from uuid import UUID, uuid4
from pydantic import BaseModel

from eventsource import (
    CommandRejectedError,
    DeciderAggregate,
    DomainCommand,
    DomainEvent,
    register_event,
)


# 1. State
class OrderState(BaseModel):
    customer_id: UUID | None = None
    total: Decimal = Decimal("0")
    status: str = "pending"
    tracking_number: str | None = None


# 2. Events
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


# 3. Commands
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


# 4. Decider Aggregate
class OrderAggregate(DeciderAggregate[OrderState, OrderCommand]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state() -> OrderState:
        return OrderState()

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        match event:
            case OrderCreated(customer_id=customer_id, total=total):
                return state.model_copy(
                    update={
                        "customer_id": customer_id,
                        "total": total,
                        "status": "created",
                    }
                )
            case OrderShipped(tracking_number=tracking_number):
                return state.model_copy(
                    update={"tracking_number": tracking_number, "status": "shipped"}
                )
            case OrderCancelled():
                return state.model_copy(update={"status": "cancelled"})
            case _:
                return state

    @staticmethod
    def decide(command: OrderCommand, state: OrderState) -> list[DomainEvent]:
        match command, state:
            case CreateOrder(order_id=order_id, customer_id=customer_id, total=total), OrderState(status="pending"):
                return [OrderCreated(aggregate_id=order_id, customer_id=customer_id, total=total)]
            case CreateOrder(), _:
                raise CommandRejectedError("Order already exists", command=command)
            case ShipOrder(order_id=order_id, tracking_number=tracking_number), OrderState(status="created"):
                return [OrderShipped(aggregate_id=order_id, tracking_number=tracking_number)]
            case ShipOrder(), OrderState(status="cancelled"):
                raise CommandRejectedError("Cannot ship a cancelled order", command=command)
            case ShipOrder(), _:
                raise CommandRejectedError("Order is not ready to ship", command=command)
            case CancelOrder(order_id=order_id, reason=reason), OrderState(status="created"):
                return [OrderCancelled(aggregate_id=order_id, reason=reason)]
            case CancelOrder(), OrderState(status="cancelled"):
                raise CommandRejectedError("Order is already cancelled", command=command)
            case _:
                raise CommandRejectedError(f"Unknown command: {command!r}", command=command)
```

---

## Step 1: Initializing the Repository

To construct an `AggregateRepository`, pass the underlying `event_store` and the `aggregate_factory`:

```python
from eventsource import AggregateRepository, InMemoryEventStore

store = InMemoryEventStore()
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
)
```

### Type Inference
Notice you do not pass `aggregate_type="Order"` to the repository constructor.
The repository automatically inspects `OrderAggregate.aggregate_type`. If the class attribute
is missing or empty, `AggregateRepository` raises an error immediately at configuration time.
This prevents configuration mismatches between what your events declare and what your repository queries.

---

## Step 2: Saving an Aggregate (`repo.save`)

When you create or mutate an aggregate and pass it to `repo.save(order)`, the repository
executes the following sequence:

```
+----------------------------------------------------------------+
|                       repo.save(aggregate)                     |
+----------------------------------------------------------------+
                               |
                Are there uncommitted events?
                               |
               +---------------+---------------+
               | No                            | Yes
               v                               v
        [ Return No-Op ]        Calculate expected_version:
                                expected = version - len(events)
                                               |
                                               v
                                Append to store atomically:
                                store.append(stream, events, exact(expected))
                                               |
                                               v
                                Mark events as committed:
                                aggregate.mark_events_as_committed()
                                               |
                                               v
                                Publish events (if bus configured):
                                event_publisher.publish(events)
```

Let's test this in code:

```python
order_id = uuid4()
order = OrderAggregate(order_id)

# Execute command: creates 1 uncommitted event, advances order.version to 1
order.execute(CreateOrder(order_id=order_id, customer_id=uuid4(), total=Decimal("199.95")))

assert order.version == 1
assert len(order.uncommitted_events) == 1

# Persist via repository
await repo.save(order)

# Post-save invariant: uncommitted events have been cleared
assert not order.has_uncommitted_events
assert len(order.uncommitted_events) == 0
assert order.version == 1
```

If you call `await repo.save(order)` again without executing new commands, the repository
detects that `aggregate.uncommitted_events` is empty and immediately returns without performing
unnecessary I/O.

---

## Step 3: Loading an Aggregate (`repo.load`)

To handle subsequent requests for the same order, call `await repo.load(order_id)`:

```python
loaded_order = await repo.load(order_id)

print(f"Reconstituted Order ID: {loaded_order.aggregate_id}")
print(f"Current Version: {loaded_order.version}")
print(f"Status: {loaded_order.state.status}")
print(f"Total: ${loaded_order.state.total}")
```

### What happens during `load()`?

1. **Stream query**: The repository asks the event store for all events matching `StreamId(order_id, "Order")`.
2. **Missing stream detection**: If no events exist in the stream, the repository raises `AggregateNotFoundError(order_id, "Order")`.
3. **Instantiation**: The repository instantiates a fresh aggregate via `OrderAggregate(order_id)`. Its initial version is `0`, and its state is `OrderAggregate.initial_state()`.
4. **Replay**: The repository calls `loaded_order.load_from_history(events)`. Each event is fed into `evolve(state, event)` in exact chronological order.
5. **Committed state**: Events applied during replay are flagged with `is_new=False`, meaning `loaded_order.uncommitted_events` remains empty.

You now have a fully reconstituted aggregate ready to evaluate further commands.

---

## Step 4: Connecting an Event Publisher

In event-driven architectures, state changes frequently need to trigger side effects:
- Updating read models (projections).
- Sending email or SMS confirmations to customers.
- Emitting notifications to external message brokers (Kafka, RabbitMQ, Redis).

`AggregateRepository` accepts an optional `event_publisher`:

```python
from eventsource import InMemoryEventBus

bus = InMemoryEventBus()
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    event_publisher=bus,
)
```

When an `event_publisher` is present, `repo.save(aggregate)` publishes the newly committed events
**only after** the store successfully persists them. If the event store rejects the write (for example,
due to a concurrency conflict), the publisher is never called. This prevents "phantom notifications"
from leaking to the outside world.

Let's register an async event handler on the bus:

```python
async def notify_customer(event: OrderShipped) -> None:
    print(f"📧 Notification: Order {event.aggregate_id} shipped with tracking {event.tracking_number}!")

bus.subscribe(OrderShipped, notify_customer)
```

Now, loading the order, shipping it, and saving it will automatically trigger our handler:

```python
order = await repo.load(order_id)
order.execute(ShipOrder(order_id=order_id, tracking_number="1Z-TEST-999"))
await repo.save(order)
```

Console output:
```text
📧 Notification: Order 28a1c621-... shipped with tracking 1Z-TEST-999!
```

---

## Step 5: Helpful Repository Utilities

Beyond `load()` and `save()`, `AggregateRepository` provides several ergonomic helpers:

### 1. `exists(aggregate_id: UUID) -> bool`
Checks whether a stream exists without replaying and instantiating the full aggregate:

```python
if await repo.exists(order_id):
    print("Order exists in storage")
```

### 2. `load_or_create(aggregate_id: UUID) -> TAggregate`
Loads an existing aggregate if found, or initializes a new blank instance at version 0 if it doesn't:

```python
order = await repo.load_or_create(some_id)
if order.version == 0:
    order.execute(CreateOrder(...))
    await repo.save(order)
```

---

## Step 6: Complete Runnable Example

Save the following code into a file named `repository_demo.py`:

```python
import asyncio
from decimal import Decimal
from uuid import UUID, uuid4
from pydantic import BaseModel

from eventsource import (
    AggregateNotFoundError,
    AggregateRepository,
    CommandRejectedError,
    DeciderAggregate,
    DomainCommand,
    DomainEvent,
    InMemoryEventBus,
    InMemoryEventStore,
    register_event,
)


# --- 1. Domain Model ---
class OrderState(BaseModel):
    customer_id: UUID | None = None
    total: Decimal = Decimal("0")
    status: str = "pending"
    tracking_number: str | None = None


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
            case OrderCancelled():
                return state.model_copy(update={"status": "cancelled"})
            case _:
                return state

    @staticmethod
    def decide(command: OrderCommand, state: OrderState) -> list[DomainEvent]:
        match command, state:
            case CreateOrder(order_id=oid, customer_id=cid, total=tot), OrderState(status="pending"):
                return [OrderCreated(aggregate_id=oid, customer_id=cid, total=tot)]
            case CreateOrder(), _:
                raise CommandRejectedError("Order already exists", command=command)
            case ShipOrder(order_id=oid, tracking_number=trk), OrderState(status="created"):
                return [OrderShipped(aggregate_id=oid, tracking_number=trk)]
            case ShipOrder(), OrderState(status="cancelled"):
                raise CommandRejectedError("Cannot ship a cancelled order", command=command)
            case ShipOrder(), _:
                raise CommandRejectedError("Order is not ready to ship", command=command)
            case CancelOrder(order_id=oid, reason=r), OrderState(status="created"):
                return [OrderCancelled(aggregate_id=oid, reason=r)]
            case _:
                raise CommandRejectedError(f"Command not allowed: {command!r}", command=command)


# --- 2. Application Service Demo ---
async def main() -> None:
    # Set up infrastructure
    store = InMemoryEventStore()
    bus = InMemoryEventBus()

    # Wire up a subscriber to listen to shipped events
    async def shipping_listener(event: OrderShipped) -> None:
        print(f"📬 Subscriber alerted: Order {event.aggregate_id} dispatched via {event.tracking_number}!")

    bus.subscribe(OrderShipped, shipping_listener)

    # Initialize repository with event store, aggregate factory, and bus
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=OrderAggregate,
        event_publisher=bus,
    )

    # Use Case 1: Place an Order
    order_id = uuid4()
    print(f"--- Creating order {order_id} ---")
    order = OrderAggregate(order_id)
    order.execute(CreateOrder(order_id=order_id, customer_id=uuid4(), total=Decimal("89.99")))

    print(f"Pre-save: version={order.version}, uncommitted={len(order.uncommitted_events)}")
    await repo.save(order)
    print(f"Post-save: version={order.version}, uncommitted={len(order.uncommitted_events)}\n")

    # Use Case 2: Reload order in a separate request & fulfill it
    print(f"--- Fulfilling order {order_id} ---")
    loaded_order = await repo.load(order_id)
    print(f"Loaded status: {loaded_order.state.status}, version: {loaded_order.version}")

    loaded_order.execute(ShipOrder(order_id=order_id, tracking_number="FEDEX-998877"))
    # Saving here will persist the event AND trigger the shipping_listener
    await repo.save(loaded_order)
    print(f"Saved fulfilled order, new version: {loaded_order.version}\n")

    # Use Case 3: Verify aggregate non-existence
    missing_id = uuid4()
    print(f"--- Checking missing order {missing_id} ---")
    print(f"Exists in repo? {await repo.exists(missing_id)}")
    try:
        await repo.load(missing_id)
    except AggregateNotFoundError:
        print("Caught expected AggregateNotFoundError on missing order load.")


if __name__ == "__main__":
    asyncio.run(main())
```

Run the script:

```bash
python3 repository_demo.py
```

Output:
```text
--- Creating order 40a92f80-0ea8-48b4-934c-d8ae4345d3aa ---
Pre-save: version=1, uncommitted=1
Post-save: version=1, uncommitted=0

--- Fulfilling order 40a92f80-0ea8-48b4-934c-d8ae4345d3aa ---
Loaded status: created, version: 1
📬 Subscriber alerted: Order 40a92f80-0ea8-48b4-934c-d8ae4345d3aa dispatched via FEDEX-998877!
Saved fulfilled order, new version: 2

--- Checking missing order a3531bfa-b9a3-4a1d-a36c-9a4f48b11a91 ---
Exists in repo? False
Caught expected AggregateNotFoundError on missing order load.
```

---

## Key Takeaways

- **Encapsulates persistence mechanics**: `AggregateRepository` hides stream names, envelope unpacking, version calculation, and replay loops behind `load()` and `save()`.
- **Automatic version calculation**: `repo.save(aggregate)` automatically calculates `expected_version = version - len(uncommitted_events)` on your behalf.
- **Safe event publishing**: Attaching an `event_publisher` ensures events are dispatched only after successful storage persistence.
- **Inferred identity**: The aggregate category is always inferred directly from `aggregate_factory.aggregate_type`, ensuring domain model consistency.

---

## Next Steps

What happens when two users or background tasks load the same order at the same time and attempt
to modify it concurrently?

Learn how `eventsource-py` detects collisions and preserves domain invariants in:

👉 **[Tutorial 5: Optimistic Concurrency](05-optimistic-concurrency.md)**
