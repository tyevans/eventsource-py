# Tutorial 1: Getting Started with eventsource-py

Welcome to **eventsource-py**, a production-grade, async-first event sourcing library
for Python 3.13+.

In traditional CRUD systems, you store the *current state* of an entity by overwriting
rows in a database table. In an **event-sourced system**, you never overwrite state.
Instead, you append an immutable record of every business event that occurs—an append-only
log of facts. The current state is then derived by replaying those events in order.

This tutorial gets you up and running in under five minutes. You will install the library,
meet the running domain example used across the entire tutorial series (the **Ordering Service**),
define your first domain event, append events to an in-memory event store, and read them
back out.

Everything in this tutorial runs in a single Python script with no external databases,
Docker containers, or background services required.

---

## Prerequisites

Before beginning, ensure you have:

- **Python 3.13 or newer** (verify with `python3 --version`).
- A package manager: either [uv](https://docs.astral.sh/uv/) (recommended) or `pip`.

---

## Installation

Create a new directory for your experiments and install `eventsource-py`:

Using `uv`:
```bash
uv init eventsource-demo
cd eventsource-demo
uv add eventsource-py
```

Or using standard `pip` in a virtual environment:
```bash
mkdir eventsource-demo
cd eventsource-demo
python3 -m venv .venv
source .venv/bin/activate
pip install eventsource-py
```

Verify your installation:
```bash
python3 -c "import eventsource; print(eventsource.__version__)"
```

If a version number prints (or `0.0.0.dev0` in local development), you are ready to proceed.

---

## The Running Domain: The Ordering Service

Throughout this tutorial series, you will build an **Ordering Service**. We use this
single domain consistently so every new architectural concept lands against familiar
business rules.

The ordering lifecycle is intentionally lean and focused:

1. **`OrderCreated`**: A customer places an order with a specified monetary total.
2. **`OrderShipped`**: The warehouse fulfills the order and assigns a tracking number.
3. **`OrderCancelled`**: An unfulfilled order is aborted with a documented reason.

```
       +--------------+
       | OrderCreated |
       +-------+------+
               |
        +------+------+
        |             |
        v             v
+---------------+ +----------------+
| OrderShipped  | | OrderCancelled |
+---------------+ +----------------+
```

With these three events, you will explore aggregates, repositories, optimistic locking,
projections, outbox workers, snapshotting, and distributed event buses.

---

## Step 1: Define Your First Domain Event

In `eventsource-py`, all events inherit from `DomainEvent`. Under the hood, `DomainEvent`
is a [Pydantic v2](https://docs.pydantic.dev/) `BaseModel` configured with `frozen=True`.
This ensures events are strictly immutable—once instantiated, their attributes cannot be modified.

Create a file named `getting_started.py` and declare the `OrderCreated` and `OrderShipped` events:

```python
from decimal import Decimal
from uuid import UUID, uuid4

from eventsource import DomainEvent, register_event


@register_event
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: UUID
    total: Decimal


@register_event
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str
```

### What happens here?

- **`aggregate_type: str = "Order"`**: Identifies the stream category this event belongs to.
- **Payload fields**: `customer_id`, `total`, and `tracking_number` define the domain data specific to these events.
- **`@register_event`**: Registers the event class with the global event registry. This allows event stores and serializers to resolve the string name `"OrderCreated"` back to your Python class when reading from storage.
- **Inherited metadata**: You didn't have to define `event_id`, `occurred_at`, or `aggregate_version`. `DomainEvent` provisions these automatically.

---

## Step 2: Set Up an In-Memory Event Store

An **event store** is the storage engine for event streams. It provides two fundamental capabilities:
1. **Appending** new events to a stream under optimistic concurrency checks.
2. **Reading** events back from a stream in chronological sequence.

For development, testing, and getting started, `eventsource-py` provides `InMemoryEventStore`.
It implements the full event store protocol in memory without requiring PostgreSQL or SQLite:

```python
from eventsource import InMemoryEventStore, StreamId

store = InMemoryEventStore()
```

Streams in `eventsource-py` are identified by a `StreamId`, which pairs the unique ID of an entity
(`aggregate_id`) with its category (`aggregate_type`):

```python
order_id = uuid4()
stream = StreamId(aggregate_id=order_id, category="Order")
```

---

## Step 3: Append Events to the Stream

Events are appended to the store in batches. Along with the events, you supply an `ExpectedVersion`
to protect against concurrent writes:

```python
from eventsource import ExpectedVersion

# When creating an order, we expect no stream to exist yet:
created = OrderCreated(
    aggregate_id=order_id,
    customer_id=uuid4(),
    total=Decimal("89.95"),
    aggregate_version=1,
)

result = await store.append(
    stream=stream,
    events=[created],
    expected=ExpectedVersion.no_stream(),
)
print(f"Appended event 1. Stream is now at version {result.new_version}")
```

Next, append the `OrderShipped` event. Because the stream now has 1 event, we specify `ExpectedVersion.exact(1)`:

```python
shipped = OrderShipped(
    aggregate_id=order_id,
    tracking_number="TRK-987654",
    aggregate_version=2,
)

result = await store.append(
    stream=stream,
    events=[shipped],
    expected=ExpectedVersion.exact(1),
)
print(f"Appended event 2. Stream is now at version {result.new_version}")
```

If another process had written to this stream in the meantime, `store.append` would have raised
an `OptimisticLockError`, safeguarding your data from silent overwrites.

---

## Step 4: Read Events Back from the Stream

To reconstruct state or inspect history, read from the stream using `store.read_stream()`.
This method returns an async iterator of `EventEnvelope` objects:

```python
print(f"\nReading history for stream {stream.render()}:")
async for envelope in store.read_stream(stream):
    event = envelope.event
    print(f"  v{envelope.stream_version} [{envelope.stored_at.isoformat()}]: {event.event_type}")
    if isinstance(event, OrderCreated):
        print(f"     Customer: {event.customer_id}, Total: ${event.total}")
    elif isinstance(event, OrderShipped):
        print(f"     Tracking Number: {event.tracking_number}")
```

Each `EventEnvelope` wraps your domain event with persistence metadata:
- `stream_version`: The 1-based sequential position of the event in this stream.
- `stored_at`: The UTC timestamp when the event store committed the event.
- `event`: Your strongly typed `DomainEvent` instance.

---

## Step 5: Complete Runnable Example

Here is the complete, self-contained script. Save it as `getting_started.py`:

```python
import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    ExpectedVersion,
    InMemoryEventStore,
    StreamId,
    register_event,
)


# 1. Define Domain Events
@register_event
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: UUID
    total: Decimal


@register_event
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str


# 2. Main async workflow
async def main() -> None:
    # Initialize the in-memory store
    store = InMemoryEventStore()

    # Identify the event stream
    order_id = uuid4()
    stream = StreamId(aggregate_id=order_id, category="Order")

    print(f"Operating on order stream: {stream.render()}\n")

    # Step A: Append creation event (expecting no prior stream)
    created_event = OrderCreated(
        aggregate_id=order_id,
        customer_id=uuid4(),
        total=Decimal("149.99"),
        aggregate_version=1,
    )
    res1 = await store.append(stream, [created_event], ExpectedVersion.no_stream())
    print(f"Appended OrderCreated -> Current Stream Version: {res1.new_version}")

    # Step B: Append shipping event (expecting exact version 1)
    shipped_event = OrderShipped(
        aggregate_id=order_id,
        tracking_number="UPS-1Z9999999999",
        aggregate_version=2,
    )
    res2 = await store.append(stream, [shipped_event], ExpectedVersion.exact(1))
    print(f"Appended OrderShipped -> Current Stream Version: {res2.new_version}")

    # Step C: Read back the entire stream
    print("\nReading all events in chronological order:")
    async for envelope in store.read_stream(stream):
        ev = envelope.event
        print(
            f"  [Version {envelope.stream_version}] {ev.event_type} "
            f"(event_id: {ev.event_id})"
        )
        if isinstance(ev, OrderCreated):
            print(f"    -> Customer: {ev.customer_id}, Total: ${ev.total}")
        elif isinstance(ev, OrderShipped):
            print(f"    -> Tracking: {ev.tracking_number}")


if __name__ == "__main__":
    asyncio.run(main())
```

Run the script:

```bash
python3 getting_started.py
```

Output:
```text
Operating on order stream: d3fa40db-9fc0-49ae-a010-388b90fe3294:Order

Appended OrderCreated -> Current Stream Version: 1
Appended OrderShipped -> Current Stream Version: 2

Reading all events in chronological order:
  [Version 1] OrderCreated (event_id: 4899531a-e8d1-443b-8ca0-9dfa87bf916a)
    -> Customer: 86b453e0-f2c9-4fc6-b7ff-cae37cfa901a, Total: $149.99
  [Version 2] OrderShipped (event_id: e3124806-cf4c-4ec1-912b-36fdbda26f8d)
    -> Tracking: UPS-1Z9999999999
```

---

## Key Takeaways

- **Events are facts**: `DomainEvent` subclasses represent business occurrences that already happened. They are frozen and immutable.
- **Event Registry**: The `@register_event` decorator maps stored string types to Python classes.
- **Streams have identity**: `StreamId(aggregate_id, category)` uniquely addresses each entity's event log.
- **Concurrency is explicit**: `ExpectedVersion` (`no_stream`, `exact(v)`) guards against race conditions on every write.
- **No infrastructure required for learning**: `InMemoryEventStore` offers the full event store protocol in-process.

---

## Next Steps

Now that you have seen how events are defined and stored at the lowest level, explore how to model
richer event schemas, causation links, and metadata in:

👉 **[Tutorial 2: Your First Domain Event](02-first-event.md)**
