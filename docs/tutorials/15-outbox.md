# Tutorial 15: The Transactional Outbox Pattern

In event-driven architectures, updating a database and publishing an event to a message broker (Kafka, RabbitMQ, Redis, or an event bus) across separate calls creates the classic **dual-write problem**:

1. **DB Succeeds, Publish Fails**: If the database commits the order event but the message broker is temporarily unreachable (or the process crashes in between), downstream systems (such as inventory, notifications, and analytics) never learn about the new order.
2. **Publish Succeeds, DB Fails**: If publishing occurs first but the database transaction rolls back (for example, due to an optimistic concurrency violation), downstream systems act on phantom events that never actually existed.

```mermaid
flowchart TD
    subgraph Problem["The Dual-Write Problem"]
        A[Command: Place Order] --> B[(Database: events)]
        A -. Network Failure / Crash .-> C[Message Broker]
        B -- Committed! --> D[State Saved]
        C -. Never Arrives! .-> E[Downstream Services Desynced]
    end
```

The **Transactional Outbox Pattern** eliminates dual writes by staging events into an `event_outbox` table *within the exact same database transaction* that appends events to the event store. An asynchronous background poller or relay worker reads pending rows from the outbox table, publishes them to the broker, and marks them as published.

In this tutorial, you will:
1. Enable same-transaction outbox staging during event store appends.
2. Inspect pending events in the outbox using `PostgreSQLOutboxRepository`.
3. Reconstruct strongly-typed domain events from outbox payloads using `get_event_class()`.
4. Build a resilient background outbox relay worker that drains events, publishes to an `EventBus`, handles transient retries, and marks events as published.
5. Monitor outbox health and purge published records.

---

## Prerequisites

1. **Python 3.13+** with `eventsource-py` and the `postgresql` extra installed:
   ```bash
   uv sync --extra postgresql
   ```
2. **PostgreSQL 15** running via Docker Compose:
   ```bash
   docker compose -f docker-compose.test.yml up -d postgres
   ```
   PostgreSQL connection string: `postgresql+asyncpg://test:test@localhost:5433/eventsource_test`.

---

## 1. Enabling Outbox Staging on Event Appends

Both `PostgreSQLEventStore` and `SQLiteEventStore` support outbox staging via their constructors with `outbox_enabled=True` (the default is `False`):

```python
from sqlalchemy.ext.asyncio import create_async_engine
from eventsource.adapters.postgresql import PostgreSQLEventStore

DATABASE_URL = "postgresql+asyncpg://test:test@localhost:5433/eventsource_test"
engine = create_async_engine(DATABASE_URL, echo=False)

# Enable transactional outbox staging:
store = PostgreSQLEventStore(
    engine=engine,
    outbox_enabled=True,
)
```

When `outbox_enabled=True`, every call to `store.append(stream_id, events, ...)` writes to both the `events` table and the `event_outbox` table in a single database transaction. If the transaction commits, both rows are persisted; if it fails, both roll back.

---

## 2. Inspecting the Outbox Table with OutboxRepository

The library ships concrete outbox repositories for PostgreSQL (`PostgreSQLOutboxRepository`), SQLite (`SQLiteOutboxRepository`), and testing (`InMemoryOutboxRepository`).

Let's initialize the repository:

```python
from eventsource.adapters.postgresql import PostgreSQLOutboxRepository

# PostgreSQLOutboxRepository accepts an AsyncEngine or AsyncConnection
outbox_repo = PostgreSQLOutboxRepository(engine)

# Fetch un-published events (ordered by created_at ASC)
pending_entries = await outbox_repo.get_pending_events(limit=100)
for entry in pending_entries:
    print(f"Pending Outbox ID: {entry.id}")
    print(f"  Event ID: {entry.event_id}")
    print(f"  Event Type: {entry.event_type}")
    print(f"  Aggregate ID: {entry.aggregate_id} ({entry.aggregate_type})")
    print(f"  Retry Count: {entry.retry_count}")
    print(f"  Payload Data: {entry.event_data}")
```

Each `OutboxEntry` contains:
- `id`: Unique UUID identifier for the outbox row.
- `event_id`: Unique UUID of the domain event.
- `event_type`: Registered string type of the event (e.g. `"OrderPlaced"`).
- `aggregate_id`: UUID of the aggregate.
- `aggregate_type`: Name of the aggregate type (e.g. `"Order"`).
- `tenant_id`: Optional tenant UUID if using multi-tenancy.
- `event_data`: Serialized JSON dictionary conforming to `outbox_event_data()` format.
- `created_at`: UTC timestamp when the row was staged.
- `status`: Current status (`"pending"`, `"published"`, or `"failed"`).
- `retry_count`: Number of failed publish attempts.

---

## 3. Reconstructing Domain Events from Outbox Entries

The `event_data` column contains a JSON structure formatted by `outbox_event_data()`:

```json
{
  "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "aggregate_id": "a4f8d689-130a-4fc8-9f37-67c9c0dcfd5c",
  "aggregate_type": "Order",
  "tenant_id": null,
  "occurred_at": "2026-10-09T21:40:00+00:00",
  "payload": {
    "customer_id": "cust_123",
    "total_cents": 19900
  }
}
```

To reconstruct the original, strongly-typed `DomainEvent` instance for publishing to your event bus or message broker, use `get_event_class()` from the domain event registry:

```python
import json
from eventsource import get_event_class, DomainEvent
from eventsource.ports.outbox import OutboxEntry

def reconstruct_event(entry: OutboxEntry) -> DomainEvent:
    # 1. Resolve registered DomainEvent class by name
    event_cls = get_event_class(entry.event_type)

    # 2. Extract payload dictionary
    data = entry.event_data
    if isinstance(data, str):
        data = json.loads(data)

    payload = data.get("payload", {})

    # 3. Instantiate the validated DomainEvent
    return event_cls.model_validate(payload)
```

---

## 4. Building the Outbox Relay Worker

The outbox relay worker runs as a background task. It periodically polls for pending entries, publishes them to the message broker, and marks them as published or handles retries:

```python
import asyncio
import logging
from eventsource.ports.bus import EventBus
from eventsource.adapters.postgresql import PostgreSQLOutboxRepository

logger = logging.getLogger(__name__)

async def outbox_relay_worker(
    outbox_repo: PostgreSQLOutboxRepository,
    event_bus: EventBus,
    poll_interval: float = 0.2,
    batch_size: int = 50,
    max_retries: int = 3,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Continuously drains pending events from the outbox and publishes them."""
    logger.info("Outbox relay worker started.")

    while stop_event is None or not stop_event.is_set():
        try:
            pending_entries = await outbox_repo.get_pending_events(limit=batch_size)
            if not pending_entries:
                await asyncio.sleep(poll_interval)
                continue

            for entry in pending_entries:
                try:
                    # Reconstruct domain event
                    domain_event = reconstruct_event(entry)

                    # Publish to the event bus / message broker
                    await event_bus.publish([domain_event])

                    # Mark published in PostgreSQL
                    await outbox_repo.mark_published(entry.id)
                    logger.debug("Published outbox event %s (%s)", entry.id, entry.event_type)

                except Exception as exc:
                    logger.warning("Failed to publish event %s: %s", entry.id, exc)
                    if entry.retry_count + 1 >= max_retries:
                        await outbox_repo.mark_failed(entry.id, error=str(exc))
                    else:
                        await outbox_repo.increment_retry(entry.id, error=str(exc))

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Error in outbox relay loop: %s", exc)
            await asyncio.sleep(poll_interval)
```

---

## 5. Complete Working Example

Let's combine everything into a runnable example. We will create an order, verify the outbox entry, start our outbox relay worker, and observe the event bus receiving the event.

Create `outbox_tutorial.py`:

```python
import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4
from sqlalchemy.ext.asyncio import create_async_engine

from eventsource import (
    DomainEvent,
    ExpectedVersion,
    InMemoryEventBus,
    get_event_class,
)
from eventsource.adapters.postgresql import (
    PostgreSQLEventStore,
    PostgreSQLOutboxRepository,
)
from eventsource.adapters.sql.schemas import get_all_schemas
from eventsource.ports.outbox import OutboxEntry


# --- Ordering Domain Events ---

class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: str
    item_id: str
    price_cents: int


class OrderPaid(DomainEvent):
    aggregate_type: str = "Order"
    transaction_id: str


def reconstruct_event(entry: OutboxEntry) -> DomainEvent:
    event_cls = get_event_class(entry.event_type)
    data = entry.event_data
    if isinstance(data, str):
        data = json.loads(data)
    payload = data.get("payload", {})
    return event_cls.model_validate(payload)


DATABASE_URL = "postgresql+asyncpg://test:test@localhost:5433/eventsource_test"


async def main() -> None:
    engine = create_async_engine(DATABASE_URL, echo=False)

    # 1. Ensure schemas exist
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        await raw_conn.driver_connection.execute(get_all_schemas())

    # 2. Configure EventStore with outbox_enabled=True
    event_store = PostgreSQLEventStore(engine=engine, outbox_enabled=True)
    outbox_repo = PostgreSQLOutboxRepository(engine=engine)
    event_bus = InMemoryEventBus()

    # 3. Set up an event bus subscriber to confirm message delivery
    received_events: list[DomainEvent] = []

    async def on_order_placed(event: OrderPlaced) -> None:
        print(f"[Subscriber Notification] Order placed for customer: {event.customer_id} (${event.price_cents/100:.2f})")
        received_events.append(event)

    event_bus.subscribe(OrderPlaced, on_order_placed)

    # 4. Append an order
    order_id = uuid4()
    stream_id = str(order_id)
    placed_event = OrderPlaced(
        aggregate_id=order_id,
        aggregate_version=1,
        customer_id="cust_samuel",
        item_id="item_standing_desk",
        price_cents=45000,
    )

    print(f"\n1. Appending OrderPlaced with outbox staging enabled...")
    await event_store.append(
        stream_id=stream_id,
        events=[placed_event],
        expected_version=ExpectedVersion.NO_STREAM,
    )

    # 5. Inspect the pending outbox records
    pending = await outbox_repo.get_pending_events(limit=10)
    print(f"2. Pending events in event_outbox table: {len(pending)}")
    assert len(pending) >= 1
    latest_entry = [p for p in pending if p.aggregate_id == order_id][0]
    print(f"   Found outbox entry {latest_entry.id} for aggregate {latest_entry.aggregate_id}")

    # 6. Run the outbox relay drainer for one cycle
    print(f"\n3. Draining outbox and publishing to event bus...")
    for entry in pending:
        event = reconstruct_event(entry)
        await event_bus.publish([event])
        await outbox_repo.mark_published(entry.id)

    # Verify subscriber received the event
    assert len(received_events) >= 1
    print(f"4. Successfully received event on bus! Total received: {len(received_events)}")

    # 7. Check outbox statistics
    stats = await outbox_repo.get_stats()
    print(f"\n5. Outbox Statistics:")
    print(f"   Pending:   {stats.pending_count}")
    print(f"   Published: {stats.published_count}")
    print(f"   Failed:    {stats.failed_count}")

    # 8. Clean up old published entries
    deleted = await outbox_repo.cleanup_published(days=0)
    print(f"6. Cleaned up {deleted} published outbox record(s).")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
```

Run the script:

```bash
uv run python outbox_tutorial.py
```

Expected output:
```text
1. Appending OrderPlaced with outbox staging enabled...
2. Pending events in event_outbox table: 1
   Found outbox entry 53a9fe4b-4b11-4770-bbfb-37209ca4c07d for aggregate a980df9b-75e1-4560-a2ea-9e79cf90a2cf

3. Draining outbox and publishing to event bus...
[Subscriber Notification] Order placed for customer: cust_samuel ($450.00)
4. Successfully received event on bus! Total received: 1

5. Outbox Statistics:
   Pending:   0
   Published: 1
   Failed:    0
6. Cleaned up 1 published outbox record(s).
```

---

## 6. Retention & Maintenance

Over time, high-volume production systems accumulate millions of rows in `event_outbox`. Since published events are already durably stored in the `events` table and consumed by the message broker, published outbox records should be periodically deleted.

Schedule `cleanup_published()` to run once daily:

```python
# Delete all published entries older than 7 days
deleted_count = await outbox_repo.cleanup_published(days=7)
print(f"Purged {deleted_count} stale outbox records.")
```

Because the PostgreSQL schema includes partial indexes (such as `idx_outbox_pending` on `created_at WHERE status = 'pending'`), the presence of historical published rows will not degrade polling query performance.

---

## Summary

In this tutorial:
- You eliminated the dual-write risk by staging events atomically into `event_outbox` using `outbox_enabled=True`.
- You inspected pending outbox rows with `PostgreSQLOutboxRepository`.
- You reconstructed strongly-typed domain events from `event_data` using `get_event_class()`.
- You implemented a background relay loop that guarantees at-least-once message delivery to downstream message brokers.
- You monitored queue health with `get_stats()` and purged processed rows using `cleanup_published()`.

With Phase 3 complete, your Ordering Service is durable, concurrent across distributed processes, accelerated by snapshots, and reliably integrated with downstream systems via the transactional outbox! Continue to [Tutorial 16: Multi-Tenancy](16-multi-tenancy.md) to isolate event streams across multiple tenant organizations.
