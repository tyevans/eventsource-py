# Tutorial 15: The Transactional Outbox Pattern

In event-driven architectures, updating a database and publishing a message to a message broker (Kafka, RabbitMQ, Redis) across separate calls creates the classic **dual-write problem**:
1. If the database commit succeeds but the message broker publish fails (or the process crashes in between), downstream consumers never learn about the state change.
2. If publishing succeeds first but the database transaction rolls back, downstream consumers act on phantom events that never happened.

The **Transactional Outbox Pattern** eliminates dual writes by writing events into an `event_outbox` table *within the exact same database transaction* that appends events to the event store. An asynchronous background poller or worker then reads from the outbox table, publishes to the broker, and marks the records as published.

In this tutorial, you will:
1. Enable same-transaction outbox staging during event store appends.
2. Inspect pending events in the outbox using `OutboxRepository`.
3. Build a resilient outbox publisher loop that drains staged events, publishes to an `EventBus`, and cleans up processed records.

---

## 1. Enabling Outbox Staging on Event Appends

Both `PostgreSQLEventStore` and `SQLiteEventStore` support outbox staging via their constructors:

```python
from eventsource.adapters.postgresql import PostgreSQLEventStore

# Instantiate store with outbox_enabled=True (default is True on PostgreSQL and SQLite)
store = PostgreSQLEventStore(
    engine=engine,
    outbox_enabled=True,
)
```

When `outbox_enabled=True`, every call to `store.append(stream_id, events, ...)` inserts records into both the `events` table and the `event_outbox` table atomically:

```python
await store.append(
    stream_id=str(order_id),
    events=[OrderPlaced(...)],
    expected_version=ExpectedVersion.NO_STREAM,
)
# At this point, the event is durably in both events and event_outbox tables.
```

---

## 2. Reading Pending Events with OutboxRepository

The library ships with concrete outbox repositories for PostgreSQL (`PostgreSQLOutboxRepository`), SQLite (`SQLiteOutboxRepository`), and testing (`InMemoryOutboxRepository`):

```python
from eventsource.adapters.postgresql import PostgreSQLOutboxRepository

outbox_repo = PostgreSQLOutboxRepository(engine=engine)

# Fetch un-published events
pending = await outbox_repo.get_pending_events(batch_size=100)
for entry in pending:
    print(f"Pending event {entry.event_id} of type {entry.event_type} for stream {entry.stream_id}")
```

Each outbox entry contains:
- `outbox_id`: Primary key identifier for the outbox row
- `event_id`: UUID of the domain event
- `stream_id`: Stream identifier
- `event_type`: Type name of the event
- `event_data`: Serialized JSON payload
- `occurred_at`: Event timestamp
- `retry_count`: Number of failed publish attempts

---

## 3. Building an Outbox Relay Worker

An outbox relay process polls pending events, publishes them to the message broker, and marks them published:

```python
import asyncio
from eventsource.ports.bus import EventBus
from eventsource.adapters.postgresql import PostgreSQLOutboxRepository

async def outbox_relay_loop(
    outbox_repo: PostgreSQLOutboxRepository,
    event_bus: EventBus,
    poll_interval: float = 0.5,
    batch_size: int = 100,
) -> None:
    while True:
        pending_entries = await outbox_repo.get_pending_events(batch_size=batch_size)
        if not pending_entries:
            await asyncio.sleep(poll_interval)
            continue

        for entry in pending_entries:
            try:
                # Reconstruct domain event or publish raw envelope
                event = entry.to_domain_event()
                await event_bus.publish([event])

                # Mark successfully published
                await outbox_repo.mark_published(entry.outbox_id)
            except Exception as exc:
                # Increment retry count or mark failed
                await outbox_repo.mark_failed(entry.outbox_id, error=str(exc))

        # Optionally purge published entries past a retention threshold (e.g., 7 days)
        await outbox_repo.cleanup_published_events(older_than_seconds=7 * 86400)
```

---

## 4. Monitoring Outbox Health

Inspect outbox performance and backlog using `get_stats()`:

```python
stats = await outbox_repo.get_stats()

print(f"Total pending: {stats.pending_count}")
print(f"Total published: {stats.published_count}")
print(f"Total failed: {stats.failed_count}")
```

If `pending_count` grows beyond operational thresholds, it indicates broker downtime or insufficient outbox poller capacity.

---

## Summary

By combining `outbox_enabled=True` on your event store with an `OutboxRepository` relay:
- You guarantee at-least-once message delivery to message brokers without distributed two-phase commits.
- Aggregates can commit state changes rapidly with zero external network latency in the primary write path.
- Temporary broker outages do not fail client writes; events remain safely queued in the outbox table until connectivity is restored.
