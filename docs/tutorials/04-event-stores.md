# Tutorial 4: Event Stores - Persisting Events

**Difficulty:** Beginner
**Progress:** Tutorial 4 of 21 | Phase 1: Foundations

---

## Prerequisites

- [Tutorial 1-3](01-introduction.md) completed
- Python 3.11+, eventsource-py installed

---

## What is an Event Store?

The **event store** is the database of record for event-sourced systems. It stores events in append-only fashion and provides:

- Appending events for an aggregate
- Reading events by aggregate ID
- Optimistic concurrency control
- Reading all events (for projections)

**Implementations:**
- `InMemoryEventStore`: Development/testing
- `PostgreSQLEventStore`: Production
- `SQLiteEventStore`: Embedded/development

---

## Using InMemoryEventStore

```python
from eventsource import InMemoryEventStore

# Create store
store = InMemoryEventStore()

# Check if aggregate exists
exists = await store.exists(aggregate_id)

# Get current version
version = await store.get_version(aggregate_id)
```

---

## Appending Events

```python
from uuid import uuid4
from eventsource import InMemoryEventStore, DomainEvent, register_event

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str

# Create and append
store = InMemoryEventStore()
task_id = uuid4()

event = TaskCreated(
    aggregate_id=task_id,
    title="First task",
    aggregate_version=1,
)

await store.append(task_id, [event], expected_version=0)
```

**Parameters:**
- `aggregate_id`: Which aggregate
- `events`: List of events to append
- `expected_version`: Current version (for optimistic locking)

---

## Reading Events

```python
# Read all events for an aggregate
stream = await store.read(task_id)

print(f"Aggregate ID: {stream.aggregate_id}")
print(f"Version: {stream.version}")
print(f"Event count: {len(stream.events)}")

for stored_event in stream.events:
    event = stored_event.event
    print(f"  - {event.event_type} (v{event.aggregate_version})")
```

---

## Optimistic Locking

Prevents conflicting concurrent writes:

```python
from eventsource import OptimisticLockError

# Process 1
stream1 = await store.read(task_id)  # version = 1

# Process 2
stream2 = await store.read(task_id)  # version = 1

# Process 1 saves first
event1 = TaskCompleted(aggregate_id=task_id, aggregate_version=2)
await store.append(task_id, [event1], expected_version=1)  # Success

# Process 2 tries to save
event2 = TaskReassigned(aggregate_id=task_id, aggregate_version=2)
try:
    await store.append(task_id, [event2], expected_version=1)  # Fails!
except OptimisticLockError as e:
    print(f"Conflict: expected v{e.expected_version}, actual v{e.actual_version}")
    # Must reload and retry
```

**Solution:** Load aggregate, retry operation, save with new version.

---

## ExpectedVersion Constants

```python
from eventsource import ExpectedVersion

# Must not exist
await store.append(task_id, events, expected_version=ExpectedVersion.NOT_EXISTS)

# Must exist
await store.append(task_id, events, expected_version=ExpectedVersion.EXISTS)

# Don't care about version (dangerous!)
await store.append(task_id, events, expected_version=ExpectedVersion.ANY)

# Exact version
await store.append(task_id, events, expected_version=5)
```

---

## Reading All Events

For projections that process all events:

```python
# Read all events from all aggregates
stream = await store.read_all()

for stored_event in stream.events:
    event = stored_event.event
    print(f"{event.aggregate_type}/{event.aggregate_id}: {event.event_type}")
```

**Performance note:** Use pagination for large stores.

---

## Event Store Comparison

| Feature | InMemory | PostgreSQL | SQLite |
|---------|----------|------------|--------|
| **Persistence** | No | Yes | Yes |
| **Concurrency** | Single process | Multi-process | Single process |
| **Production** | No | Yes | Limited |
| **Setup** | None | DB required | File required |
| **Best for** | Tests | Production | Embedded apps |

---

## Complete Example

```python
"""
Tutorial 4: Event Stores
Run with: python tutorial_04_event_stores.py
"""
import asyncio
from uuid import UUID, uuid4
from eventsource import (
    InMemoryEventStore,
    DomainEvent,
    register_event,
    OptimisticLockError,
    ExpectedVersion,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


async def main():
    store = InMemoryEventStore()
    task_id = uuid4()

    # Append events
    created = TaskCreated(
        aggregate_id=task_id,
        title="Learn event stores",
        aggregate_version=1,
    )
    await store.append(task_id, [created], expected_version=0)
    print(f"Appended TaskCreated")

    # Read back
    stream = await store.read(task_id)
    print(f"\nVersion: {stream.version}")
    print(f"Events: {len(stream.events)}")

    # Append another
    completed = TaskCompleted(
        aggregate_id=task_id,
        aggregate_version=2,
    )
    await store.append(task_id, [completed], expected_version=1)
    print(f"\nAppended TaskCompleted")

    # Read again
    stream = await store.read(task_id)
    print(f"Version: {stream.version}")
    for stored_event in stream.events:
        print(f"  - {stored_event.event.event_type}")

    # Demonstrate optimistic locking
    print(f"\n=== Optimistic Locking Demo ===")
    try:
        # Try to append with wrong version
        await store.append(task_id, [completed], expected_version=1)
    except OptimisticLockError as e:
        print(f"Conflict detected!")
        print(f"  Expected: v{e.expected_version}")
        print(f"  Actual: v{e.actual_version}")

    # Read all events
    print(f"\n=== All Events ===")
    all_stream = await store.read_all()
    print(f"Total events: {len(all_stream.events)}")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Next Steps

Continue to [Tutorial 5: Repositories](05-repositories.md).
