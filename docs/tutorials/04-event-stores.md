# Tutorial 4: Event Stores - Persisting Your Events

**Difficulty:** Beginner

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- Python 3.10 or higher
- Understanding of async/await

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain what an event store is and why it's the source of truth
2. Use `InMemoryEventStore` for development and testing
3. Append events with optimistic concurrency control
4. Read events by aggregate ID using `get_events()`
5. Stream events efficiently with `read_stream()` and `read_all()`
6. Use `ExpectedVersion` constants for concurrency control
7. Filter events by aggregate type, tenant, and timestamp
8. Choose the right event store implementation for your needs

---

## What is an Event Store?

The **event store** is the database of record in event sourcing. While traditional databases store current state, the event store stores the complete history of everything that happened.

Think of it like a blockchain or an accounting ledger - once an event is written, it's permanent. You can always reconstruct current state by replaying events from the beginning.

### Event Store Responsibilities

- **Append events atomically**: Write one or more events for an aggregate
- **Optimistic locking**: Prevent concurrent modification conflicts
- **Event retrieval**: Load events by aggregate ID to rebuild state
- **Event streaming**: Iterate over all events for projections
- **Idempotency**: Prevent duplicate events from being stored twice
- **Global ordering**: Maintain a global sequence of all events

---

## Event Store Implementations

eventsource-py provides three event store implementations:

| Implementation | Use Case | Persistence | Concurrency | Setup |
|----------------|----------|-------------|-------------|-------|
| **InMemoryEventStore** | Testing, development, prototyping | No (in-memory only) | Single process | None |
| **PostgreSQLEventStore** | Production deployments | Yes (PostgreSQL) | Multi-process, distributed | Database required |
| **SQLiteEventStore** | Embedded apps, edge computing | Yes (SQLite file) | Single process | File required |

**For this tutorial**, we'll use `InMemoryEventStore`. The concepts apply to all implementations - they share the same `EventStore` interface.

---

## Setting Up InMemoryEventStore

```python
from eventsource import InMemoryEventStore

# Create an in-memory event store
store = InMemoryEventStore()
```

That's it! No configuration needed. Perfect for testing and learning.

**Important:** All events are lost when the process terminates. For persistence, use PostgreSQL or SQLite.

---

## Core Data Structures

### EventStream

An `EventStream` represents all events for a single aggregate:

```python
from eventsource import EventStream

stream = EventStream(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[order_created, order_paid, order_shipped],
    version=3,
)

print(f"Aggregate ID: {stream.aggregate_id}")
print(f"Current version: {stream.version}")
print(f"Event count: {len(stream.events)}")
print(f"Latest event: {stream.latest_event.event_type}")
print(f"Is empty: {stream.is_empty}")
```

### StoredEvent

A `StoredEvent` wraps a domain event with persistence metadata:

```python
from eventsource import StoredEvent

stored = StoredEvent(
    event=order_created_event,
    stream_id="order-123:Order",
    stream_position=1,        # Position in the aggregate's stream
    global_position=1000,     # Position across ALL events
    stored_at=datetime.now(UTC),
)

# Access the underlying event
print(stored.event.event_type)
print(stored.aggregate_id)

# Access position metadata
print(f"Stream position: {stored.stream_position}")
print(f"Global position: {stored.global_position}")
```

`StoredEvent` is used in streaming operations (`read_stream()` and `read_all()`).

---

## Appending Events

Use `append_events()` to persist events for an aggregate:

```python
from uuid import uuid4
from eventsource import InMemoryEventStore, DomainEvent, register_event

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str

async def main():
    store = InMemoryEventStore()
    task_id = uuid4()

    # Create the event
    event = TaskCreated(
        aggregate_id=task_id,
        title="Learn event stores",
        aggregate_version=1,
    )

    # Append to the store
    result = await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[event],
        expected_version=0,  # Expect version 0 (new aggregate)
    )

    print(f"Success: {result.success}")
    print(f"New version: {result.new_version}")
    print(f"Global position: {result.global_position}")
```

**Parameters:**

- `aggregate_id`: Which aggregate these events belong to
- `aggregate_type`: Type of aggregate (e.g., "Task", "Order")
- `events`: List of events to append (can be multiple)
- `expected_version`: Current version for optimistic locking

**Returns:** `AppendResult` with:
- `success`: Whether append succeeded
- `new_version`: The version after appending
- `global_position`: Global position of last appended event
- `conflict`: Whether there was a version conflict

---

## Reading Events by Aggregate ID

Use `get_events()` to retrieve all events for an aggregate:

```python
@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"

async def main():
    store = InMemoryEventStore()
    task_id = uuid4()

    # Append some events
    await store.append_events(
        task_id,
        "Task",
        [
            TaskCreated(
                aggregate_id=task_id,
                title="First task",
                aggregate_version=1,
            )
        ],
        expected_version=0,
    )

    await store.append_events(
        task_id,
        "Task",
        [
            TaskCompleted(
                aggregate_id=task_id,
                aggregate_version=2,
            )
        ],
        expected_version=1,
    )

    # Read events back
    stream = await store.get_events(task_id, "Task")

    print(f"Aggregate ID: {stream.aggregate_id}")
    print(f"Aggregate type: {stream.aggregate_type}")
    print(f"Version: {stream.version}")
    print(f"Events:")
    for event in stream.events:
        print(f"  - v{event.aggregate_version}: {event.event_type}")
```

**Output:**
```
Aggregate ID: [task_id UUID]
Aggregate type: Task
Version: 2
Events:
  - v1: TaskCreated
  - v2: TaskCompleted
```

### Optional Filters

`get_events()` supports filtering:

```python
from datetime import datetime, UTC, timedelta

# Get events from version 5 onwards
stream = await store.get_events(
    task_id,
    "Task",
    from_version=5,
)

# Get events after a specific time
one_hour_ago = datetime.now(UTC) - timedelta(hours=1)
stream = await store.get_events(
    task_id,
    "Task",
    from_timestamp=one_hour_ago,
)

# Get events in a time range
stream = await store.get_events(
    task_id,
    "Task",
    from_timestamp=start_time,
    to_timestamp=end_time,
)
```

---

## Optimistic Concurrency Control

Event stores prevent concurrent modification conflicts using **optimistic locking**. You specify the expected version when appending - if it doesn't match the actual version, the append fails.

### The Conflict Scenario

```python
from eventsource import OptimisticLockError

async def demonstrate_conflict():
    store = InMemoryEventStore()
    task_id = uuid4()

    # Initial event
    await store.append_events(
        task_id,
        "Task",
        [TaskCreated(aggregate_id=task_id, title="Task", aggregate_version=1)],
        expected_version=0,
    )

    # Simulate two processes loading the same aggregate
    stream1 = await store.get_events(task_id, "Task")  # version = 1
    stream2 = await store.get_events(task_id, "Task")  # version = 1

    # Process 1 appends an event
    await store.append_events(
        task_id,
        "Task",
        [TaskCompleted(aggregate_id=task_id, aggregate_version=2)],
        expected_version=1,  # Success! Version was 1
    )

    # Process 2 tries to append (still thinks version is 1)
    try:
        await store.append_events(
            task_id,
            "Task",
            [TaskReassigned(aggregate_id=task_id, aggregate_version=2)],
            expected_version=1,  # Fails! Actual version is now 2
        )
    except OptimisticLockError as e:
        print(f"Conflict detected!")
        print(f"  Expected version: {e.expected_version}")
        print(f"  Actual version: {e.actual_version}")
        print(f"  Aggregate ID: {e.aggregate_id}")
```

**Output:**
```
Conflict detected!
  Expected version: 1
  Actual version: 2
  Aggregate ID: [task_id UUID]
```

### Handling Conflicts

When you get an `OptimisticLockError`, the standard pattern is:

1. Reload the aggregate from the event store
2. Replay all events to get current state
3. Re-execute the command (which may now fail due to business rules)
4. Try saving again with the new expected version

This is typically handled by the repository (covered in Tutorial 5).

---

## ExpectedVersion Constants

For common scenarios, use `ExpectedVersion` constants instead of specific numbers:

```python
from eventsource import ExpectedVersion

# Expect the stream to NOT exist (for creating new aggregates)
await store.append_events(
    task_id,
    "Task",
    events=[task_created],
    expected_version=ExpectedVersion.NO_STREAM,  # Same as 0
)

# Expect the stream to exist (for updating existing aggregates)
await store.append_events(
    task_id,
    "Task",
    events=[task_completed],
    expected_version=ExpectedVersion.STREAM_EXISTS,  # Must have version > 0
)

# Don't check version at all (dangerous! disables optimistic locking)
await store.append_events(
    task_id,
    "Task",
    events=[some_event],
    expected_version=ExpectedVersion.ANY,  # No conflict detection
)
```

**Constants:**

| Constant | Value | Meaning |
|----------|-------|---------|
| `ExpectedVersion.NO_STREAM` | 0 | Stream must not exist (new aggregate) |
| `ExpectedVersion.STREAM_EXISTS` | -2 | Stream must exist (existing aggregate) |
| `ExpectedVersion.ANY` | -1 | Skip version check (no optimistic locking) |

**Warning:** `ExpectedVersion.ANY` disables conflict detection. Only use when you're certain concurrent modifications won't cause issues.

---

## Streaming Events

For large event sets, use async iterators to avoid loading everything into memory at once.

### read_stream() - Stream Events for One Aggregate

```python
from eventsource import ReadOptions, ReadDirection

async def stream_aggregate_events():
    store = InMemoryEventStore()
    task_id = uuid4()

    # ... append some events ...

    # Stream events forward
    stream_id = f"{task_id}:Task"
    async for stored_event in store.read_stream(stream_id):
        print(f"Position {stored_event.stream_position}: {stored_event.event_type}")

    # Stream with options
    options = ReadOptions(
        direction=ReadDirection.BACKWARD,  # Read newest first
        limit=10,                          # Only get last 10 events
        from_position=0,                   # Start position
    )

    async for stored_event in store.read_stream(stream_id, options):
        print(f"Event: {stored_event.event.event_type}")
```

### read_all() - Stream All Events Across All Aggregates

```python
async def stream_all_events():
    store = InMemoryEventStore()

    # ... create many aggregates and events ...

    # Stream all events in global order
    async for stored_event in store.read_all():
        print(
            f"Global pos {stored_event.global_position}: "
            f"{stored_event.aggregate_type}/{stored_event.event_type}"
        )

    # Stream with filters
    options = ReadOptions(
        from_position=100,        # Start from global position 100
        limit=50,                 # Get next 50 events
        tenant_id=my_tenant_uuid, # Filter by tenant (multi-tenancy)
    )

    async for stored_event in store.read_all(options):
        await projection.handle(stored_event.event)
```

**ReadOptions parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `direction` | ReadDirection | FORWARD or BACKWARD (default: FORWARD) |
| `from_position` | int | Starting position (default: 0) |
| `limit` | int \| None | Max events to retrieve (default: None = unlimited) |
| `from_timestamp` | datetime \| None | Only events after this time |
| `to_timestamp` | datetime \| None | Only events before this time |
| `tenant_id` | UUID \| None | Filter by tenant (multi-tenancy) |

---

## Reading Events by Type

Use `get_events_by_type()` to get all events for a specific aggregate type:

```python
async def get_all_tasks():
    store = InMemoryEventStore()

    # ... create multiple tasks ...

    # Get all events for Task aggregates
    task_events = await store.get_events_by_type("Task")

    for event in task_events:
        print(f"Task {event.aggregate_id}: {event.event_type}")

    # Filter by tenant and timestamp
    from datetime import datetime, UTC, timedelta

    recent_tasks = await store.get_events_by_type(
        "Task",
        tenant_id=my_tenant_uuid,
        from_timestamp=datetime.now(UTC) - timedelta(hours=24),
    )
```

**Use cases:**
- Building projections that process all events of a type
- Analytics and reporting
- Data migration
- Debugging and auditing

**Warning:** This can return large result sets. For production, use `read_all()` with checkpointing (covered in later tutorials).

---

## Additional Event Store Methods

### Check if Event Exists

```python
# Idempotency check
event_id = uuid4()
if await store.event_exists(event_id):
    print("Event already processed, skipping")
    return

# Process and append event
await store.append_events(...)
```

### Get Stream Version

```python
# Get current version without loading events
version = await store.get_stream_version(task_id, "Task")
print(f"Current version: {version}")  # 0 if doesn't exist
```

### Get Global Position

```python
# Get the highest global position in the store
max_position = await store.get_global_position()
print(f"Event store has {max_position} events")
```

---

## InMemoryEventStore Testing Helpers

`InMemoryEventStore` provides additional methods useful for testing:

```python
async def test_example():
    store = InMemoryEventStore()

    # ... append events ...

    # Get all events from all aggregates
    all_events = await store.get_all_events()
    assert len(all_events) == 5

    # Get total event count
    count = await store.get_event_count()
    assert count == 5

    # Get all aggregate IDs
    aggregate_ids = await store.get_aggregate_ids()
    assert len(aggregate_ids) == 2

    # Clear the store (reset between tests)
    await store.clear()
    assert await store.get_event_count() == 0
```

**Note:** These methods are only available on `InMemoryEventStore`, not the abstract `EventStore` interface.

---

## Complete Working Example

```python
"""
Tutorial 4: Event Stores
Run with: python tutorial_04_event_stores.py
"""

import asyncio
from datetime import datetime, UTC
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    ExpectedVersion,
    InMemoryEventStore,
    OptimisticLockError,
    ReadDirection,
    ReadOptions,
    register_event,
)


# =============================================================================
# Events
# =============================================================================


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    new_assignee: UUID


# =============================================================================
# Demo
# =============================================================================


async def main() -> None:
    """Demonstrate event store operations."""

    print("=" * 60)
    print("Tutorial 4: Event Stores")
    print("=" * 60)

    store = InMemoryEventStore()
    task_id = uuid4()

    # 1. Appending events
    print("\n1. Appending events")
    print("-" * 60)

    created = TaskCreated(
        aggregate_id=task_id,
        title="Learn event stores",
        aggregate_version=1,
    )

    result = await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[created],
        expected_version=ExpectedVersion.NO_STREAM,
    )

    print(f"   Success: {result.success}")
    print(f"   New version: {result.new_version}")
    print(f"   Global position: {result.global_position}")

    # 2. Reading events
    print("\n2. Reading events")
    print("-" * 60)

    stream = await store.get_events(task_id, "Task")

    print(f"   Aggregate ID: {stream.aggregate_id}")
    print(f"   Aggregate type: {stream.aggregate_type}")
    print(f"   Version: {stream.version}")
    print(f"   Events:")
    for event in stream.events:
        print(f"     - v{event.aggregate_version}: {event.event_type}")

    # 3. Append another event
    print("\n3. Appending another event")
    print("-" * 60)

    completed = TaskCompleted(
        aggregate_id=task_id,
        aggregate_version=2,
    )

    result = await store.append_events(
        task_id,
        "Task",
        [completed],
        expected_version=1,  # We know current version is 1
    )

    print(f"   New version: {result.new_version}")

    # 4. Optimistic locking
    print("\n4. Optimistic locking (conflict detection)")
    print("-" * 60)

    try:
        # Try to append with wrong version
        await store.append_events(
            task_id,
            "Task",
            [
                TaskReassigned(
                    aggregate_id=task_id,
                    new_assignee=uuid4(),
                    aggregate_version=3,
                )
            ],
            expected_version=1,  # Wrong! Actual version is 2
        )
    except OptimisticLockError as e:
        print(f"   Conflict detected!")
        print(f"   Expected version: {e.expected_version}")
        print(f"   Actual version: {e.actual_version}")

    # 5. Streaming events
    print("\n5. Streaming events with read_stream()")
    print("-" * 60)

    stream_id = f"{task_id}:Task"
    print(f"   Stream ID: {stream_id}")
    print(f"   Events:")

    async for stored_event in store.read_stream(stream_id):
        print(
            f"     - Position {stored_event.stream_position}: "
            f"{stored_event.event_type}"
        )

    # 6. Reading all events
    print("\n6. Reading all events with read_all()")
    print("-" * 60)

    # Create another task for demonstration
    task2_id = uuid4()
    await store.append_events(
        task2_id,
        "Task",
        [
            TaskCreated(
                aggregate_id=task2_id,
                title="Second task",
                aggregate_version=1,
            )
        ],
        expected_version=0,
    )

    print(f"   All events in global order:")
    async for stored_event in store.read_all():
        print(
            f"     - Global pos {stored_event.global_position}: "
            f"{stored_event.aggregate_type}/{stored_event.event_type}"
        )

    # 7. Reading with options
    print("\n7. Reading with ReadOptions (backward, limited)")
    print("-" * 60)

    options = ReadOptions(
        direction=ReadDirection.BACKWARD,
        limit=2,
    )

    print(f"   Last 2 events (newest first):")
    async for stored_event in store.read_all(options):
        print(
            f"     - Global pos {stored_event.global_position}: "
            f"{stored_event.event_type}"
        )

    # 8. Get events by type
    print("\n8. Get events by aggregate type")
    print("-" * 60)

    task_events = await store.get_events_by_type("Task")
    print(f"   Total Task events: {len(task_events)}")
    for event in task_events:
        print(f"     - {event.event_type} for task {event.aggregate_id}")

    # 9. Event store metadata
    print("\n9. Event store metadata")
    print("-" * 60)

    total_events = await store.get_event_count()
    total_aggregates = len(await store.get_aggregate_ids())
    global_position = await store.get_global_position()

    print(f"   Total events: {total_events}")
    print(f"   Total aggregates: {total_aggregates}")
    print(f"   Current global position: {global_position}")

    print("\n" + "=" * 60)
    print("Tutorial complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**
```
============================================================
Tutorial 4: Event Stores
============================================================

1. Appending events
------------------------------------------------------------
   Success: True
   New version: 1
   Global position: 1

2. Reading events
------------------------------------------------------------
   Aggregate ID: [UUID]
   Aggregate type: Task
   Version: 1
   Events:
     - v1: TaskCreated

3. Appending another event
------------------------------------------------------------
   New version: 2

4. Optimistic locking (conflict detection)
------------------------------------------------------------
   Conflict detected!
   Expected version: 1
   Actual version: 2

5. Streaming events with read_stream()
------------------------------------------------------------
   Stream ID: [UUID]:Task
   Events:
     - Position 1: TaskCreated
     - Position 2: TaskCompleted

6. Reading all events with read_all()
------------------------------------------------------------
   All events in global order:
     - Global pos 1: Task/TaskCreated
     - Global pos 2: Task/TaskCompleted
     - Global pos 3: Task/TaskCreated

7. Reading with ReadOptions (backward, limited)
------------------------------------------------------------
   Last 2 events (newest first):
     - Global pos 3: TaskCreated
     - Global pos 2: TaskCompleted

8. Get events by aggregate type
------------------------------------------------------------
   Total Task events: 3
     - TaskCreated for task [UUID]
     - TaskCompleted for task [UUID]
     - TaskCreated for task [UUID]

9. Event store metadata
------------------------------------------------------------
   Total events: 3
   Total aggregates: 2
   Current global position: 3

============================================================
Tutorial complete!
============================================================
```

---

## PostgreSQL Event Store

For production, use `PostgreSQLEventStore`:

```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource import PostgreSQLEventStore

# Create database connection
engine = create_async_engine(
    "postgresql+asyncpg://user:password@localhost/eventstore",
    echo=False,
)

session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,
)

# Create event store
store = PostgreSQLEventStore(session_factory)

# Use it exactly like InMemoryEventStore
result = await store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[order_created],
    expected_version=0,
)
```

**Features:**
- Full ACID compliance
- Multi-process concurrency
- Partition support for high-volume events
- Outbox pattern integration
- Production-ready performance

**Setup:** See the deployment documentation for database schema setup and migrations.

---

## SQLite Event Store

For embedded applications:

```python
from eventsource import SQLiteEventStore

# File-based database
store = SQLiteEventStore(
    database="events.db",
    wal_mode=True,  # Enable WAL mode for better concurrency
)

# Initialize schema
async with store:
    await store.initialize()

    # Use it like any other event store
    result = await store.append_events(...)
```

**Features:**
- Zero configuration
- Single file database
- WAL mode for improved concurrency
- Perfect for edge computing, CLI tools, desktop apps

**Limitations:**
- Single process only (no distributed deployments)
- Lower concurrency than PostgreSQL

---

## Event Store Comparison

| Feature | InMemory | PostgreSQL | SQLite |
|---------|----------|------------|--------|
| **Persistence** | No | Yes | Yes |
| **Distributed** | No | Yes | No |
| **Concurrency** | Single process | Multi-process | Single process |
| **Setup** | None | Database + schema | File + schema |
| **Production** | No | Yes | Limited |
| **Best for** | Testing, development | Production apps | Embedded apps, CLI tools |
| **read_all()** | Yes | Yes | Yes |
| **Multi-tenancy** | Yes | Yes | Yes |
| **Performance** | Fast (in-memory) | Very fast (indexed) | Fast (file-based) |

---

## Common Patterns

### Checking if Aggregate Exists

```python
# Using get_stream_version
version = await store.get_stream_version(task_id, "Task")
exists = version > 0

# Or load and check
stream = await store.get_events(task_id, "Task")
exists = not stream.is_empty
```

### Idempotent Event Processing

```python
# Check before processing
if await store.event_exists(incoming_event.event_id):
    print("Already processed")
    return

# Process event
result = await process_event(incoming_event)

# Store it
await store.append_events(...)
```

### Batching Multiple Events

```python
# Append multiple events atomically
events = [
    TaskCreated(aggregate_id=task_id, ...),
    TaskAssigned(aggregate_id=task_id, ...),
    TaskStarted(aggregate_id=task_id, ...),
]

await store.append_events(
    task_id,
    "Task",
    events,
    expected_version=0,
)
```

---

## Key Takeaways

1. **Event store is the source of truth**: All state is derived from events stored here
2. **Three implementations available**: InMemory (testing), PostgreSQL (production), SQLite (embedded)
3. **Optimistic locking prevents conflicts**: Use `expected_version` to detect concurrent modifications
4. **ExpectedVersion constants**: Use NO_STREAM, STREAM_EXISTS, or ANY for common scenarios
5. **Two reading patterns**: `get_events()` for aggregate reconstruction, `read_all()` for projections
6. **Streaming is memory-efficient**: Use `read_stream()` and `read_all()` for large event sets
7. **All operations are async**: Always use `await` with event store methods
8. **Idempotency is built-in**: Duplicate events (same event_id) are automatically skipped

---

## Next Steps

Now that you understand event stores, you're ready to learn about repositories - a higher-level abstraction that combines aggregates and event stores.

Continue to [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md) to learn about:
- Using `AggregateRepository` to load and save aggregates
- Automatic event storage and version management
- Publishing events to event buses
- Snapshots for performance optimization

For more examples, see:
- `examples/basic_usage.py` - Complete working example with event stores
- `tests/unit/stores/` - Comprehensive test examples for all event store implementations
