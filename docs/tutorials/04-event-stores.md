# Tutorial 4: Event Stores Overview

**Estimated Time:** 30-45 minutes
**Difficulty:** Beginner
**Progress:** Tutorial 4 of 21 | Phase 1: Foundations

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- Completed [Tutorial 2: Your First Domain Event](02-first-event.md)
- Completed [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial builds on the concepts from previous tutorials. We will use `TaskCreated` and `TaskCompleted` events to demonstrate event store operations.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain the role of event stores in event sourcing and why they matter
2. Use `InMemoryEventStore` for development and testing
3. Append events with optimistic locking using `append_events()`
4. Retrieve events to reconstruct aggregate state using `get_events()`
5. Handle version conflicts using `OptimisticLockError`
6. Understand when to use each `ExpectedVersion` constant

---

## What is an Event Store?

In Tutorial 3, you learned how aggregates emit events and track them in `uncommitted_events`. But what happens when your program terminates? Those events are lost forever - unless you persist them somewhere.

This is where event stores come in.

### The Source of Truth

An **event store** is the persistence layer for events in an event-sourced system. It serves as the **single source of truth** - the authoritative record of everything that has happened in your system.

Traditional databases store the current state: "Order #123 has status 'shipped'." Event stores are different. They store the history: "Order #123 was created, then confirmed, then paid, then shipped." From this history, you can always derive the current state.

```
Traditional Database:         Event Store:
+-------------------+        +-------------------+
| orders            |        | events            |
+-------------------+        +-------------------+
| id: 123           |        | OrderCreated      |
| status: shipped   |        | OrderConfirmed    |
| total: $99.00     |        | OrderPaid         |
| ...               |        | OrderShipped      |
+-------------------+        +-------------------+
     Current State                Full History
```

### What Event Stores Do

Event stores have three primary responsibilities:

1. **Append Events**: Store new events atomically with optimistic locking to prevent concurrent modification conflicts

2. **Retrieve Events**: Load events for an aggregate so you can reconstruct its current state

3. **Maintain Order**: Ensure events are stored and retrieved in the correct chronological order

### The EventStore Interface

eventsource-py defines an `EventStore` abstract base class that all implementations follow. This ensures you can switch between stores (e.g., `InMemoryEventStore` for tests, `PostgreSQLEventStore` for production) without changing your business logic.

Key methods include:

| Method | Description |
|--------|-------------|
| `append_events()` | Persist events with optimistic locking |
| `get_events()` | Retrieve events for an aggregate |
| `get_events_by_type()` | Retrieve all events for an aggregate type |
| `event_exists()` | Check if an event already exists (idempotency) |
| `get_stream_version()` | Get current version of an aggregate |
| `get_global_position()` | Get the maximum global position |

---

## Using InMemoryEventStore

For development and testing, eventsource-py provides `InMemoryEventStore` - a simple, fast implementation that stores events in memory.

### Creating the Store

Creating an in-memory event store is straightforward:

```python
from eventsource import InMemoryEventStore

# Create a new in-memory event store
store = InMemoryEventStore()
```

That is it! No database connection, no configuration. The store is ready to use.

### When to Use InMemoryEventStore

`InMemoryEventStore` is suitable for:

- **Unit testing**: Fast, isolated tests without database setup
- **Development**: Quick iteration without external dependencies
- **Prototyping**: Explore ideas before committing to infrastructure
- **Single-process applications**: When persistence is not required

It is **not suitable** for:

- Production deployments requiring durability
- Distributed systems or multiple processes
- High-volume event storage

---

## Appending Events

The `append_events()` method is how you persist events. Let us walk through the API.

### Basic Usage

```python
import asyncio
from uuid import uuid4

from eventsource import (
    DomainEvent,
    register_event,
    InMemoryEventStore,
)


@register_event
class TaskCreated(DomainEvent):
    """Event emitted when a task is created."""
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str


async def main():
    # Create the store
    store = InMemoryEventStore()

    # Create a task ID
    task_id = uuid4()

    # Create an event
    event = TaskCreated(
        aggregate_id=task_id,
        title="Learn Event Stores",
        description="Complete Tutorial 4",
        aggregate_version=1,
    )

    # Append the event to the store
    result = await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[event],
        expected_version=0,  # New aggregate, no previous events
    )

    print(f"Success: {result.success}")
    print(f"New version: {result.new_version}")
    print(f"Global position: {result.global_position}")


if __name__ == "__main__":
    asyncio.run(main())
```

Output:

```
Success: True
New version: 1
Global position: 1
```

### Understanding the Parameters

Let us break down each parameter:

- **aggregate_id**: The UUID that identifies which aggregate these events belong to
- **aggregate_type**: A string that categorizes the aggregate (e.g., "Task", "Order", "User")
- **events**: A list of `DomainEvent` objects to append
- **expected_version**: The version you expect the aggregate to be at (more on this shortly)

### Understanding AppendResult

The `append_events()` method returns an `AppendResult` dataclass:

```python
from eventsource import AppendResult

# AppendResult fields:
# - success: bool - Whether the append succeeded
# - new_version: int - The version after appending
# - global_position: int - Position of the last event in the global stream
# - conflict: bool - Whether there was a version conflict
```

The `global_position` is particularly useful for projections and event processing - it gives you a total ordering across all events in the store.

---

## Reading Events Back

Once events are stored, you can retrieve them using `get_events()`.

### Basic Retrieval

```python
async def read_events():
    store = InMemoryEventStore()
    task_id = uuid4()

    # Store some events
    await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[
            TaskCreated(
                aggregate_id=task_id,
                title="My Task",
                description="A task",
                aggregate_version=1,
            ),
        ],
        expected_version=0,
    )

    # Read events back
    stream = await store.get_events(task_id, "Task")

    print(f"Aggregate ID: {stream.aggregate_id}")
    print(f"Aggregate Type: {stream.aggregate_type}")
    print(f"Version: {stream.version}")
    print(f"Event count: {len(stream.events)}")

    for event in stream.events:
        print(f"  - {event.event_type} (v{event.aggregate_version})")
```

Output:

```
Aggregate ID: <uuid>
Aggregate Type: Task
Version: 1
Event count: 1
  - TaskCreated (v1)
```

### Understanding EventStream

The `get_events()` method returns an `EventStream` dataclass:

```python
from eventsource import EventStream

# EventStream fields:
# - aggregate_id: UUID - The aggregate's identifier
# - aggregate_type: str - The type of aggregate
# - events: list[DomainEvent] - Events in chronological order
# - version: int - Current version (number of events)

# Useful properties:
stream.is_empty      # True if no events
stream.latest_event  # The most recent event, or None
```

### Filtering Events

You can filter events when reading:

```python
# Get events starting from a specific version
stream = await store.get_events(
    aggregate_id=task_id,
    aggregate_type="Task",
    from_version=5,  # Skip first 5 events
)

# Filter by timestamp
from datetime import datetime, timedelta, UTC

one_hour_ago = datetime.now(UTC) - timedelta(hours=1)

stream = await store.get_events(
    aggregate_id=task_id,
    aggregate_type="Task",
    from_timestamp=one_hour_ago,
)
```

---

## Optimistic Locking

One of the most important features of event stores is **optimistic locking**. This mechanism prevents two processes from simultaneously modifying the same aggregate and creating conflicting state.

### How It Works

When you call `append_events()`, you provide an `expected_version`. The event store checks if the aggregate's actual version matches your expectation:

- **Match**: Events are appended, version incremented
- **Mismatch**: `OptimisticLockError` is raised

This is called "optimistic" because we optimistically assume there will be no conflict and only check at write time.

### Demonstrating a Conflict

```python
from eventsource import InMemoryEventStore, OptimisticLockError


@register_event
class TaskCompleted(DomainEvent):
    """Event emitted when a task is completed."""
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"
    completed_by: UUID


async def conflict_demo():
    store = InMemoryEventStore()
    task_id = uuid4()
    user_id = uuid4()

    # Create the task (version becomes 1)
    await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[TaskCreated(
            aggregate_id=task_id,
            title="Shared Task",
            description="Demo",
            aggregate_version=1,
        )],
        expected_version=0,
    )
    print("Task created at version 1")

    # Simulate two users reading the task
    # Both see version 1 and decide to complete it

    # User A completes successfully (version becomes 2)
    await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[TaskCompleted(
            aggregate_id=task_id,
            completed_by=user_id,
            aggregate_version=2,
        )],
        expected_version=1,
    )
    print("User A completed the task (version now 2)")

    # User B tries to complete with stale version (expected 1, but it is 2)
    try:
        await store.append_events(
            aggregate_id=task_id,
            aggregate_type="Task",
            events=[TaskCompleted(
                aggregate_id=task_id,
                completed_by=uuid4(),  # Different user
                aggregate_version=2,
            )],
            expected_version=1,  # Wrong! Version is now 2
        )
    except OptimisticLockError as e:
        print(f"\nOptimisticLockError caught!")
        print(f"  Expected version: {e.expected_version}")
        print(f"  Actual version: {e.actual_version}")
        print(f"  Aggregate ID: {e.aggregate_id}")
```

Output:

```
Task created at version 1
User A completed the task (version now 2)

OptimisticLockError caught!
  Expected version: 1
  Actual version: 2
  Aggregate ID: <uuid>
```

### Handling Conflicts

When you catch an `OptimisticLockError`, the typical recovery strategy is:

1. **Reload** the aggregate from the event store
2. **Re-validate** your business logic against the new state
3. **Retry** the operation if still valid

```python
async def complete_task_with_retry(store, task_id, completed_by, max_retries=3):
    """Complete a task with automatic retry on conflict."""
    for attempt in range(max_retries):
        try:
            # Get current state
            stream = await store.get_events(task_id, "Task")
            current_version = stream.version

            # Check if task can still be completed
            # (In a real app, you would reconstruct the aggregate
            # and check its state)

            # Attempt to complete
            await store.append_events(
                aggregate_id=task_id,
                aggregate_type="Task",
                events=[TaskCompleted(
                    aggregate_id=task_id,
                    completed_by=completed_by,
                    aggregate_version=current_version + 1,
                )],
                expected_version=current_version,
            )
            print(f"Task completed on attempt {attempt + 1}")
            return True

        except OptimisticLockError:
            print(f"Conflict on attempt {attempt + 1}, retrying...")
            continue

    print("Failed after max retries")
    return False
```

---

## ExpectedVersion Constants

eventsource-py provides semantic constants for common version expectations through the `ExpectedVersion` class:

| Constant | Value | Use Case |
|----------|-------|----------|
| `NO_STREAM` | 0 | Creating a new aggregate |
| `ANY` | -1 | Skip version check (use carefully!) |
| `STREAM_EXISTS` | -2 | Updating an existing aggregate (any version) |

### Using ExpectedVersion Constants

```python
from eventsource import ExpectedVersion, InMemoryEventStore, OptimisticLockError


async def expected_version_demo():
    store = InMemoryEventStore()

    # NO_STREAM: For creating new aggregates
    # Fails if the aggregate already exists
    task_id = uuid4()

    result = await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[TaskCreated(
            aggregate_id=task_id,
            title="New Task",
            description="Demo",
            aggregate_version=1,
        )],
        expected_version=ExpectedVersion.NO_STREAM,
    )
    print(f"NO_STREAM: Created new aggregate, version={result.new_version}")

    # Trying NO_STREAM again would fail
    try:
        await store.append_events(
            aggregate_id=task_id,
            aggregate_type="Task",
            events=[TaskCreated(
                aggregate_id=task_id,
                title="Duplicate",
                description="Will fail",
                aggregate_version=1,
            )],
            expected_version=ExpectedVersion.NO_STREAM,
        )
    except OptimisticLockError:
        print("NO_STREAM: Failed - aggregate already exists")

    # ANY: Skip version check entirely
    # Useful for event processors that must not lose events
    result = await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[TaskCompleted(
            aggregate_id=task_id,
            completed_by=uuid4(),
            aggregate_version=2,
        )],
        expected_version=ExpectedVersion.ANY,
    )
    print(f"ANY: Appended without version check, version={result.new_version}")

    # STREAM_EXISTS: For updates when you know aggregate exists
    # but do not care about specific version
    another_task = uuid4()

    try:
        await store.append_events(
            aggregate_id=another_task,
            aggregate_type="Task",
            events=[TaskCompleted(
                aggregate_id=another_task,
                completed_by=uuid4(),
                aggregate_version=1,
            )],
            expected_version=ExpectedVersion.STREAM_EXISTS,
        )
    except OptimisticLockError:
        print("STREAM_EXISTS: Failed - stream does not exist yet")
```

### When to Use Each Constant

- **Specific version number (e.g., 5)**: Use when you need strict consistency and have loaded the aggregate. This is the safest option.

- **NO_STREAM**: Use when creating new aggregates to prevent duplicates.

- **STREAM_EXISTS**: Use when you know the aggregate exists but do not need to enforce a specific version. Useful for idempotent operations.

- **ANY**: Use sparingly! This disables optimistic locking. Appropriate for:
  - Event processors that must not lose events
  - Migrating data where ordering does not matter
  - Test fixtures

---

## Understanding Positions

Events have two types of positions that serve different purposes.

### Stream Position

The **stream position** (also called version) is the event's position within a specific aggregate's stream:

```
Task #123 Event Stream:
  Position 1: TaskCreated
  Position 2: TaskReassigned
  Position 3: TaskCompleted
```

Stream position is used for:
- Optimistic locking
- Aggregate version tracking
- Loading events for a specific aggregate

### Global Position

The **global position** is the event's position across ALL events in the store:

```
Global Event Stream:
  Position 1: Order #A - OrderCreated
  Position 2: Task #123 - TaskCreated
  Position 3: Order #A - OrderShipped
  Position 4: Task #123 - TaskReassigned
  Position 5: User #U1 - UserRegistered
  Position 6: Task #123 - TaskCompleted
```

Global position is used for:
- Building projections that process all events
- Event subscriptions and catch-up
- Cross-aggregate ordering

You can get the current global position:

```python
max_position = await store.get_global_position()
print(f"Store has events up to position {max_position}")
```

---

## Event Store Comparison

eventsource-py provides three event store implementations:

| Feature | InMemoryEventStore | SQLiteEventStore | PostgreSQLEventStore |
|---------|-------------------|------------------|---------------------|
| **Use Case** | Testing, development | Development, embedded | Production |
| **Persistence** | None (memory only) | File or memory | Full database |
| **Concurrency** | Single process | Single writer | Multiple writers |
| **Setup** | None required | Optional (file path) | Database required |
| **Performance** | Fastest | Fast | Production-grade |
| **Multi-tenancy** | Yes | Yes | Yes |
| **Global ordering** | Yes | Yes | Yes |
| **Recommended For** | Unit tests | Integration tests, small apps | Production deployments |

### Choosing the Right Store

```python
# Development and unit tests: InMemoryEventStore
from eventsource import InMemoryEventStore
store = InMemoryEventStore()

# Integration tests or embedded apps: SQLiteEventStore (Tutorial 12)
# Note: Requires aiosqlite: pip install eventsource-py[sqlite]
from eventsource import SQLiteEventStore
async with SQLiteEventStore(":memory:") as store:
    await store.initialize()
    # Use store...

# Production: PostgreSQLEventStore (Tutorial 11)
from eventsource import PostgreSQLEventStore
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

engine = create_async_engine("postgresql+asyncpg://...")
session_factory = async_sessionmaker(engine, expire_on_commit=False)
store = PostgreSQLEventStore(session_factory)
```

Production stores (PostgreSQL, SQLite) are covered in detail in Tutorials 11 and 12.

---

## Complete Example

Here is a complete example demonstrating event store operations:

```python
"""
Tutorial 4: Event Stores Overview

This example demonstrates direct interaction with event stores.
Run with: python tutorial_04_event_stores.py
"""
import asyncio
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    register_event,
    InMemoryEventStore,
    ExpectedVersion,
    OptimisticLockError,
)


# =============================================================================
# Events (from Tutorial 2)
# =============================================================================

@register_event
class TaskCreated(DomainEvent):
    """Event emitted when a task is created."""
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str


@register_event
class TaskCompleted(DomainEvent):
    """Event emitted when a task is completed."""
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"
    completed_by: UUID


# =============================================================================
# Event Store Examples
# =============================================================================

async def basic_usage():
    """Demonstrate basic event store operations."""
    print("=" * 60)
    print("Basic Event Store Usage")
    print("=" * 60)
    print()

    # Create the store
    store = InMemoryEventStore()

    # Create some events
    task_id = uuid4()
    user_id = uuid4()

    created_event = TaskCreated(
        aggregate_id=task_id,
        title="Learn Event Stores",
        description="Complete Tutorial 4",
        aggregate_version=1,
    )

    # Append the first event
    # expected_version=0 means we expect a new aggregate (no existing events)
    result = await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[created_event],
        expected_version=0,
    )

    print("1. Append first event:")
    print(f"   Success: {result.success}")
    print(f"   New version: {result.new_version}")
    print(f"   Global position: {result.global_position}")
    print()

    # Read the events back
    stream = await store.get_events(task_id, "Task")

    print("2. Read events back:")
    print(f"   Aggregate ID: {stream.aggregate_id}")
    print(f"   Aggregate Type: {stream.aggregate_type}")
    print(f"   Version: {stream.version}")
    print(f"   Is empty: {stream.is_empty}")
    print()

    # Append another event
    completed_event = TaskCompleted(
        aggregate_id=task_id,
        completed_by=user_id,
        aggregate_version=2,
    )

    result = await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[completed_event],
        expected_version=1,  # We know version is 1 after first event
    )

    print("3. Append second event:")
    print(f"   New version: {result.new_version}")
    print(f"   Global position: {result.global_position}")
    print()

    # Read all events
    stream = await store.get_events(task_id, "Task")
    print("4. All events for task:")
    for i, event in enumerate(stream.events):
        print(f"   {i + 1}. {event.event_type} (v{event.aggregate_version})")


async def expected_version_demo():
    """Demonstrate ExpectedVersion constants."""
    print()
    print("=" * 60)
    print("ExpectedVersion Constants")
    print("=" * 60)
    print()

    store = InMemoryEventStore()
    task_id = uuid4()

    # NO_STREAM (0): Expect the aggregate does not exist yet
    print("1. Using ExpectedVersion.NO_STREAM for new aggregate...")
    result = await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[TaskCreated(
            aggregate_id=task_id,
            title="New Task",
            description="Demo",
            aggregate_version=1,
        )],
        expected_version=ExpectedVersion.NO_STREAM,
    )
    print(f"   Result: {'Success' if result.success else 'Failed'}")

    # ANY (-1): Do not check version (use carefully!)
    print()
    print("2. Using ExpectedVersion.ANY (no version check)...")
    result = await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[TaskCompleted(
            aggregate_id=task_id,
            completed_by=uuid4(),
            aggregate_version=2,
        )],
        expected_version=ExpectedVersion.ANY,
    )
    print(f"   Result: {'Success' if result.success else 'Failed'}")

    # STREAM_EXISTS (-2): Expect aggregate exists (any version)
    print()
    print("3. Using ExpectedVersion.STREAM_EXISTS for existing aggregate...")
    another_task = uuid4()

    try:
        await store.append_events(
            aggregate_id=another_task,
            aggregate_type="Task",
            events=[TaskCreated(
                aggregate_id=another_task,
                title="Another",
                description="Demo",
                aggregate_version=1,
            )],
            expected_version=ExpectedVersion.STREAM_EXISTS,
        )
    except OptimisticLockError:
        print("   Expected failure: Stream does not exist")


async def conflict_demo():
    """Demonstrate optimistic locking conflict."""
    print()
    print("=" * 60)
    print("Optimistic Locking Demo")
    print("=" * 60)
    print()

    store = InMemoryEventStore()
    task_id = uuid4()

    # Create the task
    await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[TaskCreated(
            aggregate_id=task_id,
            title="Shared Task",
            description="Demo",
            aggregate_version=1,
        )],
        expected_version=0,
    )
    print("1. Task created at version 1")

    # Simulate two concurrent operations both thinking version is 1
    # First one succeeds
    await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[TaskCompleted(
            aggregate_id=task_id,
            completed_by=uuid4(),
            aggregate_version=2,
        )],
        expected_version=1,
    )
    print("2. First concurrent update succeeded (version now 2)")

    # Second one fails - version is now 2, not 1
    print()
    print("3. Second concurrent update with stale version...")
    try:
        await store.append_events(
            aggregate_id=task_id,
            aggregate_type="Task",
            events=[TaskCompleted(
                aggregate_id=task_id,
                completed_by=uuid4(),
                aggregate_version=2,
            )],
            expected_version=1,  # Wrong! Version is now 2
        )
    except OptimisticLockError as e:
        print(f"   OptimisticLockError caught!")
        print(f"   Expected version: {e.expected_version}")
        print(f"   Actual version: {e.actual_version}")
        print()
        print("   Recovery strategy: Reload aggregate and retry")


async def main():
    """Run all examples."""
    await basic_usage()
    await expected_version_demo()
    await conflict_demo()
    print()
    print("=" * 60)
    print("Tutorial 4 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_04_event_stores.py` and run it:

```bash
python tutorial_04_event_stores.py
```

---

## Exercises

Now it is time to practice what you have learned!

### Exercise 1: Conflict Resolution

**Objective:** Simulate and handle a concurrent modification conflict with proper retry logic.

**Time:** 10-15 minutes

**Instructions:**

1. Create a task and save its first event with `expected_version=0`
2. Load the events and note the current version
3. Try to append an event with an intentionally wrong `expected_version`
4. Catch the `OptimisticLockError` and extract the actual version
5. Retry the operation with the correct version

**Starter Code:**

```python
"""
Tutorial 4 - Exercise 1: Conflict Resolution

Your task: Implement conflict detection and recovery.
"""
import asyncio
from uuid import uuid4

from eventsource import (
    DomainEvent,
    register_event,
    InMemoryEventStore,
    OptimisticLockError,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskUpdated(DomainEvent):
    event_type: str = "TaskUpdated"
    aggregate_type: str = "Task"
    new_title: str


async def main():
    store = InMemoryEventStore()
    task_id = uuid4()

    # Step 1: Create the task
    # TODO: Append TaskCreated event with expected_version=0

    # Step 2: Load events and note version
    # TODO: Get events and print current version

    # Step 3: Try to append with WRONG expected_version
    wrong_version = 0  # This should be 1!

    # TODO: Try to append TaskUpdated with wrong_version
    # Catch OptimisticLockError and print the expected vs actual version

    # Step 5: Retry with correct version
    # TODO: Use e.actual_version to retry the operation


if __name__ == "__main__":
    asyncio.run(main())
```

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 4 - Exercise 1 Solution: Conflict Resolution
"""
import asyncio
from uuid import uuid4

from eventsource import (
    DomainEvent,
    register_event,
    InMemoryEventStore,
    OptimisticLockError,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskUpdated(DomainEvent):
    event_type: str = "TaskUpdated"
    aggregate_type: str = "Task"
    new_title: str


async def main():
    store = InMemoryEventStore()
    task_id = uuid4()

    # Step 1: Create the task
    await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[TaskCreated(
            aggregate_id=task_id,
            title="Original Title",
            aggregate_version=1,
        )],
        expected_version=0,
    )
    print("Task created at version 1")

    # Step 2: Load events and note version
    stream = await store.get_events(task_id, "Task")
    print(f"Current version: {stream.version}")

    # Step 3: Try to append with WRONG expected_version
    wrong_version = 0  # Should be 1
    print(f"\nTrying to append with wrong version ({wrong_version})...")

    try:
        await store.append_events(
            aggregate_id=task_id,
            aggregate_type="Task",
            events=[TaskUpdated(
                aggregate_id=task_id,
                new_title="Updated Title",
                aggregate_version=2,
            )],
            expected_version=wrong_version,
        )
    except OptimisticLockError as e:
        # Step 4: Handle the error
        print(f"Caught OptimisticLockError!")
        print(f"  Expected: {e.expected_version}, Actual: {e.actual_version}")

        # Step 5: Retry with correct version
        print(f"\nRetrying with correct version ({e.actual_version})...")

        result = await store.append_events(
            aggregate_id=task_id,
            aggregate_type="Task",
            events=[TaskUpdated(
                aggregate_id=task_id,
                new_title="Updated Title",
                aggregate_version=e.actual_version + 1,
            )],
            expected_version=e.actual_version,
        )

        print(f"Retry successful! New version: {result.new_version}")

    # Verify final state
    final_stream = await store.get_events(task_id, "Task")
    print(f"\nFinal state:")
    for event in final_stream.events:
        print(f"  - {event.event_type}")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

---

## Summary

In this tutorial, you learned:

- **Event stores** are the persistence layer and single source of truth in event sourcing
- **InMemoryEventStore** is perfect for development and testing - no setup required
- **append_events()** persists events with optimistic locking to prevent conflicts
- **get_events()** retrieves events to reconstruct aggregate state
- **OptimisticLockError** is raised when expected version does not match actual version
- **ExpectedVersion constants** provide semantic meaning: `NO_STREAM`, `ANY`, `STREAM_EXISTS`
- **Stream position** is within an aggregate; **global position** is across all events

---

## Key Takeaways

!!! note "Remember"
    - Event stores are **append-only** - events are never modified or deleted
    - Use **InMemoryEventStore** for tests, **PostgreSQLEventStore** for production
    - Always handle **OptimisticLockError** in production code
    - **Global position** enables consistent ordering across all aggregates
    - The `expected_version` parameter is your guard against concurrent modifications

!!! tip "Best Practice"
    In production, always use a specific version number (not `ANY`) for optimistic locking unless you have a specific reason to skip the check. This prevents subtle bugs from concurrent modifications.

!!! warning "Common Mistake"
    Do not use `ExpectedVersion.ANY` for normal business operations. It disables the safety mechanism that prevents concurrent modifications. Only use it for event processors or data migrations where ordering does not matter.

---

## Next Steps

You now know how to persist events directly to an event store. But in Tutorial 3, you learned that aggregates track events in `uncommitted_events`. How do you connect these two concepts?

In the next tutorial, you will learn about **repositories** - the pattern that bridges aggregates and event stores, providing a clean API for loading and saving aggregates.

Continue to [Tutorial 5: Repositories Pattern](05-repositories.md) to learn the recommended way to work with aggregates and event stores together.

---

## Related Documentation

- [API Reference: Event Stores](../api/stores.md) - Complete event store API documentation
- [Error Handling Guide](../guides/error-handling.md) - Detailed error handling strategies
- [Tutorial 11: PostgreSQL Event Store](11-postgresql.md) - Production store setup
- [Tutorial 12: SQLite Event Store](12-sqlite.md) - Development store setup
