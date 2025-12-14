# Tutorial 5: Repositories - Managing Aggregate Lifecycle

**Difficulty:** Beginner

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- Python 3.10 or higher
- Understanding of async/await

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain what the repository pattern is and why it's essential
2. Create an `AggregateRepository` with proper configuration
3. Use `create_new()` to instantiate aggregates
4. Load aggregates from event history with `load()`
5. Save aggregates and persist uncommitted events with `save()`
6. Handle the load-modify-save workflow correctly
7. Configure event publishing through repositories
8. Understand optimistic concurrency control
9. Use utility methods like `exists()`, `get_version()`, and `load_or_create()`

---

## What is the Repository Pattern?

The **repository pattern** provides a clean abstraction for managing aggregate persistence. Instead of manually interacting with the event store, repositories handle the complexity of:

- **Loading aggregates** by fetching events and replaying them
- **Saving aggregates** by persisting uncommitted events atomically
- **Optimistic locking** to prevent concurrent modification conflicts
- **Event publishing** to distribute events to subscribers
- **Version tracking** to ensure consistency

### Without Repository

```python
# Manual approach - error-prone!
event_store = InMemoryEventStore()
task_id = uuid4()

# Loading: manual event fetching and replay
stream = await event_store.get_events(task_id, "Task")
if not stream.events:
    raise ValueError("Task not found")

task = TaskAggregate(task_id)
task.load_from_history(stream.events)

# Modifying
task.complete(user_id)

# Saving: manual version calculation
expected_version = task.version - len(task.uncommitted_events)
await event_store.append_events(
    task_id,
    "Task",
    task.uncommitted_events,
    expected_version,
)
task.mark_events_as_committed()
```

### With Repository

```python
# Clean approach - repository handles everything
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
)

# Loading
task = await repo.load(task_id)

# Modifying
task.complete(user_id)

# Saving
await repo.save(task)
```

The repository abstracts all the complexity into simple, type-safe methods.

---

## Creating a Repository

### Basic Repository

```python
from eventsource import AggregateRepository, InMemoryEventStore

# Create event store
event_store = InMemoryEventStore()

# Create repository
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
)
```

**Required parameters:**

- `event_store`: The event store for persistence (InMemory, PostgreSQL, SQLite)
- `aggregate_factory`: The aggregate class to instantiate (e.g., `TaskAggregate`)
- `aggregate_type`: String identifier matching the aggregate's type (e.g., "Task")

### Repository with Event Publishing

```python
from eventsource import InMemoryEventBus

# Create event bus for pub/sub
event_bus = InMemoryEventBus()

# Create repository with publishing
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
    event_publisher=event_bus,  # Events published after save
)
```

When configured with an `event_publisher`, the repository automatically publishes uncommitted events after successful saves. This enables real-time event distribution to projections and other subscribers.

---

## Creating New Aggregates

Use `create_new()` to instantiate a new aggregate:

```python
from uuid import uuid4

# Create a new aggregate instance
task_id = uuid4()
task = repo.create_new(task_id)

print(f"Version: {task.version}")  # 0 (no events yet)
print(f"State: {task.state}")      # None (not initialized)

# Execute commands
task.create("Learn repositories", "Complete tutorial 5")

print(f"Version: {task.version}")  # 1
print(f"State: {task.state.title}")  # "Learn repositories"

# Save to event store
await repo.save(task)
```

**Important:** `create_new()` only creates an in-memory instance. You must call `save()` to persist events to the event store.

---

## Loading Aggregates

Use `load()` to reconstitute aggregates from event history:

```python
# Load aggregate from event store
task = await repo.load(task_id)

print(f"Title: {task.state.title}")
print(f"Status: {task.state.status}")
print(f"Version: {task.version}")
```

**What happens during load:**

1. Repository fetches all events for the aggregate from the event store
2. Creates a new aggregate instance via the factory
3. Calls `aggregate.load_from_history(events)` to replay events
4. Returns the reconstituted aggregate with current state

**Error handling:**

```python
from eventsource import AggregateNotFoundError

try:
    task = await repo.load(nonexistent_id)
except AggregateNotFoundError as e:
    print(f"Aggregate {e.aggregate_id} not found")
    print(f"Aggregate type: {e.aggregate_type}")
```

If no events exist for the aggregate ID, `load()` raises `AggregateNotFoundError`.

---

## Saving Aggregates

Use `save()` to persist uncommitted events:

```python
# Load aggregate
task = await repo.load(task_id)

# Execute commands
user_id = uuid4()
task.assign(user_id)
task.complete(user_id)

# Save changes
await repo.save(task)

print(f"Saved version {task.version}")
print(f"Has uncommitted events: {task.has_uncommitted_events}")  # False
```

**What happens during save:**

1. Repository checks if there are uncommitted events (if not, it's a no-op)
2. Calculates expected version: `current_version - uncommitted_event_count`
3. Calls `event_store.append_events()` with optimistic locking
4. On success:
   - Marks events as committed on the aggregate
   - Publishes events if `event_publisher` is configured
   - Returns immediately (events are now persisted)

**Important:** After `save()`, calling `save()` again without new commands does nothing:

```python
task = await repo.load(task_id)
task.complete(user_id)
await repo.save(task)  # Persists 1 event

await repo.save(task)  # No-op (no uncommitted events)
```

---

## The Load-Modify-Save Pattern

The standard workflow for updating aggregates:

```python
# Pattern: Load → Modify → Save

# 1. Load current state
task = await repo.load(task_id)

# 2. Execute business logic
if task.state.status == "pending":
    task.assign(user_id)
    task.start_work()

# 3. Persist changes
await repo.save(task)
```

**Why this pattern?**

- **Load** ensures you have the latest state
- **Modify** applies business rules and generates events
- **Save** persists events with optimistic locking

This pattern is safe for concurrent access thanks to optimistic locking (covered next).

---

## Optimistic Concurrency Control

Repositories use **optimistic locking** to detect and prevent concurrent modification conflicts.

### How It Works

```python
# Process 1: Load task at version 5
task1 = await repo.load(task_id)
print(task1.version)  # 5

# Process 2: Load same task at version 5
task2 = await repo.load(task_id)
print(task2.version)  # 5

# Process 1: Modify and save (succeeds)
task1.complete(user_id)
await repo.save(task1)  # Version is now 6

# Process 2: Modify and save (fails!)
task2.reassign(other_user_id)
await repo.save(task2)  # OptimisticLockError!
```

When Process 2 tries to save, the repository:
1. Calculates expected version: 5 (task2's version before new event)
2. Calls event store with `expected_version=5`
3. Event store sees actual version is 6 (Process 1 already saved)
4. Event store raises `OptimisticLockError`

### Handling Conflicts

```python
from eventsource import OptimisticLockError

async def complete_task_with_retry(task_id: UUID, user_id: UUID, max_retries: int = 3):
    """Complete task with automatic conflict resolution."""
    for attempt in range(max_retries):
        try:
            # Always load fresh state
            task = await repo.load(task_id)

            # Check if already completed
            if task.state.status == "completed":
                print("Already completed")
                return

            # Execute command
            task.complete(user_id)

            # Try to save
            await repo.save(task)
            print(f"Saved on attempt {attempt + 1}")
            return

        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise
            print(f"Conflict on attempt {attempt + 1}, retrying...")
```

**Key pattern:** On conflict, reload the aggregate to get the latest state, then retry the operation. The business rules may reject the command with fresh state (e.g., task already completed).

---

## Repository Utility Methods

### exists()

Check if an aggregate exists without loading it:

```python
if await repo.exists(task_id):
    task = await repo.load(task_id)
else:
    task = repo.create_new(task_id)
    task.create("New Task", "Description")
    await repo.save(task)
```

### get_version()

Get the current version without loading events:

```python
version = await repo.get_version(task_id)
print(f"Current version: {version}")  # 0 if doesn't exist
```

### load_or_create()

Load existing aggregate or create new one:

```python
# Load if exists, otherwise create new
task = await repo.load_or_create(task_id)

if task.version == 0:
    # New aggregate - initialize it
    task.create("Task Title", "Description")
else:
    # Existing aggregate - use current state
    print(f"Existing task: {task.state.title}")

await repo.save(task)
```

### get_or_raise()

Alias for `load()` that makes intent clearer:

```python
# These are identical
task = await repo.load(task_id)  # Raises AggregateNotFoundError
task = await repo.get_or_raise(task_id)  # Same behavior, clearer intent
```

---

## Event Publishing

Repositories can publish events to an event bus after successful saves:

```python
from eventsource import InMemoryEventBus

# Create event bus
event_bus = InMemoryEventBus()

# Create repository with publishing
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
    event_publisher=event_bus,
)

# Subscribe to events
async def on_task_created(event: TaskCreated):
    print(f"Task created: {event.title}")

async def on_task_completed(event: TaskCompleted):
    print(f"Task {event.aggregate_id} completed by {event.completed_by}")

await event_bus.subscribe(TaskCreated, on_task_created)
await event_bus.subscribe(TaskCompleted, on_task_completed)

# Create and save task
task = repo.create_new(uuid4())
task.create("Published task", "Will trigger handler")
await repo.save(task)  # Triggers on_task_created()

# Complete task
task = await repo.load(task.aggregate_id)
task.complete(user_id)
await repo.save(task)  # Triggers on_task_completed()
```

**Publishing sequence:**

1. Events are persisted to the event store
2. Events are marked as committed on the aggregate
3. Events are published to the event publisher (if configured)

This ensures events are only published after they're safely persisted.

---

## Repository Properties

Access repository configuration:

```python
# Get event store reference
store = repo.event_store

# Get aggregate type
agg_type = repo.aggregate_type  # "Task"

# Get event publisher (if configured)
publisher = repo.event_publisher  # EventBus or None
```

---

## Complete Working Example

```python
"""
Tutorial 5: Repositories
Run with: python tutorial_05_repositories.py
"""

import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    OptimisticLockError,
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
    description: str


@register_event
class TaskAssigned(DomainEvent):
    event_type: str = "TaskAssigned"
    aggregate_type: str = "Task"

    assigned_to: UUID


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"

    completed_by: UUID


# =============================================================================
# State
# =============================================================================


class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"
    completed_by: UUID | None = None


# =============================================================================
# Aggregate
# =============================================================================


class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=self.aggregate_id,
                title=event.title,
                description=event.description,
                status="pending",
            )
        elif isinstance(event, TaskAssigned):
            if self._state:
                self._state = self._state.model_copy(
                    update={"assigned_to": event.assigned_to}
                )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "completed",
                        "completed_by": event.completed_by,
                    }
                )

    def create(self, title: str, description: str) -> None:
        """Create a new task."""
        if self.version > 0:
            raise ValueError("Task already exists")

        self.apply_event(
            TaskCreated(
                aggregate_id=self.aggregate_id,
                title=title,
                description=description,
                aggregate_version=self.get_next_version(),
            )
        )

    def assign(self, user_id: UUID) -> None:
        """Assign task to a user."""
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot assign completed task")

        self.apply_event(
            TaskAssigned(
                aggregate_id=self.aggregate_id,
                assigned_to=user_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def complete(self, completed_by: UUID) -> None:
        """Mark task as complete."""
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")

        self.apply_event(
            TaskCompleted(
                aggregate_id=self.aggregate_id,
                completed_by=completed_by,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Demo
# =============================================================================


async def main():
    """Demonstrate repository usage."""
    print("=" * 60)
    print("Tutorial 5: Repositories")
    print("=" * 60)

    # Setup
    event_store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # 1. Create new task
    print("\n1. Creating new task")
    print("-" * 60)

    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Learn repositories", "Complete tutorial 5")
    await repo.save(task)

    print(f"   Created task: {task.state.title}")
    print(f"   Version: {task.version}")
    print(f"   Uncommitted events: {len(task.uncommitted_events)}")

    # 2. Load and modify
    print("\n2. Load and modify")
    print("-" * 60)

    task = await repo.load(task_id)
    print(f"   Loaded task v{task.version}: {task.state.title}")

    user_id = uuid4()
    task.assign(user_id)
    await repo.save(task)

    print(f"   Assigned to: {task.state.assigned_to}")
    print(f"   Version: {task.version}")

    # 3. Load-modify-save pattern
    print("\n3. Load-modify-save pattern")
    print("-" * 60)

    task = await repo.load(task_id)
    task.complete(user_id)
    await repo.save(task)

    print(f"   Completed task")
    print(f"   Status: {task.state.status}")
    print(f"   Version: {task.version}")

    # 4. Check existence
    print("\n4. Checking existence")
    print("-" * 60)

    exists = await repo.exists(task_id)
    version = await repo.get_version(task_id)

    print(f"   Task exists: {exists}")
    print(f"   Current version: {version}")

    # 5. Load or create
    print("\n5. Load or create")
    print("-" * 60)

    new_id = uuid4()
    task2 = await repo.load_or_create(new_id)

    print(f"   Task version: {task2.version}")  # 0 (new)
    print(f"   Is new: {task2.version == 0}")

    task2.create("Another task", "Created via load_or_create")
    await repo.save(task2)

    print(f"   Saved new task v{task2.version}")

    # 6. Optimistic locking
    print("\n6. Optimistic locking")
    print("-" * 60)

    # Load twice
    task_a = await repo.load(task_id)
    task_b = await repo.load(task_id)

    # Modify first
    task_a.assign(uuid4())
    await repo.save(task_a)
    print(f"   Process A saved successfully")

    # Try to save second (conflict!)
    try:
        task_b.assign(uuid4())
        await repo.save(task_b)
    except OptimisticLockError as e:
        print(f"   Process B failed with conflict:")
        print(f"   - Expected version: {e.expected_version}")
        print(f"   - Actual version: {e.actual_version}")

    # 7. Event history
    print("\n7. Event history")
    print("-" * 60)

    stream = await event_store.get_events(task_id, "Task")
    print(f"   Total events: {len(stream.events)}")
    for event in stream.events:
        print(f"   - v{event.aggregate_version}: {event.event_type}")

    # 8. Final state
    print("\n8. Final state")
    print("-" * 60)

    final_task = await repo.load(task_id)
    print(f"   Task ID: {final_task.aggregate_id}")
    print(f"   Title: {final_task.state.title}")
    print(f"   Status: {final_task.state.status}")
    print(f"   Assigned to: {final_task.state.assigned_to}")
    print(f"   Version: {final_task.version}")

    print("\n" + "=" * 60)
    print("Tutorial complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
============================================================
Tutorial 5: Repositories
============================================================

1. Creating new task
------------------------------------------------------------
   Created task: Learn repositories
   Version: 1
   Uncommitted events: 0

2. Load and modify
------------------------------------------------------------
   Loaded task v1: Learn repositories
   Assigned to: [UUID]
   Version: 2

3. Load-modify-save pattern
------------------------------------------------------------
   Completed task
   Status: completed
   Version: 3

4. Checking existence
------------------------------------------------------------
   Task exists: True
   Current version: 3

5. Load or create
------------------------------------------------------------
   Task version: 0
   Is new: True
   Saved new task v1

6. Optimistic locking
------------------------------------------------------------
   Process A saved successfully
   Process B failed with conflict:
   - Expected version: 3
   - Actual version: 4

7. Event history
------------------------------------------------------------
   Total events: 4
   - v1: TaskCreated
   - v2: TaskAssigned
   - v3: TaskCompleted
   - v4: TaskAssigned

8. Final state
------------------------------------------------------------
   Task ID: [UUID]
   Title: Learn repositories
   Status: completed
   Assigned to: [UUID]
   Version: 4

============================================================
Tutorial complete!
============================================================
```

---

## Advanced: Snapshot Support

Repositories support **snapshots** for performance optimization with large event histories. Snapshots cache aggregate state, reducing the number of events to replay during load.

### Configuring Snapshots

```python
from eventsource import InMemorySnapshotStore

# Create snapshot store
snapshot_store = InMemorySnapshotStore()

# Create repository with snapshots
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,  # Snapshot every 100 events
    snapshot_mode="sync",    # "sync" | "background" | "manual"
)
```

**Snapshot modes:**

- **`"sync"`**: Create snapshot immediately after save (default, simple)
- **`"background"`**: Create snapshot asynchronously (best for high throughput)
- **`"manual"`**: Only create snapshots via `create_snapshot()` method

### Snapshot-Aware Loading

When snapshots are configured, loading changes:

1. Check for valid snapshot at current aggregate version
2. If snapshot exists: restore state, load events from snapshot version
3. If no snapshot: load all events from version 0
4. Replay remaining events to reach current state

```python
# Without snapshot: loads 1000 events
task = await repo.load(task_id)  # Version 1000, replays 1000 events

# With snapshot at version 900: loads 100 events
task = await repo.load(task_id)  # Version 1000, restores from snapshot, replays 100 events
```

### Manual Snapshot Creation

```python
# Load aggregate
task = await repo.load(task_id)

# Create snapshot manually
snapshot = await repo.create_snapshot(task)

print(f"Created snapshot at version {snapshot.version}")
```

Snapshots are covered in detail in Tutorial 8.

---

## Key Takeaways

1. **Repositories abstract persistence complexity**: Load and save aggregates with simple methods
2. **Always use async/await**: All repository methods are async (`load`, `save`, `exists`)
3. **Load-modify-save is the standard pattern**: Load fresh state, execute commands, save changes
4. **Optimistic locking prevents conflicts**: Repository automatically detects concurrent modifications
5. **save() is idempotent**: Calling save() with no uncommitted events is a no-op
6. **Event publishing is optional**: Configure `event_publisher` to broadcast events after save
7. **Use utility methods**: `exists()`, `get_version()`, `load_or_create()` for common patterns
8. **Type safety through generics**: `AggregateRepository[TaskAggregate]` provides full type hints
9. **Snapshots boost performance**: Configure snapshot support for aggregates with many events

---

## Common Patterns

### Check Before Create

```python
if not await repo.exists(task_id):
    task = repo.create_new(task_id)
    task.create("New Task", "Description")
    await repo.save(task)
```

### Conditional Modification

```python
task = await repo.load(task_id)

if task.state.status == "pending":
    task.assign(user_id)
    await repo.save(task)
```

### Retry on Conflict

```python
for attempt in range(3):
    try:
        task = await repo.load(task_id)
        task.complete(user_id)
        await repo.save(task)
        break
    except OptimisticLockError:
        if attempt == 2:
            raise
        await asyncio.sleep(0.1)
```

### Batch Command Execution

```python
# Multiple commands before save
task = await repo.load(task_id)
task.assign(user_id)
task.add_comment("Starting work")
task.set_priority("high")
await repo.save(task)  # Saves all 3 events atomically
```

---

## Common Pitfalls

### 1. Not Loading Before Modify

```python
# BAD: Using stale state
task = await repo.load(task_id)
# ... time passes, other processes modify task ...
task.complete(user_id)  # State is stale!
await repo.save(task)   # Might conflict or violate business rules

# GOOD: Always load fresh
task = await repo.load(task_id)
task.complete(user_id)
await repo.save(task)
```

### 2. Forgetting to Save

```python
# BAD: Commands without save
task = await repo.load(task_id)
task.complete(user_id)  # Event created but not persisted!
# Events lost!

# GOOD: Always save after commands
task = await repo.load(task_id)
task.complete(user_id)
await repo.save(task)  # Persisted to event store
```

### 3. Catching Wrong Exception

```python
# BAD: Catching generic Exception
try:
    task = await repo.load(task_id)
except Exception:  # Too broad!
    pass

# GOOD: Catch specific exception
from eventsource import AggregateNotFoundError

try:
    task = await repo.load(task_id)
except AggregateNotFoundError:
    # Handle case where aggregate doesn't exist
    task = repo.create_new(task_id)
```

### 4. Reusing Aggregate Instances

```python
# BAD: Reusing aggregate after save
task = repo.create_new(task_id)
task.create("Task 1", "First")
await repo.save(task)

task.create("Task 2", "Second")  # Will fail: already exists!
await repo.save(task)

# GOOD: Create new instance or reload
task1 = repo.create_new(uuid4())
task1.create("Task 1", "First")
await repo.save(task1)

task2 = repo.create_new(uuid4())
task2.create("Task 2", "Second")
await repo.save(task2)
```

---

## Phase 1 Complete!

You've completed **Phase 1: Foundations**! You now understand:

- **Events**: Immutable facts about what happened
- **Aggregates**: Business logic and consistency boundaries
- **Event Stores**: Persisting events with optimistic locking
- **Repositories**: Loading and saving aggregates

These are the building blocks of event sourcing. Everything else builds on these foundations.

---

## Next Steps

Continue to [Tutorial 6: Projections - Building Read Models](06-projections.md) to learn about:

- Creating read models optimized for queries
- Subscribing to events with event handlers
- Using SubscriptionManager for projection coordination
- Building multiple views from the same events
- Checkpoint management for resumable processing

For working examples, see:
- `examples/basic_usage.py` - Complete repository usage
- `examples/aggregate_example.py` - Advanced patterns with shopping cart
- `tests/unit/test_aggregate_repository.py` - Comprehensive test examples
