# Tutorial 5: Repositories Pattern

**Estimated Time:** 30-45 minutes
**Difficulty:** Beginner
**Progress:** Tutorial 5 of 21 | Phase 1: Foundations (Final Tutorial)

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- Completed [Tutorial 2: Your First Domain Event](02-first-event.md)
- Completed [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- Completed [Tutorial 4: Event Stores Overview](04-event-stores.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial brings together all the concepts from Phase 1. We will use the `TaskAggregate` and events from previous tutorials along with the `InMemoryEventStore` to demonstrate the complete workflow.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain why repositories abstract event store operations for aggregates
2. Create and configure an `AggregateRepository`
3. Create new aggregates using `create_new()` and persist them with `save()`
4. Load existing aggregates using `load()` and handle `AggregateNotFoundError`
5. Implement the complete load-modify-save workflow with proper error handling

---

## What is the Repository Pattern?

In Tutorial 3, you learned how aggregates emit events and track them in `uncommitted_events`. In Tutorial 4, you learned how to persist events directly to an event store using `append_events()`. But manually coordinating between aggregates and event stores is tedious and error-prone.

This is where the **Repository Pattern** comes in.

### The Bridge Between Aggregates and Storage

A **repository** is an abstraction that provides a clean interface for working with aggregates. It hides the complexity of:

- Loading events from the event store
- Replaying events to reconstruct aggregate state
- Persisting uncommitted events with correct version handling
- Managing optimistic locking

Think of a repository as a "smart collection" of aggregates. You ask it for an aggregate by ID, and it handles all the event sourcing mechanics behind the scenes.

```
Without Repository:                   With Repository:
--------------------                  ----------------
1. Get events from store              1. repo.load(id)
2. Create aggregate instance          2. aggregate.do_something()
3. Replay events on aggregate         3. await repo.save(aggregate)
4. Call command on aggregate
5. Get uncommitted events
6. Calculate expected version
7. Append to store
8. Mark events committed
```

### Why Not Use Event Store Directly?

You could manually coordinate aggregates and event stores, but there are several reasons to use a repository instead:

| Manual Approach | Repository Approach |
|----------------|---------------------|
| Multiple steps to load an aggregate | Single `load()` call |
| Must calculate expected version | Handles version automatically |
| Easy to forget clearing uncommitted events | Clears events after save |
| Version mismatch bugs are common | Consistent versioning |
| Repeated boilerplate code | DRY - logic in one place |
| No standard pattern for team | Consistent API for everyone |

The repository encapsulates best practices and ensures correctness, letting you focus on business logic.

---

## The AggregateRepository Class

eventsource-py provides `AggregateRepository` - a generic repository that works with any aggregate type.

### Creating a Repository

```python
from eventsource import (
    AggregateRepository,
    InMemoryEventStore,
)

# Create the event store
store = InMemoryEventStore()

# Create a repository for TaskAggregate
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
)
```

### Constructor Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `event_store` | `EventStore` | Yes | The event store for persistence |
| `aggregate_factory` | `type[TAggregate]` | Yes | The aggregate class to instantiate |
| `aggregate_type` | `str` | Yes | Type name matching your aggregate (e.g., "Task") |
| `event_publisher` | `EventPublisher` | No | Optional publisher for event broadcasting |
| `snapshot_store` | `SnapshotStore` | No | Optional snapshot support (Tutorial 14) |
| `snapshot_threshold` | `int` | No | Events between automatic snapshots |
| `snapshot_mode` | `str` | No | "sync", "background", or "manual" |

For now, focus on the three required parameters. We will cover event publishers in Tutorial 7 and snapshots in Tutorial 14.

### Key Methods

| Method | Description |
|--------|-------------|
| `create_new(aggregate_id)` | Create a new, empty aggregate instance |
| `load(aggregate_id)` | Load aggregate from event history |
| `load_or_create(aggregate_id)` | Load if exists, otherwise create new |
| `save(aggregate)` | Persist uncommitted events |
| `exists(aggregate_id)` | Check if aggregate has any events |
| `get_version(aggregate_id)` | Get current version of aggregate |

---

## Creating New Aggregates

When creating a new aggregate, use `create_new()` to get a fresh instance, then apply your commands and save.

### The create_new() Method

```python
from uuid import uuid4

# Create a new aggregate instance
task_id = uuid4()
task = repo.create_new(task_id)

print(f"Version: {task.version}")  # 0
print(f"State: {task.state}")      # None (initial state)
```

The `create_new()` method:

1. Creates a new aggregate instance using the factory (your aggregate class)
2. Passes the provided ID to the aggregate constructor
3. Returns an aggregate at version 0 with no events

**Important**: `create_new()` does not persist anything. It just creates an in-memory instance. You must call `save()` to persist events.

### Complete Create Workflow

```python
import asyncio
from uuid import uuid4

from eventsource import (
    AggregateRepository,
    InMemoryEventStore,
)

# Assuming TaskAggregate from Tutorial 3
async def create_task_example():
    # Set up infrastructure
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Step 1: Create new aggregate instance
    task_id = uuid4()
    user_id = uuid4()
    task = repo.create_new(task_id)

    # Step 2: Execute command (this emits events)
    task.create(
        title="Learn Repositories",
        description="Complete Tutorial 5",
        assigned_to=user_id,
    )

    print(f"Before save:")
    print(f"  Version: {task.version}")
    print(f"  Uncommitted events: {len(task.uncommitted_events)}")

    # Step 3: Persist events to event store
    await repo.save(task)

    print(f"After save:")
    print(f"  Version: {task.version}")
    print(f"  Uncommitted events: {len(task.uncommitted_events)}")

    return task_id
```

Output:

```
Before save:
  Version: 1
  Uncommitted events: 1
After save:
  Version: 1
  Uncommitted events: 0
```

Notice that after `save()`:
- The version remains the same (1)
- The uncommitted events are cleared (0)

The repository automatically calls `mark_events_as_committed()` on the aggregate after successfully appending events to the store.

---

## Loading Existing Aggregates

To work with an existing aggregate, use `load()` to retrieve it from the event store.

### The load() Method

```python
async def load_task_example(task_id):
    # Load the aggregate
    task = await repo.load(task_id)

    print(f"Loaded aggregate:")
    print(f"  ID: {task.aggregate_id}")
    print(f"  Version: {task.version}")
    print(f"  Title: {task.state.title}")
    print(f"  Status: {task.state.status}")
```

The `load()` method:

1. Retrieves all events for the aggregate from the event store
2. Creates a new aggregate instance using the factory
3. Replays events using `load_from_history()`
4. Returns the fully reconstituted aggregate

If no events exist for the given ID, `load()` raises `AggregateNotFoundError`.

### Handling AggregateNotFoundError

```python
from eventsource import AggregateNotFoundError

async def load_safely(repo, aggregate_id):
    try:
        task = await repo.load(aggregate_id)
        print(f"Found task: {task.state.title}")
        return task
    except AggregateNotFoundError as e:
        print(f"Task not found!")
        print(f"  Aggregate ID: {e.aggregate_id}")
        print(f"  Aggregate Type: {e.aggregate_type}")
        return None
```

The exception provides:
- `aggregate_id`: The UUID that was not found
- `aggregate_type`: The type that was being loaded (e.g., "Task")

### Using exists() Before Load

If you want to check existence without catching exceptions:

```python
async def load_if_exists(repo, aggregate_id):
    if await repo.exists(aggregate_id):
        return await repo.load(aggregate_id)
    else:
        print(f"Aggregate {aggregate_id} does not exist")
        return None
```

### Using load_or_create()

For cases where you want an aggregate regardless of whether it exists:

```python
async def get_or_create_task(repo, task_id):
    task = await repo.load_or_create(task_id)

    if task.version == 0:
        # New aggregate - needs to be created
        print("Creating new task...")
        task.create("New Task", "Created on demand")
        await repo.save(task)
    else:
        # Existing aggregate
        print(f"Found existing task at version {task.version}")

    return task
```

---

## The Complete Load-Modify-Save Workflow

The most common pattern in event sourcing is the load-modify-save cycle:

1. **Load** the aggregate from the event store
2. **Modify** by calling command methods
3. **Save** to persist the new events

### Basic Workflow

```python
async def complete_task(repo, task_id, user_id):
    # Load
    task = await repo.load(task_id)
    print(f"Loaded task at version {task.version}")

    # Modify
    task.complete(completed_by=user_id)
    print(f"Applied command, version now {task.version}")

    # Save
    await repo.save(task)
    print(f"Saved, uncommitted events cleared")
```

### Workflow with Multiple Commands

You can apply multiple commands before saving:

```python
async def reassign_and_complete(repo, task_id, new_assignee, completer):
    # Load
    task = await repo.load(task_id)

    # Apply multiple commands
    task.reassign(new_assignee=new_assignee)
    task.complete(completed_by=completer)

    # Save all events at once
    await repo.save(task)

    print(f"Applied 2 commands, saved {len(task.uncommitted_events)} events")
    # Note: uncommitted_events is already 0 after save
```

All events are appended atomically in a single call to the event store. This ensures consistency.

---

## Handling Concurrent Modifications

When two processes try to modify the same aggregate simultaneously, one will succeed and the other will get an `OptimisticLockError`.

### Understanding the Problem

```
Process A:                      Process B:
-----------                     -----------
1. load(task_id) -> v1          1. load(task_id) -> v1
2. task.reassign(...)           2. task.complete(...)
3. save() -> success (v2)       3. save() -> ERROR!
                                   (expected v1, but actual v2)
```

Both processes loaded the aggregate at version 1. Process A saved first, incrementing to version 2. When Process B tries to save expecting version 1, the event store rejects it.

### Handling OptimisticLockError

```python
from eventsource import OptimisticLockError

async def complete_task_safely(repo, task_id, user_id, max_retries=3):
    """Complete a task with retry on conflict."""
    for attempt in range(max_retries):
        try:
            # Load current state
            task = await repo.load(task_id)

            # Validate business rules
            if task.state.status == "completed":
                print("Task already completed")
                return task

            # Apply command
            task.complete(completed_by=user_id)

            # Save
            await repo.save(task)
            print(f"Completed on attempt {attempt + 1}")
            return task

        except OptimisticLockError as e:
            print(f"Conflict on attempt {attempt + 1}")
            print(f"  Expected version: {e.expected_version}")
            print(f"  Actual version: {e.actual_version}")

            if attempt < max_retries - 1:
                print("  Retrying...")
                continue
            else:
                raise

    raise RuntimeError("Max retries exceeded")
```

The retry pattern:

1. **Load** the latest state
2. **Validate** that the operation is still valid
3. **Execute** the command
4. **Save** with optimistic locking
5. On conflict, **reload** and **retry**

### Demonstrating Concurrent Modification

```python
async def concurrent_modification_demo():
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Create a task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Shared Task", "Will be modified concurrently")
    await repo.save(task)
    print(f"Created task at version {task.version}")

    # Simulate two users loading the same task
    user1_task = await repo.load(task_id)
    user2_task = await repo.load(task_id)
    print(f"User 1 loaded version: {user1_task.version}")
    print(f"User 2 loaded version: {user2_task.version}")

    # User 1 makes a change and saves
    user1_task.reassign(uuid4())
    await repo.save(user1_task)
    print(f"User 1 saved, new version: {user1_task.version}")

    # User 2 tries to save - will fail!
    user2_task.reassign(uuid4())
    try:
        await repo.save(user2_task)
    except OptimisticLockError as e:
        print(f"\nUser 2 got OptimisticLockError!")
        print(f"  Expected: {e.expected_version}")
        print(f"  Actual: {e.actual_version}")

        # Recovery: reload and retry
        print("\nRecovering: reload and retry...")
        user2_task = await repo.load(task_id)
        user2_task.reassign(uuid4())
        await repo.save(user2_task)
        print(f"User 2 successfully saved, version: {user2_task.version}")
```

---

## Optional: Event Publishing

The repository can optionally publish events after saving. This is useful for notifying other parts of your system about changes.

### Setting Up Event Publishing

```python
from eventsource import InMemoryEventBus

# Create an event bus
event_bus = InMemoryEventBus()

# Register a handler
@event_bus.subscribe(TaskCompleted)
async def on_task_completed(event: TaskCompleted):
    print(f"Task {event.aggregate_id} was completed!")

# Create repository with publisher
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
    event_publisher=event_bus,  # Events published after save
)
```

When you save an aggregate, events are:

1. Appended to the event store
2. Marked as committed on the aggregate
3. Published to the event publisher (if configured)

We will cover event buses in detail in Tutorial 7.

---

## Complete Example

Here is a complete example demonstrating all repository operations:

```python
"""
Tutorial 5: Repositories Pattern

This example demonstrates the recommended way to work with aggregates.
Run with: python tutorial_05_repositories.py
"""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateNotFoundError,
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    OptimisticLockError,
    register_event,
)


# =============================================================================
# Events (from Tutorial 2)
# =============================================================================

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str
    assigned_to: UUID | None = None


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"
    completed_by: UUID


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    previous_assignee: UUID | None
    new_assignee: UUID


# =============================================================================
# State and Aggregate (from Tutorial 3)
# =============================================================================

class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"


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
                assigned_to=event.assigned_to,
                status="pending",
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})
        elif isinstance(event, TaskReassigned):
            if self._state:
                self._state = self._state.model_copy(update={
                    "assigned_to": event.new_assignee,
                })

    def create(self, title: str, description: str, assigned_to: UUID | None = None) -> None:
        if self.version > 0:
            raise ValueError("Task already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            assigned_to=assigned_to,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self, completed_by: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        ))

    def reassign(self, new_assignee: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot reassign completed task")
        self.apply_event(TaskReassigned(
            aggregate_id=self.aggregate_id,
            previous_assignee=self.state.assigned_to,
            new_assignee=new_assignee,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Repository Examples
# =============================================================================

async def basic_workflow():
    """Demonstrate the basic repository workflow."""
    print("=" * 60)
    print("Basic Repository Workflow")
    print("=" * 60)
    print()

    # Set up infrastructure
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Create a new task
    task_id = uuid4()
    user_id = uuid4()

    print("1. Creating new task...")
    task = repo.create_new(task_id)
    print(f"   Created instance: version={task.version}, state={task.state}")

    # Execute command
    task.create(
        title="Learn Repositories",
        description="Complete Tutorial 5",
        assigned_to=user_id,
    )
    print(f"   After create command: version={task.version}")
    print(f"   Uncommitted events: {len(task.uncommitted_events)}")

    # Save to persist events
    print("\n2. Saving task...")
    await repo.save(task)
    print(f"   After save: uncommitted={len(task.uncommitted_events)}")

    # Load the task back
    print("\n3. Loading task...")
    loaded = await repo.load(task_id)
    print(f"   Loaded version: {loaded.version}")
    print(f"   Title: {loaded.state.title}")
    print(f"   Status: {loaded.state.status}")

    # Modify and save again
    print("\n4. Completing task...")
    loaded.complete(completed_by=user_id)
    await repo.save(loaded)
    print(f"   Final version: {loaded.version}")
    print(f"   Final status: {loaded.state.status}")

    return task_id, repo


async def error_handling_demo():
    """Demonstrate error handling with repositories."""
    print()
    print("=" * 60)
    print("Error Handling Demo")
    print("=" * 60)
    print()

    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Try to load non-existent aggregate
    print("1. Loading non-existent aggregate...")
    try:
        await repo.load(uuid4())
    except AggregateNotFoundError as e:
        print(f"   Caught AggregateNotFoundError!")
        print(f"   Aggregate ID: {e.aggregate_id}")
        print(f"   Aggregate Type: {e.aggregate_type}")

    # Demonstrate check before load
    print("\n2. Using exists() before load...")
    random_id = uuid4()
    if await repo.exists(random_id):
        task = await repo.load(random_id)
    else:
        print(f"   Task {random_id} does not exist - creating new")
        task = repo.create_new(random_id)
        task.create("New Task", "Created because it did not exist")
        await repo.save(task)
        print(f"   Created and saved!")

    # Demonstrate load_or_create
    print("\n3. Using load_or_create()...")
    another_id = uuid4()
    task = await repo.load_or_create(another_id)
    print(f"   Got aggregate at version {task.version}")
    if task.version == 0:
        print("   Version 0 means new - need to create it")


async def concurrent_modification_demo():
    """Demonstrate concurrent modification handling."""
    print()
    print("=" * 60)
    print("Concurrent Modification Demo")
    print("=" * 60)
    print()

    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Create a task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Shared Task", "Will be modified concurrently")
    await repo.save(task)
    print(f"Created task at version {task.version}")

    # Simulate two "users" loading the same task
    user1_task = await repo.load(task_id)
    user2_task = await repo.load(task_id)
    print(f"User 1 loaded version: {user1_task.version}")
    print(f"User 2 loaded version: {user2_task.version}")

    # User 1 makes a change and saves
    user1_task.reassign(uuid4())
    await repo.save(user1_task)
    print(f"User 1 saved, new version: {user1_task.version}")

    # User 2 tries to save their change - will fail!
    user2_task.reassign(uuid4())
    try:
        await repo.save(user2_task)
    except OptimisticLockError as e:
        print(f"\nUser 2 got OptimisticLockError!")
        print(f"  Expected: {e.expected_version}, Actual: {e.actual_version}")

        # Recovery: reload and retry
        print("\nRecovering: reload and retry...")
        user2_task = await repo.load(task_id)
        user2_task.reassign(uuid4())
        await repo.save(user2_task)
        print(f"User 2 successfully saved, version: {user2_task.version}")


async def main():
    """Run all examples."""
    await basic_workflow()
    await error_handling_demo()
    await concurrent_modification_demo()

    print()
    print("=" * 60)
    print("Tutorial 5 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_05_repositories.py` and run it:

```bash
python tutorial_05_repositories.py
```

---

## Exercises

Now it is time to practice what you have learned!

### Exercise 1: Complete Task Management Workflow

**Objective:** Build a complete task management workflow using all Phase 1 components.

**Time:** 15-20 minutes

**Requirements:**

1. Create a task repository
2. Create a new task with a title and description
3. Save the task to the event store
4. Load the task back by ID
5. Reassign and complete the task
6. Save the changes
7. Verify the final state by loading again
8. Print the events stored in the event store

**Starter Code:**

```python
"""
Tutorial 5 - Exercise 1: Task Management Workflow

Your task: Implement a complete create-load-modify-save workflow.
"""
import asyncio
from uuid import uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)


# Events (copy from tutorial or import)
@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str
    assigned_to: str | None = None


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    new_assignee: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


# State
class TaskState(BaseModel):
    task_id: str
    title: str = ""
    description: str = ""
    assigned_to: str | None = None
    status: str = "pending"


# Aggregate
class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=str(self.aggregate_id),
                title=event.title,
                description=event.description,
                assigned_to=event.assigned_to,
                status="pending",
            )
        elif isinstance(event, TaskReassigned):
            if self._state:
                self._state = self._state.model_copy(update={
                    "assigned_to": event.new_assignee,
                })
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, title: str, description: str, assigned_to: str | None = None) -> None:
        if self.version > 0:
            raise ValueError("Already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            assigned_to=assigned_to,
            aggregate_version=self.get_next_version(),
        ))

    def reassign(self, new_assignee: str) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(TaskReassigned(
            aggregate_id=self.aggregate_id,
            new_assignee=new_assignee,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


async def main():
    print("=== Task Management Workflow ===\n")

    # Step 1: Create repository
    # TODO: Create InMemoryEventStore and AggregateRepository

    # Step 2: Create a new task
    # TODO: Use create_new() and call task.create()

    # Step 3: Save the task
    # TODO: Call repo.save()

    # Step 4: Load the task by ID
    # TODO: Call repo.load()

    # Step 5: Reassign and complete
    # TODO: Call reassign() and complete() on loaded task

    # Step 6: Save changes
    # TODO: Call repo.save()

    # Step 7: Verify final state
    # TODO: Load again and print final state

    # Step 8: Print events
    # TODO: Get events from store and print them


if __name__ == "__main__":
    asyncio.run(main())
```

**Hint:** Use `await store.get_events(task_id, "Task")` to retrieve the stored events.

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 5 - Exercise 1 Solution: Task Management Workflow
"""
import asyncio
from uuid import uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str
    assigned_to: str | None = None


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    new_assignee: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


class TaskState(BaseModel):
    task_id: str
    title: str = ""
    description: str = ""
    assigned_to: str | None = None
    status: str = "pending"


class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=str(self.aggregate_id),
                title=event.title,
                description=event.description,
                assigned_to=event.assigned_to,
                status="pending",
            )
        elif isinstance(event, TaskReassigned):
            if self._state:
                self._state = self._state.model_copy(update={
                    "assigned_to": event.new_assignee,
                })
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, title: str, description: str, assigned_to: str | None = None) -> None:
        if self.version > 0:
            raise ValueError("Already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            assigned_to=assigned_to,
            aggregate_version=self.get_next_version(),
        ))

    def reassign(self, new_assignee: str) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(TaskReassigned(
            aggregate_id=self.aggregate_id,
            new_assignee=new_assignee,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


async def main():
    print("=== Task Management Workflow ===\n")

    # Step 1: Create repository
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )
    print("1. Created repository")

    # Step 2: Create a new task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create(
        title="Complete Tutorial Series",
        description="Finish all 21 tutorials",
        assigned_to="Alice",
    )
    print(f"2. Created task: {task.state.title}")

    # Step 3: Save the task
    await repo.save(task)
    print(f"3. Saved task (version {task.version})")

    # Step 4: Load the task by ID
    loaded = await repo.load(task_id)
    print(f"4. Loaded task: {loaded.state.title} (v{loaded.version})")

    # Step 5: Reassign and complete
    loaded.reassign("Bob")
    print(f"5a. Reassigned to: {loaded.state.assigned_to}")

    loaded.complete()
    print(f"5b. Completed task")

    # Step 6: Save changes
    await repo.save(loaded)
    print(f"6. Saved changes (version {loaded.version})")

    # Step 7: Verify final state
    final = await repo.load(task_id)
    print(f"\n=== Final State ===")
    print(f"Title: {final.state.title}")
    print(f"Assigned to: {final.state.assigned_to}")
    print(f"Status: {final.state.status}")
    print(f"Version: {final.version}")

    # Step 8: Print events
    stream = await store.get_events(task_id, "Task")
    print(f"\n=== Events Stored ({len(stream.events)}) ===")
    for i, event in enumerate(stream.events):
        print(f"  {i+1}. {event.event_type} (v{event.aggregate_version})")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

The solution file is also available at: `docs/tutorials/exercises/solutions/05-1.py`

---

## Summary

In this tutorial, you learned:

- **Repositories** provide a clean interface over event stores for aggregate persistence
- **create_new()** creates in-memory aggregate instances ready for commands
- **load()** reconstructs aggregates by retrieving and replaying events
- **save()** persists uncommitted events atomically with optimistic locking
- **AggregateNotFoundError** is raised when loading non-existent aggregates
- **OptimisticLockError** signals concurrent modification conflicts
- The **load-modify-save** workflow is the standard pattern for working with aggregates

---

## Phase 1 Complete!

Congratulations! You have completed **Phase 1: Foundations** of the eventsource-py tutorial series. You now have a solid understanding of the core concepts that power event sourcing:

| Tutorial | Concept | Key Takeaway |
|----------|---------|--------------|
| Tutorial 1 | Introduction | Event sourcing stores facts, not just current state |
| Tutorial 2 | Events | Immutable records of what happened in the domain |
| Tutorial 3 | Aggregates | Consistency boundaries with business logic |
| Tutorial 4 | Event Stores | Append-only persistence with optimistic locking |
| Tutorial 5 | Repositories | Clean abstraction for aggregate lifecycle |

### What You Can Build Now

With these foundations, you can:

- Define domain events that capture business facts
- Build aggregates that enforce business rules
- Persist events with full audit trail
- Load and modify aggregates safely
- Handle concurrent modifications

### Phase 2 Preview

In Phase 2: Core Patterns, you will learn:

- **Projections**: Building optimized read models from events
- **Event Bus**: Publishing events for decoupled communication
- **Testing**: Patterns for testing event-sourced systems

---

## Key Takeaways

!!! note "Remember"
    - Repositories are the recommended way to work with aggregates
    - Always use `create_new()` for new aggregates, `load()` for existing ones
    - The repository handles version tracking and event replay automatically
    - Events are cleared from the aggregate after successful save

!!! tip "Best Practice"
    When handling concurrent modifications, always reload the aggregate before retrying. The retry should validate business rules against the new state - what was valid before may not be valid after another user's changes.

!!! warning "Common Mistake"
    Do not forget to call `save()` after modifying an aggregate. Events remain in `uncommitted_events` until explicitly saved, and will be lost if your program terminates.

---

## Next Steps

You have mastered the foundations! Now it is time to learn how to build **read models** from your events.

Continue to [Tutorial 6: Building Read Models with Projections](06-projections.md) to learn how to create optimized query views from your event streams.

---

## Related Documentation

- [API Reference: Aggregates](../api/aggregates.md) - Complete aggregate and repository API
- [Error Handling Guide](../guides/error-handling.md) - Comprehensive error handling strategies
- [Tutorial 7: Event Bus Basics](07-event-bus.md) - Event publishing after saves
- [Tutorial 14: Snapshotting](14-snapshotting.md) - Performance optimization for repositories
