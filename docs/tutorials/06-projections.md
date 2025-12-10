# Tutorial 6: Building Read Models with Projections

**Estimated Time:** 45-60 minutes
**Difficulty:** Intermediate
**Progress:** Tutorial 6 of 21 | Phase 2: Core Patterns

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 5: Repositories Pattern](05-repositories.md)
- Understanding of events and aggregates from Phase 1
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial introduces **projections**, a core pattern in event sourcing that enables building optimized read models from event streams. We will use the events and aggregates from previous tutorials.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain why projections are needed in event sourcing (CQRS pattern)
2. Create a projection using the `DeclarativeProjection` base class
3. Use the `@handles` decorator for declarative event routing
4. Implement query methods on projections for efficient data access
5. Reset and rebuild projections from scratch

---

## What are Projections?

In the previous tutorials, you learned how aggregates maintain state by replaying events. But what happens when you need to query data across multiple aggregates? Or when you need to display data in a format that does not match your aggregate structure?

### The Query Problem in Event Sourcing

Consider these common scenarios:

- **List all tasks**: You want to show all tasks in a dashboard, but tasks are stored as separate event streams
- **Filter by status**: You need all "pending" tasks, but status is computed by replaying events
- **Search by assignee**: Find all tasks for a specific user across the entire system
- **Generate reports**: Calculate statistics like "tasks completed this week"

Loading every aggregate and replaying all their events just to answer a query would be extremely slow. This is where **projections** come in.

### The CQRS Pattern

**CQRS** (Command Query Responsibility Segregation) separates the write model (aggregates) from the read model (projections):

```
Write Side (Commands)             Read Side (Queries)
=====================             ===================
   TaskAggregate                    TaskListProjection
        |                                  |
   Domain Events  ----dispatch--->    Read Model
        |                                  |
   Event Store                    Optimized for queries
```

- **Write Model (Aggregates)**: Enforce business rules, emit events, optimized for consistency
- **Read Model (Projections)**: Listen to events, build query-optimized views, optimized for reads

### How Projections Work

A projection:

1. **Subscribes** to specific event types
2. **Handles** each event by updating its internal read model
3. **Provides** query methods for efficient data access
4. **Can be rebuilt** from scratch by replaying all events

```
Event Stream          Projection             Read Model
============         ===========            ===========
TaskCreated    --->   handle()   --->   tasks["abc"] = {...}
TaskCompleted  --->   handle()   --->   tasks["abc"]["status"] = "completed"
TaskReassigned --->   handle()   --->   tasks["abc"]["assignee"] = user2
```

### Eventually Consistent Reads

An important concept: projections are **eventually consistent** with the write model. When an aggregate emits an event, the projection will process it shortly after - not instantaneously. In most systems, this delay is milliseconds, but it is not zero.

This is a trade-off: you accept slight delays in read model updates in exchange for:

- Fast, optimized queries
- Flexible data structures for different use cases
- Scalability (read and write models can scale independently)

---

## The Projection Base Classes

eventsource-py provides several projection base classes with increasing functionality:

| Class | Description | Use When |
|-------|-------------|----------|
| `Projection` | Abstract base with `handle()` and `reset()` | Simple projections without tracking |
| `DeclarativeProjection` | Adds `@handles` decorator support | Most common choice |
| `CheckpointTrackingProjection` | Adds checkpoint, retry, and DLQ | Production projections |
| `DatabaseProjection` | Adds database connection for handlers | PostgreSQL read models |

For this tutorial, we will focus on `DeclarativeProjection` - the most commonly used base class that provides a clean, declarative way to define event handlers.

---

## Creating Your First Projection

Let us build a `TaskListProjection` that maintains a queryable list of tasks.

### Step 1: Import Dependencies

```python
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    register_event,
    DeclarativeProjection,
    handles,
)
```

Note: We import `DeclarativeProjection` and `handles` from the top-level `eventsource` package. This is the canonical import path.

### Step 2: Define Events

We will use the events from Tutorial 2:

```python
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
```

### Step 3: Create the Projection Class

```python
class TaskListProjection(DeclarativeProjection):
    """
    Projection that maintains a queryable list of tasks.

    This read model is optimized for:
    - Listing all tasks
    - Filtering by status
    - Filtering by assignee
    """

    def __init__(self):
        super().__init__()
        # In-memory read model (dictionary)
        self.tasks: dict[UUID, dict] = {}
```

The `__init__` method calls `super().__init__()` to initialize the base class, then creates an in-memory dictionary to store our read model.

### Step 4: Add Event Handlers

Use the `@handles` decorator to register handlers for each event type:

```python
class TaskListProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks: dict[UUID, dict] = {}

    @handles(TaskCreated)
    async def _on_task_created(self, event: TaskCreated) -> None:
        """Handle task creation - add to read model."""
        self.tasks[event.aggregate_id] = {
            "id": event.aggregate_id,
            "title": event.title,
            "description": event.description,
            "assigned_to": event.assigned_to,
            "status": "pending",
            "created_at": event.occurred_at,
        }

    @handles(TaskCompleted)
    async def _on_task_completed(self, event: TaskCompleted) -> None:
        """Handle task completion - update status."""
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["status"] = "completed"
            self.tasks[event.aggregate_id]["completed_by"] = event.completed_by
            self.tasks[event.aggregate_id]["completed_at"] = event.occurred_at

    @handles(TaskReassigned)
    async def _on_task_reassigned(self, event: TaskReassigned) -> None:
        """Handle task reassignment - update assignee."""
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["assigned_to"] = event.new_assignee
```

**Key Points:**

- Handler methods must be `async`
- The `@handles(EventType)` decorator specifies which event the method handles
- Handler methods receive the event as a parameter
- Handler names can be anything - the decorator does the routing
- Handlers should be **idempotent** (safe to run multiple times with the same event)

### Step 5: Implement the Reset Method

Projections need a way to clear their read model for rebuilding:

```python
    async def _truncate_read_models(self) -> None:
        """Clear all data when resetting the projection."""
        self.tasks.clear()
```

The `_truncate_read_models()` method is called by `reset()` to clear the read model data.

### Step 6: Add Query Methods

Now add methods to query the read model:

```python
    def get_all_tasks(self) -> list[dict]:
        """Get all tasks."""
        return list(self.tasks.values())

    def get_pending_tasks(self) -> list[dict]:
        """Get only pending (incomplete) tasks."""
        return [t for t in self.tasks.values() if t["status"] == "pending"]

    def get_completed_tasks(self) -> list[dict]:
        """Get only completed tasks."""
        return [t for t in self.tasks.values() if t["status"] == "completed"]

    def get_tasks_by_assignee(self, assignee_id: UUID) -> list[dict]:
        """Get tasks assigned to a specific user."""
        return [t for t in self.tasks.values() if t["assigned_to"] == assignee_id]

    def get_task(self, task_id: UUID) -> dict | None:
        """Get a single task by ID."""
        return self.tasks.get(task_id)

    def count_by_status(self) -> dict[str, int]:
        """Get counts by status."""
        counts = {"pending": 0, "completed": 0}
        for task in self.tasks.values():
            counts[task["status"]] += 1
        return counts
```

These query methods provide efficient access to the data without needing to replay events.

---

## How DeclarativeProjection Works

The `DeclarativeProjection` class provides several key features:

### Automatic Handler Discovery

When you create a `DeclarativeProjection` subclass, it automatically:

1. Scans all methods for the `@handles` decorator
2. Builds an internal routing map from event types to handlers
3. Auto-generates the `subscribed_to()` method

You can see which events a projection handles:

```python
projection = TaskListProjection()

# Check subscribed event types
print("Subscribed to events:")
for event_type in projection.subscribed_to():
    print(f"  - {event_type.__name__}")
```

Output:
```
Subscribed to events:
  - TaskCreated
  - TaskCompleted
  - TaskReassigned
```

### Event Routing

When you call `handle(event)`, the projection:

1. Looks up the handler for the event type
2. Calls the appropriate handler method
3. Updates checkpoint tracking (in production configurations)
4. Handles errors with retry logic (in production configurations)

```python
# Events are automatically routed to the correct handler
await projection.handle(task_created_event)   # -> _on_task_created
await projection.handle(task_completed_event) # -> _on_task_completed
await projection.handle(task_reassigned_event) # -> _on_task_reassigned
```

### Unhandled Events

By default, `DeclarativeProjection` ignores events that have no handler. This is useful for:

- Forward compatibility (new event types added later)
- Selective projections (only caring about specific events)

You can change this behavior:

```python
class StrictTaskProjection(DeclarativeProjection):
    # Options: "ignore" (default), "warn", "error"
    unregistered_event_handling = "warn"  # Log warning for unhandled events
```

---

## Using the Projection

Let us put it all together and see the projection in action:

```python
import asyncio
from uuid import uuid4

async def main():
    # Create the projection
    projection = TaskListProjection()

    # Create some users
    user1 = uuid4()
    user2 = uuid4()

    # Create task IDs
    task1_id = uuid4()
    task2_id = uuid4()
    task3_id = uuid4()

    # Simulate events (normally these come from the event store)
    events = [
        TaskCreated(
            aggregate_id=task1_id,
            title="Implement projections",
            description="Complete Tutorial 6",
            assigned_to=user1,
            aggregate_version=1,
        ),
        TaskCreated(
            aggregate_id=task2_id,
            title="Write tests",
            description="Add unit tests for projections",
            assigned_to=user1,
            aggregate_version=1,
        ),
        TaskCreated(
            aggregate_id=task3_id,
            title="Review code",
            description="Review PR for projections",
            assigned_to=user2,
            aggregate_version=1,
        ),
    ]

    # Process events through the projection
    print("Processing events...")
    for event in events:
        await projection.handle(event)
    print(f"Processed {len(events)} events\n")

    # Query the read model
    print("=== Query Results ===\n")

    print(f"All tasks: {len(projection.get_all_tasks())}")
    print(f"Pending: {len(projection.get_pending_tasks())}")
    print(f"Completed: {len(projection.get_completed_tasks())}")
    print()

    print(f"Tasks for user1: {len(projection.get_tasks_by_assignee(user1))}")
    print(f"Tasks for user2: {len(projection.get_tasks_by_assignee(user2))}")
    print()

    # Complete a task
    complete_event = TaskCompleted(
        aggregate_id=task1_id,
        completed_by=user1,
        aggregate_version=2,
    )
    await projection.handle(complete_event)

    print("After completing task 1:")
    print(f"  Pending: {len(projection.get_pending_tasks())}")
    print(f"  Completed: {len(projection.get_completed_tasks())}")
    print()

    # Reassign a task
    reassign_event = TaskReassigned(
        aggregate_id=task2_id,
        previous_assignee=user1,
        new_assignee=user2,
        aggregate_version=2,
    )
    await projection.handle(reassign_event)

    print("After reassigning task 2:")
    print(f"  User1 tasks: {len(projection.get_tasks_by_assignee(user1))}")
    print(f"  User2 tasks: {len(projection.get_tasks_by_assignee(user2))}")
    print()

    # Show status counts
    print("Final counts:", projection.count_by_status())


if __name__ == "__main__":
    asyncio.run(main())
```

Output:
```
Processing events...
Processed 3 events

=== Query Results ===

All tasks: 3
Pending: 3
Completed: 0

Tasks for user1: 2
Tasks for user2: 1

After completing task 1:
  Pending: 2
  Completed: 1

After reassigning task 2:
  User1 tasks: 0
  User2 tasks: 2

Final counts: {'pending': 2, 'completed': 1}
```

---

## Resetting and Rebuilding Projections

One of the powerful features of projections is the ability to rebuild them from scratch. This is useful when:

- You fix a bug in your projection logic
- You add new fields to your read model
- You need to recover from data corruption

### The Reset Method

```python
# Before reset
print(f"Before reset: {len(projection.get_all_tasks())} tasks")

# Reset clears all data
await projection.reset()

print(f"After reset: {len(projection.get_all_tasks())} tasks")
```

Output:
```
Before reset: 3 tasks
After reset: 0 tasks
```

### Rebuilding from Events

To rebuild, reset the projection and replay all events:

```python
async def rebuild_projection(projection, event_store, aggregate_type):
    """Rebuild a projection from all events in the store."""
    # Clear existing data
    await projection.reset()

    # Get all events from the store
    stream = await event_store.read_all()

    # Replay events through the projection
    for stored_event in stream.events:
        event = stored_event.event
        # Only process events the projection cares about
        if type(event) in projection.subscribed_to():
            await projection.handle(event)

    print(f"Rebuilt projection with {len(stream.events)} events")
```

This is a key advantage of event sourcing: you can always recreate your read models from the source of truth (the events).

---

## Comparing Projection Approaches

### The Simple Approach (isinstance checks)

You can create projections using the base `Projection` class with manual type checking:

```python
from eventsource import Projection

class SimpleTaskProjection(Projection):
    def __init__(self):
        self.tasks = {}

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self.tasks[event.aggregate_id] = {
                "title": event.title,
                "status": "pending",
            }
        elif isinstance(event, TaskCompleted):
            if event.aggregate_id in self.tasks:
                self.tasks[event.aggregate_id]["status"] = "completed"
        elif isinstance(event, TaskReassigned):
            if event.aggregate_id in self.tasks:
                self.tasks[event.aggregate_id]["assignee"] = event.new_assignee

    async def reset(self) -> None:
        self.tasks.clear()
```

### The Declarative Approach (@handles decorator)

The `DeclarativeProjection` with `@handles` is cleaner and more maintainable:

```python
from eventsource import DeclarativeProjection, handles

class DeclarativeTaskProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[event.aggregate_id] = {
            "title": event.title,
            "status": "pending",
        }

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["status"] = "completed"

    @handles(TaskReassigned)
    async def _on_reassigned(self, event: TaskReassigned) -> None:
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["assignee"] = event.new_assignee

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()
```

**Benefits of the Declarative Approach:**

| Feature | Simple | Declarative |
|---------|--------|-------------|
| Auto-generated `subscribed_to()` | No | Yes |
| Handler routing | Manual if/elif | Automatic |
| Handler discovery | None | Built-in |
| Code organization | All in one method | Separate methods |
| Adding new handlers | Modify handle() | Add new method |

---

## Multiple Projections for Different Needs

You can have multiple projections for the same events, each optimized for different query patterns:

```python
# Projection 1: Task list with all details
class TaskListProjection(DeclarativeProjection):
    """Full task details for the task list view."""
    # ... handlers that store all task fields ...

# Projection 2: Task statistics
class TaskStatsProjection(DeclarativeProjection):
    """Aggregated statistics about tasks."""

    def __init__(self):
        super().__init__()
        self.total_created = 0
        self.completed_count = 0
        self.tasks_by_assignee: dict[UUID, int] = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.total_created += 1
        if event.assigned_to:
            self.tasks_by_assignee[event.assigned_to] = \
                self.tasks_by_assignee.get(event.assigned_to, 0) + 1

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        self.completed_count += 1

    async def _truncate_read_models(self) -> None:
        self.total_created = 0
        self.completed_count = 0
        self.tasks_by_assignee.clear()

    def get_completion_rate(self) -> float:
        if self.total_created == 0:
            return 0.0
        return self.completed_count / self.total_created

# Projection 3: Recent activity feed
class ActivityFeedProjection(DeclarativeProjection):
    """Last N events for activity feed."""
    # ... handlers that maintain a rolling list of recent events ...
```

Each projection processes the same events but maintains different data structures optimized for specific queries.

---

## Best Practices

### 1. Make Handlers Idempotent

Handlers should produce the same result if called multiple times with the same event:

```python
@handles(TaskCreated)
async def _on_created(self, event: TaskCreated) -> None:
    # GOOD: Using aggregate_id as key ensures idempotency
    self.tasks[event.aggregate_id] = {
        "title": event.title,
        "status": "pending",
    }

    # BAD: Appending would duplicate on replay
    # self.task_list.append({"title": event.title})
```

### 2. Handle Missing Data Gracefully

Events may arrive out of order or you may receive events for entities you have not seen:

```python
@handles(TaskCompleted)
async def _on_completed(self, event: TaskCompleted) -> None:
    # Check if the task exists before updating
    if event.aggregate_id in self.tasks:
        self.tasks[event.aggregate_id]["status"] = "completed"
    # else: silently ignore or log warning
```

### 3. Keep Projections Focused

Each projection should serve a specific query need:

```python
# GOOD: Focused projection
class TaskSearchProjection:
    """Optimized for full-text search on tasks."""

# GOOD: Focused projection
class TaskDashboardProjection:
    """Optimized for dashboard widgets and counts."""

# BAD: Everything projection
class AllTaskDataProjection:
    """Stores everything about tasks."""  # Too broad!
```

### 4. Design for Rebuild

Always implement `_truncate_read_models()` properly so projections can be rebuilt:

```python
async def _truncate_read_models(self) -> None:
    """Clear all data structures used by this projection."""
    self.tasks.clear()
    self.index_by_assignee.clear()
    self.statistics = {}
```

### 5. Consider Denormalization

In projections, denormalization is your friend. Store data in the format you need for queries:

```python
@handles(TaskCreated)
async def _on_created(self, event: TaskCreated) -> None:
    # Store denormalized data for fast queries
    task_data = {
        "id": event.aggregate_id,
        "title": event.title,
        "status": "pending",
        # Include data you will filter/sort by
        "created_date": event.occurred_at.date().isoformat(),
        "created_week": event.occurred_at.isocalendar()[1],
    }
    self.tasks[event.aggregate_id] = task_data

    # Maintain secondary indexes for common queries
    self.by_status["pending"].add(event.aggregate_id)
    self.by_week[task_data["created_week"]].add(event.aggregate_id)
```

---

## Complete Example

Here is the complete, runnable example:

```python
"""
Tutorial 6: Building Read Models with Projections

This example demonstrates building projections from domain events.
Run with: python tutorial_06_projections.py
"""
import asyncio
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    register_event,
    DeclarativeProjection,
    handles,
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
# Projection
# =============================================================================

class TaskListProjection(DeclarativeProjection):
    """
    Projection that maintains a queryable list of tasks.

    This read model is optimized for:
    - Listing all tasks
    - Filtering by status
    - Filtering by assignee
    """

    def __init__(self):
        super().__init__()
        # In-memory read model
        self.tasks: dict[UUID, dict] = {}

    # =========================================================================
    # Event Handlers
    # =========================================================================

    @handles(TaskCreated)
    async def _on_task_created(self, event: TaskCreated) -> None:
        """Handle task creation - add to read model."""
        self.tasks[event.aggregate_id] = {
            "id": event.aggregate_id,
            "title": event.title,
            "description": event.description,
            "assigned_to": event.assigned_to,
            "status": "pending",
            "created_at": event.occurred_at,
        }

    @handles(TaskCompleted)
    async def _on_task_completed(self, event: TaskCompleted) -> None:
        """Handle task completion - update status."""
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["status"] = "completed"
            self.tasks[event.aggregate_id]["completed_by"] = event.completed_by
            self.tasks[event.aggregate_id]["completed_at"] = event.occurred_at

    @handles(TaskReassigned)
    async def _on_task_reassigned(self, event: TaskReassigned) -> None:
        """Handle task reassignment - update assignee."""
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["assigned_to"] = event.new_assignee

    # =========================================================================
    # Reset (required by DeclarativeProjection)
    # =========================================================================

    async def _truncate_read_models(self) -> None:
        """Clear all data when resetting the projection."""
        self.tasks.clear()

    # =========================================================================
    # Query Methods
    # =========================================================================

    def get_all_tasks(self) -> list[dict]:
        """Get all tasks."""
        return list(self.tasks.values())

    def get_pending_tasks(self) -> list[dict]:
        """Get only pending (incomplete) tasks."""
        return [t for t in self.tasks.values() if t["status"] == "pending"]

    def get_completed_tasks(self) -> list[dict]:
        """Get only completed tasks."""
        return [t for t in self.tasks.values() if t["status"] == "completed"]

    def get_tasks_by_assignee(self, assignee_id: UUID) -> list[dict]:
        """Get tasks assigned to a specific user."""
        return [t for t in self.tasks.values() if t["assigned_to"] == assignee_id]

    def get_task(self, task_id: UUID) -> dict | None:
        """Get a single task by ID."""
        return self.tasks.get(task_id)

    def count_by_status(self) -> dict[str, int]:
        """Get counts by status."""
        counts = {"pending": 0, "completed": 0}
        for task in self.tasks.values():
            counts[task["status"]] += 1
        return counts


# =============================================================================
# Usage Example
# =============================================================================

async def main():
    print("=== Projection Demo ===\n")

    # Create the projection
    projection = TaskListProjection()

    # Check what events it subscribes to
    print("Subscribed to events:")
    for event_type in projection.subscribed_to():
        print(f"  - {event_type.__name__}")
    print()

    # Simulate some events
    user1 = uuid4()
    user2 = uuid4()
    task1_id = uuid4()
    task2_id = uuid4()
    task3_id = uuid4()

    # Create some tasks
    events = [
        TaskCreated(
            aggregate_id=task1_id,
            title="Implement projections",
            description="Complete Tutorial 6",
            assigned_to=user1,
            aggregate_version=1,
        ),
        TaskCreated(
            aggregate_id=task2_id,
            title="Write tests",
            description="Add unit tests for projections",
            assigned_to=user1,
            aggregate_version=1,
        ),
        TaskCreated(
            aggregate_id=task3_id,
            title="Review code",
            description="Review PR for projections",
            assigned_to=user2,
            aggregate_version=1,
        ),
    ]

    # Process events through the projection
    print("Processing events...")
    for event in events:
        await projection.handle(event)
    print(f"Processed {len(events)} events\n")

    # Query the read model
    print("=== Query Results ===\n")

    print(f"All tasks: {len(projection.get_all_tasks())}")
    print(f"Pending: {len(projection.get_pending_tasks())}")
    print(f"Completed: {len(projection.get_completed_tasks())}")
    print()

    print(f"Tasks for user1: {len(projection.get_tasks_by_assignee(user1))}")
    print(f"Tasks for user2: {len(projection.get_tasks_by_assignee(user2))}")
    print()

    # Complete a task
    complete_event = TaskCompleted(
        aggregate_id=task1_id,
        completed_by=user1,
        aggregate_version=2,
    )
    await projection.handle(complete_event)

    print("After completing task 1:")
    print(f"  Pending: {len(projection.get_pending_tasks())}")
    print(f"  Completed: {len(projection.get_completed_tasks())}")
    print()

    # Reassign a task
    reassign_event = TaskReassigned(
        aggregate_id=task2_id,
        previous_assignee=user1,
        new_assignee=user2,
        aggregate_version=2,
    )
    await projection.handle(reassign_event)

    print("After reassigning task 2:")
    print(f"  User1 tasks: {len(projection.get_tasks_by_assignee(user1))}")
    print(f"  User2 tasks: {len(projection.get_tasks_by_assignee(user2))}")
    print()

    # Show status counts
    print("Final counts:", projection.count_by_status())

    # Demonstrate reset
    print("\n=== Reset Demo ===")
    print(f"Before reset: {len(projection.get_all_tasks())} tasks")
    await projection.reset()
    print(f"After reset: {len(projection.get_all_tasks())} tasks")


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_06_projections.py` and run it:

```bash
python tutorial_06_projections.py
```

---

## Exercises

Now it is time to practice what you have learned!

### Exercise 1: Task Statistics Projection

**Objective:** Create a projection that tracks task statistics.

**Time:** 15-20 minutes

**Requirements:**

1. Track total tasks created
2. Track completed vs pending counts
3. Track tasks per assignee
4. Add query methods for each statistic
5. Calculate a completion rate percentage

**Starter Code:**

```python
"""
Tutorial 6 - Exercise 1: Task Statistics Projection

Your task: Create a projection that tracks various task statistics.
"""
import asyncio
from collections import defaultdict
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    register_event,
    DeclarativeProjection,
    handles,
)


# Events (from tutorial)
@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    assigned_to: UUID | None = None


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    previous_assignee: UUID | None
    new_assignee: UUID


class TaskStatisticsProjection(DeclarativeProjection):
    """
    Projection that tracks task statistics.

    TODO: Implement the following:
    1. Track total_created count
    2. Track completed_count and pending_count
    3. Track tasks_by_assignee (dict mapping assignee UUID to count)
    4. Implement handlers for TaskCreated, TaskCompleted, TaskReassigned
    5. Implement query methods
    """

    def __init__(self):
        super().__init__()
        # TODO: Initialize counters
        pass

    # TODO: Add @handles decorators and handler methods

    async def _truncate_read_models(self) -> None:
        # TODO: Reset all counters
        pass

    # TODO: Add query methods:
    # - get_total_created() -> int
    # - get_completion_stats() -> dict with completed, pending, total
    # - get_tasks_per_assignee() -> dict[UUID, int]
    # - get_completion_rate() -> float (0.0 to 1.0)


async def main():
    projection = TaskStatisticsProjection()

    user1, user2 = uuid4(), uuid4()

    # Create tasks
    for i in range(5):
        await projection.handle(TaskCreated(
            aggregate_id=uuid4(),
            title=f"Task {i+1}",
            assigned_to=user1 if i < 3 else user2,
            aggregate_version=1,
        ))

    # Complete some tasks
    # TODO: Create and handle TaskCompleted events

    print("After creating 5 tasks:")
    print(f"  Total: {projection.get_total_created()}")
    print(f"  Stats: {projection.get_completion_stats()}")
    print(f"  Completion rate: {projection.get_completion_rate():.0%}")


if __name__ == "__main__":
    asyncio.run(main())
```

**Hints:**

- Use `defaultdict(int)` for `tasks_by_assignee` to simplify counting
- Track which task is assigned to whom so reassignments update counts correctly
- Remember: `completed_count + pending_count` should equal `total_created`

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 6 - Exercise 1 Solution: Task Statistics Projection
"""
import asyncio
from collections import defaultdict
from uuid import UUID, uuid4

from eventsource import (
    DomainEvent,
    register_event,
    DeclarativeProjection,
    handles,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    assigned_to: UUID | None = None


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    previous_assignee: UUID | None
    new_assignee: UUID


class TaskStatisticsProjection(DeclarativeProjection):
    """
    Projection that tracks task statistics.

    Tracks:
    - Total tasks created
    - Completed vs pending counts
    - Tasks per assignee
    """

    def __init__(self):
        super().__init__()
        self.total_created = 0
        self.completed_count = 0
        self.pending_count = 0
        self.tasks_by_assignee: dict[UUID | None, int] = defaultdict(int)
        self._task_assignees: dict[UUID, UUID | None] = {}  # Track current assignee

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.total_created += 1
        self.pending_count += 1
        self.tasks_by_assignee[event.assigned_to] += 1
        self._task_assignees[event.aggregate_id] = event.assigned_to

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        self.completed_count += 1
        self.pending_count -= 1

    @handles(TaskReassigned)
    async def _on_reassigned(self, event: TaskReassigned) -> None:
        # Decrement old assignee count
        old_assignee = self._task_assignees.get(event.aggregate_id)
        if old_assignee in self.tasks_by_assignee:
            self.tasks_by_assignee[old_assignee] -= 1
            if self.tasks_by_assignee[old_assignee] <= 0:
                del self.tasks_by_assignee[old_assignee]

        # Increment new assignee count
        self.tasks_by_assignee[event.new_assignee] += 1
        self._task_assignees[event.aggregate_id] = event.new_assignee

    async def _truncate_read_models(self) -> None:
        self.total_created = 0
        self.completed_count = 0
        self.pending_count = 0
        self.tasks_by_assignee.clear()
        self._task_assignees.clear()

    # Query methods
    def get_total_created(self) -> int:
        return self.total_created

    def get_completion_stats(self) -> dict[str, int]:
        return {
            "completed": self.completed_count,
            "pending": self.pending_count,
            "total": self.total_created,
        }

    def get_tasks_per_assignee(self) -> dict[UUID | None, int]:
        return dict(self.tasks_by_assignee)

    def get_completion_rate(self) -> float:
        if self.total_created == 0:
            return 0.0
        return self.completed_count / self.total_created


async def main():
    projection = TaskStatisticsProjection()

    user1, user2 = uuid4(), uuid4()
    task_ids = []

    # Create tasks
    for i in range(5):
        task_id = uuid4()
        task_ids.append(task_id)
        await projection.handle(TaskCreated(
            aggregate_id=task_id,
            title=f"Task {i+1}",
            assigned_to=user1 if i < 3 else user2,
            aggregate_version=1,
        ))

    print("After creating 5 tasks:")
    print(f"  Total: {projection.get_total_created()}")
    print(f"  Stats: {projection.get_completion_stats()}")
    print(f"  By assignee: {len(projection.get_tasks_per_assignee())} assignees")
    print(f"  Completion rate: {projection.get_completion_rate():.0%}")

    # Complete 2 tasks
    for task_id in task_ids[:2]:
        await projection.handle(TaskCompleted(
            aggregate_id=task_id,
            aggregate_version=2,
        ))

    print("\nAfter completing 2 tasks:")
    print(f"  Stats: {projection.get_completion_stats()}")
    print(f"  Completion rate: {projection.get_completion_rate():.0%}")

    # Reassign a task
    await projection.handle(TaskReassigned(
        aggregate_id=task_ids[2],
        previous_assignee=user1,
        new_assignee=user2,
        aggregate_version=2,
    ))

    print("\nAfter reassigning 1 task from user1 to user2:")
    stats = projection.get_tasks_per_assignee()
    print(f"  User1 tasks: {stats.get(user1, 0)}")
    print(f"  User2 tasks: {stats.get(user2, 0)}")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

The solution file is also available at: `docs/tutorials/exercises/solutions/06-1.py`

---

## Summary

In this tutorial, you learned:

- **Projections** build optimized read models from event streams
- The **CQRS pattern** separates write models (aggregates) from read models (projections)
- **DeclarativeProjection** provides a clean, decorator-based approach to event handling
- The **@handles decorator** routes events to the appropriate handler methods
- **Query methods** provide efficient access to projection data
- Projections can be **reset and rebuilt** from scratch by replaying events
- Projections are **eventually consistent** with the write model
- You can have **multiple projections** for different query needs

---

## Key Takeaways

!!! note "Remember"
    - Projections are **eventually consistent** with aggregates
    - Read models can be **denormalized** for query performance
    - You can have **multiple projections** for different query needs
    - **Rebuilding** is a feature, not a bug - embrace it!

!!! tip "Best Practice"
    Design handlers to be idempotent. Use aggregate IDs as keys rather than appending to lists. This ensures that replaying events produces the same result.

!!! warning "Common Mistake"
    Do not put business logic in projections. Projections are for building read models, not for enforcing business rules. Business rules belong in aggregates.

---

## Next Steps

You have learned how projections build read models from events. But how do events get from aggregates to projections?

Continue to [Tutorial 7: Distributing Events with Event Bus](07-event-bus.md) to learn how to deliver events to projections automatically using the event bus pattern.

---

## Related Documentation

- [Projections API Reference](../api/projections.md) - Complete projections API
- [Error Handling Guide](../guides/error-handling.md) - Handling projection errors
- [Tutorial 10: Checkpoint Tracking](10-checkpoints.md) - Production checkpoint management
- [Tutorial 11: PostgreSQL Projections](11-postgresql-projections.md) - Database-backed read models
