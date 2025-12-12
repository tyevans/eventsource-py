# Tutorial 5: Repositories - Loading and Saving Aggregates

**Difficulty:** Beginner
**Progress:** Tutorial 5 of 21 | Phase 1: Foundations

---

## Prerequisites

- [Tutorial 1-4](01-introduction.md) completed
- Python 3.11+, eventsource-py installed

---

## What is the Repository Pattern?

**Repository** provides a clean interface for loading and saving aggregates. It handles:

- Event store interaction
- State reconstruction from events
- Optimistic locking
- Event publishing (optional)

**Without repository:** Manually read events, replay, save, handle conflicts.
**With repository:** Simple `load()` and `save()` methods.

---

## The AggregateRepository Class

```python
from eventsource import AggregateRepository, InMemoryEventStore

# Create repository
store = InMemoryEventStore()
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
)
```

**Key methods:**
- `create_new(id)`: Create empty aggregate
- `load(id)`: Load aggregate from events
- `save(aggregate)`: Persist uncommitted events
- `exists(id)`: Check if aggregate exists
- `load_or_create(id)`: Load if exists, create otherwise

---

## Creating New Aggregates

```python
from uuid import uuid4

# Create new aggregate
task_id = uuid4()
task = repo.create_new(task_id)

# Execute command
task.create("Learn repositories", "Complete tutorial 5")

# Save to event store
await repo.save(task)
print(f"Saved task v{task.version}")
```

---

## Loading Existing Aggregates

```python
# Load aggregate (reconstructs state from events)
task = await repo.load(task_id)

print(f"Title: {task.state.title}")
print(f"Version: {task.version}")
print(f"Status: {task.state.status}")
```

**Behind the scenes:**
1. Read events from store
2. Replay events to rebuild state
3. Return aggregate ready for commands

---

## Load-Modify-Save Workflow

Standard pattern for updating aggregates:

```python
# 1. Load
task = await repo.load(task_id)

# 2. Modify (execute commands)
user_id = uuid4()
task.complete(user_id)

# 3. Save
await repo.save(task)
```

**Important:** After `save()`, events are marked committed. Call `save()` again without new commands does nothing.

---

## Handling Concurrent Modifications

```python
from eventsource import OptimisticLockError

async def complete_with_retry(task_id: UUID, user_id: UUID, max_retries: int = 3):
    """Complete task with automatic retry on conflicts."""
    for attempt in range(max_retries):
        try:
            # Load fresh copy
            task = await repo.load(task_id)

            # Check business rules
            if task.state.status == "completed":
                print("Already completed")
                return

            # Execute command
            task.complete(user_id)

            # Save
            await repo.save(task)
            print(f"Saved on attempt {attempt + 1}")
            return

        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise
            print(f"Conflict, retrying...")
```

---

## Event Publishing

Repositories can publish events to an event bus:

```python
from eventsource import InMemoryEventBus

# Create bus
event_bus = InMemoryEventBus()

# Create repository with publishing
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
    event_publisher=event_bus,  # Events published on save
)

# Subscribe to events
def on_task_created(event):
    print(f"Task created: {event.title}")

event_bus.subscribe(TaskCreated, on_task_created)

# Save triggers publication
task = repo.create_new(uuid4())
task.create("Published task", "Will trigger handler")
await repo.save(task)  # Calls on_task_created()
```

---

## Complete Example

```python
"""
Tutorial 5: Repositories
Run with: python tutorial_05_repositories.py
"""
import asyncio
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    AggregateRoot,
    AggregateRepository,
    InMemoryEventStore,
    DomainEvent,
    register_event,
    OptimisticLockError,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"
    completed_by: UUID


class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
    description: str = ""
    status: str = "pending"
    completed_by: UUID | None = None


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
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "completed",
                        "completed_by": event.completed_by,
                    }
                )

    def create(self, title: str, description: str) -> None:
        if self.version > 0:
            raise ValueError("Task already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
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


async def main():
    # Setup
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Create new task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Learn repositories", "Complete tutorial 5")
    await repo.save(task)
    print(f"Created task: {task.state.title}")

    # Load and modify
    task = await repo.load(task_id)
    print(f"Loaded task v{task.version}: {task.state.title}")

    user_id = uuid4()
    task.complete(user_id)
    await repo.save(task)
    print(f"Completed task v{task.version}")

    # Reload to verify
    task = await repo.load(task_id)
    print(f"\nFinal state:")
    print(f"  Status: {task.state.status}")
    print(f"  Version: {task.version}")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Phase 1 Complete!

You've completed Phase 1: Foundations. You now understand:

- Events: Immutable facts about what happened
- Aggregates: Business logic and consistency boundaries
- Event Stores: Persisting events with optimistic locking
- Repositories: Loading and saving aggregates

**Next:** Phase 2 covers projections, event buses, and testing.

---

## Next Steps

Continue to [Tutorial 6: Projections](06-projections.md).
