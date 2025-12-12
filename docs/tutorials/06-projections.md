# Tutorial 6: Projections - Building Read Models

**Difficulty:** Intermediate | **Progress:** Tutorial 6 of 21 | Phase 2: Core Patterns

Projections transform events into query-optimized read models. They're rebuildable, allow multiple views from the same events, and scale independently.

## Creating Your First Projection

```python
from uuid import UUID
from eventsource import DeclarativeProjection, handles, DomainEvent, register_event

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

class TaskListProjection(DeclarativeProjection):
    """Maintains a list of all tasks."""

    def __init__(self):
        super().__init__()
        self.tasks: dict[UUID, dict] = {}

    @handles(TaskCreated)
    async def _on_task_created(self, event: TaskCreated) -> None:
        """Add task to list."""
        self.tasks[event.aggregate_id] = {
            "id": event.aggregate_id,
            "title": event.title,
            "description": event.description,
            "status": "pending",
        }

    @handles(TaskCompleted)
    async def _on_task_completed(self, event: TaskCompleted) -> None:
        """Mark task as completed."""
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["status"] = "completed"
            self.tasks[event.aggregate_id]["completed_by"] = event.completed_by

    async def _truncate_read_models(self) -> None:
        """Clear data for rebuild."""
        self.tasks.clear()
```

## Using the Projection

```python
import asyncio
from uuid import uuid4

async def main():
    projection = TaskListProjection()

    # Process events
    task_id = uuid4()
    user_id = uuid4()

    events = [
        TaskCreated(
            aggregate_id=task_id,
            title="Learn projections",
            description="Complete tutorial 6",
            aggregate_version=1,
        ),
        TaskCompleted(
            aggregate_id=task_id,
            completed_by=user_id,
            aggregate_version=2,
        ),
    ]

    for event in events:
        await projection.handle(event)

    # Query the projection
    print(f"Total tasks: {len(projection.tasks)}")
    for task_id, task_data in projection.tasks.items():
        print(f"  {task_data['title']}: {task_data['status']}")

asyncio.run(main())
```

## Resetting and Rebuilding

```python
# Reset projection
await projection.reset()  # Clears data

# Rebuild by replaying all events
all_events = await event_store.read_all()
for stored_event in all_events.events:
    await projection.handle(stored_event.event)
```

## Multiple Projections

```python
class TaskCountProjection(DeclarativeProjection):
    """Count tasks by status."""

    def __init__(self):
        super().__init__()
        self.pending = 0
        self.completed = 0

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.pending += 1

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        self.pending -= 1
        self.completed += 1

    async def _truncate_read_models(self) -> None:
        self.pending = 0
        self.completed = 0

class TaskByUserProjection(DeclarativeProjection):
    """Group tasks by assigned user."""

    def __init__(self):
        super().__init__()
        self.by_user: dict[UUID, list[UUID]] = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        # Add to "unassigned" initially
        self.by_user.setdefault(None, []).append(event.aggregate_id)

    async def _truncate_read_models(self) -> None:
        self.by_user.clear()
```

## Complete Example

```python
"""
Tutorial 6: Projections
Run with: python tutorial_06_projections.py
"""
import asyncio
from uuid import UUID, uuid4
from eventsource import DeclarativeProjection, handles, DomainEvent, register_event


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


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    new_assignee: UUID


class TaskListProjection(DeclarativeProjection):
    """List all tasks with details."""

    def __init__(self):
        super().__init__()
        self.tasks: dict[UUID, dict] = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[event.aggregate_id] = {
            "title": event.title,
            "status": "pending",
            "assigned_to": None,
        }

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["status"] = "completed"

    @handles(TaskReassigned)
    async def _on_reassigned(self, event: TaskReassigned) -> None:
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["assigned_to"] = event.new_assignee

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()


class TaskStatsProjection(DeclarativeProjection):
    """Track task statistics."""

    def __init__(self):
        super().__init__()
        self.total_created = 0
        self.total_completed = 0

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.total_created += 1

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        self.total_completed += 1

    async def _truncate_read_models(self) -> None:
        self.total_created = 0
        self.total_completed = 0


async def main():
    # Create projections
    task_list = TaskListProjection()
    task_stats = TaskStatsProjection()

    # Generate events
    task1_id = uuid4()
    task2_id = uuid4()
    user_id = uuid4()

    events = [
        TaskCreated(
            aggregate_id=task1_id,
            title="First task",
            description="Learn projections",
            aggregate_version=1,
        ),
        TaskCreated(
            aggregate_id=task2_id,
            title="Second task",
            description="Build projections",
            aggregate_version=1,
        ),
        TaskReassigned(
            aggregate_id=task1_id,
            new_assignee=user_id,
            aggregate_version=2,
        ),
        TaskCompleted(
            aggregate_id=task1_id,
            completed_by=user_id,
            aggregate_version=3,
        ),
    ]

    # Process events in both projections
    for event in events:
        await task_list.handle(event)
        await task_stats.handle(event)

    # Query projections
    print("=== Task List ===")
    for task_id, task in task_list.tasks.items():
        print(f"{task['title']}: {task['status']}")

    print(f"\n=== Task Stats ===")
    print(f"Created: {task_stats.total_created}")
    print(f"Completed: {task_stats.total_completed}")
    print(f"Pending: {task_stats.total_created - task_stats.total_completed}")

    # Demonstrate reset
    print(f"\n=== After Reset ===")
    await task_list.reset()
    print(f"Tasks in list: {len(task_list.tasks)}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Next Steps

Continue to [Tutorial 7: Event Bus](07-event-bus.md).
