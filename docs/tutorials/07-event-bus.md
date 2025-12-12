# Tutorial 7: Event Bus - Distributing Events

**Difficulty:** Intermediate | **Progress:** Tutorial 7 of 21 | Phase 2: Core Patterns

An **event bus** distributes events to multiple subscribers - useful for updating projections, sending notifications, triggering integrations, and analytics.

## InMemoryEventBus

```python
from eventsource import InMemoryEventBus, DomainEvent, register_event
from uuid import UUID, uuid4

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str

# Create bus
bus = InMemoryEventBus()

# Subscribe to events
async def on_task_created(event: TaskCreated):
    print(f"Task created: {event.title}")

bus.subscribe(TaskCreated, on_task_created)

# Publish event
event = TaskCreated(
    aggregate_id=uuid4(),
    title="New task",
    aggregate_version=1,
)
await bus.publish(event)  # Triggers on_task_created()
```

## Subscribing to Events

```python
from eventsource import InMemoryEventBus

bus = InMemoryEventBus()

# Subscribe to specific event type
async def handle_created(event):
    print(f"Created: {event.title}")

bus.subscribe(TaskCreated, handle_created)

# Subscribe to multiple types
async def handle_any(event):
    print(f"Event: {event.event_type}")

bus.subscribe(TaskCreated, handle_any)
bus.subscribe(TaskCompleted, handle_any)

# Subscribe to all events from a projection
bus.subscribe_all(my_projection)
```

## Integration with Repository

```python
from eventsource import AggregateRepository, InMemoryEventStore, InMemoryEventBus

# Create bus and repository
bus = InMemoryEventBus()
store = InMemoryEventStore()

repo = AggregateRepository(
    event_store=store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
    event_publisher=bus,  # Auto-publish on save
)

# Subscribe handlers
async def send_notification(event: TaskCreated):
    print(f"Notification: New task '{event.title}'")

bus.subscribe(TaskCreated, send_notification)

# Save triggers publication
task = repo.create_new(uuid4())
task.create("Publish test", "Auto-published")
await repo.save(task)  # Calls send_notification()
```

## Multiple Projections

```python
from eventsource import DeclarativeProjection, handles

class TaskListProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[event.aggregate_id] = event.title

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()

class TaskStatsProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.count = 0

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.count += 1

    async def _truncate_read_models(self) -> None:
        self.count = 0

# Subscribe both
bus.subscribe_all(task_list)
bus.subscribe_all(task_stats)

# Both update on save
await repo.save(task)
```

## Complete Example

```python
"""
Tutorial 7: Event Bus
Run with: python tutorial_07_event_bus.py
"""
import asyncio
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    AggregateRoot,
    AggregateRepository,
    InMemoryEventStore,
    InMemoryEventBus,
    DeclarativeProjection,
    DomainEvent,
    register_event,
    handles,
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
                self._state = self._state.model_copy(update={"status": "completed"})

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
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        ))


class TaskListProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks: dict[UUID, str] = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[event.aggregate_id] = event.title

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()


class TaskStatsProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.total = 0
        self.completed = 0

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.total += 1

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        self.completed += 1

    async def _truncate_read_models(self) -> None:
        self.total = 0
        self.completed = 0


async def main():
    # Setup infrastructure
    store = InMemoryEventStore()
    bus = InMemoryEventBus()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=bus,
    )

    # Create projections
    task_list = TaskListProjection()
    task_stats = TaskStatsProjection()

    # Subscribe to bus
    bus.subscribe_all(task_list)
    bus.subscribe_all(task_stats)

    # Add notification handler
    async def notify(event: TaskCreated):
        print(f"  [Notification] New task: {event.title}")

    bus.subscribe(TaskCreated, notify)

    # Create tasks
    print("Creating tasks...\n")
    for i in range(3):
        task = repo.create_new(uuid4())
        task.create(f"Task {i+1}", f"Description {i+1}")
        await repo.save(task)

    # Complete one
    print("\nCompleting task...\n")
    task_id = list(task_list.tasks.keys())[0]
    task = await repo.load(task_id)
    task.complete(uuid4())
    await repo.save(task)

    # Query projections
    print("\n=== Task List ===")
    for task_id, title in task_list.tasks.items():
        print(f"  - {title}")

    print(f"\n=== Stats ===")
    print(f"Total: {task_stats.total}")
    print(f"Completed: {task_stats.completed}")
    print(f"Pending: {task_stats.total - task_stats.completed}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Next Steps

Continue to [Tutorial 8: Testing](08-testing.md).
