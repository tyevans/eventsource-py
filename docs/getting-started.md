# Getting Started Guide

This guide will walk you through setting up eventsource-py and implementing your first event-sourced application.

## Installation

### Quick Start

```bash
# Basic installation (in-memory stores only)
pip install eventsource-py

# Most common: with PostgreSQL support for production
pip install eventsource-py[postgresql]
```

### With Optional Dependencies

```bash
# SQLite support (development/testing/embedded)
pip install eventsource-py[sqlite]

# Redis support (distributed event bus)
pip install eventsource-py[redis]

# OpenTelemetry support (observability)
pip install eventsource-py[telemetry]

# All optional dependencies
pip install eventsource-py[all]
```

### Optional Dependencies

eventsource uses optional dependencies to keep the core package lightweight:

| Extra | Purpose | Install Command |
|-------|---------|-----------------|
| `postgresql` | Production event store with asyncpg | `pip install eventsource-py[postgresql]` |
| `redis` | Distributed event bus with Redis Streams | `pip install eventsource-py[redis]` |
| `telemetry` | OpenTelemetry tracing integration | `pip install eventsource-py[telemetry]` |
| `all` | All optional dependencies | `pip install eventsource-py[all]` |

For detailed installation instructions, troubleshooting, and version compatibility information, see the [Installation Guide](installation.md).

## Core Concepts

Before diving in, understand these key concepts:

- **Events**: Immutable facts that capture state changes
- **Aggregates**: Consistency boundaries that emit events
- **Event Store**: Persists events as the source of truth
- **Repository**: Loads/saves aggregates via the event store
- **Projections**: Build read models from event streams
- **Event Bus**: Distributes events to subscribers

## Step 1: Define Your Events

Events capture what happened in your domain. They are immutable and named in past tense.

```python
from uuid import UUID
from eventsource import DomainEvent, register_event

@register_event
class TaskCreated(DomainEvent):
    """Event emitted when a task is created."""
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"

    title: str
    description: str
    assigned_to: UUID | None = None

@register_event
class TaskCompleted(DomainEvent):
    """Event emitted when a task is completed."""
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"

    completed_by: UUID

@register_event
class TaskReassigned(DomainEvent):
    """Event emitted when a task is reassigned."""
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"

    previous_assignee: UUID | None
    new_assignee: UUID
```

**Key points:**
- Inherit from `DomainEvent`
- Use `@register_event` for serialization support
- Set `event_type` and `aggregate_type` class attributes
- Use descriptive, domain-focused names

## Step 2: Define Your State

The state represents the current data of your aggregate. Use Pydantic for validation.

```python
from pydantic import BaseModel

class TaskState(BaseModel):
    """Current state of a Task aggregate."""
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"
    completed_by: UUID | None = None
```

## Step 3: Create Your Aggregate

The aggregate is where business logic lives. It validates commands and emits events.

```python
from eventsource import AggregateRoot

class TaskAggregate(AggregateRoot[TaskState]):
    """Event-sourced Task aggregate."""

    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        """Return initial state for new tasks."""
        return TaskState(task_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        """Update state based on event type."""
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
                self._state = self._state.model_copy(update={
                    "status": "completed",
                    "completed_by": event.completed_by,
                })
        elif isinstance(event, TaskReassigned):
            if self._state:
                self._state = self._state.model_copy(update={
                    "assigned_to": event.new_assignee,
                })

    # Command methods (business logic)

    def create(self, title: str, description: str, assigned_to: UUID | None = None) -> None:
        """Create a new task."""
        if self.version > 0:
            raise ValueError("Task already exists")

        event = TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            assigned_to=assigned_to,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def complete(self, completed_by: UUID) -> None:
        """Mark task as completed."""
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")

        event = TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def reassign(self, new_assignee: UUID) -> None:
        """Reassign task to a different user."""
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot reassign completed task")

        event = TaskReassigned(
            aggregate_id=self.aggregate_id,
            previous_assignee=self.state.assigned_to,
            new_assignee=new_assignee,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)
```

## Step 4: Set Up the Event Store

Choose an event store based on your needs:

```python
from eventsource import InMemoryEventStore, AggregateRepository

# Development: In-memory store (no persistence)
event_store = InMemoryEventStore()

# Development/Testing: SQLite store (file-based or in-memory)
# from eventsource import SQLiteEventStore
#
# # File-based (persistent)
# async with SQLiteEventStore("./events.db") as store:
#     await store.initialize()
#     event_store = store
#
# # In-memory (fast tests)
# async with SQLiteEventStore(":memory:") as store:
#     await store.initialize()
#     event_store = store

# Production: PostgreSQL store
# from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
# from eventsource import PostgreSQLEventStore
#
# engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
# session_factory = async_sessionmaker(engine, expire_on_commit=False)
# event_store = PostgreSQLEventStore(session_factory)
```

| Store | Use Case | Persistence | Concurrency |
|-------|----------|-------------|-------------|
| `InMemoryEventStore` | Unit tests, prototyping | None | Single process |
| `SQLiteEventStore` | Dev, testing, embedded | File or memory | Single writer |
| `PostgreSQLEventStore` | Production | Full | Multiple writers |

## Step 5: Create the Repository

The repository handles loading and saving aggregates.

```python
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
)
```

## Step 6: Use It!

```python
import asyncio
from uuid import uuid4

async def main():
    # Create a new task
    task_id = uuid4()
    user_id = uuid4()

    task = repo.create_new(task_id)
    task.create(
        title="Implement event sourcing",
        description="Add eventsource library to the project",
        assigned_to=user_id,
    )
    await repo.save(task)
    print(f"Created task {task_id}")

    # Load and modify the task
    loaded_task = await repo.load(task_id)
    print(f"Task status: {loaded_task.state.status}")

    # Reassign to someone else
    new_assignee = uuid4()
    loaded_task.reassign(new_assignee)
    await repo.save(loaded_task)
    print(f"Reassigned to: {loaded_task.state.assigned_to}")

    # Complete the task
    final_task = await repo.load(task_id)
    final_task.complete(completed_by=new_assignee)
    await repo.save(final_task)
    print(f"Final status: {final_task.state.status}")

    # Check version (should be 3 - created, reassigned, completed)
    print(f"Version: {final_task.version}")

asyncio.run(main())
```

## Step 7: Add Projections (Optional)

Projections build read models optimized for queries.

```python
from eventsource.projections import DeclarativeProjection, handles

class TaskListProjection(DeclarativeProjection):
    """Maintains a list of tasks for quick querying."""

    def __init__(self):
        super().__init__()
        self.tasks = {}  # In-memory read model

    @handles(TaskCreated)
    async def _handle_created(self, event: TaskCreated) -> None:
        self.tasks[event.aggregate_id] = {
            "id": event.aggregate_id,
            "title": event.title,
            "status": "pending",
            "assigned_to": event.assigned_to,
        }

    @handles(TaskCompleted)
    async def _handle_completed(self, event: TaskCompleted) -> None:
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["status"] = "completed"

    @handles(TaskReassigned)
    async def _handle_reassigned(self, event: TaskReassigned) -> None:
        if event.aggregate_id in self.tasks:
            self.tasks[event.aggregate_id]["assigned_to"] = event.new_assignee

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()

    # Query methods
    def get_pending_tasks(self) -> list[dict]:
        return [t for t in self.tasks.values() if t["status"] == "pending"]

    def get_tasks_for_user(self, user_id: UUID) -> list[dict]:
        return [t for t in self.tasks.values() if t["assigned_to"] == user_id]
```

## Step 8: Add Event Bus with SubscriptionManager (Recommended)

For production use, use `SubscriptionManager` to manage projections. It handles:
- **Catch-up**: Processing historical events on startup
- **Live events**: Real-time event delivery
- **Checkpoints**: Resumable processing after restarts

```python
from eventsource import InMemoryEventBus, InMemoryCheckpointRepository
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

event_bus = InMemoryEventBus()
checkpoint_repo = InMemoryCheckpointRepository()

# Create repository with event publishing
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
    event_publisher=event_bus,
)

# Set up SubscriptionManager
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
)

# Subscribe projection with catch-up from beginning
projection = TaskListProjection()
config = SubscriptionConfig(start_from="beginning")
await manager.subscribe(projection, config=config, name="TaskList")

# Start the manager (catches up, then goes live)
await manager.start()

# Now when you save an aggregate, events flow to the projection
task = repo.create_new(uuid4())
task.create(title="New task", description="Description")
await repo.save(task)

# Projection has both historical and new events
print(projection.get_pending_tasks())

# When done, stop gracefully
await manager.stop()
```

See the [Subscriptions Guide](guides/subscriptions.md) for comprehensive documentation.

## Next Steps

- Read the [Architecture Overview](architecture.md) to understand the full system
- Explore the [API Reference](api/index.md) for detailed documentation
- Check out [Examples](examples/basic-order.md) for more complex scenarios
- Use [SQLite Backend](guides/sqlite-backend.md) for development and testing
- Set up [Production Deployment](guides/production.md) with PostgreSQL

## Complete Example

Here's a complete working example using SubscriptionManager:

```python
"""Complete event sourcing example with SubscriptionManager."""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    InMemoryEventBus,
    InMemoryCheckpointRepository,
    AggregateRepository,
)
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig


# Events
@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str

@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


# State
class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
    status: str = "pending"


# Aggregate
class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=self.aggregate_id,
                title=event.title,
                status="pending",
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, title: str) -> None:
        if self.version > 0:
            raise ValueError("Already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


# Projection - implements Subscriber protocol
class TaskProjection:
    def __init__(self):
        self.tasks = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [TaskCreated, TaskCompleted]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self.tasks[event.aggregate_id] = {"title": event.title, "status": "pending"}
        elif isinstance(event, TaskCompleted):
            if event.aggregate_id in self.tasks:
                self.tasks[event.aggregate_id]["status"] = "completed"


async def main():
    # Setup infrastructure
    store = InMemoryEventStore()
    bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()

    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=bus,
    )

    # Set up SubscriptionManager with projection
    manager = SubscriptionManager(
        event_store=store,
        event_bus=bus,
        checkpoint_repo=checkpoint_repo,
    )

    projection = TaskProjection()
    config = SubscriptionConfig(start_from="beginning")
    await manager.subscribe(projection, config=config, name="TaskList")
    await manager.start()

    # Create tasks
    for i in range(3):
        task = repo.create_new(uuid4())
        task.create(f"Task {i + 1}")
        await repo.save(task)

    # Wait for events to be processed
    await asyncio.sleep(0.1)

    # Complete first task
    first_task_id = list(projection.tasks.keys())[0]
    task = await repo.load(first_task_id)
    task.complete()
    await repo.save(task)

    await asyncio.sleep(0.1)

    # Show results
    print("All tasks:")
    for task_id, data in projection.tasks.items():
        print(f"  - {data['title']}: {data['status']}")

    # Graceful shutdown
    await manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

Output:
```
All tasks:
  - Task 1: completed
  - Task 2: pending
  - Task 3: pending
```
