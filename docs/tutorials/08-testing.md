# Tutorial 8: Testing Event-Sourced Systems

**Difficulty:** Intermediate | **Progress:** Tutorial 8 of 21 | Phase 2: Core Patterns

**Prerequisites:** [Tutorials 1-7](01-introduction.md), Python 3.11+, eventsource-py, pytest, pytest-asyncio

Event sourcing simplifies testing - events are explicit inputs/outputs, state is deterministic. Test at three layers: unit (aggregates/projections), integration (repositories), and end-to-end (workflows).

---

## Testing Aggregates

```python
import pytest
from uuid import uuid4

def test_task_creation():
    """Test creating a task."""
    task_id = uuid4()
    task = TaskAggregate(task_id)

    # Execute command
    task.create("Test task", "Description")

    # Assert state
    assert task.state.title == "Test task"
    assert task.state.status == "pending"
    assert task.version == 1

    # Assert events
    assert len(task.uncommitted_events) == 1
    event = task.uncommitted_events[0]
    assert event.event_type == "TaskCreated"
    assert event.title == "Test task"

def test_complete_task():
    """Test completing a task."""
    task_id = uuid4()
    user_id = uuid4()
    task = TaskAggregate(task_id)

    # Given: task exists
    task.create("Test task", "Description")
    task.mark_events_committed()

    # When: complete task
    task.complete(user_id)

    # Then: status updated
    assert task.state.status == "completed"
    assert task.state.completed_by == user_id
    assert task.version == 2

def test_cannot_complete_twice():
    """Test business rule: can't complete completed task."""
    task_id = uuid4()
    user_id = uuid4()
    task = TaskAggregate(task_id)

    task.create("Test task", "Description")
    task.complete(user_id)

    # Should raise
    with pytest.raises(ValueError, match="already completed"):
        task.complete(user_id)
```

## Testing Event Replay

```python
def test_rebuild_from_events():
    """Test state reconstruction from events."""
    task_id = uuid4()
    user_id = uuid4()

    # Create events
    events = [
        TaskCreated(
            aggregate_id=task_id,
            title="Historical task",
            description="From events",
            aggregate_version=1,
        ),
        TaskCompleted(
            aggregate_id=task_id,
            completed_by=user_id,
            aggregate_version=2,
        ),
    ]

    # Replay
    task = TaskAggregate(task_id)
    task.load_from_history(events)

    # Assert reconstructed state
    assert task.version == 2
    assert task.state.status == "completed"
    assert task.state.completed_by == user_id
    assert len(task.uncommitted_events) == 0
```

## Testing Projections

```python
import pytest

@pytest.mark.asyncio
async def test_projection_handles_created():
    """Test projection processes TaskCreated."""
    projection = TaskListProjection()
    task_id = uuid4()

    event = TaskCreated(
        aggregate_id=task_id,
        title="Test task",
        description="Description",
        aggregate_version=1,
    )

    await projection.handle(event)

    assert task_id in projection.tasks
    assert projection.tasks[task_id]["title"] == "Test task"
    assert projection.tasks[task_id]["status"] == "pending"

@pytest.mark.asyncio
async def test_projection_handles_completed():
    """Test projection processes TaskCompleted."""
    projection = TaskListProjection()
    task_id = uuid4()
    user_id = uuid4()

    # First create
    await projection.handle(TaskCreated(
        aggregate_id=task_id,
        title="Test",
        description="Desc",
        aggregate_version=1,
    ))

    # Then complete
    await projection.handle(TaskCompleted(
        aggregate_id=task_id,
        completed_by=user_id,
        aggregate_version=2,
    ))

    assert projection.tasks[task_id]["status"] == "completed"

@pytest.mark.asyncio
async def test_projection_reset():
    """Test projection can be reset."""
    projection = TaskListProjection()

    await projection.handle(TaskCreated(
        aggregate_id=uuid4(),
        title="Test",
        description="Desc",
        aggregate_version=1,
    ))

    assert len(projection.tasks) == 1

    await projection.reset()

    assert len(projection.tasks) == 0
```

## Integration Testing with Event Store

```python
import pytest
from eventsource import InMemoryEventStore, AggregateRepository

@pytest.mark.asyncio
async def test_save_and_load_workflow():
    """Test complete save and load workflow."""
    # Setup
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Create and save
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Integration test", "Description")
    await repo.save(task)

    # Load and verify
    loaded_task = await repo.load(task_id)
    assert loaded_task.state.title == "Integration test"
    assert loaded_task.version == 1

    # Modify and save again
    user_id = uuid4()
    loaded_task.complete(user_id)
    await repo.save(loaded_task)

    # Reload and verify
    final_task = await repo.load(task_id)
    assert final_task.state.status == "completed"
    assert final_task.version == 2
```

## Testing with Event Bus

```python
@pytest.mark.asyncio
async def test_event_bus_publishes():
    """Test event bus publishes to subscribers."""
    bus = InMemoryEventBus()
    published_events = []

    # Subscribe
    async def capture(event):
        published_events.append(event)

    bus.subscribe(TaskCreated, capture)

    # Publish
    event = TaskCreated(
        aggregate_id=uuid4(),
        title="Test",
        description="Desc",
        aggregate_version=1,
    )
    await bus.publish(event)

    # Assert
    assert len(published_events) == 1
    assert published_events[0].title == "Test"

@pytest.mark.asyncio
async def test_repository_with_bus():
    """Test repository publishes to bus on save."""
    store = InMemoryEventStore()
    bus = InMemoryEventBus()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=bus,
    )

    published = []
    bus.subscribe(TaskCreated, lambda e: published.append(e))

    # Save triggers publish
    task = repo.create_new(uuid4())
    task.create("Test", "Desc")
    await repo.save(task)

    assert len(published) == 1
```

## Fixtures for Reusability

```python
import pytest
from eventsource import InMemoryEventStore, InMemoryEventBus, AggregateRepository

@pytest.fixture
def event_store():
    """Provide clean event store for each test."""
    return InMemoryEventStore()

@pytest.fixture
def event_bus():
    """Provide clean event bus for each test."""
    return InMemoryEventBus()

@pytest.fixture
def task_repo(event_store, event_bus):
    """Provide configured task repository."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=event_bus,
    )

@pytest.mark.asyncio
async def test_with_fixtures(task_repo):
    """Test using fixtures."""
    task = task_repo.create_new(uuid4())
    task.create("Fixture test", "Using fixtures")
    await task_repo.save(task)

    loaded = await task_repo.load(task.aggregate_id)
    assert loaded.state.title == "Fixture test"
```

## Complete Example

```python
"""
Tutorial 8: Testing
Run with: pytest tutorial_08_testing.py
"""
import pytest
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


class TaskListProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks: dict[UUID, dict] = {}

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

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()


# Fixtures
@pytest.fixture
def event_store():
    return InMemoryEventStore()

@pytest.fixture
def event_bus():
    return InMemoryEventBus()

@pytest.fixture
def task_repo(event_store, event_bus):
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=event_bus,
    )


# Unit Tests
def test_create_task():
    task = TaskAggregate(uuid4())
    task.create("Test", "Description")

    assert task.state.title == "Test"
    assert task.version == 1
    assert len(task.uncommitted_events) == 1


def test_complete_task():
    task = TaskAggregate(uuid4())
    task.create("Test", "Description")
    task.mark_events_committed()

    user_id = uuid4()
    task.complete(user_id)

    assert task.state.status == "completed"
    assert task.version == 2


def test_cannot_complete_twice():
    task = TaskAggregate(uuid4())
    task.create("Test", "Description")
    user_id = uuid4()
    task.complete(user_id)

    with pytest.raises(ValueError, match="already completed"):
        task.complete(user_id)


# Integration Tests
@pytest.mark.asyncio
async def test_save_and_load(task_repo):
    task_id = uuid4()
    task = task_repo.create_new(task_id)
    task.create("Integration", "Test")
    await task_repo.save(task)

    loaded = await task_repo.load(task_id)
    assert loaded.state.title == "Integration"
    assert loaded.version == 1


@pytest.mark.asyncio
async def test_projection_integration():
    projection = TaskListProjection()

    await projection.handle(TaskCreated(
        aggregate_id=uuid4(),
        title="Test",
        description="Desc",
        aggregate_version=1,
    ))

    assert len(projection.tasks) == 1
```

## Next Steps

Continue to [Tutorial 9: Error Handling](09-error-handling.md).
