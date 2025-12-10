# Tutorial 8: Testing Event-Sourced Applications

**Estimated Time:** 45-60 minutes
**Difficulty:** Intermediate
**Progress:** Tutorial 8 of 21 | Phase 2: Core Patterns

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 7: Distributing Events with Event Bus](07-event-bus.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)
- pytest installed (`pip install pytest pytest-asyncio`)

This tutorial builds on concepts from all previous tutorials. You will use the aggregates, projections, repositories, and event bus patterns you have learned to demonstrate comprehensive testing strategies.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain why event sourcing makes testing easier and more deterministic
2. Write unit tests for aggregates using the Given-When-Then pattern
3. Write unit tests for projections by feeding events and asserting state
4. Create integration tests with in-memory infrastructure
5. Build reusable pytest fixtures for event sourcing components
6. Apply best practices for testable event-sourced application design

---

## Why Event Sourcing Makes Testing Easier

Event sourcing provides several advantages that make testing more straightforward and reliable than traditional state-based approaches.

### Deterministic Replay

Because aggregate state is derived from events, you can reproduce any state by replaying a known sequence of events:

```python
# You can always recreate the exact same state
events = [
    TaskCreated(aggregate_id=task_id, title="Test", aggregate_version=1),
    TaskCompleted(aggregate_id=task_id, aggregate_version=2),
]

task = TaskAggregate(task_id)
task.load_from_history(events)

# task is now in a known, reproducible state
assert task.state.status == "completed"
```

### Events as Test Fixtures

Events serve as perfect test fixtures. Unlike mocking database state, events tell you exactly what happened:

```python
# Traditional approach: Mock complex database state
# Event sourcing approach: Create a simple list of events
test_events = [
    OrderCreated(aggregate_id=order_id, customer_id=customer_id, aggregate_version=1),
    OrderItemAdded(aggregate_id=order_id, item_name="Widget", price=10.0, aggregate_version=2),
]
```

### Separation of Concerns

Event sourcing naturally separates:

- **Commands**: Input validation and business rules (aggregates)
- **Events**: Facts about what happened (immutable records)
- **Queries**: Read models derived from events (projections)

Each can be tested independently.

### No Mocking Required

With in-memory implementations of event stores and event buses, you rarely need to mock anything. Your tests use real implementations that behave identically to production:

```python
# Real implementations, not mocks!
event_store = InMemoryEventStore()
event_bus = InMemoryEventBus()
repository = AggregateRepository(
    event_store=event_store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
    event_publisher=event_bus,
)
```

---

## The Given-When-Then Pattern

The Given-When-Then (GWT) pattern maps naturally to event sourcing:

| GWT Step | Event Sourcing Equivalent |
|----------|--------------------------|
| **Given** | Historical events (setup state) |
| **When** | Execute a command |
| **Then** | Assert new events and/or state |

This pattern makes tests readable and self-documenting:

```python
def test_complete_task():
    """Given a pending task, when complete is called, then TaskCompleted is emitted."""
    # Given: A task that was created
    task_id = uuid4()
    task = TaskAggregate(task_id)
    task.load_from_history([
        TaskCreated(
            aggregate_id=task_id,
            title="Test Task",
            description="A test task",
            aggregate_version=1,
        ),
    ])

    # When: Complete the task
    task.complete()

    # Then: TaskCompleted event is emitted
    assert len(task.uncommitted_events) == 1
    assert isinstance(task.uncommitted_events[0], TaskCompleted)
    assert task.state.status == "completed"
```

---

## Testing Aggregates

Aggregates are the heart of your domain logic. Testing them thoroughly ensures your business rules are correctly implemented.

### Setting Up Test Events and Aggregates

First, let's define the domain code we will test:

```python
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    AggregateRoot,
    DomainEvent,
    register_event,
)


# Events
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


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    previous_assignee: UUID | None
    new_assignee: UUID


# State
class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
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
                description=event.description,
                status="pending",
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})
        elif isinstance(event, TaskReassigned):
            if self._state:
                self._state = self._state.model_copy(
                    update={"assigned_to": event.new_assignee}
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

    def complete(self) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
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
```

### Unit Testing Commands

Test that commands produce the expected events:

```python
import pytest
from uuid import uuid4


class TestTaskAggregateCreate:
    """Tests for TaskAggregate.create() command."""

    def test_create_produces_task_created_event(self):
        """Given a new task, when create is called, then TaskCreated is emitted."""
        # Given
        task = TaskAggregate(uuid4())

        # When
        task.create("My Task", "Description")

        # Then
        assert task.version == 1
        assert len(task.uncommitted_events) == 1

        event = task.uncommitted_events[0]
        assert isinstance(event, TaskCreated)
        assert event.title == "My Task"
        assert event.description == "Description"

    def test_create_updates_state(self):
        """Given a new task, when create is called, then state is updated."""
        task = TaskAggregate(uuid4())

        task.create("My Task", "Description")

        assert task.state is not None
        assert task.state.title == "My Task"
        assert task.state.status == "pending"

    def test_cannot_create_twice(self):
        """Given an existing task, when create is called again, then error."""
        task = TaskAggregate(uuid4())
        task.create("First", "Desc")

        with pytest.raises(ValueError, match="already exists"):
            task.create("Second", "Desc")
```

### Testing Business Rules

Business rules are the most important thing to test. Ensure your aggregates enforce invariants:

```python
class TestTaskAggregateComplete:
    """Tests for TaskAggregate.complete() command."""

    def test_complete_produces_task_completed_event(self):
        """Given a pending task, when complete is called, then TaskCompleted is emitted."""
        task = TaskAggregate(uuid4())
        task.create("Task", "Desc")
        task.mark_events_as_committed()  # Clear for clean assertion

        task.complete()

        assert len(task.uncommitted_events) == 1
        assert isinstance(task.uncommitted_events[0], TaskCompleted)

    def test_complete_updates_status(self):
        """Given a pending task, when complete is called, then status is completed."""
        task = TaskAggregate(uuid4())
        task.create("Task", "Desc")

        task.complete()

        assert task.state.status == "completed"

    def test_cannot_complete_non_existent_task(self):
        """Given a new task (no create), when complete is called, then error."""
        task = TaskAggregate(uuid4())

        with pytest.raises(ValueError, match="does not exist"):
            task.complete()

    def test_cannot_complete_twice(self):
        """Given a completed task, when complete is called again, then error."""
        task = TaskAggregate(uuid4())
        task.create("Task", "Desc")
        task.complete()

        with pytest.raises(ValueError, match="already completed"):
            task.complete()
```

### Testing Event Replay

Ensure aggregates can be reconstituted from historical events:

```python
class TestTaskAggregateReplay:
    """Tests for aggregate reconstitution from events."""

    def test_replay_from_history(self):
        """Given historical events, when loaded, then state is reconstructed."""
        task_id = uuid4()
        user_id = uuid4()
        events = [
            TaskCreated(
                aggregate_id=task_id,
                title="Replayed Task",
                description="From history",
                aggregate_version=1,
            ),
            TaskReassigned(
                aggregate_id=task_id,
                previous_assignee=None,
                new_assignee=user_id,
                aggregate_version=2,
            ),
            TaskCompleted(
                aggregate_id=task_id,
                aggregate_version=3,
            ),
        ]

        task = TaskAggregate(task_id)
        task.load_from_history(events)

        assert task.version == 3
        assert task.state.title == "Replayed Task"
        assert task.state.assigned_to == user_id
        assert task.state.status == "completed"
        assert len(task.uncommitted_events) == 0  # No new events

    def test_replay_empty_history(self):
        """Given no history, when loaded, then aggregate is at initial state."""
        task = TaskAggregate(uuid4())
        task.load_from_history([])

        assert task.version == 0
        assert task.state is None
```

---

## Testing Projections

Projections transform events into read models. Testing them ensures your query side works correctly.

### Setting Up a Test Projection

```python
from eventsource import DeclarativeProjection, handles


class TaskListProjection(DeclarativeProjection):
    """Projection that maintains a list of tasks."""

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
```

### Unit Testing Projection Handlers

Test that projections correctly handle each event type:

```python
import pytest


class TestTaskListProjection:
    """Tests for TaskListProjection."""

    @pytest.fixture
    def projection(self):
        """Create a fresh projection for each test."""
        return TaskListProjection()

    @pytest.mark.asyncio
    async def test_handles_task_created(self, projection):
        """Given a TaskCreated event, projection adds task to list."""
        task_id = uuid4()
        event = TaskCreated(
            aggregate_id=task_id,
            title="New Task",
            description="Desc",
            aggregate_version=1,
        )

        await projection.handle(event)

        assert task_id in projection.tasks
        assert projection.tasks[task_id]["title"] == "New Task"
        assert projection.tasks[task_id]["status"] == "pending"

    @pytest.mark.asyncio
    async def test_handles_task_completed(self, projection):
        """Given TaskCreated then TaskCompleted, projection updates status."""
        task_id = uuid4()
        created = TaskCreated(
            aggregate_id=task_id,
            title="Task",
            description="Desc",
            aggregate_version=1,
        )
        completed = TaskCompleted(
            aggregate_id=task_id,
            aggregate_version=2,
        )

        await projection.handle(created)
        await projection.handle(completed)

        assert projection.tasks[task_id]["status"] == "completed"

    @pytest.mark.asyncio
    async def test_handles_multiple_tasks(self, projection):
        """Projection correctly tracks multiple tasks."""
        task1_id = uuid4()
        task2_id = uuid4()

        await projection.handle(TaskCreated(
            aggregate_id=task1_id,
            title="Task 1",
            description="First",
            aggregate_version=1,
        ))
        await projection.handle(TaskCreated(
            aggregate_id=task2_id,
            title="Task 2",
            description="Second",
            aggregate_version=1,
        ))
        await projection.handle(TaskCompleted(
            aggregate_id=task1_id,
            aggregate_version=2,
        ))

        assert len(projection.tasks) == 2
        assert projection.tasks[task1_id]["status"] == "completed"
        assert projection.tasks[task2_id]["status"] == "pending"

    @pytest.mark.asyncio
    async def test_reset_clears_data(self, projection):
        """Given populated projection, when reset, then data is cleared."""
        event = TaskCreated(
            aggregate_id=uuid4(),
            title="Task",
            description="Desc",
            aggregate_version=1,
        )
        await projection.handle(event)
        assert len(projection.tasks) == 1

        await projection.reset()

        assert len(projection.tasks) == 0
```

### Testing Projection with Event Replay

Test that projections correctly process sequences of events:

```python
@pytest.mark.asyncio
async def test_projection_replay_scenario():
    """Test replaying a complete event sequence through projection."""
    projection = TaskListProjection()
    task_id = uuid4()

    # Simulate replaying events from event store
    events = [
        TaskCreated(
            aggregate_id=task_id,
            title="Important Task",
            description="Must complete",
            aggregate_version=1,
        ),
        TaskCompleted(
            aggregate_id=task_id,
            aggregate_version=2,
        ),
    ]

    for event in events:
        await projection.handle(event)

    assert task_id in projection.tasks
    assert projection.tasks[task_id]["title"] == "Important Task"
    assert projection.tasks[task_id]["status"] == "completed"
```

---

## Integration Testing

Integration tests verify that components work together correctly.

### Testing with In-Memory Infrastructure

The in-memory implementations provide fast, isolated integration tests:

```python
from eventsource import (
    AggregateRepository,
    InMemoryEventStore,
    InMemoryEventBus,
)


class TestRepositoryIntegration:
    """Integration tests for repository with event store."""

    @pytest.fixture
    def event_store(self):
        """Fresh event store for each test."""
        return InMemoryEventStore()

    @pytest.fixture
    def event_bus(self):
        """Fresh event bus for each test."""
        return InMemoryEventBus()

    @pytest.fixture
    def repository(self, event_store, event_bus):
        """Repository with event publishing."""
        return AggregateRepository(
            event_store=event_store,
            aggregate_factory=TaskAggregate,
            aggregate_type="Task",
            event_publisher=event_bus,
        )

    @pytest.mark.asyncio
    async def test_save_and_load_round_trip(self, repository):
        """Given a saved task, when loaded, then state matches."""
        task_id = uuid4()

        # Save
        task = repository.create_new(task_id)
        task.create("Integration Test", "Testing save/load")
        await repository.save(task)

        # Load
        loaded = await repository.load(task_id)

        assert loaded.version == 1
        assert loaded.state.title == "Integration Test"
        assert len(loaded.uncommitted_events) == 0

    @pytest.mark.asyncio
    async def test_multiple_commands_persist(self, repository):
        """Given multiple commands, when saved and loaded, then all events applied."""
        task_id = uuid4()
        user_id = uuid4()

        # Create and save
        task = repository.create_new(task_id)
        task.create("Multi-step Task", "Desc")
        await repository.save(task)

        # Load, modify, and save again
        task = await repository.load(task_id)
        task.reassign(user_id)
        await repository.save(task)

        # Load and complete
        task = await repository.load(task_id)
        task.complete()
        await repository.save(task)

        # Verify final state
        final = await repository.load(task_id)
        assert final.version == 3
        assert final.state.status == "completed"
        assert final.state.assigned_to == user_id
```

### Testing Event Bus Integration

Verify that events flow correctly from repository to projections:

```python
class TestEventBusIntegration:
    """Integration tests for event bus with projection."""

    @pytest.fixture
    def event_store(self):
        return InMemoryEventStore()

    @pytest.fixture
    def event_bus(self):
        return InMemoryEventBus()

    @pytest.fixture
    def projection(self):
        return TaskListProjection()

    @pytest.fixture
    def repository(self, event_store, event_bus):
        return AggregateRepository(
            event_store=event_store,
            aggregate_factory=TaskAggregate,
            aggregate_type="Task",
            event_publisher=event_bus,
        )

    @pytest.mark.asyncio
    async def test_projection_receives_events_via_bus(
        self, repository, event_bus, projection
    ):
        """Given projection subscribed to bus, when task saved, projection updated."""
        # Subscribe projection to events
        event_bus.subscribe(TaskCreated, projection.handle)
        event_bus.subscribe(TaskCompleted, projection.handle)

        # Create and save task
        task_id = uuid4()
        task = repository.create_new(task_id)
        task.create("Bus Test", "Testing event flow")
        await repository.save(task)

        # Verify projection received event
        assert task_id in projection.tasks
        assert projection.tasks[task_id]["title"] == "Bus Test"

    @pytest.mark.asyncio
    async def test_full_event_flow(self, repository, event_bus, projection):
        """Test complete flow: command -> event store -> event bus -> projection."""
        # Subscribe projection
        event_bus.subscribe_all(projection)

        # Execute commands
        task_id = uuid4()
        task = repository.create_new(task_id)
        task.create("Full Flow Test", "Description")
        await repository.save(task)

        task = await repository.load(task_id)
        task.complete()
        await repository.save(task)

        # Verify projection state
        assert task_id in projection.tasks
        assert projection.tasks[task_id]["status"] == "completed"

        # Verify event store state
        loaded = await repository.load(task_id)
        assert loaded.version == 2
```

---

## Pytest Fixtures for Event Sourcing

Creating reusable fixtures makes your tests cleaner and more maintainable.

### Common Fixture Patterns

```python
# conftest.py
import pytest
from uuid import uuid4
from eventsource import (
    InMemoryEventStore,
    InMemoryEventBus,
    AggregateRepository,
)


@pytest.fixture
def event_store():
    """Provide a fresh in-memory event store."""
    return InMemoryEventStore()


@pytest.fixture
def event_bus():
    """Provide a fresh in-memory event bus."""
    return InMemoryEventBus()


@pytest.fixture
def task_repository(event_store, event_bus):
    """Provide a task repository with event publishing."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=event_bus,
    )


@pytest.fixture
def task_projection():
    """Provide a fresh task list projection."""
    return TaskListProjection()


@pytest.fixture
def aggregate_id():
    """Provide a random aggregate ID."""
    return uuid4()


@pytest.fixture
def user_id():
    """Provide a random user ID."""
    return uuid4()
```

### Event Factory Fixture

A factory fixture helps create test events with sensible defaults:

```python
@pytest.fixture
def event_factory():
    """Factory for creating test events with defaults."""
    def create(
        event_class,
        aggregate_id=None,
        aggregate_version=1,
        **kwargs
    ):
        return event_class(
            aggregate_id=aggregate_id or uuid4(),
            aggregate_version=aggregate_version,
            **kwargs
        )
    return create


# Usage in tests
def test_with_factory(event_factory):
    event = event_factory(
        TaskCreated,
        title="Test",
        description="Desc"
    )
    assert event.title == "Test"
```

### Pre-populated Aggregate Fixture

```python
@pytest.fixture
def created_task(aggregate_id):
    """Provide a task that has been created."""
    task = TaskAggregate(aggregate_id)
    task.create("Pre-created Task", "For testing")
    task.mark_events_as_committed()
    return task


@pytest.fixture
def completed_task(aggregate_id):
    """Provide a task that has been completed."""
    task = TaskAggregate(aggregate_id)
    task.create("Completed Task", "For testing")
    task.complete()
    task.mark_events_as_committed()
    return task


# Usage in tests
def test_cannot_complete_again(completed_task):
    """A completed task cannot be completed again."""
    with pytest.raises(ValueError, match="already completed"):
        completed_task.complete()
```

---

## Best Practices for Testable Design

### 1. Keep Aggregates Pure

Aggregates should not have external dependencies. All inputs come through command parameters:

```python
# Good: Pure aggregate
class TaskAggregate(AggregateRoot[TaskState]):
    def create(self, title: str, description: str) -> None:
        # All data comes from parameters
        self.apply_event(TaskCreated(...))

# Bad: Aggregate with external dependency
class TaskAggregate(AggregateRoot[TaskState]):
    def create(self, title: str) -> None:
        # Don't do this - calling external service
        description = external_service.get_template()
        self.apply_event(TaskCreated(...))
```

### 2. Use In-Memory Implementations in Tests

Always use in-memory implementations for unit and integration tests:

```python
# In tests: use in-memory
store = InMemoryEventStore()
bus = InMemoryEventBus()

# In production: use real implementations
store = PostgreSQLEventStore(...)
bus = RedisEventBus(...)
```

### 3. Test Business Rules Explicitly

Every business rule should have at least one test:

```python
class TestTaskBusinessRules:
    """Explicit tests for each business rule."""

    def test_cannot_create_task_twice(self):
        """Business rule: A task can only be created once."""
        ...

    def test_cannot_complete_non_existent_task(self):
        """Business rule: Only existing tasks can be completed."""
        ...

    def test_cannot_complete_task_twice(self):
        """Business rule: A task can only be completed once."""
        ...

    def test_cannot_reassign_completed_task(self):
        """Business rule: Completed tasks cannot be reassigned."""
        ...
```

### 4. Use Clear Test Names

Test names should describe the scenario being tested:

```python
# Good: Describes Given-When-Then
def test_given_pending_task_when_completed_then_status_is_completed():
    ...

# Good: Describes the rule being tested
def test_cannot_complete_already_completed_task():
    ...

# Bad: Vague name
def test_complete():
    ...
```

### 5. Test Event Content, Not Just Types

Verify that events contain the correct data, not just that they are the right type:

```python
def test_reassign_captures_both_assignees(self):
    """TaskReassigned event should capture both old and new assignee."""
    task = TaskAggregate(uuid4())
    task.create("Task", "Desc")

    user1 = uuid4()
    user2 = uuid4()

    task.reassign(user1)
    task.mark_events_as_committed()
    task.reassign(user2)

    event = task.uncommitted_events[0]
    assert isinstance(event, TaskReassigned)
    assert event.previous_assignee == user1  # Check content
    assert event.new_assignee == user2       # Check content
```

### 6. Use mark_events_as_committed() for Clean Assertions

When testing multiple commands, clear uncommitted events between commands:

```python
def test_multiple_commands(self):
    task = TaskAggregate(uuid4())

    # First command
    task.create("Task", "Desc")
    assert len(task.uncommitted_events) == 1
    task.mark_events_as_committed()  # Clear

    # Second command - now we only see the new event
    task.complete()
    assert len(task.uncommitted_events) == 1  # Only TaskCompleted
    assert isinstance(task.uncommitted_events[0], TaskCompleted)
```

---

## Complete Example

Here is a complete test file demonstrating all the patterns:

```python
"""
Tutorial 8: Testing Event-Sourced Applications

This example demonstrates comprehensive testing patterns for event sourcing.
Run with: pytest tutorial_08_testing.py -v
"""
import pytest
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DeclarativeProjection,
    DomainEvent,
    InMemoryEventBus,
    InMemoryEventStore,
    handles,
    register_event,
)


# =============================================================================
# Domain Code
# =============================================================================

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


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    previous_assignee: UUID | None
    new_assignee: UUID


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
                status="pending",
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(
                    update={"status": "completed"}
                )
        elif isinstance(event, TaskReassigned):
            if self._state:
                self._state = self._state.model_copy(
                    update={"assigned_to": event.new_assignee}
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

    def complete(self) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
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


# =============================================================================
# Pytest Fixtures
# =============================================================================

@pytest.fixture
def event_store():
    """Fresh in-memory event store."""
    return InMemoryEventStore()


@pytest.fixture
def event_bus():
    """Fresh in-memory event bus."""
    return InMemoryEventBus()


@pytest.fixture
def task_repository(event_store, event_bus):
    """Task repository with event publishing."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=event_bus,
    )


@pytest.fixture
def task_projection():
    """Fresh task list projection."""
    return TaskListProjection()


# =============================================================================
# Unit Tests: Aggregates
# =============================================================================

class TestTaskAggregateCreate:
    """Tests for TaskAggregate.create() command."""

    def test_create_produces_task_created_event(self):
        """Given a new task, when create is called, then TaskCreated is emitted."""
        task = TaskAggregate(uuid4())

        task.create("My Task", "Description")

        assert task.version == 1
        assert len(task.uncommitted_events) == 1
        event = task.uncommitted_events[0]
        assert isinstance(event, TaskCreated)
        assert event.title == "My Task"
        assert event.description == "Description"

    def test_create_updates_state(self):
        """Given a new task, when create is called, then state is updated."""
        task = TaskAggregate(uuid4())

        task.create("My Task", "Description")

        assert task.state is not None
        assert task.state.title == "My Task"
        assert task.state.status == "pending"

    def test_cannot_create_twice(self):
        """Given an existing task, when create is called again, then error."""
        task = TaskAggregate(uuid4())
        task.create("First", "Desc")

        with pytest.raises(ValueError, match="already exists"):
            task.create("Second", "Desc")


class TestTaskAggregateComplete:
    """Tests for TaskAggregate.complete() command."""

    def test_complete_produces_task_completed_event(self):
        """Given a pending task, when complete is called, then TaskCompleted is emitted."""
        task = TaskAggregate(uuid4())
        task.create("Task", "Desc")
        task.mark_events_as_committed()

        task.complete()

        assert len(task.uncommitted_events) == 1
        assert isinstance(task.uncommitted_events[0], TaskCompleted)

    def test_cannot_complete_non_existent_task(self):
        """Given a new task (no create), when complete is called, then error."""
        task = TaskAggregate(uuid4())

        with pytest.raises(ValueError, match="does not exist"):
            task.complete()

    def test_cannot_complete_twice(self):
        """Given a completed task, when complete is called again, then error."""
        task = TaskAggregate(uuid4())
        task.create("Task", "Desc")
        task.complete()

        with pytest.raises(ValueError, match="already completed"):
            task.complete()


class TestTaskAggregateReassign:
    """Tests for TaskAggregate.reassign() command."""

    def test_reassign_produces_event(self):
        """Given a pending task, when reassign is called, then event is emitted."""
        task = TaskAggregate(uuid4())
        task.create("Task", "Desc")
        task.mark_events_as_committed()
        user = uuid4()

        task.reassign(user)

        assert len(task.uncommitted_events) == 1
        event = task.uncommitted_events[0]
        assert isinstance(event, TaskReassigned)
        assert event.new_assignee == user

    def test_cannot_reassign_completed_task(self):
        """Given a completed task, when reassign is called, then error."""
        task = TaskAggregate(uuid4())
        task.create("Task", "Desc")
        task.complete()

        with pytest.raises(ValueError, match="Cannot reassign"):
            task.reassign(uuid4())


class TestTaskAggregateReplay:
    """Tests for aggregate reconstitution from events."""

    def test_replay_from_history(self):
        """Given historical events, when loaded, then state is reconstructed."""
        task_id = uuid4()
        events = [
            TaskCreated(
                aggregate_id=task_id,
                title="Replayed Task",
                description="From history",
                aggregate_version=1,
            ),
            TaskCompleted(
                aggregate_id=task_id,
                aggregate_version=2,
            ),
        ]

        task = TaskAggregate(task_id)
        task.load_from_history(events)

        assert task.version == 2
        assert task.state.title == "Replayed Task"
        assert task.state.status == "completed"
        assert len(task.uncommitted_events) == 0


# =============================================================================
# Unit Tests: Projections
# =============================================================================

class TestTaskListProjection:
    """Tests for TaskListProjection."""

    @pytest.mark.asyncio
    async def test_handles_task_created(self, task_projection):
        """Given a TaskCreated event, projection adds task to list."""
        task_id = uuid4()
        event = TaskCreated(
            aggregate_id=task_id,
            title="New Task",
            description="Desc",
            aggregate_version=1,
        )

        await task_projection.handle(event)

        assert task_id in task_projection.tasks
        assert task_projection.tasks[task_id]["title"] == "New Task"
        assert task_projection.tasks[task_id]["status"] == "pending"

    @pytest.mark.asyncio
    async def test_handles_task_completed(self, task_projection):
        """Given TaskCreated then TaskCompleted, projection updates status."""
        task_id = uuid4()
        created = TaskCreated(
            aggregate_id=task_id,
            title="Task",
            description="Desc",
            aggregate_version=1,
        )
        completed = TaskCompleted(
            aggregate_id=task_id,
            aggregate_version=2,
        )

        await task_projection.handle(created)
        await task_projection.handle(completed)

        assert task_projection.tasks[task_id]["status"] == "completed"

    @pytest.mark.asyncio
    async def test_reset_clears_data(self, task_projection):
        """Given populated projection, when reset, then data is cleared."""
        event = TaskCreated(
            aggregate_id=uuid4(),
            title="Task",
            description="Desc",
            aggregate_version=1,
        )
        await task_projection.handle(event)
        assert len(task_projection.tasks) == 1

        await task_projection.reset()

        assert len(task_projection.tasks) == 0


# =============================================================================
# Integration Tests
# =============================================================================

class TestRepositoryIntegration:
    """Integration tests for repository with event store."""

    @pytest.mark.asyncio
    async def test_save_and_load_round_trip(self, task_repository):
        """Given a saved task, when loaded, then state matches."""
        task_id = uuid4()

        task = task_repository.create_new(task_id)
        task.create("Integration Test", "Testing save/load")
        await task_repository.save(task)

        loaded = await task_repository.load(task_id)

        assert loaded.version == 1
        assert loaded.state.title == "Integration Test"
        assert len(loaded.uncommitted_events) == 0

    @pytest.mark.asyncio
    async def test_multiple_commands_persist(self, task_repository):
        """Given multiple commands, all events are persisted."""
        task_id = uuid4()

        task = task_repository.create_new(task_id)
        task.create("Multi-step Task", "Desc")
        await task_repository.save(task)

        task = await task_repository.load(task_id)
        task.complete()
        await task_repository.save(task)

        final = await task_repository.load(task_id)
        assert final.version == 2
        assert final.state.status == "completed"


class TestEventBusIntegration:
    """Integration tests for event bus with projection."""

    @pytest.mark.asyncio
    async def test_projection_receives_events_via_bus(
        self, task_repository, event_bus, task_projection
    ):
        """Given projection subscribed to bus, when task saved, projection updated."""
        event_bus.subscribe(TaskCreated, task_projection.handle)
        event_bus.subscribe(TaskCompleted, task_projection.handle)

        task_id = uuid4()
        task = task_repository.create_new(task_id)
        task.create("Bus Test", "Testing event flow")
        await task_repository.save(task)

        assert task_id in task_projection.tasks
        assert task_projection.tasks[task_id]["title"] == "Bus Test"
```

Save this as `tutorial_08_testing.py` and run it:

```bash
pytest tutorial_08_testing.py -v
```

---

## Exercises

Now it is time to practice what you have learned!

### Exercise 1: Complete Test Suite for Task Domain

**Objective:** Write a complete test suite covering all business rules for a task management domain.

**Time:** 20-30 minutes

**Requirements:**

1. Test that `create` produces `TaskCreated` with correct data
2. Test that `complete` produces `TaskCompleted`
3. Test that a task cannot be completed twice
4. Test that a non-existent task cannot be completed
5. Test that `reassign` produces `TaskReassigned` with both old and new assignee
6. Test that a completed task cannot be reassigned
7. Test that event replay correctly reconstructs state through multiple events

**Starter Code:**

```python
"""
Tutorial 8 - Exercise 1: Complete Test Suite

Your task: Implement all the test methods to verify business rules.
Run with: pytest exercise_08_1.py -v
"""
import pytest
from uuid import uuid4

# Import the domain code from the tutorial example above
# (Or define it in this file)


class TestTaskAggregateComplete:
    """Complete test suite for TaskAggregate."""

    # 1. Test create command produces TaskCreated
    def test_create_produces_event(self):
        """TODO: Implement this test."""
        pass

    # 2. Test complete command produces TaskCompleted
    def test_complete_produces_event(self):
        """TODO: Implement this test."""
        pass

    # 3. Test cannot complete twice (business rule)
    def test_cannot_complete_twice(self):
        """TODO: Implement this test."""
        pass

    # 4. Test cannot complete non-existent task
    def test_cannot_complete_nonexistent(self):
        """TODO: Implement this test."""
        pass

    # 5. Test reassign command
    def test_reassign_produces_event(self):
        """TODO: Implement this test."""
        pass

    def test_reassign_captures_previous_assignee(self):
        """TODO: Implement this test - verify both old and new assignee."""
        pass

    # 6. Test cannot reassign completed task
    def test_cannot_reassign_completed_task(self):
        """TODO: Implement this test."""
        pass

    # 7. Test event replay reconstructs state
    def test_replay_reconstructs_state(self):
        """TODO: Implement this test - replay multiple events."""
        pass
```

<details>
<summary>Click to see the solution</summary>

See the complete solution in `docs/tutorials/exercises/solutions/08-1.py`

</details>

---

## Summary

In this tutorial, you learned:

- **Event sourcing enables deterministic, reproducible tests** - events serve as perfect fixtures
- **Given-When-Then maps naturally to event sourcing** - history, command, assertions
- **In-memory infrastructure makes tests fast and isolated** - no mocking required
- **Aggregates test pattern: command -> events** - verify events and state changes
- **Projections test pattern: events -> state** - feed events and check results
- **Integration tests verify component interaction** - repository + bus + projection

---

## Key Takeaways

!!! note "Remember"
    - Test aggregates by asserting uncommitted events after commands
    - Test projections by feeding events and checking the resulting state
    - Use `mark_events_as_committed()` for clean assertions between commands
    - Use in-memory stores for fast, isolated tests without mocking
    - Business rules should be tested explicitly, one test per rule

!!! tip "Best Practice"
    Write tests using the Given-When-Then pattern. It makes tests readable, maintainable, and directly maps to event sourcing concepts. Each test should focus on one scenario.

!!! warning "Common Mistake"
    Do not forget to test event replay. Aggregates must correctly reconstruct state from historical events. This is critical for event sourcing to work correctly.

---

## Next Steps

You now understand how to test event-sourced applications effectively. In the next tutorial, you will learn how to handle errors and implement recovery strategies.

Continue to [Tutorial 9: Error Handling and Recovery](09-error-handling.md) to learn about handling failures gracefully.

---

## Related Documentation

- [Testing Guide](../development/testing.md) - Comprehensive testing reference
- [API Reference: Aggregates](../api/aggregates.md) - Complete aggregate API
- [API Reference: Projections](../api/projections.md) - Complete projection API
- [Tutorial 3: First Aggregate](03-first-aggregate.md) - Aggregate fundamentals
- [Tutorial 6: Projections](06-projections.md) - Projection fundamentals
