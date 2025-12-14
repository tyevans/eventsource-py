# Tutorial 8: Testing Event-Sourced Systems

**Difficulty:** Intermediate

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- [Tutorial 6: Projections](06-projections.md)
- Python 3.10 or higher
- pytest and pytest-asyncio installed

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Understand why event sourcing simplifies testing
2. Write unit tests for aggregates using direct event application
3. Test event replay and state reconstruction
4. Use InMemoryEventStore for fast, isolated integration tests
5. Create pytest fixtures for reusable test infrastructure
6. Test repositories with optimistic concurrency control
7. Test projections with event handlers
8. Apply given-when-then testing patterns for aggregates
9. Test business rule validation in command methods

---

## Why Event Sourcing Simplifies Testing

Event-sourced systems are inherently testable because:

- **Events are explicit**: Inputs and outputs are clearly defined domain events
- **State is deterministic**: Same events always produce the same state
- **No database required**: InMemoryEventStore provides fast, isolated tests
- **Time travel**: Test historical scenarios by replaying events
- **Business rules visible**: Validation logic is separate from state updates

### Traditional vs Event-Sourced Testing

**Traditional CRUD testing:**
```python
# Setup database
db = TestDatabase()
await db.migrate()

# Create test data
task = Task(title="Test")
await db.save(task)

# Test
task = await db.get(task.id)
task.complete()
await db.save(task)

# Assert
task = await db.get(task.id)
assert task.status == "completed"

# Teardown
await db.drop_all()
```

**Event-sourced testing:**
```python
# No database setup needed
task = TaskAggregate(uuid4())

# Test
task.create("Test", "Description")
task.complete(user_id)

# Assert
assert task.state.status == "completed"
assert len(task.uncommitted_events) == 2
```

Event sourcing eliminates database setup, teardown, and slow I/O operations.

---

## Testing Aggregates: Unit Tests

Aggregate tests verify business rules and state transitions without touching the event store.

### Basic Aggregate Test

```python
from uuid import uuid4
import pytest
from my_domain import TaskAggregate

def test_create_task():
    """Test creating a new task."""
    # Arrange
    task_id = uuid4()
    task = TaskAggregate(task_id)

    # Act
    task.create("Learn testing", "Master event sourcing tests")

    # Assert: State updated correctly
    assert task.state.title == "Learn testing"
    assert task.state.description == "Master event sourcing tests"
    assert task.state.status == "pending"

    # Assert: Version incremented
    assert task.version == 1

    # Assert: Event emitted
    assert len(task.uncommitted_events) == 1
    event = task.uncommitted_events[0]
    assert event.event_type == "TaskCreated"
    assert event.aggregate_version == 1
    assert event.title == "Learn testing"
```

**Key pattern:**
1. **Arrange**: Create aggregate instance
2. **Act**: Execute command
3. **Assert**: Check state, version, and events

### Testing Business Rules

Commands should validate business rules before emitting events:

```python
def test_cannot_complete_task_twice():
    """Test business rule: completed tasks cannot be completed again."""
    # Arrange
    task = TaskAggregate(uuid4())
    task.create("Test task", "Description")
    user_id = uuid4()
    task.complete(user_id)

    # Act & Assert
    with pytest.raises(ValueError, match="already completed"):
        task.complete(user_id)

def test_cannot_assign_completed_task():
    """Test business rule: completed tasks cannot be reassigned."""
    task = TaskAggregate(uuid4())
    task.create("Test task", "Description")
    user_id = uuid4()
    task.complete(user_id)

    with pytest.raises(ValueError, match="Cannot assign completed task"):
        task.assign(uuid4())

def test_create_validates_duplicate():
    """Test business rule: cannot create task twice."""
    task = TaskAggregate(uuid4())
    task.create("First", "Description")

    with pytest.raises(ValueError, match="already exists"):
        task.create("Second", "Description")
```

**Testing philosophy:** Business rules should fail before events are emitted. Events represent facts that always succeed.

---

## Given-When-Then Pattern

The given-when-then pattern makes tests read like specifications:

```python
def test_complete_assigned_task():
    """
    Given a task that has been created and assigned
    When the assigned user completes the task
    Then the task status is completed
    """
    # Given: task exists and is assigned
    task = TaskAggregate(uuid4())
    user_id = uuid4()
    task.create("Test task", "Description")
    task.assign(user_id)
    task.mark_events_as_committed()  # Simulate persistence

    # When: user completes the task
    task.complete(user_id)

    # Then: status is completed
    assert task.state.status == "completed"
    assert task.state.completed_by == user_id
    assert task.version == 3

    # Then: only the completion event is uncommitted
    assert len(task.uncommitted_events) == 1
    assert task.uncommitted_events[0].event_type == "TaskCompleted"
```

**Note:** Use `mark_events_as_committed()` to simulate event persistence between commands. This keeps uncommitted_events clean for the "when" phase.

---

## Testing Event Replay

Aggregates must rebuild state correctly by replaying historical events:

```python
def test_rebuild_task_from_events():
    """Test state reconstruction from event history."""
    # Arrange: Create event sequence
    task_id = uuid4()
    user_id = uuid4()

    events = [
        TaskCreated(
            aggregate_id=task_id,
            title="Historical task",
            description="From event store",
            aggregate_version=1,
        ),
        TaskAssigned(
            aggregate_id=task_id,
            assigned_to=user_id,
            aggregate_version=2,
        ),
        TaskCompleted(
            aggregate_id=task_id,
            completed_by=user_id,
            aggregate_version=3,
        ),
    ]

    # Act: Replay events
    task = TaskAggregate(task_id)
    task.load_from_history(events)

    # Assert: State reconstructed correctly
    assert task.version == 3
    assert task.state.title == "Historical task"
    assert task.state.status == "completed"
    assert task.state.assigned_to == user_id
    assert task.state.completed_by == user_id

    # Assert: No uncommitted events (historical replay)
    assert len(task.uncommitted_events) == 0
```

**Critical test:** Every aggregate must correctly replay its events. This test ensures your event handlers work correctly.

---

## Testing with InMemoryEventStore

Use `InMemoryEventStore` for integration tests without database setup:

```python
import pytest
from eventsource import InMemoryEventStore, AggregateRepository

@pytest.mark.asyncio
async def test_save_and_load_task():
    """Test complete save and load workflow."""
    # Arrange
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Act: Create and save
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Integration test", "Testing with event store")
    await repo.save(task)

    # Assert: Load and verify
    loaded_task = await repo.load(task_id)
    assert loaded_task.state.title == "Integration test"
    assert loaded_task.version == 1
    assert loaded_task.aggregate_id == task_id

@pytest.mark.asyncio
async def test_repository_optimistic_locking():
    """Test optimistic concurrency control."""
    from eventsource import OptimisticLockError

    # Arrange
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Test", "Description")
    await repo.save(task)

    # Act: Load twice (simulate concurrent access)
    task1 = await repo.load(task_id)
    task2 = await repo.load(task_id)

    # Task 1 saves successfully
    task1.assign(uuid4())
    await repo.save(task1)

    # Task 2 save fails with conflict
    task2.assign(uuid4())
    with pytest.raises(OptimisticLockError) as exc_info:
        await repo.save(task2)

    # Assert: Error details
    assert exc_info.value.expected_version == 1
    assert exc_info.value.actual_version == 2
    assert exc_info.value.aggregate_id == task_id
```

**Why InMemoryEventStore?**
- No database setup or teardown
- Tests run in milliseconds, not seconds
- Perfect isolation between tests
- Same interface as PostgreSQL/SQLite stores

---

## Pytest Fixtures for Test Infrastructure

Create reusable fixtures to eliminate boilerplate:

```python
import pytest
from uuid import uuid4
from eventsource import InMemoryEventStore, AggregateRepository

# conftest.py or test file

@pytest.fixture
def event_store():
    """Provide a fresh event store for each test."""
    return InMemoryEventStore()

@pytest.fixture
def task_repo(event_store):
    """Provide a configured task repository."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

@pytest.fixture
def task_id():
    """Provide a random task ID."""
    return uuid4()

@pytest.fixture
def user_id():
    """Provide a random user ID."""
    return uuid4()

# Now tests are concise

@pytest.mark.asyncio
async def test_create_task_with_fixtures(task_repo, task_id):
    """Test using fixtures."""
    task = task_repo.create_new(task_id)
    task.create("Fixture test", "Using pytest fixtures")
    await task_repo.save(task)

    loaded = await task_repo.load(task_id)
    assert loaded.state.title == "Fixture test"

@pytest.mark.asyncio
async def test_assign_task_with_fixtures(task_repo, task_id, user_id):
    """Test using multiple fixtures."""
    task = task_repo.create_new(task_id)
    task.create("Test", "Description")
    task.assign(user_id)
    await task_repo.save(task)

    loaded = await task_repo.load(task_id)
    assert loaded.state.assigned_to == user_id
```

**Fixture benefits:**
- Eliminate repetitive setup code
- Make tests more readable
- Easy to update all tests by changing fixture
- pytest automatically provides isolation

---

## Testing Projections

Projections are tested by feeding events and verifying read model updates:

```python
import pytest
from collections import defaultdict
from eventsource import DomainEvent

class TaskListProjection:
    """Example projection for testing."""

    def __init__(self):
        self.tasks: dict[UUID, dict] = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [TaskCreated, TaskCompleted]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self.tasks[event.aggregate_id] = {
                "title": event.title,
                "status": "pending",
            }
        elif isinstance(event, TaskCompleted):
            if event.aggregate_id in self.tasks:
                self.tasks[event.aggregate_id]["status"] = "completed"

@pytest.mark.asyncio
async def test_projection_handles_task_created():
    """Test projection processes TaskCreated event."""
    # Arrange
    projection = TaskListProjection()
    task_id = uuid4()
    event = TaskCreated(
        aggregate_id=task_id,
        title="Test task",
        description="Description",
        aggregate_version=1,
    )

    # Act
    await projection.handle(event)

    # Assert
    assert task_id in projection.tasks
    assert projection.tasks[task_id]["title"] == "Test task"
    assert projection.tasks[task_id]["status"] == "pending"

@pytest.mark.asyncio
async def test_projection_handles_task_completed():
    """Test projection processes TaskCompleted event."""
    # Arrange
    projection = TaskListProjection()
    task_id = uuid4()

    # Create task first
    await projection.handle(TaskCreated(
        aggregate_id=task_id,
        title="Test",
        description="Desc",
        aggregate_version=1,
    ))

    # Act: Complete task
    await projection.handle(TaskCompleted(
        aggregate_id=task_id,
        completed_by=uuid4(),
        aggregate_version=2,
    ))

    # Assert
    assert projection.tasks[task_id]["status"] == "completed"

@pytest.mark.asyncio
async def test_projection_processes_event_sequence():
    """Test projection handles multiple events correctly."""
    # Arrange
    projection = TaskListProjection()

    # Act: Process multiple task lifecycles
    task1_id = uuid4()
    task2_id = uuid4()

    await projection.handle(TaskCreated(
        aggregate_id=task1_id,
        title="First task",
        description="Desc",
        aggregate_version=1,
    ))

    await projection.handle(TaskCreated(
        aggregate_id=task2_id,
        title="Second task",
        description="Desc",
        aggregate_version=1,
    ))

    await projection.handle(TaskCompleted(
        aggregate_id=task1_id,
        completed_by=uuid4(),
        aggregate_version=2,
    ))

    # Assert
    assert len(projection.tasks) == 2
    assert projection.tasks[task1_id]["status"] == "completed"
    assert projection.tasks[task2_id]["status"] == "pending"
```

**Projection testing pattern:**
1. Create projection instance
2. Send events via `handle()`
3. Verify read model state
4. No event store needed

---

## Testing Event Publishing

Test that repositories publish events after successful saves:

```python
@pytest.mark.asyncio
async def test_repository_publishes_events():
    """Test repository publishes events to event bus."""
    from eventsource import InMemoryEventBus

    # Arrange
    store = InMemoryEventStore()
    bus = InMemoryEventBus()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=bus,  # Configure event publishing
    )

    # Track published events
    published_events = []

    async def capture_event(event):
        published_events.append(event)

    await bus.subscribe(TaskCreated, capture_event)

    # Act
    task = repo.create_new(uuid4())
    task.create("Published task", "Will trigger event")
    await repo.save(task)

    # Assert
    assert len(published_events) == 1
    assert published_events[0].event_type == "TaskCreated"
    assert published_events[0].title == "Published task"
```

---

## Advanced Testing Patterns

### Testing Aggregate Lifecycle

```python
@pytest.mark.asyncio
async def test_complete_task_lifecycle(task_repo):
    """Test full task lifecycle: create -> assign -> complete."""
    # Arrange
    task_id = uuid4()
    user_id = uuid4()

    # Act: Create
    task = task_repo.create_new(task_id)
    task.create("Lifecycle test", "Full workflow")
    await task_repo.save(task)

    # Act: Assign
    task = await task_repo.load(task_id)
    task.assign(user_id)
    await task_repo.save(task)

    # Act: Complete
    task = await task_repo.load(task_id)
    task.complete(user_id)
    await task_repo.save(task)

    # Assert: Final state
    final_task = await task_repo.load(task_id)
    assert final_task.version == 3
    assert final_task.state.status == "completed"
    assert final_task.state.assigned_to == user_id
    assert final_task.state.completed_by == user_id

    # Assert: Event history
    from eventsource import ExpectedVersion
    stream = await task_repo.event_store.get_events(task_id, "Task")
    assert len(stream.events) == 3
    assert stream.events[0].event_type == "TaskCreated"
    assert stream.events[1].event_type == "TaskAssigned"
    assert stream.events[2].event_type == "TaskCompleted"
```

### Testing Event Metadata

```python
def test_event_metadata_populated():
    """Test that events have proper metadata."""
    task = TaskAggregate(uuid4())
    task.create("Test", "Description")

    event = task.uncommitted_events[0]

    # Standard metadata
    assert event.event_id is not None
    assert event.event_type == "TaskCreated"
    assert event.aggregate_type == "Task"
    assert event.aggregate_id == task.aggregate_id
    assert event.aggregate_version == 1
    assert event.occurred_at is not None
    assert event.correlation_id is not None
```

### Testing Correlation and Causation

```python
def test_event_causation_tracking():
    """Test events can track causation chains."""
    # Create initial event
    task = TaskAggregate(uuid4())
    task.create("Parent task", "Main task")
    parent_event = task.uncommitted_events[0]

    # Create caused event
    subtask = TaskAggregate(uuid4())
    subtask.create("Subtask", "Child task")
    child_event = subtask.uncommitted_events[0].with_causation(parent_event)

    # Assert causation
    assert child_event.causation_id == parent_event.event_id
    assert child_event.is_caused_by(parent_event)
    assert child_event.correlation_id == parent_event.correlation_id
```

---

## Complete Working Example

Here's a complete test file demonstrating all concepts:

```python
"""
Tutorial 8: Testing Event-Sourced Systems
Run with: pytest tutorial_08_testing.py -v
"""

import pytest
from uuid import UUID, uuid4
from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    InMemoryEventBus,
    OptimisticLockError,
    register_event,
)


# =============================================================================
# Domain Model
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


class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
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
        if self.version > 0:
            raise ValueError("Task already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            aggregate_version=self.get_next_version(),
        ))

    def assign(self, user_id: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot assign completed task")
        self.apply_event(TaskAssigned(
            aggregate_id=self.aggregate_id,
            assigned_to=user_id,
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


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def event_store():
    return InMemoryEventStore()


@pytest.fixture
def task_repo(event_store):
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )


# =============================================================================
# Unit Tests: Aggregates
# =============================================================================

class TestTaskAggregate:
    """Unit tests for TaskAggregate business logic."""

    def test_create_task(self):
        """Test creating a new task."""
        task = TaskAggregate(uuid4())
        task.create("Test task", "Description")

        assert task.state.title == "Test task"
        assert task.state.status == "pending"
        assert task.version == 1
        assert len(task.uncommitted_events) == 1

    def test_assign_task(self):
        """Test assigning a task to a user."""
        task = TaskAggregate(uuid4())
        task.create("Test", "Desc")

        user_id = uuid4()
        task.assign(user_id)

        assert task.state.assigned_to == user_id
        assert task.version == 2

    def test_complete_task(self):
        """Test completing a task."""
        task = TaskAggregate(uuid4())
        task.create("Test", "Desc")

        user_id = uuid4()
        task.complete(user_id)

        assert task.state.status == "completed"
        assert task.state.completed_by == user_id

    def test_cannot_complete_twice(self):
        """Test business rule: cannot complete completed task."""
        task = TaskAggregate(uuid4())
        task.create("Test", "Desc")
        user_id = uuid4()
        task.complete(user_id)

        with pytest.raises(ValueError, match="already completed"):
            task.complete(user_id)

    def test_cannot_assign_completed_task(self):
        """Test business rule: cannot assign completed task."""
        task = TaskAggregate(uuid4())
        task.create("Test", "Desc")
        user_id = uuid4()
        task.complete(user_id)

        with pytest.raises(ValueError, match="Cannot assign completed"):
            task.assign(uuid4())

    def test_rebuild_from_events(self):
        """Test state reconstruction from events."""
        task_id = uuid4()
        user_id = uuid4()

        events = [
            TaskCreated(
                aggregate_id=task_id,
                title="Historical task",
                description="From events",
                aggregate_version=1,
            ),
            TaskAssigned(
                aggregate_id=task_id,
                assigned_to=user_id,
                aggregate_version=2,
            ),
            TaskCompleted(
                aggregate_id=task_id,
                completed_by=user_id,
                aggregate_version=3,
            ),
        ]

        task = TaskAggregate(task_id)
        task.load_from_history(events)

        assert task.version == 3
        assert task.state.status == "completed"
        assert task.state.assigned_to == user_id
        assert len(task.uncommitted_events) == 0


# =============================================================================
# Integration Tests: Repository
# =============================================================================

class TestTaskRepository:
    """Integration tests for TaskAggregate with repository."""

    @pytest.mark.asyncio
    async def test_save_and_load(self, task_repo):
        """Test saving and loading a task."""
        task_id = uuid4()
        task = task_repo.create_new(task_id)
        task.create("Integration test", "Repository test")
        await task_repo.save(task)

        loaded = await task_repo.load(task_id)
        assert loaded.state.title == "Integration test"
        assert loaded.version == 1

    @pytest.mark.asyncio
    async def test_load_modify_save(self, task_repo):
        """Test load-modify-save pattern."""
        task_id = uuid4()

        # Create
        task = task_repo.create_new(task_id)
        task.create("Test", "Description")
        await task_repo.save(task)

        # Load and modify
        task = await task_repo.load(task_id)
        user_id = uuid4()
        task.assign(user_id)
        await task_repo.save(task)

        # Verify
        task = await task_repo.load(task_id)
        assert task.state.assigned_to == user_id
        assert task.version == 2

    @pytest.mark.asyncio
    async def test_optimistic_locking(self, task_repo):
        """Test optimistic concurrency control."""
        task_id = uuid4()
        task = task_repo.create_new(task_id)
        task.create("Test", "Description")
        await task_repo.save(task)

        # Load twice
        task1 = await task_repo.load(task_id)
        task2 = await task_repo.load(task_id)

        # First save succeeds
        task1.assign(uuid4())
        await task_repo.save(task1)

        # Second save fails
        task2.assign(uuid4())
        with pytest.raises(OptimisticLockError):
            await task_repo.save(task2)

    @pytest.mark.asyncio
    async def test_event_history(self, task_repo):
        """Test event history is preserved."""
        task_id = uuid4()
        user_id = uuid4()

        task = task_repo.create_new(task_id)
        task.create("Test", "Description")
        task.assign(user_id)
        task.complete(user_id)
        await task_repo.save(task)

        stream = await task_repo.event_store.get_events(task_id, "Task")
        assert len(stream.events) == 3
        assert stream.events[0].event_type == "TaskCreated"
        assert stream.events[1].event_type == "TaskAssigned"
        assert stream.events[2].event_type == "TaskCompleted"


# =============================================================================
# Integration Tests: Event Publishing
# =============================================================================

class TestEventPublishing:
    """Tests for event publishing via repository."""

    @pytest.mark.asyncio
    async def test_repository_publishes_events(self, event_store):
        """Test repository publishes events after save."""
        bus = InMemoryEventBus()
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TaskAggregate,
            aggregate_type="Task",
            event_publisher=bus,
        )

        published = []

        async def capture(event):
            published.append(event)

        await bus.subscribe(TaskCreated, capture)

        task = repo.create_new(uuid4())
        task.create("Published task", "Will trigger handler")
        await repo.save(task)

        assert len(published) == 1
        assert published[0].event_type == "TaskCreated"
```

**Run the tests:**
```bash
pytest tutorial_08_testing.py -v
```

**Expected output:**
```
test_create_task PASSED
test_assign_task PASSED
test_complete_task PASSED
test_cannot_complete_twice PASSED
test_cannot_assign_completed_task PASSED
test_rebuild_from_events PASSED
test_save_and_load PASSED
test_load_modify_save PASSED
test_optimistic_locking PASSED
test_event_history PASSED
test_repository_publishes_events PASSED
```

---

## Testing Best Practices

### 1. Use InMemoryEventStore for Speed

```python
# Fast: No database I/O
@pytest.fixture
def event_store():
    return InMemoryEventStore()

# Slow: Database setup and teardown
@pytest.fixture
async def event_store():
    db = await create_test_database()
    await db.migrate()
    yield db
    await db.drop_all()
```

InMemoryEventStore runs tests 100x faster than database-backed stores.

### 2. Test One Thing at a Time

```python
# Good: Tests one business rule
def test_cannot_complete_twice():
    task = TaskAggregate(uuid4())
    task.create("Test", "Desc")
    task.complete(uuid4())

    with pytest.raises(ValueError, match="already completed"):
        task.complete(uuid4())

# Bad: Tests multiple things
def test_task_lifecycle():
    task = TaskAggregate(uuid4())
    task.create("Test", "Desc")
    task.assign(uuid4())
    task.complete(uuid4())
    with pytest.raises(ValueError):
        task.complete(uuid4())
    # Too much in one test
```

### 3. Use Descriptive Test Names

```python
# Good
def test_cannot_assign_completed_task()
def test_rebuild_task_from_historical_events()
def test_repository_detects_concurrent_modification()

# Bad
def test_task_1()
def test_error_case()
def test_repo()
```

### 4. Assert on Multiple Levels

```python
def test_complete_task():
    task = TaskAggregate(uuid4())
    task.create("Test", "Desc")
    user_id = uuid4()
    task.complete(user_id)

    # Assert state
    assert task.state.status == "completed"

    # Assert version
    assert task.version == 2

    # Assert events
    assert len(task.uncommitted_events) == 2
    assert task.uncommitted_events[1].event_type == "TaskCompleted"
```

### 5. Use mark_events_as_committed() for Multi-Step Tests

```python
def test_multi_step_workflow():
    task = TaskAggregate(uuid4())

    # Step 1
    task.create("Test", "Desc")
    task.mark_events_as_committed()  # Simulate save

    # Step 2
    task.assign(uuid4())
    task.mark_events_as_committed()  # Simulate save

    # Step 3
    task.complete(uuid4())

    # Only last event uncommitted
    assert len(task.uncommitted_events) == 1
```

---

## Common Testing Patterns

### Testing Exception Messages

```python
def test_error_message_clarity():
    """Test error messages are helpful."""
    task = TaskAggregate(uuid4())
    task.create("Test", "Desc")
    task.complete(uuid4())

    with pytest.raises(ValueError) as exc_info:
        task.complete(uuid4())

    assert "already completed" in str(exc_info.value)
```

### Testing Event Metadata

```python
def test_events_have_correlation_id():
    """Test events track correlation."""
    task = TaskAggregate(uuid4())
    task.create("Test", "Desc")

    event1 = task.uncommitted_events[0]

    task.assign(uuid4())
    event2 = task.uncommitted_events[1]

    # Same correlation chain
    assert event1.correlation_id == event2.correlation_id
```

### Testing Aggregate Exists

```python
@pytest.mark.asyncio
async def test_check_aggregate_exists(task_repo):
    """Test checking if aggregate exists."""
    task_id = uuid4()

    assert not await task_repo.exists(task_id)

    task = task_repo.create_new(task_id)
    task.create("Test", "Desc")
    await task_repo.save(task)

    assert await task_repo.exists(task_id)
```

---

## Key Takeaways

1. **InMemoryEventStore eliminates database overhead**: Tests run in milliseconds
2. **Test aggregates in isolation**: No event store needed for unit tests
3. **Use given-when-then pattern**: Makes tests read like specifications
4. **Test business rules explicitly**: Verify commands fail with invalid state
5. **Test event replay**: Ensure aggregates rebuild correctly from history
6. **Use pytest fixtures**: Eliminate boilerplate and improve readability
7. **Assert on state, version, and events**: Comprehensive verification
8. **Test optimistic locking**: Verify concurrent modification detection
9. **mark_events_as_committed() for multi-step tests**: Clean uncommitted events between steps

---

## Next Steps

You've completed the testing tutorial! You now know how to:
- Write fast, isolated tests with InMemoryEventStore
- Test aggregate business rules and state transitions
- Test event replay and state reconstruction
- Use pytest fixtures for clean test code
- Test repositories with optimistic concurrency control

Continue to [Tutorial 9: Error Handling](09-error-handling.md) to learn about:
- Handling business rule violations
- Dealing with OptimisticLockError
- Dead letter queues for projection failures
- Retry strategies
- Error recovery patterns

For more examples, see:
- `tests/unit/test_aggregate_root.py` - Comprehensive aggregate tests
- `tests/unit/test_aggregate_repository.py` - Repository test patterns
- `tests/unit/test_projection_base.py` - Projection testing examples
- `tests/conftest.py` - Shared fixtures and testing utilities
