# Tutorial 9: Error Handling and Resilience

**Difficulty:** Intermediate | **Progress:** Tutorial 9 of 21 | Phase 2: Core Patterns

Event sourcing requires handling command validation, concurrency conflicts, projection failures, and infrastructure errors. The key principle: fail fast in commands, be resilient in event handlers.

## Command Validation

Validate business rules before emitting events:

```python
class TaskAggregate(AggregateRoot[TaskState]):
    def complete(self, completed_by: UUID) -> None:
        # Validate before emitting events
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")
        if not completed_by:
            raise ValueError("completed_by is required")

        # Only emit if validation passes
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        ))
```

## Optimistic Lock Conflicts

Retry on concurrent modification:

```python
from eventsource import OptimisticLockError

async def complete_task_with_retry(
    repo: AggregateRepository,
    task_id: UUID,
    user_id: UUID,
    max_retries: int = 3
):
    """Complete task with automatic retry on conflicts."""
    for attempt in range(max_retries):
        try:
            task = await repo.load(task_id)
            if task.state.status == "completed":
                return  # Already completed
            task.complete(user_id)
            await repo.save(task)
            return  # Success
        except OptimisticLockError as e:
            if attempt == max_retries - 1:
                raise  # Give up after max retries
            continue  # Retry with fresh copy
```

## Projection Error Handling

Log errors, don't crash projections, queue failures for retry:

```python
class ResilientProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks = {}
        self.failed_events = []

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        try:
            self.tasks[event.aggregate_id] = event.title
            # await notify_external_system(event)  # Might fail
        except Exception as e:
            print(f"Error handling {event.event_type}: {e}")
            self.failed_events.append(event.event_id)

    async def retry_failed(self, event_store):
        """Retry failed events."""
        for event_id in self.failed_events:
            # Reload and retry
            pass
```

## Circuit Breaker

Prevent cascading failures from external dependencies:

```python
from datetime import datetime, timedelta

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, timeout_seconds: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timedelta(seconds=timeout_seconds)
        self.failures = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open

    def call(self, func, *args, **kwargs):
        if self.state == "open":
            if datetime.now() - self.last_failure_time > self.timeout:
                self.state = "half-open"
            else:
                raise Exception("Circuit breaker is OPEN")
        try:
            result = func(*args, **kwargs)
            if self.state == "half-open":
                self.state = "closed"
                self.failures = 0
            return result
        except Exception as e:
            self.failures += 1
            self.last_failure_time = datetime.now()
            if self.failures >= self.failure_threshold:
                self.state = "open"
            raise

# Usage
circuit_breaker = CircuitBreaker()
async def send_notification(event):
    circuit_breaker.call(external_api.notify, event)
```

## Idempotent Handlers

Handle duplicate event delivery (at-least-once semantics):

```python
class IdempotentProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks = {}
        self.processed_events = set()  # Track processed event IDs

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        if event.event_id in self.processed_events:
            return  # Skip if already processed
        self.tasks[event.aggregate_id] = event.title
        self.processed_events.add(event.event_id)

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()
        self.processed_events.clear()
```

## Graceful Degradation

Continue operating when optional dependencies fail:

```python
class TaskListProjection(DeclarativeProjection):
    def __init__(self, cache=None):
        super().__init__()
        self.tasks = {}
        self.cache = cache  # Optional Redis/Memcached
        self.cache_available = True

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[event.aggregate_id] = event.title  # Always update in-memory

        # Try cache, but don't fail if unavailable
        if self.cache and self.cache_available:
            try:
                await self.cache.set(str(event.aggregate_id), event.title)
            except Exception as e:
                print(f"Cache unavailable: {e}")
                self.cache_available = False

    async def get_task(self, task_id: UUID) -> str | None:
        if self.cache and self.cache_available:
            try:
                return await self.cache.get(str(task_id))
            except:
                pass
        return self.tasks.get(task_id)  # Fallback
```

## Complete Example

```python
"""
Tutorial 9: Error Handling
Run with: python tutorial_09_error_handling.py
"""
import asyncio
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    AggregateRoot,
    AggregateRepository,
    InMemoryEventStore,
    DeclarativeProjection,
    DomainEvent,
    register_event,
    handles,
    OptimisticLockError,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"
    completed_by: UUID


class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
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
                status="pending",
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, title: str) -> None:
        if self.version > 0:
            raise ValueError("Task already exists")
        if not title or not title.strip():
            raise ValueError("Title cannot be empty")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self, completed_by: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")
        if not completed_by:
            raise ValueError("completed_by is required")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        ))


class ResilientProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks = {}
        self.errors = []

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        try:
            self.tasks[event.aggregate_id] = event.title
        except Exception as e:
            self.errors.append((event.event_id, str(e)))

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()
        self.errors.clear()


async def complete_with_retry(repo, task_id, user_id, max_retries=3):
    """Complete task with retry logic."""
    for attempt in range(max_retries):
        try:
            task = await repo.load(task_id)
            if task.state.status == "completed":
                print("Already completed")
                return
            task.complete(user_id)
            await repo.save(task)
            print(f"Completed on attempt {attempt + 1}")
            return
        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise
            print(f"Conflict, retrying... (attempt {attempt + 1})")


async def main():
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Test validation errors
    print("=== Validation Errors ===")
    task = repo.create_new(uuid4())
    try:
        task.create("")  # Empty title
    except ValueError as e:
        print(f"Caught: {e}")

    # Test retry logic
    print("\n=== Retry Logic ===")
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Test task")
    await repo.save(task)

    await complete_with_retry(repo, task_id, uuid4())

    # Test projection resilience
    print("\n=== Projection Resilience ===")
    projection = ResilientProjection()
    await projection.handle(TaskCreated(
        aggregate_id=uuid4(),
        title="Test",
        aggregate_version=1,
    ))
    print(f"Tasks: {len(projection.tasks)}")
    print(f"Errors: {len(projection.errors)}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Next Steps

Continue to [Tutorial 10: Checkpoints](10-checkpoints.md).
