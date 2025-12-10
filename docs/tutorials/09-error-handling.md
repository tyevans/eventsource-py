# Tutorial 9: Error Handling and Recovery

**Estimated Time:** 30-45 minutes
**Difficulty:** Intermediate
**Progress:** Tutorial 9 of 21 | Phase 2: Core Patterns

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 8: Testing Event-Sourced Applications](08-testing.md)
- Understanding of optimistic locking from [Tutorial 4: Event Stores](04-event-stores.md)
- Understanding of projections from [Tutorial 6: Projections](06-projections.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial covers error handling patterns specific to event-sourced applications. We will explore the library's exception hierarchy, retry strategies, and the Dead Letter Queue (DLQ) concept for handling persistent failures.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Identify and handle common eventsource exceptions
2. Implement retry logic for `OptimisticLockError`
3. Handle missing aggregates gracefully with `AggregateNotFoundError`
4. Configure strict mode and understand `UnhandledEventError`
5. Use retry policies for projection processing
6. Understand the Dead Letter Queue concept for persistent failures

---

## Exception Overview

eventsource-py provides a clear exception hierarchy. All library exceptions inherit from `EventSourceError`, making it easy to catch library-specific errors while letting other exceptions propagate.

### Exception Hierarchy

```
EventSourceError (base)
    |
    +-- OptimisticLockError       # Version conflict during save
    +-- AggregateNotFoundError    # Aggregate not found
    +-- EventNotFoundError        # Event not found
    +-- EventVersionError         # Event version mismatch
    +-- UnhandledEventError       # No handler in strict mode
    +-- ProjectionError           # Projection processing failed
    +-- EventStoreError           # Event store infrastructure error
    +-- EventBusError             # Event bus error
    +-- CheckpointError           # Checkpoint operations failed
    +-- SerializationError        # Event serialization failed
```

### Importing Exceptions

All exceptions are available from the top-level package:

```python
from eventsource import (
    # Base exception
    EventSourceError,

    # Common exceptions
    OptimisticLockError,
    AggregateNotFoundError,
    EventNotFoundError,

    # Aggregate/Projection exceptions
    EventVersionError,

    # Projection-specific
    ProjectionError,
)

# For UnhandledEventError, import from exceptions module
from eventsource.exceptions import UnhandledEventError
```

---

## EventSourceError: The Base Exception

`EventSourceError` is the base class for all library exceptions. Use it to catch any library-specific error:

```python
from eventsource import EventSourceError

try:
    await repo.load(task_id)
    # ... other operations
except EventSourceError as e:
    # Catch any eventsource-py error
    logger.error(f"Event sourcing error: {e}")
```

This is useful when you want to handle all library errors uniformly, for example in an API error handler.

---

## Handling OptimisticLockError

`OptimisticLockError` is the most common exception you will encounter. It occurs when two processes try to save changes to the same aggregate simultaneously.

### When It Occurs

```
Process A:                      Process B:
-----------                     -----------
1. load(task_id) -> v1          1. load(task_id) -> v1
2. task.complete()              2. task.reassign(...)
3. save() -> SUCCESS (v2)       3. save() -> OptimisticLockError!
                                   (expected v1, but actual is v2)
```

### Exception Attributes

```python
from eventsource import OptimisticLockError

try:
    await repo.save(task)
except OptimisticLockError as e:
    print(f"Aggregate ID: {e.aggregate_id}")
    print(f"Expected version: {e.expected_version}")
    print(f"Actual version: {e.actual_version}")
```

### The Retry Pattern

The standard pattern for handling `OptimisticLockError` is to reload the aggregate and retry the operation:

```python
from eventsource import OptimisticLockError

async def complete_task_with_retry(
    repo: AggregateRepository,
    task_id: UUID,
    user_id: UUID,
    max_retries: int = 3,
) -> None:
    """Complete a task with automatic retry on conflict."""
    for attempt in range(max_retries):
        try:
            # Step 1: Load fresh aggregate state
            task = await repo.load(task_id)

            # Step 2: Check if operation is still valid
            if task.state.status == "completed":
                print("Task already completed")
                return

            # Step 3: Apply the command
            task.complete(completed_by=user_id)

            # Step 4: Save
            await repo.save(task)
            print(f"Completed on attempt {attempt + 1}")
            return

        except OptimisticLockError:
            print(f"Conflict on attempt {attempt + 1}, retrying...")

            if attempt == max_retries - 1:
                # Re-raise on final attempt
                raise
```

**Key Points:**

1. **Always reload** the aggregate before retrying - you need the latest state
2. **Validate business rules** again after reload - conditions may have changed
3. **Set a reasonable retry limit** - infinite retries can cause performance issues
4. **Consider adding delay** between retries to reduce contention

### Adding Backoff Delay

For high-contention scenarios, add exponential backoff:

```python
import asyncio
from eventsource import OptimisticLockError

async def save_with_backoff(
    repo: AggregateRepository,
    aggregate_id: UUID,
    operation,  # Callable that modifies aggregate
    max_retries: int = 3,
    initial_delay: float = 0.1,
) -> None:
    """Save with exponential backoff on conflict."""
    for attempt in range(max_retries):
        try:
            aggregate = await repo.load(aggregate_id)
            operation(aggregate)
            await repo.save(aggregate)
            return

        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise

            # Exponential backoff: 0.1s, 0.2s, 0.4s, ...
            delay = initial_delay * (2 ** attempt)
            await asyncio.sleep(delay)
```

---

## Handling AggregateNotFoundError

`AggregateNotFoundError` is raised when you try to load an aggregate that does not exist (has no events).

### Exception Attributes

```python
from eventsource import AggregateNotFoundError

try:
    task = await repo.load(task_id)
except AggregateNotFoundError as e:
    print(f"Aggregate ID: {e.aggregate_id}")
    print(f"Aggregate Type: {e.aggregate_type}")
```

### Handling Strategies

**Option 1: Try/Except Pattern**

```python
from eventsource import AggregateNotFoundError

async def get_task_or_none(repo, task_id):
    """Load task if it exists, return None otherwise."""
    try:
        return await repo.load(task_id)
    except AggregateNotFoundError:
        return None
```

**Option 2: Check Existence First**

```python
async def get_task_safe(repo, task_id):
    """Check existence before loading."""
    if await repo.exists(task_id):
        return await repo.load(task_id)
    return None
```

**Option 3: Use load_or_create()**

```python
async def ensure_task(repo, task_id):
    """Get existing task or create new one."""
    task = await repo.load_or_create(task_id)

    if task.version == 0:
        # New task - needs initialization
        task.create(title="New Task", description="Auto-created")
        await repo.save(task)

    return task
```

### Choosing the Right Strategy

| Strategy | Use When |
|----------|----------|
| Try/Except | You expect the aggregate to usually exist |
| exists() check | You often expect the aggregate to not exist |
| load_or_create() | You want an aggregate regardless of existence |

---

## EventVersionError: Version Validation

`EventVersionError` occurs when event version validation fails during aggregate event application. This typically indicates:

- Events with version gaps (e.g., jumping from v2 to v5)
- Events with version regression (e.g., going from v5 to v3)
- Incorrectly versioned events

### Exception Attributes

```python
from eventsource.exceptions import EventVersionError

try:
    aggregate.apply_event(event)
except EventVersionError as e:
    print(f"Expected version: {e.expected_version}")
    print(f"Actual version: {e.actual_version}")
    print(f"Event ID: {e.event_id}")
    print(f"Aggregate ID: {e.aggregate_id}")
```

### Disabling Version Validation

For specific use cases (like testing or migration), you can disable validation:

```python
class FlexibleAggregate(AggregateRoot[MyState]):
    # Disable version validation
    validate_versions = False

    # ... rest of aggregate implementation
```

When disabled, version mismatches are logged as warnings but allowed to proceed.

---

## UnhandledEventError: Strict Mode

`UnhandledEventError` is raised when using `DeclarativeAggregate` or `DeclarativeProjection` with strict mode enabled (`unregistered_event_handling = "error"`) and an event has no registered handler.

### Handling Modes

| Mode | Behavior |
|------|----------|
| `"ignore"` | Silently skip unhandled events (default) |
| `"warn"` | Log a warning for unhandled events |
| `"error"` | Raise `UnhandledEventError` |

### Setting Up Strict Mode

```python
from eventsource import DeclarativeAggregate, handles
from eventsource.exceptions import UnhandledEventError

class StrictTaskAggregate(DeclarativeAggregate[TaskState]):
    """Aggregate that raises error on unhandled events."""
    aggregate_type = "Task"
    unregistered_event_handling = "error"  # Strict mode

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    @handles(TaskCreated)
    def _on_created(self, event: TaskCreated) -> None:
        self._state = TaskState(
            task_id=self.aggregate_id,
            title=event.title,
            status="pending",
        )

    # Note: No handler for TaskCompleted - will raise error!
```

### Exception Attributes

```python
try:
    task.apply_event(unknown_event, is_new=False)
except UnhandledEventError as e:
    print(f"Event type: {e.event_type}")
    print(f"Event ID: {e.event_id}")
    print(f"Handler class: {e.handler_class}")
    print(f"Available handlers: {e.available_handlers}")
```

### When to Use Strict Mode

| Mode | Use When |
|------|----------|
| `"ignore"` | Production - forward compatibility with new event types |
| `"warn"` | Development - catch missing handlers while staying resilient |
| `"error"` | Testing - ensure all event types are handled |

---

## ProjectionError: Handling Projection Failures

`ProjectionError` is raised when a projection fails to process an event.

### Exception Attributes

```python
from eventsource import ProjectionError

try:
    await projection.handle(event)
except ProjectionError as e:
    print(f"Projection: {e.projection_name}")
    print(f"Event ID: {e.event_id}")
    print(f"Message: {str(e)}")
```

### Retry Policies for Projections

eventsource-py provides retry policy abstractions for projections:

```python
from eventsource.projections.retry import (
    ExponentialBackoffRetryPolicy,
    NoRetryPolicy,
    FilteredRetryPolicy,
    RetryPolicy,
)
from eventsource.subscriptions.retry import RetryConfig
```

**ExponentialBackoffRetryPolicy:**

```python
from eventsource.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.subscriptions.retry import RetryConfig

# Use default settings (3 retries, 2s initial delay)
policy = ExponentialBackoffRetryPolicy()

# Or customize
policy = ExponentialBackoffRetryPolicy(
    config=RetryConfig(
        max_retries=5,
        initial_delay=1.0,
        max_delay=60.0,
        exponential_base=2.0,
    )
)

# Check policy settings
print(f"Max retries: {policy.max_retries}")
print(f"Backoff for attempt 0: {policy.get_backoff(0)} seconds")
print(f"Should retry: {policy.should_retry(0, ValueError('test'))}")
```

**NoRetryPolicy:**

```python
from eventsource.projections.retry import NoRetryPolicy

# Fail immediately - no retries
policy = NoRetryPolicy()
assert policy.max_retries == 0
assert policy.should_retry(0, ValueError("test")) is False
```

**FilteredRetryPolicy:**

```python
from eventsource.projections.retry import (
    ExponentialBackoffRetryPolicy,
    FilteredRetryPolicy,
)
from eventsource.subscriptions.retry import TRANSIENT_EXCEPTIONS

# Only retry transient failures (network errors, timeouts)
policy = FilteredRetryPolicy(
    base_policy=ExponentialBackoffRetryPolicy(),
    retryable_exceptions=TRANSIENT_EXCEPTIONS,
)

# This will retry (ConnectionError is transient)
policy.should_retry(0, ConnectionError("timeout"))  # True

# This will not retry (ValueError is not transient)
policy.should_retry(0, ValueError("invalid data"))  # False
```

---

## Dead Letter Queue (DLQ) Concept

When an event fails processing after all retries are exhausted, it should not be lost. A **Dead Letter Queue** (DLQ) stores failed events for later investigation and reprocessing.

### Why Use a DLQ?

- **No data loss**: Failed events are preserved for analysis
- **Debugging**: Full error context is captured (stack trace, retry count)
- **Recovery**: Events can be reprocessed after fixing the issue
- **Monitoring**: DLQ size indicates system health

### DLQ Entry Structure

```python
from eventsource import DLQEntry, DLQStats, ProjectionFailureCount

# DLQEntry attributes:
# - id: Unique DLQ entry identifier
# - event_id: Original event ID that failed
# - projection_name: Name of the projection that failed
# - event_type: Type of the failed event
# - event_data: Serialized event data
# - error_message: Error message from the failure
# - error_stacktrace: Full stack trace
# - retry_count: Number of retry attempts made
# - first_failed_at: When the event first failed
# - last_failed_at: When the event most recently failed
# - status: Current status (failed, retrying, resolved)
```

### Using the DLQ Repository

```python
from eventsource import (
    InMemoryDLQRepository,
    DLQEntry,
    DLQStats,
)

# Create DLQ repository
dlq_repo = InMemoryDLQRepository()

# Add a failed event
await dlq_repo.add_failed_event(
    event_id=event.event_id,
    projection_name="TaskListProjection",
    event_type=event.event_type,
    event_data=event.model_dump(mode="json"),
    error=exc,
    retry_count=3,
)

# Get failed events
failed = await dlq_repo.get_failed_events(
    projection_name="TaskListProjection",  # Optional filter
    status="failed",
    limit=100,
)

# Get DLQ statistics
stats: DLQStats = await dlq_repo.get_failure_stats()
print(f"Total failed: {stats.total_failed}")
print(f"Total retrying: {stats.total_retrying}")
print(f"Affected projections: {stats.affected_projections}")

# Mark as resolved after manual fix
await dlq_repo.mark_resolved(dlq_id=entry.id, resolved_by="admin")

# Cleanup old resolved entries
deleted = await dlq_repo.delete_resolved_events(older_than_days=30)
```

### Using ProjectionDLQManager

For easier integration with projections, use `ProjectionDLQManager`:

```python
from eventsource.projections.dlq_manager import ProjectionDLQManager
from eventsource import InMemoryDLQRepository

# Create manager for a specific projection
manager = ProjectionDLQManager(
    projection_name="OrderProjection",
    dlq_repo=InMemoryDLQRepository(),
)

# Send failed event to DLQ
success = await manager.send_to_dlq(
    event=failed_event,
    error=processing_error,
    retry_count=3,
)

# Get failed events for this projection
failed = await manager.get_failed_events(limit=50)
```

---

## Best Practices

### 1. Always Handle OptimisticLockError

In any concurrent scenario, wrap save operations with retry logic:

```python
async def update_aggregate_safely(repo, aggregate_id, operation):
    """Generic retry wrapper for aggregate updates."""
    max_retries = 3

    for attempt in range(max_retries):
        try:
            aggregate = await repo.load(aggregate_id)
            operation(aggregate)
            await repo.save(aggregate)
            return aggregate
        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise
```

### 2. Use Specific Exception Types

Catch specific exceptions rather than base `EventSourceError` when you need different handling:

```python
# Good - specific handling
try:
    task = await repo.load(task_id)
except AggregateNotFoundError:
    return create_new_task(task_id)
except OptimisticLockError:
    return await retry_load(task_id)

# Less good - generic handling loses context
try:
    task = await repo.load(task_id)
except EventSourceError:
    logger.error("Something went wrong")
```

### 3. Log Errors with Context

Include relevant context when logging errors:

```python
import logging

logger = logging.getLogger(__name__)

try:
    await repo.save(task)
except OptimisticLockError as e:
    logger.warning(
        "Version conflict saving task",
        extra={
            "aggregate_id": str(e.aggregate_id),
            "expected_version": e.expected_version,
            "actual_version": e.actual_version,
            "operation": "complete_task",
        },
    )
```

### 4. Consider Strict Mode for Development

Use strict mode (`unregistered_event_handling = "error"`) during development to catch missing handlers early:

```python
import os

class TaskAggregate(DeclarativeAggregate[TaskState]):
    # Strict in dev, lenient in prod
    unregistered_event_handling = (
        "error" if os.getenv("ENV") == "development" else "warn"
    )
```

### 5. Monitor Your DLQ

A growing DLQ indicates problems. Set up alerts:

```python
async def check_dlq_health(dlq_repo):
    """Check DLQ health and alert if needed."""
    stats = await dlq_repo.get_failure_stats()

    if stats.total_failed > 100:
        alert("DLQ has over 100 failed events!")

    # Check for specific projections with many failures
    failures = await dlq_repo.get_projection_failure_counts()
    for pf in failures:
        if pf.failure_count > 10:
            alert(f"Projection {pf.projection_name} has {pf.failure_count} failures")
```

---

## Complete Example

Here is a complete example demonstrating error handling patterns:

```python
"""
Tutorial 9: Error Handling and Recovery

This example demonstrates error handling patterns for event sourcing.
Run with: python tutorial_09_error_handling.py
"""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    DeclarativeAggregate,
    InMemoryEventStore,
    AggregateRepository,
    handles,
    OptimisticLockError,
    AggregateNotFoundError,
    InMemoryDLQRepository,
)
from eventsource.exceptions import UnhandledEventError


# =============================================================================
# Events
# =============================================================================

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


@register_event
class TaskArchived(DomainEvent):
    """Event that we intentionally do not handle (for demo)."""
    event_type: str = "TaskArchived"
    aggregate_type: str = "Task"


# =============================================================================
# State and Aggregates
# =============================================================================

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
            raise ValueError("Already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        if not self.state:
            raise ValueError("Does not exist")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


class StrictTaskAggregate(DeclarativeAggregate[TaskState]):
    """Aggregate that raises error on unhandled events."""
    aggregate_type = "Task"
    unregistered_event_handling = "error"  # Strict mode

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    @handles(TaskCreated)
    def _on_created(self, event: TaskCreated) -> None:
        self._state = TaskState(
            task_id=self.aggregate_id,
            title=event.title,
            status="pending",
        )

    # Note: No handler for TaskCompleted or TaskArchived


# =============================================================================
# Error Handling Examples
# =============================================================================

async def optimistic_lock_demo():
    """Demonstrate OptimisticLockError handling."""
    print("=== OptimisticLockError Demo ===\n")

    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Create a task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Shared Task")
    await repo.save(task)

    # Simulate two "users" loading the same task
    user1_task = await repo.load(task_id)
    user2_task = await repo.load(task_id)

    # User 1 saves first
    user1_task.complete()
    await repo.save(user1_task)
    print("User 1 saved successfully")

    # User 2 tries to save - will fail
    user2_task.complete()
    try:
        await repo.save(user2_task)
    except OptimisticLockError as e:
        print(f"\nOptimisticLockError caught!")
        print(f"  Aggregate ID: {e.aggregate_id}")
        print(f"  Expected version: {e.expected_version}")
        print(f"  Actual version: {e.actual_version}")


async def optimistic_lock_retry():
    """Demonstrate retry pattern for OptimisticLockError."""
    print("\n=== Optimistic Lock Retry Pattern ===\n")

    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    # Create task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Task for Retry Demo")
    await repo.save(task)

    async def complete_with_retry(task_id: UUID, max_retries: int = 3) -> None:
        """Complete a task with automatic retry on conflict."""
        for attempt in range(max_retries):
            try:
                # Load fresh copy
                task = await repo.load(task_id)

                # Skip if already completed
                if task.state.status == "completed":
                    print("  Task already completed")
                    return

                # Apply command
                task.complete()
                await repo.save(task)
                print(f"  Saved on attempt {attempt + 1}")
                return

            except OptimisticLockError:
                print(f"  Conflict on attempt {attempt + 1}, retrying...")
                if attempt == max_retries - 1:
                    raise

    await complete_with_retry(task_id)


async def aggregate_not_found_demo():
    """Demonstrate AggregateNotFoundError handling."""
    print("\n=== AggregateNotFoundError Demo ===\n")

    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )

    random_id = uuid4()

    # Option 1: Check existence first
    print("Option 1: Check with exists()")
    if await repo.exists(random_id):
        task = await repo.load(random_id)
    else:
        print(f"  Task {random_id} does not exist")

    # Option 2: Try/except
    print("\nOption 2: Try/except")
    try:
        task = await repo.load(random_id)
    except AggregateNotFoundError as e:
        print(f"  AggregateNotFoundError caught!")
        print(f"  Aggregate ID: {e.aggregate_id}")
        print(f"  Aggregate Type: {e.aggregate_type}")


async def unhandled_event_demo():
    """Demonstrate UnhandledEventError in strict mode."""
    print("\n=== UnhandledEventError Demo ===\n")

    task = StrictTaskAggregate(uuid4())

    # This works - we have a handler
    created = TaskCreated(
        aggregate_id=task.aggregate_id,
        title="Test",
        aggregate_version=1,
    )
    task.apply_event(created, is_new=False)
    print("TaskCreated handled successfully")

    # This will fail - no handler in strict mode
    archived = TaskArchived(
        aggregate_id=task.aggregate_id,
        aggregate_version=2,
    )

    try:
        task.apply_event(archived, is_new=False)
    except UnhandledEventError as e:
        print(f"\nUnhandledEventError caught!")
        print(f"  Event type: {e.event_type}")
        print(f"  Handler class: {e.handler_class}")
        print(f"  Available handlers: {e.available_handlers}")


async def dlq_demo():
    """Demonstrate Dead Letter Queue usage."""
    print("\n=== Dead Letter Queue Demo ===\n")

    dlq_repo = InMemoryDLQRepository()

    # Simulate a failed event
    failed_event_id = uuid4()
    error = ValueError("Projection processing failed: invalid data")

    await dlq_repo.add_failed_event(
        event_id=failed_event_id,
        projection_name="TaskListProjection",
        event_type="TaskCreated",
        event_data={"title": "Test Task", "aggregate_id": str(uuid4())},
        error=error,
        retry_count=3,
    )
    print("Added failed event to DLQ")

    # Get statistics
    stats = await dlq_repo.get_failure_stats()
    print(f"\nDLQ Statistics:")
    print(f"  Total failed: {stats.total_failed}")
    print(f"  Affected projections: {stats.affected_projections}")

    # Get failed events
    failed = await dlq_repo.get_failed_events()
    print(f"\nFailed events: {len(failed)}")
    for entry in failed:
        print(f"  - Event {entry.event_id}")
        print(f"    Projection: {entry.projection_name}")
        print(f"    Error: {entry.error_message}")
        print(f"    Retry count: {entry.retry_count}")

    # Mark as resolved
    await dlq_repo.mark_resolved(dlq_id=failed[0].id, resolved_by="admin")
    print(f"\nMarked entry {failed[0].id} as resolved")


async def main():
    await optimistic_lock_demo()
    await optimistic_lock_retry()
    await aggregate_not_found_demo()
    await unhandled_event_demo()
    await dlq_demo()


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_09_error_handling.py` and run it:

```bash
python tutorial_09_error_handling.py
```

---

## Exercises

### Exercise 1: Implement Retry Logic

**Objective:** Implement a generic retry mechanism for concurrent updates.

**Time:** 15-20 minutes

**Requirements:**

1. Create a `save_with_retry()` function that:
   - Takes a repository, aggregate ID, and an operation function
   - Loads the aggregate, applies the operation, and saves
   - Retries up to 3 times on `OptimisticLockError`
   - Re-raises if all retries are exhausted
2. Use exponential backoff between retries
3. Test with simulated concurrent updates

**Starter Code:**

```python
"""
Tutorial 9 - Exercise 1: Retry Logic Implementation

Your task: Implement save_with_retry() with exponential backoff.
"""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
    OptimisticLockError,
)


@register_event
class CounterIncremented(DomainEvent):
    event_type: str = "CounterIncremented"
    aggregate_type: str = "Counter"


class CounterState(BaseModel):
    counter_id: UUID
    value: int = 0


class CounterAggregate(AggregateRoot[CounterState]):
    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            if self._state is None:
                self._state = CounterState(counter_id=self.aggregate_id, value=1)
            else:
                self._state = self._state.model_copy(update={
                    "value": self._state.value + 1,
                })

    def increment(self) -> None:
        self.apply_event(CounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


async def save_with_retry(
    repo: AggregateRepository,
    aggregate_id: UUID,
    operation,  # Callable that modifies aggregate
    max_retries: int = 3,
    initial_delay: float = 0.01,
) -> None:
    """
    Save an aggregate with automatic retry on optimistic lock conflicts.

    TODO: Implement the following:
    1. Loop for max_retries attempts
    2. Load the aggregate fresh each time
    3. Apply the operation
    4. Save the aggregate
    5. On OptimisticLockError:
       - If not final attempt, wait with exponential backoff and retry
       - If final attempt, re-raise the error
    """
    pass  # TODO: Implement


async def main():
    print("=== Retry Logic Exercise ===\n")

    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
        aggregate_type="Counter",
    )

    # Create counter
    counter_id = uuid4()
    counter = repo.create_new(counter_id)
    counter.increment()  # Start at 1
    await repo.save(counter)
    print(f"Counter created with value: {counter.state.value}\n")

    # Simulate concurrent increments
    async def concurrent_increment():
        await save_with_retry(
            repo,
            counter_id,
            lambda agg: agg.increment(),
            max_retries=3,
        )

    # Run multiple increments concurrently
    print("Running 5 concurrent increments...")
    await asyncio.gather(*[concurrent_increment() for _ in range(5)])

    # Check final value
    final = await repo.load(counter_id)
    print(f"\nFinal counter value: {final.state.value}")
    print(f"(Started at 1, incremented 5 times = expected 6)")


if __name__ == "__main__":
    asyncio.run(main())
```

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 9 - Exercise 1 Solution: Retry Logic Implementation
"""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
    OptimisticLockError,
)


@register_event
class CounterIncremented(DomainEvent):
    event_type: str = "CounterIncremented"
    aggregate_type: str = "Counter"


class CounterState(BaseModel):
    counter_id: UUID
    value: int = 0


class CounterAggregate(AggregateRoot[CounterState]):
    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            if self._state is None:
                self._state = CounterState(counter_id=self.aggregate_id, value=1)
            else:
                self._state = self._state.model_copy(update={
                    "value": self._state.value + 1,
                })

    def increment(self) -> None:
        self.apply_event(CounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


async def save_with_retry(
    repo: AggregateRepository,
    aggregate_id: UUID,
    operation,  # Callable that modifies aggregate
    max_retries: int = 3,
    initial_delay: float = 0.01,
) -> None:
    """
    Save an aggregate with automatic retry on optimistic lock conflicts.

    Args:
        repo: The aggregate repository
        aggregate_id: ID of the aggregate to modify
        operation: Function that takes aggregate and modifies it
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay before first retry (seconds)

    Raises:
        OptimisticLockError: If all retries exhausted
    """
    for attempt in range(max_retries):
        try:
            # Step 1: Load fresh aggregate
            aggregate = await repo.load(aggregate_id)

            # Step 2: Apply operation
            operation(aggregate)

            # Step 3: Save
            await repo.save(aggregate)

            print(f"Saved successfully on attempt {attempt + 1}")
            return

        except OptimisticLockError:
            print(f"Conflict on attempt {attempt + 1}/{max_retries}")

            # Step 4: Check if we should retry
            if attempt == max_retries - 1:
                # Step 5: Re-raise if all retries exhausted
                print("All retries exhausted!")
                raise

            # Exponential backoff
            delay = initial_delay * (2 ** attempt)
            await asyncio.sleep(delay)


async def main():
    print("=== Retry Logic Exercise ===\n")

    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
        aggregate_type="Counter",
    )

    # Create counter
    counter_id = uuid4()
    counter = repo.create_new(counter_id)
    counter.increment()  # Start at 1
    await repo.save(counter)
    print(f"Counter created with value: {counter.state.value}\n")

    # Simulate concurrent increments
    async def concurrent_increment():
        await save_with_retry(
            repo,
            counter_id,
            lambda agg: agg.increment(),
            max_retries=3,
        )

    # Run multiple increments concurrently
    print("Running 5 concurrent increments...")
    await asyncio.gather(*[concurrent_increment() for _ in range(5)])

    # Check final value
    final = await repo.load(counter_id)
    print(f"\nFinal counter value: {final.state.value}")
    print(f"(Started at 1, incremented 5 times = expected 6)")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

The solution file is also available at: `docs/tutorials/exercises/solutions/09-1.py`

---

## Summary

In this tutorial, you learned:

- **EventSourceError** is the base for all library exceptions
- **OptimisticLockError** requires reload and retry when concurrent modifications conflict
- **AggregateNotFoundError** means the aggregate does not exist (no events)
- **UnhandledEventError** occurs in strict mode without handlers
- **Retry policies** provide configurable backoff strategies for projections
- **Dead Letter Queues** store failed events for investigation and reprocessing

---

## Key Takeaways

!!! warning "Important"
    - Always handle `OptimisticLockError` in concurrent scenarios
    - Use `exists()` or try/except for aggregate lookups
    - Consider strict mode (`unregistered_event_handling="error"`) during development
    - Log all errors with context for debugging
    - Monitor your DLQ for system health

!!! tip "Best Practice"
    When retrying after `OptimisticLockError`, always reload the aggregate first. The state may have changed in ways that make your original operation invalid.

!!! note "Production Consideration"
    In production, use `PostgreSQLDLQRepository` instead of `InMemoryDLQRepository` to persist failed events across application restarts.

---

## Next Steps

You now understand how to handle errors gracefully in event-sourced applications. Continue to [Tutorial 10: Checkpoint Management](10-checkpoints.md) to learn how projections track their progress through the event stream.

---

## Related Documentation

- [Error Handling Guide](../guides/error-handling.md) - Comprehensive error handling strategies
- [Tutorial 6: Projections](06-projections.md) - Projection fundamentals
- [Tutorial 15: Production Patterns](15-production.md) - Production error handling
