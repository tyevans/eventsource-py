# Tutorial 9: Error Handling and Resilience

**Difficulty:** Intermediate

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- [Tutorial 6: Projections](06-projections.md)
- Python 3.10 or higher
- Understanding of async/await patterns

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Understand the eventsource-py exception hierarchy
2. Handle optimistic concurrency conflicts with retry logic
3. Implement graceful error handling in projections
4. Use the Dead Letter Queue (DLQ) for failed events
5. Apply retry strategies with exponential backoff
6. Use circuit breakers to prevent cascading failures
7. Classify errors as transient vs permanent
8. Monitor error rates and health status

---

## Event Sourcing Error Philosophy

Event sourcing has a clear separation of concerns for error handling:

**Commands fail fast**: Business rule violations in aggregates raise exceptions before events are emitted. Commands can and should fail.

**Events never fail**: Events are historical facts that always succeed when applied. Event handlers should be resilient and never throw exceptions that stop processing.

**Projections are resilient**: Projections handle errors gracefully, using retry logic, DLQs, and error callbacks to prevent data loss.

---

## Exception Hierarchy

eventsource-py provides a comprehensive exception hierarchy for different error scenarios.

### Core Exceptions

All library exceptions inherit from `EventSourceError`:

```python
from eventsource import (
    EventSourceError,           # Base exception
    OptimisticLockError,        # Concurrent modification conflict
    AggregateNotFoundError,     # Aggregate doesn't exist
    EventNotFoundError,         # Event not found by ID
    EventVersionError,          # Event version mismatch
    UnhandledEventError,        # No @handles for event type
    ProjectionError,            # Projection processing failure
    EventStoreError,            # Event store operation failure
    EventBusError,              # Event bus operation failure
    CheckpointError,            # Checkpoint operation failure
    SerializationError,         # Event serialization failure
)
```

### Subscription Exceptions

Subscription-specific errors:

```python
from eventsource.subscriptions.exceptions import (
    SubscriptionError,              # Base subscription exception
    SubscriptionConfigError,        # Invalid configuration
    SubscriptionStateError,         # Invalid state transition
    SubscriptionAlreadyExistsError, # Duplicate subscription name
    CheckpointNotFoundError,        # Missing checkpoint
    EventStoreConnectionError,      # Event store unavailable
    EventBusConnectionError,        # Event bus unavailable
    TransitionError,                # Catch-up to live transition failed
)
```

### Read Model Exceptions

Read model repository errors (note: different from aggregate `OptimisticLockError`):

```python
from eventsource.readmodels.exceptions import (
    ReadModelError,             # Base read model exception
    OptimisticLockError,        # Read model version conflict
    ReadModelNotFoundError,     # Read model doesn't exist
)
```

### Snapshot Exceptions

Snapshot-related errors (typically handled internally):

```python
from eventsource.snapshots.exceptions import (
    SnapshotError,                  # Base snapshot exception
    SnapshotDeserializationError,   # Snapshot data invalid
    SnapshotSchemaVersionError,     # Schema version mismatch
    SnapshotNotFoundError,          # Snapshot doesn't exist
)
```

---

## Optimistic Concurrency Control

The most common error in event sourcing is `OptimisticLockError`, which occurs when two processes try to modify the same aggregate simultaneously.

### Understanding OptimisticLockError

```python
from uuid import UUID, uuid4
from eventsource import OptimisticLockError

# Error has detailed information
try:
    await repo.save(task)
except OptimisticLockError as e:
    print(f"Aggregate: {e.aggregate_id}")     # UUID of conflicted aggregate
    print(f"Expected: v{e.expected_version}") # Version we tried to save
    print(f"Actual: v{e.actual_version}")     # Current version in store
```

### Manual Retry Pattern

The simplest retry approach is manual:

```python
from eventsource import OptimisticLockError, AggregateRepository

async def complete_task_with_retry(
    repo: AggregateRepository,
    task_id: UUID,
    user_id: UUID,
    max_retries: int = 3,
) -> None:
    """Complete a task with automatic retry on conflicts."""
    for attempt in range(max_retries):
        try:
            # Load aggregate (gets latest version)
            task = await repo.load(task_id)

            # Check if already completed (idempotency)
            if task.state.status == "completed":
                return

            # Execute command
            task.complete(user_id)

            # Save (may conflict with concurrent modification)
            await repo.save(task)

            return  # Success!

        except OptimisticLockError as e:
            if attempt == max_retries - 1:
                # Final attempt failed
                raise

            # Log and retry with fresh state
            print(f"Conflict on attempt {attempt + 1}, retrying...")
            continue  # Loop reloads aggregate with latest version
```

**Key points:**
- Each retry loads fresh state from the event store
- Idempotency checks prevent duplicate operations
- Final attempt re-raises the exception
- Commands are re-validated on each attempt

### Using Retry Utilities

eventsource-py provides built-in retry utilities:

```python
from eventsource.subscriptions.retry import (
    RetryConfig,
    retry_async,
)

async def complete_task_robust(
    repo: AggregateRepository,
    task_id: UUID,
    user_id: UUID,
) -> None:
    """Complete task with exponential backoff retry."""

    config = RetryConfig(
        max_retries=5,           # Retry up to 5 times
        initial_delay=0.1,       # Start with 100ms delay
        max_delay=5.0,           # Cap at 5 second delay
        exponential_base=2.0,    # Double delay each time
        jitter=0.1,              # Add 10% random jitter
    )

    async def operation():
        task = await repo.load(task_id)
        if task.state.status != "completed":
            task.complete(user_id)
            await repo.save(task)

    from eventsource import OptimisticLockError

    await retry_async(
        operation=operation,
        config=config,
        retryable_exceptions=(OptimisticLockError,),
        operation_name="complete_task",
    )
```

**Benefits:**
- Exponential backoff reduces contention
- Jitter prevents thundering herd
- Automatic logging of retry attempts
- Configurable retry behavior

---

## Command Validation Errors

Aggregates enforce business rules by raising `ValueError` or custom exceptions:

```python
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent

class TaskAggregate(AggregateRoot[TaskState]):
    def complete(self, completed_by: UUID) -> None:
        """Mark task as complete."""

        # Validate business rules before emitting events
        if not self.state:
            raise ValueError("Task does not exist")

        if self.state.status == "completed":
            raise ValueError("Task already completed")

        if not completed_by:
            raise ValueError("completed_by is required")

        # Rules passed - emit event
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        ))
```

**Handling command errors:**

```python
from uuid import uuid4

async def handle_complete_task_command(
    repo: AggregateRepository,
    task_id: UUID,
    user_id: UUID,
) -> dict[str, Any]:
    """Command handler with error handling."""
    try:
        task = await repo.load(task_id)
        task.complete(user_id)
        await repo.save(task)

        return {
            "success": True,
            "task_id": str(task_id),
            "version": task.version,
        }

    except AggregateNotFoundError:
        return {
            "success": False,
            "error": "Task not found",
            "error_type": "not_found",
        }

    except ValueError as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": "validation_error",
        }

    except OptimisticLockError as e:
        # Retry or return conflict error
        return {
            "success": False,
            "error": "Task was modified by another user",
            "error_type": "conflict",
            "expected_version": e.expected_version,
            "actual_version": e.actual_version,
        }
```

---

## Projection Error Handling

Projections must be resilient to errors to prevent data loss. eventsource-py provides comprehensive error handling for subscriptions.

### Error Classification

The error classifier categorizes exceptions to determine handling strategy:

```python
from eventsource.subscriptions.error_handling import (
    ErrorClassifier,
    ErrorCategory,
    ErrorSeverity,
)

classifier = ErrorClassifier()

# Classify different error types
connection_error = ConnectionError("Network timeout")
classification = classifier.classify(connection_error)

print(f"Category: {classification.category}")     # TRANSIENT
print(f"Severity: {classification.severity}")     # MEDIUM
print(f"Retryable: {classification.retryable}")   # True

# Permanent errors
value_error = ValueError("Invalid data")
classification = classifier.classify(value_error)

print(f"Category: {classification.category}")     # APPLICATION
print(f"Retryable: {classification.retryable}")   # False
```

**Built-in classifications:**
- `ConnectionError`, `TimeoutError`, `OSError` → Transient, retryable
- `ValueError`, `KeyError` → Application, not retryable
- `TypeError`, `AttributeError` → Application (high severity), not retryable
- `MemoryError`, `SystemError` → Infrastructure (critical), not retryable

### Custom Error Classification

```python
from eventsource.subscriptions.error_handling import (
    ErrorClassifier,
    ErrorClassification,
    ErrorCategory,
    ErrorSeverity,
)

classifier = ErrorClassifier()

# Register custom exception type
class TemporaryDatabaseError(Exception):
    pass

classifier.register_classification(
    TemporaryDatabaseError,
    ErrorClassification(
        category=ErrorCategory.TRANSIENT,
        severity=ErrorSeverity.HIGH,
        retryable=True,
        description="Temporary database issue",
    )
)

# Register pattern-based rule
classifier.register_pattern_rule(
    "deadlock",  # Match this substring in error message
    ErrorClassification(
        category=ErrorCategory.TRANSIENT,
        severity=ErrorSeverity.HIGH,
        retryable=True,
        description="Database deadlock detected",
    )
)
```

### Subscription Error Handler

The `SubscriptionErrorHandler` provides unified error handling for projections:

```python
from eventsource.subscriptions.error_handling import (
    SubscriptionErrorHandler,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorSeverity,
)
from eventsource.repositories.dlq import InMemoryDLQRepository

# Create DLQ repository
dlq_repo = InMemoryDLQRepository()

# Configure error handling
config = ErrorHandlingConfig(
    strategy=ErrorHandlingStrategy.RETRY_THEN_DLQ,
    max_recent_errors=100,          # Keep last 100 errors in memory
    max_errors_before_stop=1000,    # Stop after 1000 total errors
    error_rate_threshold=10.0,      # Alert if >10 errors/minute
    dlq_enabled=True,               # Send failed events to DLQ
    notify_on_severity=ErrorSeverity.HIGH,  # Notify on high+ severity
)

# Create error handler
error_handler = SubscriptionErrorHandler(
    subscription_name="TaskProjection",
    config=config,
    dlq_repo=dlq_repo,
)

# Register error callback
async def on_critical_error(error_info):
    """Alert on critical errors."""
    if error_info.classification.severity == ErrorSeverity.CRITICAL:
        print(f"CRITICAL: {error_info.error_message}")
        # Send to monitoring system, PagerDuty, etc.

error_handler.on_error(on_critical_error)

# Use in projection
try:
    await projection.handle(event)
except Exception as e:
    error_info = await error_handler.handle_error(
        error=e,
        stored_event=stored_event,
        retry_count=retry_count,
    )

    # Check if should continue processing
    if not error_handler.should_continue():
        raise RuntimeError("Too many errors, stopping projection")
```

### Error Handling Strategies

```python
from eventsource.subscriptions.error_handling import ErrorHandlingStrategy

# Stop on first error (strict mode)
ErrorHandlingStrategy.STOP

# Log and continue (ignore errors)
ErrorHandlingStrategy.CONTINUE

# Retry with backoff, then continue if all retries fail
ErrorHandlingStrategy.RETRY_THEN_CONTINUE

# Retry with backoff, then send to DLQ (recommended)
ErrorHandlingStrategy.RETRY_THEN_DLQ

# Send to DLQ immediately without retry
ErrorHandlingStrategy.DLQ_ONLY
```

---

## Dead Letter Queue (DLQ)

The DLQ stores events that failed processing after all retry attempts, enabling manual investigation and replay.

### Using the DLQ

```python
from eventsource.repositories.dlq import (
    PostgreSQLDLQRepository,
    InMemoryDLQRepository,
)

# For production (PostgreSQL)
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("postgresql+asyncpg://...")
dlq_repo = PostgreSQLDLQRepository(engine)

# For testing (in-memory)
dlq_repo = InMemoryDLQRepository()

# Add failed event
try:
    await projection.handle(event)
except Exception as e:
    await dlq_repo.add_failed_event(
        event_id=event.event_id,
        projection_name="TaskListProjection",
        event_type=event.event_type,
        event_data=event.to_dict(),
        error=e,
        retry_count=3,
    )
```

### Querying Failed Events

```python
# Get all failed events for a projection
failed_events = await dlq_repo.get_failed_events(
    projection_name="TaskListProjection",
    status="failed",
    limit=100,
)

for entry in failed_events:
    print(f"Event: {entry.event_type}")
    print(f"Error: {entry.error_message}")
    print(f"Retries: {entry.retry_count}")
    print(f"First failed: {entry.first_failed_at}")

# Get specific failed event by DLQ ID
entry = await dlq_repo.get_failed_event_by_id(dlq_id=123)
if entry:
    print(f"Event data: {entry.event_data}")
    print(f"Stacktrace: {entry.error_stacktrace}")
```

### Managing DLQ Entries

```python
# Mark as being retried
await dlq_repo.mark_retrying(dlq_id=123)

# Mark as resolved (after manual fix)
await dlq_repo.mark_resolved(
    dlq_id=123,
    resolved_by="admin@example.com",
)

# Get statistics
stats = await dlq_repo.get_failure_stats()
print(f"Total failed: {stats.total_failed}")
print(f"Total retrying: {stats.total_retrying}")
print(f"Affected projections: {stats.affected_projections}")
print(f"Oldest failure: {stats.oldest_failure}")

# Get failure counts by projection
counts = await dlq_repo.get_projection_failure_counts()
for count in counts:
    print(f"{count.projection_name}: {count.failure_count} failures")
    print(f"  Oldest: {count.oldest_failure}")
    print(f"  Most recent: {count.most_recent_failure}")

# Clean up old resolved events
deleted = await dlq_repo.delete_resolved_events(older_than_days=30)
print(f"Deleted {deleted} resolved events")
```

---

## Retry Strategies

eventsource-py provides flexible retry mechanisms with exponential backoff and jitter.

### RetryConfig

```python
from eventsource.subscriptions.retry import RetryConfig

# Conservative retry (fast failures)
config = RetryConfig(
    max_retries=3,
    initial_delay=0.1,      # 100ms
    max_delay=1.0,          # 1 second max
    exponential_base=2.0,
    jitter=0.1,
)

# Aggressive retry (resilient to longer outages)
config = RetryConfig(
    max_retries=10,
    initial_delay=1.0,      # 1 second
    max_delay=60.0,         # 1 minute max
    exponential_base=2.0,
    jitter=0.2,
)
```

### Calculating Backoff

```python
from eventsource.subscriptions.retry import calculate_backoff, RetryConfig

config = RetryConfig(
    initial_delay=1.0,
    max_delay=60.0,
    exponential_base=2.0,
    jitter=0.1,
)

# Calculate delays for first 5 retries
for attempt in range(5):
    delay = calculate_backoff(attempt, config)
    print(f"Attempt {attempt + 1}: wait {delay:.2f}s")

# Output (approximate, jitter adds randomness):
# Attempt 1: wait 1.05s
# Attempt 2: wait 2.10s
# Attempt 3: wait 4.15s
# Attempt 4: wait 8.05s
# Attempt 5: wait 15.90s
```

### RetryableOperation

```python
from eventsource.subscriptions.retry import (
    RetryableOperation,
    RetryConfig,
)

# Create retryable operation
retry = RetryableOperation(
    config=RetryConfig(max_retries=5),
)

# Execute with retry
result = await retry.execute(
    operation=lambda: fetch_data_from_api(),
    name="fetch_data",
    retryable_exceptions=(ConnectionError, TimeoutError),
)

# Check retry statistics
print(f"Attempts: {retry.stats.attempts}")
print(f"Successes: {retry.stats.successes}")
print(f"Failures: {retry.stats.failures}")
print(f"Total delay: {retry.stats.total_delay_seconds}s")
```

---

## Circuit Breaker Pattern

Circuit breakers prevent repeated calls to failing services, giving them time to recover.

### Basic Circuit Breaker

```python
from eventsource.subscriptions.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)

# Configure circuit breaker
config = CircuitBreakerConfig(
    failure_threshold=5,        # Open after 5 failures
    recovery_timeout=30.0,      # Wait 30s before testing recovery
    half_open_max_calls=1,      # Allow 1 test call in half-open state
)

breaker = CircuitBreaker(config)

# Execute operation through circuit breaker
async def call_external_service():
    return await breaker.execute(
        operation=lambda: http_client.get("/api/data"),
        operation_name="external_api",
    )

# Check circuit state
print(f"State: {breaker.state}")           # CLOSED, OPEN, or HALF_OPEN
print(f"Is open: {breaker.is_open}")
print(f"Failures: {breaker.failure_count}")
```

### Circuit Breaker States

```
CLOSED → Normal operation, requests flow through
   ↓ (failure_threshold reached)
OPEN → Requests blocked, service given time to recover
   ↓ (recovery_timeout elapsed)
HALF_OPEN → Test if service recovered (limited requests)
   ↓ (success)        ↓ (failure)
CLOSED              OPEN
```

### Combining Retry and Circuit Breaker

```python
from eventsource.subscriptions.retry import (
    RetryableOperation,
    RetryConfig,
    CircuitBreaker,
    CircuitBreakerConfig,
)

# Create circuit breaker
breaker = CircuitBreaker(
    CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=30.0,
    )
)

# Create retryable operation with circuit breaker
retry = RetryableOperation(
    config=RetryConfig(max_retries=3),
    circuit_breaker=breaker,
)

# Execute with both retry and circuit breaker protection
try:
    result = await retry.execute(
        operation=lambda: call_external_api(),
        name="api_call",
    )
except CircuitBreakerOpenError as e:
    print(f"Circuit open, try again at {e.recovery_time}")
except RetryError as e:
    print(f"All retries exhausted after {e.attempts} attempts")
```

---

## Error Monitoring and Health Checks

Track error statistics and health status for monitoring.

### Error Statistics

```python
from eventsource.subscriptions.error_handling import ErrorStats

stats = error_handler.stats

print(f"Total errors: {stats.total_errors}")
print(f"Transient errors: {stats.transient_errors}")
print(f"Permanent errors: {stats.permanent_errors}")
print(f"DLQ count: {stats.dlq_count}")
print(f"Error rate: {stats.error_rate_per_minute} errors/min")

# Errors by type
for error_type, count in stats.errors_by_type.items():
    print(f"  {error_type}: {count}")

# Errors by category
for category, count in stats.errors_by_category.items():
    print(f"  {category}: {count}")
```

### Health Status

```python
# Get health status
health = error_handler.get_health_status()

if health["healthy"]:
    print("Projection is healthy")
else:
    print("Projection has issues:")
    for error in health["errors"]:
        print(f"  ERROR: {error}")

for warning in health["warnings"]:
    print(f"  WARNING: {warning}")

# Health status includes:
# - healthy: bool
# - warnings: list[str]
# - errors: list[str]
# - stats: dict (error statistics)
# - recent_error_count: int
```

### Recent Errors

```python
# Get recent errors for debugging
recent_errors = error_handler.recent_errors

for error_info in recent_errors[-10:]:  # Last 10 errors
    print(f"Event: {error_info.event_type}")
    print(f"Error: {error_info.error_message}")
    print(f"Category: {error_info.classification.category.value}")
    print(f"Severity: {error_info.classification.severity.value}")
    print(f"Timestamp: {error_info.timestamp}")
    print(f"Sent to DLQ: {error_info.sent_to_dlq}")
    print("---")
```

---

## Complete Working Example

Here's a comprehensive example demonstrating all error handling concepts:

```python
"""
Tutorial 9: Error Handling and Resilience
Run with: python tutorial_09_error_handling.py
"""

import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateNotFoundError,
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    OptimisticLockError,
    register_event,
)
from eventsource.repositories.dlq import InMemoryDLQRepository
from eventsource.subscriptions.error_handling import (
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorSeverity,
    SubscriptionErrorHandler,
)
from eventsource.subscriptions.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    RetryConfig,
    retry_async,
)


# =============================================================================
# Domain Model
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
        self.apply_event(
            TaskCreated(
                aggregate_id=self.aggregate_id,
                title=title,
                aggregate_version=self.get_next_version(),
            )
        )

    def complete(self, completed_by: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")
        if not completed_by:
            raise ValueError("completed_by is required")
        self.apply_event(
            TaskCompleted(
                aggregate_id=self.aggregate_id,
                completed_by=completed_by,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Error Handling Utilities
# =============================================================================


async def complete_task_with_retry(
    repo: AggregateRepository,
    task_id: UUID,
    user_id: UUID,
    max_retries: int = 3,
) -> dict:
    """Complete task with manual retry logic."""
    for attempt in range(max_retries):
        try:
            task = await repo.load(task_id)

            # Idempotency check
            if task.state.status == "completed":
                return {"success": True, "already_completed": True}

            task.complete(user_id)
            await repo.save(task)

            return {
                "success": True,
                "attempt": attempt + 1,
                "version": task.version,
            }

        except OptimisticLockError as e:
            if attempt == max_retries - 1:
                return {
                    "success": False,
                    "error": "Conflict after max retries",
                    "expected_version": e.expected_version,
                    "actual_version": e.actual_version,
                }
            print(f"Conflict on attempt {attempt + 1}, retrying...")
            await asyncio.sleep(0.1)  # Brief delay before retry


async def complete_task_with_retry_utility(
    repo: AggregateRepository,
    task_id: UUID,
    user_id: UUID,
) -> None:
    """Complete task using retry_async utility."""

    config = RetryConfig(
        max_retries=5,
        initial_delay=0.1,
        max_delay=2.0,
        exponential_base=2.0,
        jitter=0.1,
    )

    async def operation():
        task = await repo.load(task_id)
        if task.state.status != "completed":
            task.complete(user_id)
            await repo.save(task)

    await retry_async(
        operation=operation,
        config=config,
        retryable_exceptions=(OptimisticLockError,),
        operation_name="complete_task",
    )


class ResilientProjection:
    """Projection with comprehensive error handling."""

    def __init__(self, dlq_repo: InMemoryDLQRepository):
        self.tasks: dict[UUID, str] = {}
        self.dlq_repo = dlq_repo

        # Configure error handler
        self.error_handler = SubscriptionErrorHandler(
            subscription_name="TaskProjection",
            config=ErrorHandlingConfig(
                strategy=ErrorHandlingStrategy.RETRY_THEN_DLQ,
                dlq_enabled=True,
                notify_on_severity=ErrorSeverity.HIGH,
            ),
            dlq_repo=dlq_repo,
        )

        # Register error callback
        async def on_error(error_info):
            print(f"Error handling event: {error_info.error_message}")

        self.error_handler.on_error(on_error)

    async def handle(self, event: DomainEvent) -> None:
        """Handle event with error handling."""
        try:
            if isinstance(event, TaskCreated):
                self.tasks[event.aggregate_id] = event.title
            elif isinstance(event, TaskCompleted):
                # Simulate occasional error
                if event.aggregate_id not in self.tasks:
                    raise KeyError(f"Task {event.aggregate_id} not found in projection")

        except Exception as e:
            # Use error handler to classify, log, and handle error
            await self.error_handler.handle_error(
                error=e,
                event=event,
            )

            # Check if should continue processing
            if not self.error_handler.should_continue():
                raise RuntimeError("Too many errors, stopping projection")


# =============================================================================
# Main Demonstration
# =============================================================================


async def main():
    """Demonstrate error handling patterns."""

    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )
    dlq_repo = InMemoryDLQRepository()

    print("=" * 60)
    print("Tutorial 9: Error Handling and Resilience")
    print("=" * 60)

    # 1. Command validation errors
    print("\n1. Command Validation Errors")
    print("-" * 60)

    task = repo.create_new(uuid4())
    try:
        task.create("")  # Empty title
    except ValueError as e:
        print(f"✓ Validation error: {e}")

    try:
        task.create("Valid task")
        task.create("Another task")  # Already exists
    except ValueError as e:
        print(f"✓ Business rule error: {e}")

    # 2. Optimistic lock error with retry
    print("\n2. Optimistic Concurrency with Retry")
    print("-" * 60)

    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Test task")
    await repo.save(task)

    result = await complete_task_with_retry(repo, task_id, uuid4())
    print(f"✓ Completed: {result}")

    # Try completing again (idempotency)
    result = await complete_task_with_retry(repo, task_id, uuid4())
    print(f"✓ Idempotent: {result}")

    # 3. Using retry utility
    print("\n3. Using Retry Utility")
    print("-" * 60)

    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Retry test")
    await repo.save(task)

    await complete_task_with_retry_utility(repo, task_id, uuid4())
    print("✓ Completed with retry_async")

    # 4. Projection with error handling
    print("\n4. Resilient Projection with DLQ")
    print("-" * 60)

    projection = ResilientProjection(dlq_repo)

    # Process events
    event1 = TaskCreated(
        aggregate_id=uuid4(),
        title="First task",
        aggregate_version=1,
    )
    await projection.handle(event1)
    print("✓ Event 1 processed")

    # This will error and go to DLQ
    orphan_event = TaskCompleted(
        aggregate_id=uuid4(),  # Not in projection
        completed_by=uuid4(),
        aggregate_version=2,
    )
    await projection.handle(orphan_event)
    print("✓ Event 2 failed and sent to DLQ")

    # Check DLQ
    failed_events = await dlq_repo.get_failed_events(
        projection_name="TaskProjection"
    )
    print(f"✓ DLQ has {len(failed_events)} failed events")

    # 5. Circuit breaker
    print("\n5. Circuit Breaker Pattern")
    print("-" * 60)

    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=1.0,
        )
    )

    async def flaky_operation():
        """Simulated flaky operation."""
        import random

        if random.random() < 0.7:  # 70% failure rate
            raise ConnectionError("Service unavailable")
        return "Success"

    # Try until circuit opens
    for i in range(5):
        try:
            result = await breaker.execute(flaky_operation, "flaky_service")
            print(f"  Attempt {i + 1}: {result}")
        except ConnectionError:
            print(f"  Attempt {i + 1}: Failed (state={breaker.state.value})")

    # Check circuit state
    print(f"✓ Circuit state: {breaker.state.value}")
    print(f"  Failures: {breaker.failure_count}")

    # 6. Error statistics
    print("\n6. Error Statistics")
    print("-" * 60)

    stats = projection.error_handler.stats
    print(f"Total errors: {stats.total_errors}")
    print(f"Transient errors: {stats.transient_errors}")
    print(f"Permanent errors: {stats.permanent_errors}")
    print(f"DLQ count: {stats.dlq_count}")

    # 7. Health status
    print("\n7. Health Status")
    print("-" * 60)

    health = projection.error_handler.get_health_status()
    print(f"Healthy: {health['healthy']}")
    print(f"Warnings: {health['warnings']}")
    print(f"Errors: {health['errors']}")

    print("\n" + "=" * 60)
    print("✓ Tutorial complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

**Run the example:**
```bash
python tutorial_09_error_handling.py
```

---

## Best Practices

### 1. Fail Fast in Commands

```python
# Good: Validate before emitting events
def complete(self, completed_by: UUID) -> None:
    if not self.state:
        raise ValueError("Task does not exist")
    # ... validate all rules ...
    self.apply_event(TaskCompleted(...))

# Bad: Emit events before validation
def complete(self, completed_by: UUID) -> None:
    self.apply_event(TaskCompleted(...))
    if not self.state:
        raise ValueError("Task does not exist")  # Too late!
```

### 2. Idempotency in Retry Logic

```python
# Always check if operation already succeeded
async def operation():
    task = await repo.load(task_id)
    if task.state.status == "completed":
        return  # Already done, don't retry
    task.complete(user_id)
    await repo.save(task)
```

### 3. Use DLQ for Failed Events

```python
# Don't silently swallow projection errors
try:
    await projection.handle(event)
except Exception as e:
    # Log error AND send to DLQ for investigation
    await dlq_repo.add_failed_event(...)
    logger.error(f"Event failed: {e}")
```

### 4. Monitor Error Rates

```python
# Track and alert on error trends
stats = error_handler.stats
if stats.error_rate_per_minute > threshold:
    send_alert("High error rate in TaskProjection")
```

### 5. Use Circuit Breakers for External Services

```python
# Protect external API calls with circuit breaker
breaker = CircuitBreaker(config)

async def fetch_user_data(user_id):
    return await breaker.execute(
        lambda: external_api.get_user(user_id),
        "user_api",
    )
```

---

## Common Pitfalls

### 1. Not Handling OptimisticLockError

```python
# Bad: No retry logic
await repo.save(task)  # May fail with OptimisticLockError

# Good: Retry on conflict
for _ in range(max_retries):
    try:
        task = await repo.load(task_id)
        task.complete(user_id)
        await repo.save(task)
        break
    except OptimisticLockError:
        continue
```

### 2. Ignoring Projection Errors

```python
# Bad: Silent failure
try:
    await projection.handle(event)
except Exception:
    pass  # Lost the event!

# Good: Log, DLQ, and monitor
except Exception as e:
    await error_handler.handle_error(e, event)
```

### 3. No Idempotency Checks

```python
# Bad: Duplicate operations on retry
task.complete(user_id)
await repo.save(task)  # May retry and complete twice

# Good: Check current state
if task.state.status != "completed":
    task.complete(user_id)
    await repo.save(task)
```

### 4. Using Wrong Exception Type

```python
# Bad: Using readmodels.OptimisticLockError for aggregates
from eventsource.readmodels.exceptions import OptimisticLockError

# Good: Use eventsource.OptimisticLockError for aggregates
from eventsource import OptimisticLockError
```

---

## Key Takeaways

1. **Commands fail fast**: Validate business rules before emitting events
2. **Events never fail**: Event handlers should be resilient and forgiving
3. **Retry on OptimisticLockError**: Use exponential backoff with idempotency checks
4. **Use error classification**: Distinguish transient vs permanent errors
5. **DLQ for failed events**: Never lose events - store failures for investigation
6. **Circuit breakers for external services**: Prevent cascading failures
7. **Monitor error rates**: Track statistics and health status
8. **Error callbacks for alerting**: Integrate with monitoring systems
9. **Test error scenarios**: Verify retry logic, DLQ, and circuit breakers
10. **Graceful degradation**: Continue operating when optional dependencies fail

---

## Next Steps

You've mastered error handling in event-sourced systems! You now know how to:
- Handle optimistic concurrency conflicts with retry logic
- Build resilient projections with error classification and DLQs
- Use circuit breakers to protect against cascading failures
- Monitor error rates and health status
- Apply best practices for error handling at every layer

Continue to [Tutorial 10: Checkpoints and Resumable Processing](10-checkpoints.md) to learn about:
- Tracking projection progress with checkpoints
- Resuming processing after restarts
- Catch-up vs live processing
- Checkpoint repositories for PostgreSQL and SQLite

For more examples, see:
- `tests/unit/subscriptions/test_error_handling.py` - Comprehensive error handling tests
- `tests/unit/subscriptions/test_retry.py` - Retry and circuit breaker tests
- `tests/integration/readmodels/test_enhanced_features.py` - DLQ integration examples
- `src/eventsource/subscriptions/error_handling.py` - Error handling implementation
