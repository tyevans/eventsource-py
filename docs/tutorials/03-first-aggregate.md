# Tutorial 3: Building Your First Aggregate

**Difficulty:** Beginner

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- Basic Python programming knowledge
- Python 3.10 or higher
- Understanding of Pydantic BaseModel

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain what an aggregate is and why it's important
2. Create aggregates using both `AggregateRoot` and `DeclarativeAggregate`
3. Define aggregate state models with Pydantic
4. Implement event handlers with `_apply()` and `@handles`
5. Write command methods that enforce business rules
6. Manage aggregate versions and uncommitted events
7. Reconstruct aggregates from event history
8. Understand the difference between new events and historical replay

---

## What is an Aggregate?

An **aggregate** is the consistency boundary in event sourcing. It's the cornerstone of your domain model that:

- **Enforces business rules**: Validates that commands are legal before emitting events
- **Maintains state**: Rebuilds current state by replaying all historical events
- **Emits events**: Records what happened as immutable domain events
- **Ensures atomicity**: All changes within an aggregate are atomic

Think of aggregates as the gatekeepers of your domain. They decide what's allowed and record every decision as an event.

### Real-World Analogy

A bank account is a perfect aggregate:
- **State**: Current balance, account holder, status
- **Commands**: Deposit, withdraw, close account
- **Business rules**: Can't withdraw more than balance, can't use closed account
- **Events**: AccountOpened, MoneyDeposited, MoneyWithdrawn, AccountClosed

The aggregate ensures you can't break the rules - if you try to withdraw $100 from an account with $50, the aggregate rejects the command before any event is emitted.

---

## The Aggregate Pattern

Every aggregate follows this pattern:

```
Command → Validate Business Rules → Emit Event → Update State
```

**Key principle:** Commands validate first, then emit events. Events are facts that always succeed in updating state.

```python
# Command method (enforces rules)
def withdraw(self, amount: float) -> None:
    if not self.state:
        raise ValueError("Account does not exist")
    if self.state.balance < amount:
        raise ValueError("Insufficient funds")  # Business rule check

    # Rule passed - emit event
    self.apply_event(MoneyWithdrawn(
        aggregate_id=self.aggregate_id,
        amount=amount,
        aggregate_version=self.get_next_version(),
    ))

# Event handler (updates state)
def _apply(self, event: DomainEvent) -> None:
    if isinstance(event, MoneyWithdrawn):
        # No validation - events are facts
        self._state = self._state.model_copy(
            update={"balance": self._state.balance - event.amount}
        )
```

---

## Building Your First Aggregate

Let's build a task management aggregate step by step.

### Step 1: Define the State Model

The state model represents the current snapshot of your aggregate's data:

```python
from uuid import UUID
from pydantic import BaseModel

class TaskState(BaseModel):
    """Current state of a Task aggregate."""

    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"  # pending, in_progress, completed
    completed_by: UUID | None = None
```

**Important:** State models use Pydantic's `BaseModel` for validation and serialization.

### Step 2: Define Domain Events

Events represent state transitions (from Tutorial 2):

```python
from eventsource import DomainEvent, register_event

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
```

### Step 3: Create the Aggregate (AggregateRoot)

The basic approach uses `AggregateRoot` with an explicit `_apply()` method:

```python
from uuid import UUID, uuid4
from eventsource import AggregateRoot

class TaskAggregate(AggregateRoot[TaskState]):
    """Task aggregate using explicit event handling."""

    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        """Return initial state for a new aggregate."""
        return TaskState(task_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        """Apply event to update state."""
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
```

**Required methods:**
- `_get_initial_state()`: Returns a fresh state instance
- `_apply(event)`: Updates `self._state` based on event type

**Important:** The `_apply()` method is for updating state only. No business rules here - events are facts.

---

## Adding Command Methods

Commands validate business rules and emit events:

```python
class TaskAggregate(AggregateRoot[TaskState]):
    # ... previous code ...

    def create(self, title: str, description: str) -> None:
        """Create a new task."""
        # Business rule: can't create twice
        if self.version > 0:
            raise ValueError("Task already exists")

        # Emit event
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            aggregate_version=self.get_next_version(),
        ))

    def assign(self, user_id: UUID) -> None:
        """Assign task to a user."""
        # Business rules
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot assign completed task")

        # Emit event
        self.apply_event(TaskAssigned(
            aggregate_id=self.aggregate_id,
            assigned_to=user_id,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self, completed_by: UUID) -> None:
        """Mark task as complete."""
        # Business rules
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")

        # Emit event
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        ))
```

**Pattern breakdown:**

1. **Check preconditions**: Validate business rules using current state
2. **Create event**: Build event with required data
3. **Apply event**: Call `self.apply_event()` which:
   - Updates state via `_apply()`
   - Increments version
   - Adds to uncommitted events list

---

## Using the Aggregate

```python
from uuid import uuid4

# Create a new aggregate
task_id = uuid4()
task = TaskAggregate(task_id)

print(f"Version: {task.version}")        # 0 (no events yet)
print(f"State: {task.state}")            # None (not created yet)

# Execute commands
task.create("Learn Event Sourcing", "Complete all tutorials")
print(f"\nVersion: {task.version}")      # 1
print(f"State: {task.state.title}")      # "Learn Event Sourcing"
print(f"Status: {task.state.status}")    # "pending"

user_id = uuid4()
task.assign(user_id)
print(f"\nVersion: {task.version}")      # 2
print(f"Assigned: {task.state.assigned_to}")  # user_id

task.complete(user_id)
print(f"\nVersion: {task.version}")      # 3
print(f"Status: {task.state.status}")    # "completed"

# Check uncommitted events
print(f"\nUncommitted events: {len(task.uncommitted_events)}")  # 3
for event in task.uncommitted_events:
    print(f"  v{event.aggregate_version}: {event.event_type}")
```

**Output:**
```
Version: 0
State: None

Version: 1
State: Learn Event Sourcing
Status: pending

Version: 2
Assigned: [user_id UUID]

Version: 3
Status: completed

Uncommitted events: 3
  v1: TaskCreated
  v2: TaskAssigned
  v3: TaskCompleted
```

---

## Understanding Aggregate Properties

### aggregate_id
The unique identifier for this aggregate instance:

```python
task = TaskAggregate(uuid4())
print(task.aggregate_id)  # UUID assigned at creation
```

### version
The number of events that have been applied:

```python
task = TaskAggregate(uuid4())
print(task.version)  # 0 - no events yet

task.create("New Task", "Description")
print(task.version)  # 1 - one event applied

task.complete(user_id)
print(task.version)  # 2 - two events applied
```

### state
The current state rebuilt from events:

```python
task = TaskAggregate(uuid4())
print(task.state)  # None - no events applied yet

task.create("Task", "Desc")
print(task.state)  # TaskState instance
print(task.state.title)  # "Task"
```

### uncommitted_events
Events that haven't been persisted to the event store:

```python
task.create("Task", "Desc")
print(len(task.uncommitted_events))  # 1

# After saving to event store, repository calls:
task.mark_events_as_committed()
print(len(task.uncommitted_events))  # 0
```

---

## DeclarativeAggregate: The Cleaner Approach

`DeclarativeAggregate` uses the `@handles` decorator for type-safe event routing:

```python
from eventsource import DeclarativeAggregate, handles

class TaskAggregate(DeclarativeAggregate[TaskState]):
    """Task aggregate using declarative event handlers."""

    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    # Event handlers using @handles decorator

    @handles(TaskCreated)
    def _on_task_created(self, event: TaskCreated) -> None:
        self._state = TaskState(
            task_id=self.aggregate_id,
            title=event.title,
            description=event.description,
            status="pending",
        )

    @handles(TaskAssigned)
    def _on_task_assigned(self, event: TaskAssigned) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={"assigned_to": event.assigned_to}
            )

    @handles(TaskCompleted)
    def _on_task_completed(self, event: TaskCompleted) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={
                    "status": "completed",
                    "completed_by": event.completed_by,
                }
            )

    # Command methods are identical to AggregateRoot version
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
```

**Benefits of DeclarativeAggregate:**

- **Type safety**: Each handler method has the exact event type
- **Cleaner code**: No if/elif chains in `_apply()`
- **Better organization**: One method per event type
- **Auto-discovery**: Handlers are automatically registered at class initialization

**When to use which:**

- Use `AggregateRoot` for simple aggregates with few events
- Use `DeclarativeAggregate` for complex aggregates with many events

Both work identically - it's purely a style choice.

---

## Event Replay: Rebuilding State from History

Aggregates reconstruct their state by replaying events in order:

```python
# Create events (simulating what's in the event store)
task_id = uuid4()
user_id = uuid4()

events = [
    TaskCreated(
        aggregate_id=task_id,
        title="Historical Task",
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

# Replay events to rebuild state
task = TaskAggregate(task_id)
task.load_from_history(events)

print(f"Version: {task.version}")                # 3
print(f"Title: {task.state.title}")              # "Historical Task"
print(f"Status: {task.state.status}")            # "completed"
print(f"Uncommitted: {len(task.uncommitted_events)}")  # 0 (historical events)
```

**Key points:**

- `load_from_history()` replays events with `is_new=False`
- Historical events don't get added to uncommitted events
- Final state is identical to applying events live
- This is how the repository loads aggregates from the event store

---

## Version Tracking

Aggregates track version numbers for optimistic concurrency control:

```python
task = TaskAggregate(uuid4())
print(task.version)  # 0

task.create("Task", "Desc")
print(task.version)  # 1
print(task.get_next_version())  # 2

task.complete(user_id)
print(task.version)  # 2
print(task.get_next_version())  # 3
```

**How it works:**

1. New aggregate starts at version 0
2. Each `apply_event()` sets version to `event.aggregate_version`
3. Use `get_next_version()` when creating events
4. Event store enforces version matching to prevent conflicts

**Version validation:**

```python
from eventsource.exceptions import EventVersionError

task = TaskAggregate(uuid4())
task.create("Task", "Desc")  # Version becomes 1

# Try to apply event with wrong version
wrong_event = TaskCompleted(
    aggregate_id=task.aggregate_id,
    completed_by=user_id,
    aggregate_version=5,  # Should be 2, not 5
)

try:
    task.apply_event(wrong_event)
except EventVersionError as e:
    print(f"Error: Expected version 2, got {e.actual_version}")
```

---

## State Management Patterns

### Creating New State

When the first event is applied, create a complete new state:

```python
@handles(TaskCreated)
def _on_task_created(self, event: TaskCreated) -> None:
    self._state = TaskState(
        task_id=self.aggregate_id,
        title=event.title,
        description=event.description,
        status="pending",
    )
```

### Updating Existing State

For subsequent events, copy and update:

```python
@handles(TaskCompleted)
def _on_task_completed(self, event: TaskCompleted) -> None:
    if self._state:
        self._state = self._state.model_copy(
            update={
                "status": "completed",
                "completed_by": event.completed_by,
            }
        )
```

**Why `model_copy()`?**

Pydantic models are mutable by default, but we want immutability for state tracking. `model_copy()` creates a new instance with updated fields.

### Defensive Checks

Always check if state exists before updating:

```python
@handles(TaskAssigned)
def _on_task_assigned(self, event: TaskAssigned) -> None:
    if self._state:  # Only update if state exists
        self._state = self._state.model_copy(
            update={"assigned_to": event.assigned_to}
        )
```

This protects against applying events to uninitialized aggregates.

---

## Business Rule Validation

Business rules belong in **command methods**, not event handlers:

```python
def complete(self, completed_by: UUID) -> None:
    """Mark task as complete."""

    # ✓ GOOD: Validate business rules in command
    if not self.state:
        raise ValueError("Task does not exist")
    if self.state.status == "completed":
        raise ValueError("Task already completed")
    if self.state.assigned_to is None:
        raise ValueError("Cannot complete unassigned task")

    # Rules passed - emit event
    self.apply_event(TaskCompleted(
        aggregate_id=self.aggregate_id,
        completed_by=completed_by,
        aggregate_version=self.get_next_version(),
    ))

@handles(TaskCompleted)
def _on_task_completed(self, event: TaskCompleted) -> None:
    # ✗ BAD: Don't validate in event handler
    # Events are facts - they always succeed

    # ✓ GOOD: Just update state
    if self._state:
        self._state = self._state.model_copy(
            update={
                "status": "completed",
                "completed_by": event.completed_by,
            }
        )
```

**Why this matters:**

- Commands can fail (invalid state, business rule violation)
- Events never fail (they're historical facts)
- When replaying events, we don't want validation errors

---

## Complete Working Example

Here's a runnable example demonstrating all concepts:

```python
"""
Tutorial 3: Building Your First Aggregate
Run with: python tutorial_03_aggregate.py
"""

from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DeclarativeAggregate,
    DomainEvent,
    handles,
    register_event,
)


# =============================================================================
# Events
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


# =============================================================================
# State
# =============================================================================


class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"
    completed_by: UUID | None = None


# =============================================================================
# Aggregate (using DeclarativeAggregate)
# =============================================================================


class TaskAggregate(DeclarativeAggregate[TaskState]):
    """Task aggregate with declarative event handlers."""

    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    # Event handlers

    @handles(TaskCreated)
    def _on_task_created(self, event: TaskCreated) -> None:
        self._state = TaskState(
            task_id=self.aggregate_id,
            title=event.title,
            description=event.description,
            status="pending",
        )

    @handles(TaskAssigned)
    def _on_task_assigned(self, event: TaskAssigned) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={"assigned_to": event.assigned_to}
            )

    @handles(TaskCompleted)
    def _on_task_completed(self, event: TaskCompleted) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={
                    "status": "completed",
                    "completed_by": event.completed_by,
                }
            )

    # Command methods

    def create(self, title: str, description: str) -> None:
        """Create a new task."""
        if self.version > 0:
            raise ValueError("Task already exists")

        self.apply_event(
            TaskCreated(
                aggregate_id=self.aggregate_id,
                title=title,
                description=description,
                aggregate_version=self.get_next_version(),
            )
        )

    def assign(self, user_id: UUID) -> None:
        """Assign task to a user."""
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot assign completed task")

        self.apply_event(
            TaskAssigned(
                aggregate_id=self.aggregate_id,
                assigned_to=user_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def complete(self, completed_by: UUID) -> None:
        """Mark task as complete."""
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")

        self.apply_event(
            TaskCompleted(
                aggregate_id=self.aggregate_id,
                completed_by=completed_by,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Demo
# =============================================================================


def main() -> None:
    """Demonstrate aggregate usage."""

    print("=" * 60)
    print("Tutorial 3: Building Your First Aggregate")
    print("=" * 60)

    # 1. Create aggregate
    print("\n1. Creating a new task")
    task_id = uuid4()
    task = TaskAggregate(task_id)

    print(f"   Task ID: {task_id}")
    print(f"   Version: {task.version}")
    print(f"   State: {task.state}")

    # 2. Execute commands
    print("\n2. Creating task")
    task.create("Learn Event Sourcing", "Complete all tutorials")

    print(f"   Version: {task.version}")
    print(f"   Title: {task.state.title}")
    print(f"   Status: {task.state.status}")

    # 3. Assign task
    print("\n3. Assigning task")
    user_id = uuid4()
    task.assign(user_id)

    print(f"   Version: {task.version}")
    print(f"   Assigned to: {task.state.assigned_to}")

    # 4. Complete task
    print("\n4. Completing task")
    task.complete(user_id)

    print(f"   Version: {task.version}")
    print(f"   Status: {task.state.status}")
    print(f"   Completed by: {task.state.completed_by}")

    # 5. Show uncommitted events
    print("\n5. Uncommitted events")
    print(f"   Count: {len(task.uncommitted_events)}")
    for event in task.uncommitted_events:
        print(f"   - v{event.aggregate_version}: {event.event_type}")

    # 6. Business rule validation
    print("\n6. Testing business rules")
    try:
        task.complete(user_id)  # Already completed
    except ValueError as e:
        print(f"   ✓ Rule enforced: {e}")

    # 7. Event replay
    print("\n7. Replaying events from history")
    events = task.uncommitted_events.copy()

    new_task = TaskAggregate(task_id)
    new_task.load_from_history(events)

    print(f"   Version: {new_task.version}")
    print(f"   Status: {new_task.state.status}")
    print(f"   Uncommitted events: {len(new_task.uncommitted_events)}")

    print("\n" + "=" * 60)
    print("✓ Tutorial complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
```

**Expected output:**
```
============================================================
Tutorial 3: Building Your First Aggregate
============================================================

1. Creating a new task
   Task ID: [UUID]
   Version: 0
   State: None

2. Creating task
   Version: 1
   Title: Learn Event Sourcing
   Status: pending

3. Assigning task
   Version: 2
   Assigned to: [UUID]

4. Completing task
   Version: 3
   Status: completed
   Completed by: [UUID]

5. Uncommitted events
   Count: 3
   - v1: TaskCreated
   - v2: TaskAssigned
   - v3: TaskCompleted

6. Testing business rules
   ✓ Rule enforced: Task already completed

7. Replaying events from history
   Version: 3
   Status: completed
   Uncommitted events: 0

============================================================
✓ Tutorial complete!
============================================================
```

---

## Advanced Patterns

### Schema Versioning

When your state model changes, increment `schema_version` to invalidate old snapshots:

```python
class TaskAggregate(DeclarativeAggregate[TaskState]):
    aggregate_type = "Task"
    schema_version = 2  # Increment when TaskState changes

    # ... rest of aggregate ...
```

This ensures snapshots with incompatible schemas are discarded.

### Unregistered Event Handling

Control what happens when an event has no handler:

```python
class TaskAggregate(DeclarativeAggregate[TaskState]):
    aggregate_type = "Task"
    unregistered_event_handling = "warn"  # "ignore" | "warn" | "error"

    # ... handlers ...
```

Options:
- `"ignore"`: Silently skip (default, supports forward compatibility)
- `"warn"`: Log a warning
- `"error"`: Raise `UnhandledEventError`

### Disabling Version Validation

For special cases, you can disable strict version checking:

```python
class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"
    validate_versions = False  # Logs warnings instead of raising errors

    # ... rest of aggregate ...
```

Use this cautiously - version validation prevents concurrency bugs.

---

## Common Pitfalls

### 1. Validating in Event Handlers

```python
# ✗ BAD: Don't do this
@handles(TaskCompleted)
def _on_task_completed(self, event: TaskCompleted) -> None:
    if self._state.status == "completed":
        raise ValueError("Already completed")  # NO!
    # ...
```

Events are facts from the past - they always succeed. Validate in commands only.

### 2. Not Checking State Existence

```python
# ✗ BAD: Assumes state exists
@handles(TaskAssigned)
def _on_task_assigned(self, event: TaskAssigned) -> None:
    self._state = self._state.model_copy(...)  # Fails if _state is None

# ✓ GOOD: Check first
@handles(TaskAssigned)
def _on_task_assigned(self, event: TaskAssigned) -> None:
    if self._state:
        self._state = self._state.model_copy(...)
```

### 3. Mutating State Directly

```python
# ✗ BAD: Direct mutation
@handles(TaskAssigned)
def _on_task_assigned(self, event: TaskAssigned) -> None:
    self._state.assigned_to = event.assigned_to  # Don't mutate

# ✓ GOOD: Create new instance
@handles(TaskAssigned)
def _on_task_assigned(self, event: TaskAssigned) -> None:
    if self._state:
        self._state = self._state.model_copy(
            update={"assigned_to": event.assigned_to}
        )
```

### 4. Forgetting get_next_version()

```python
# ✗ BAD: Hardcoded version
self.apply_event(TaskCreated(
    aggregate_id=self.aggregate_id,
    aggregate_version=1,  # Wrong if aggregate already has events
    ...
))

# ✓ GOOD: Use get_next_version()
self.apply_event(TaskCreated(
    aggregate_id=self.aggregate_id,
    aggregate_version=self.get_next_version(),
    ...
))
```

---

## Key Takeaways

1. **Aggregates enforce business rules**: Commands validate, events record facts
2. **State is rebuilt from events**: Replay all events to get current state
3. **Use Pydantic for state models**: Validation and serialization built-in
4. **Two approaches available**: `AggregateRoot` (explicit) vs `DeclarativeAggregate` (@handles)
5. **Version tracking is critical**: Enables optimistic concurrency control
6. **Commands can fail, events cannot**: Events are immutable historical facts
7. **Uncommitted events need saving**: Use repository (Tutorial 4) to persist
8. **Check state existence**: Always verify `self._state` before updating

---

## Next Steps

Now that you understand aggregates, you're ready to persist them to an event store and load them back.

Continue to [Tutorial 4: Event Stores and Repositories](04-event-stores.md) to learn about:
- Storing events in PostgreSQL and in-memory stores
- Loading aggregates from event history
- Optimistic concurrency control
- Repository pattern for aggregate lifecycle management

For more examples, see:
- `examples/aggregate_example.py` - Advanced patterns with shopping cart
- `tests/unit/test_aggregate_root.py` - Comprehensive test examples
