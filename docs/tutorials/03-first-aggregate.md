# Tutorial 3: Building Your First Aggregate

**Estimated Time:** 45-60 minutes
**Difficulty:** Beginner
**Progress:** Tutorial 3 of 21 | Phase 1: Foundations

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- Completed [Tutorial 2: Your First Domain Event](02-first-event.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial builds directly on the events you created in Tutorial 2. We will use `TaskCreated`, `TaskCompleted`, and `TaskReassigned` events to build a complete `TaskAggregate`.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Define a state model using Pydantic that represents aggregate state
2. Create an aggregate using `AggregateRoot` that applies events to update state
3. Implement command methods that enforce business rules and emit events
4. Choose between `AggregateRoot` and `DeclarativeAggregate` patterns
5. Understand how aggregates serve as consistency boundaries in event sourcing

---

## What is an Aggregate?

An **aggregate** is the heart of an event-sourced system. It is where your business logic lives - the place where commands are validated, business rules are enforced, and events are emitted.

In Tutorial 1, we introduced aggregates as "consistency boundaries." Let us unpack what that means:

### The Consistency Boundary

Imagine you are building an e-commerce system. An order has line items, shipping information, and payment status. When a customer adds an item to their order, you need to:

1. Verify the order exists and is not yet shipped
2. Check that the item is in stock
3. Update the order total
4. Record what happened

All of these operations must succeed or fail together. You cannot have an order that shows an item was added but the total was not updated. This atomic unit of consistency is what we call an aggregate.

The Order is an aggregate. Its boundary includes everything that must be consistent together: the order itself and its line items. Operations on the order go through the Order aggregate, which ensures all business rules are enforced.

### What Aggregates Do

Aggregates have three key responsibilities:

1. **Receive Commands**: Commands are requests to do something - "create this task," "complete this order," "reassign this ticket." The aggregate receives these commands and decides whether to accept them.

2. **Enforce Business Rules**: Before accepting a command, the aggregate validates it against business rules. "You cannot complete a task that does not exist." "You cannot ship an order that has not been paid." These invariants are enforced by the aggregate.

3. **Emit Events**: When a command is valid, the aggregate creates one or more events that describe what happened. These events are the source of truth - they are persisted and can be replayed to reconstruct the aggregate's state.

### State Reconstruction

Here is the key insight of event sourcing: **aggregates do not store their current state directly**. Instead, they store events. When you need to know the current state, you replay all events from the beginning and apply each one to build up the state.

```
Events:                     State After Each Event:
[TaskCreated]         -->   Task exists, status="pending"
[TaskReassigned]      -->   Task exists, status="pending", assigned_to=Bob
[TaskCompleted]       -->   Task exists, status="completed", completed_by=Bob
```

This might seem inefficient, but it provides enormous benefits: complete audit trails, time travel, and the ability to build multiple views of the same data. We will address performance concerns with snapshotting in Tutorial 14.

---

## Building the State Model

Before we create an aggregate, we need a way to represent its current state. In eventsource-py, we use Pydantic models for state, which gives us automatic validation and serialization.

### Why Pydantic for State?

Using Pydantic for state models provides several benefits:

- **Type Safety**: Fields are validated at runtime
- **Immutability**: We can use `model_copy()` to create modified copies
- **Serialization**: State can be easily serialized for snapshots
- **IDE Support**: Full autocomplete and type checking

### Creating TaskState

Let us create a state model for our Task aggregate:

```python
from uuid import UUID

from pydantic import BaseModel


class TaskState(BaseModel):
    """
    Current state of a Task aggregate.

    This model represents the "view" of a task at any point in time.
    It is derived entirely from events - we never store this directly.
    """
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"  # pending, completed
    completed_by: UUID | None = None
```

Notice several things about this model:

1. **Identifying field**: `task_id` identifies which task this state belongs to
2. **Business fields**: `title`, `description`, `assigned_to` capture the domain data
3. **Status tracking**: `status` tracks the task lifecycle
4. **Audit fields**: `completed_by` records who completed the task

The state model captures everything we need to know about a task's current state. It does not include historical information - that is what events are for.

---

## Creating Your First Aggregate

Now let us create the `TaskAggregate` that uses this state model. We will start with the `AggregateRoot` base class, which is the traditional pattern.

### The AggregateRoot Base Class

`AggregateRoot` is a generic class that takes your state type as a type parameter. It provides:

| Member | Type | Description |
|--------|------|-------------|
| `aggregate_id` | property | UUID identifier for this instance |
| `aggregate_type` | class attribute | String type name (e.g., "Task") |
| `version` | property | Number of events applied |
| `state` | property | Current state (TState or None) |
| `uncommitted_events` | property | Events pending persistence |
| `_get_initial_state()` | abstract method | Return initial state |
| `_apply(event)` | abstract method | Update state from event |
| `apply_event(event, is_new)` | method | Apply and optionally track event |
| `get_next_version()` | method | Returns version + 1 |
| `load_from_history(events)` | method | Replay events to rebuild state |

### Implementing TaskAggregate

Let us implement our `TaskAggregate`:

```python
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    register_event,
)


# Events from Tutorial 2
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


# State model
class TaskState(BaseModel):
    """Current state of a Task aggregate."""
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"
    completed_by: UUID | None = None


# The aggregate
class TaskAggregate(AggregateRoot[TaskState]):
    """
    Event-sourced Task aggregate.

    This aggregate:
    - Receives commands (create, complete, reassign)
    - Validates business rules
    - Emits events
    - Updates state by applying events
    """

    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        """Return initial state for new aggregates."""
        return TaskState(task_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        """
        Update state based on event type.

        This method is called for both new events and replayed events.
        It must handle all event types that this aggregate can emit.
        """
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
```

Let us break down the key parts:

1. **Type Parameter**: `AggregateRoot[TaskState]` tells the base class what type our state is. This enables type checking and proper serialization.

2. **aggregate_type**: This string identifies what kind of aggregate this is. It is used when storing events to group them correctly.

3. **_get_initial_state()**: Returns the starting state for a new aggregate. Called internally when needed.

4. **_apply()**: The heart of the aggregate. This method takes an event and updates the internal state accordingly. Notice we use `isinstance()` checks to handle different event types.

### Understanding _apply()

The `_apply()` method is crucial. It must be:

- **Deterministic**: Given the same event, it must always produce the same state change
- **Side-effect free**: It should only update `self._state`, not call external services
- **Complete**: It must handle all event types the aggregate can emit

Why these requirements? Because `_apply()` is called both when new events are created AND when historical events are replayed. If replaying events could produce different results, our system would be broken.

Notice how we use `model_copy(update={...})` to create modified copies of the state. This maintains immutability - we never mutate the existing state object, we create new ones.

---

## Adding Command Methods

So far, our aggregate can apply events, but it has no way to receive commands. Let us add command methods that enforce business rules and emit events.

### The Create Command

```python
def create(self, title: str, description: str, assigned_to: UUID | None = None) -> None:
    """
    Create a new task.

    Business Rules:
    - Cannot create if task already exists (version > 0)

    Args:
        title: The task title
        description: The task description
        assigned_to: Optional user to assign the task to

    Raises:
        ValueError: If the task already exists
    """
    # Enforce business rule
    if self.version > 0:
        raise ValueError("Task already exists")

    # Create the event
    event = TaskCreated(
        aggregate_id=self.aggregate_id,
        title=title,
        description=description,
        assigned_to=assigned_to,
        aggregate_version=self.get_next_version(),
    )

    # Apply the event (this updates state and tracks for persistence)
    self.apply_event(event)
```

Let us examine the pattern:

1. **Validate business rules**: We check that the task does not already exist by checking `self.version`. A version of 0 means no events have been applied yet.

2. **Create the event**: We construct a `TaskCreated` event with all the necessary data. Note the use of `get_next_version()` to set the correct version number.

3. **Apply the event**: Calling `apply_event(event)` does two things:
   - Calls `_apply()` to update our internal state
   - Adds the event to `uncommitted_events` for later persistence

### The Complete Command

```python
def complete(self, completed_by: UUID) -> None:
    """
    Mark the task as completed.

    Business Rules:
    - Task must exist
    - Task cannot be already completed

    Args:
        completed_by: The user completing the task

    Raises:
        ValueError: If the task does not exist or is already completed
    """
    # Validate: task must exist
    if not self.state:
        raise ValueError("Task does not exist")

    # Validate: cannot complete twice
    if self.state.status == "completed":
        raise ValueError("Task is already completed")

    # Create and apply the event
    event = TaskCompleted(
        aggregate_id=self.aggregate_id,
        completed_by=completed_by,
        aggregate_version=self.get_next_version(),
    )
    self.apply_event(event)
```

This command shows more sophisticated validation:

- We check that the task exists by verifying `self.state` is not None
- We check the current status to prevent completing a task twice
- Only if all rules pass do we emit the event

### The Reassign Command

```python
def reassign(self, new_assignee: UUID) -> None:
    """
    Reassign the task to a different user.

    Business Rules:
    - Task must exist
    - Cannot reassign completed tasks

    Args:
        new_assignee: The user to reassign the task to

    Raises:
        ValueError: If the task does not exist or is completed
    """
    if not self.state:
        raise ValueError("Task does not exist")

    if self.state.status == "completed":
        raise ValueError("Cannot reassign a completed task")

    event = TaskReassigned(
        aggregate_id=self.aggregate_id,
        previous_assignee=self.state.assigned_to,
        new_assignee=new_assignee,
        aggregate_version=self.get_next_version(),
    )
    self.apply_event(event)
```

Notice how the `TaskReassigned` event captures both the previous and new assignee. This is a best practice - events should contain all the information needed to understand what happened, including the context (who it was assigned to before).

---

## Testing Your Aggregate (In Memory)

Let us test our aggregate without any persistence. This is one of the great benefits of aggregates - they can be tested in complete isolation.

```python
def main():
    # Create a new task aggregate with a unique ID
    task_id = uuid4()
    user_alice = uuid4()
    user_bob = uuid4()

    # Instantiate the aggregate
    task = TaskAggregate(task_id)

    print(f"Initial state:")
    print(f"  Version: {task.version}")
    print(f"  State: {task.state}")
    print()

    # Create the task
    task.create(
        title="Learn Event Sourcing",
        description="Complete the eventsource-py tutorial series",
        assigned_to=user_alice,
    )

    print(f"After create:")
    print(f"  Version: {task.version}")
    print(f"  Title: {task.state.title}")
    print(f"  Status: {task.state.status}")
    print(f"  Assigned to: {task.state.assigned_to}")
    print(f"  Uncommitted events: {len(task.uncommitted_events)}")
    print()

    # Reassign to Bob
    task.reassign(new_assignee=user_bob)

    print(f"After reassign:")
    print(f"  Version: {task.version}")
    print(f"  Assigned to: {task.state.assigned_to}")
    print()

    # Complete the task
    task.complete(completed_by=user_bob)

    print(f"After complete:")
    print(f"  Version: {task.version}")
    print(f"  Status: {task.state.status}")
    print(f"  Completed by: {task.state.completed_by}")
    print(f"  Total uncommitted events: {len(task.uncommitted_events)}")
    print()

    # Show all uncommitted events
    print("Event history (uncommitted):")
    for i, event in enumerate(task.uncommitted_events, 1):
        print(f"  {i}. {event.event_type} (v{event.aggregate_version})")
    print()

    # Try to complete again - should fail
    print("Attempting to complete again...")
    try:
        task.complete(completed_by=user_alice)
    except ValueError as e:
        print(f"  Business rule enforced: {e}")


if __name__ == "__main__":
    main()
```

When you run this, you will see:

```
Initial state:
  Version: 0
  State: None

After create:
  Version: 1
  Title: Learn Event Sourcing
  Status: pending
  Assigned to: <alice-uuid>
  Uncommitted events: 1

After reassign:
  Version: 2
  Assigned to: <bob-uuid>

After complete:
  Version: 3
  Status: completed
  Completed by: <bob-uuid>
  Total uncommitted events: 3

Event history (uncommitted):
  1. TaskCreated (v1)
  2. TaskReassigned (v2)
  3. TaskCompleted (v3)

Attempting to complete again...
  Business rule enforced: Task is already completed
```

This demonstrates several key concepts:

- The aggregate starts with version 0 and no state
- Each command increments the version
- Events accumulate in `uncommitted_events`
- Business rules are enforced (cannot complete twice)

---

## The DeclarativeAggregate Alternative

eventsource-py provides an alternative to `AggregateRoot` called `DeclarativeAggregate`. Instead of writing a single `_apply()` method with `isinstance` checks, you use the `@handles` decorator to register separate handler methods for each event type.

### Why DeclarativeAggregate?

The declarative pattern offers several advantages:

- **Cleaner code**: Each event type has its own method
- **Better organization**: Event handlers are clearly labeled
- **Easier to extend**: Adding a new event is just adding a new method
- **Reduced boilerplate**: No need for isinstance chains

### Implementing with DeclarativeAggregate

```python
from eventsource import DeclarativeAggregate, handles


class TaskAggregate(DeclarativeAggregate[TaskState]):
    """
    Event-sourced Task aggregate using the declarative pattern.

    Uses @handles decorator instead of isinstance checks.
    """

    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    # Event handlers using @handles decorator

    @handles(TaskCreated)
    def _on_task_created(self, event: TaskCreated) -> None:
        """Handle TaskCreated event."""
        self._state = TaskState(
            task_id=self.aggregate_id,
            title=event.title,
            description=event.description,
            assigned_to=event.assigned_to,
            status="pending",
        )

    @handles(TaskCompleted)
    def _on_task_completed(self, event: TaskCompleted) -> None:
        """Handle TaskCompleted event."""
        if self._state:
            self._state = self._state.model_copy(update={
                "status": "completed",
                "completed_by": event.completed_by,
            })

    @handles(TaskReassigned)
    def _on_task_reassigned(self, event: TaskReassigned) -> None:
        """Handle TaskReassigned event."""
        if self._state:
            self._state = self._state.model_copy(update={
                "assigned_to": event.new_assignee,
            })

    # Command methods remain the same

    def create(self, title: str, description: str, assigned_to: UUID | None = None) -> None:
        if self.version > 0:
            raise ValueError("Task already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            assigned_to=assigned_to,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self, completed_by: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task is already completed")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        ))

    def reassign(self, new_assignee: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot reassign a completed task")
        self.apply_event(TaskReassigned(
            aggregate_id=self.aggregate_id,
            previous_assignee=self.state.assigned_to,
            new_assignee=new_assignee,
            aggregate_version=self.get_next_version(),
        ))
```

### Handling Unregistered Events

`DeclarativeAggregate` provides configuration for handling events that have no registered handler:

```python
class TaskAggregate(DeclarativeAggregate[TaskState]):
    aggregate_type = "Task"

    # Options: "ignore" (default), "warn", "error"
    unregistered_event_handling = "warn"
```

- **"ignore"**: Silently skip events without handlers (default, good for forward compatibility)
- **"warn"**: Log a warning when an event has no handler
- **"error"**: Raise `UnhandledEventError` if an event has no handler (strict mode)

### When to Use Which Pattern

| Scenario | Recommended Pattern |
|----------|---------------------|
| Few event types (3-5) | Either works well |
| Many event types (10+) | DeclarativeAggregate |
| Team prefers explicit routing | AggregateRoot |
| Team prefers convention-based | DeclarativeAggregate |
| Need strict event handling | DeclarativeAggregate with `unregistered_event_handling="error"` |

Both patterns are fully supported and produce identical behavior. Choose based on your team's preferences and the complexity of your domain.

---

## Aggregate Identity and Versioning

### Understanding Aggregate ID

Every aggregate instance has a unique identifier (`aggregate_id`). This UUID:

- Identifies which entity the events belong to
- Is used to load and save the aggregate
- Should be generated once and never changed

```python
# Create a new aggregate with a specific ID
task_id = uuid4()
task = TaskAggregate(task_id)

# The ID is accessible via property
print(task.aggregate_id)  # Same UUID
```

### Understanding Version

The `version` property tracks how many events have been applied to the aggregate:

- Starts at 0 for new aggregates
- Increments by 1 for each event applied
- Used for optimistic locking (preventing concurrent modifications)

```python
task = TaskAggregate(uuid4())
print(task.version)  # 0

task.create("Title", "Description")
print(task.version)  # 1

task.complete(uuid4())
print(task.version)  # 2
```

When persisting events, the event store uses the version to detect conflicts. If two processes try to save events for the same aggregate at the same version, one will fail with an `OptimisticLockError`. We will explore this in Tutorial 4.

### Event Versions

Each event also has an `aggregate_version` field that indicates its position in the aggregate's event stream:

```python
for event in task.uncommitted_events:
    print(f"{event.event_type}: version {event.aggregate_version}")

# Output:
# TaskCreated: version 1
# TaskReassigned: version 2
# TaskCompleted: version 3
```

This version must match the aggregate's version when the event is applied. The `get_next_version()` helper ensures you always set the correct version:

```python
event = TaskCreated(
    aggregate_id=self.aggregate_id,
    title=title,
    aggregate_version=self.get_next_version(),  # Always use this!
)
```

---

## Replaying Events

One of the most powerful aspects of event sourcing is the ability to reconstruct state by replaying events. The `load_from_history()` method does this:

```python
# Suppose we have a list of events from storage
events = [
    TaskCreated(
        aggregate_id=task_id,
        title="My Task",
        description="A task",
        assigned_to=user_id,
        aggregate_version=1,
    ),
    TaskReassigned(
        aggregate_id=task_id,
        previous_assignee=user_id,
        new_assignee=other_user_id,
        aggregate_version=2,
    ),
]

# Create a fresh aggregate and replay the events
task = TaskAggregate(task_id)
task.load_from_history(events)

# The aggregate now has the same state as if we had called the commands
print(task.version)  # 2
print(task.state.assigned_to)  # other_user_id
print(task.uncommitted_events)  # [] - empty! These are historical events
```

Notice that `load_from_history()` does not add events to `uncommitted_events`. The `is_new=False` parameter in `apply_event()` controls this - when replaying history, events are not considered "new" so they are not tracked for persistence.

---

## Complete Example

Here is the complete code for this tutorial, ready to run:

```python
"""
Tutorial 3: Building Your First Aggregate

This example demonstrates creating event-sourced aggregates with eventsource-py.
Run with: python tutorial_03_aggregate.py
"""
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    register_event,
)


# =============================================================================
# Events (from Tutorial 2)
# =============================================================================

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


# =============================================================================
# State Model
# =============================================================================

class TaskState(BaseModel):
    """
    Current state of a Task aggregate.

    This is the "view" that business logic operates on.
    It is derived entirely from events.
    """
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"
    completed_by: UUID | None = None


# =============================================================================
# Aggregate (AggregateRoot Pattern)
# =============================================================================

class TaskAggregate(AggregateRoot[TaskState]):
    """
    Event-sourced Task aggregate using the traditional pattern.

    The aggregate:
    - Receives commands (create, complete, reassign)
    - Validates business rules
    - Emits events
    - Updates state by applying events
    """

    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        """Return initial state for new aggregates."""
        return TaskState(task_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        """
        Update state based on event type.

        This method is called for both new and replayed events.
        """
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

    # =========================================================================
    # Command Methods (Business Logic)
    # =========================================================================

    def create(self, title: str, description: str, assigned_to: UUID | None = None) -> None:
        """
        Create a new task.

        Business Rules:
        - Cannot create if task already exists (version > 0)
        """
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
        """
        Mark the task as completed.

        Business Rules:
        - Task must exist
        - Task cannot be already completed
        """
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task is already completed")

        event = TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def reassign(self, new_assignee: UUID) -> None:
        """
        Reassign the task to a different user.

        Business Rules:
        - Task must exist
        - Cannot reassign completed tasks
        """
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot reassign a completed task")

        event = TaskReassigned(
            aggregate_id=self.aggregate_id,
            previous_assignee=self.state.assigned_to,
            new_assignee=new_assignee,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Usage Example
# =============================================================================

def main():
    # Create IDs
    task_id = uuid4()
    user_alice = uuid4()
    user_bob = uuid4()

    print("=" * 60)
    print("Task Aggregate Example")
    print("=" * 60)

    # Create a new aggregate
    task = TaskAggregate(task_id)
    print(f"\n1. Created aggregate (ID: {task_id})")
    print(f"   Version: {task.version}")
    print(f"   State: {task.state}")

    # Create the task
    task.create(
        title="Learn Event Sourcing",
        description="Complete the eventsource-py tutorial series",
        assigned_to=user_alice,
    )
    print(f"\n2. Task created")
    print(f"   Version: {task.version}")
    print(f"   Title: {task.state.title}")
    print(f"   Status: {task.state.status}")
    print(f"   Assigned to: Alice ({user_alice})")

    # Reassign to Bob
    task.reassign(new_assignee=user_bob)
    print(f"\n3. Task reassigned")
    print(f"   Version: {task.version}")
    print(f"   Assigned to: Bob ({user_bob})")

    # Complete the task
    task.complete(completed_by=user_bob)
    print(f"\n4. Task completed")
    print(f"   Version: {task.version}")
    print(f"   Status: {task.state.status}")
    print(f"   Completed by: Bob ({user_bob})")

    # Show uncommitted events
    print(f"\n5. Uncommitted events ({len(task.uncommitted_events)} total):")
    for i, event in enumerate(task.uncommitted_events, 1):
        print(f"   [{i}] {event.event_type} (v{event.aggregate_version})")

    # Demonstrate business rule enforcement
    print("\n6. Business rule enforcement:")
    try:
        task.complete(completed_by=user_alice)
    except ValueError as e:
        print(f"   Blocked: {e}")

    try:
        task.reassign(new_assignee=user_alice)
    except ValueError as e:
        print(f"   Blocked: {e}")

    # Demonstrate replaying from history
    print("\n7. Replaying from history:")

    # Get events and create a fresh aggregate
    events = task.uncommitted_events
    fresh_task = TaskAggregate(task_id)
    fresh_task.load_from_history(events)

    print(f"   Fresh aggregate has same state:")
    print(f"   Version: {fresh_task.version}")
    print(f"   Status: {fresh_task.state.status}")
    print(f"   Uncommitted events: {len(fresh_task.uncommitted_events)}")

    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
```

Save this as `tutorial_03_aggregate.py` and run it:

```bash
python tutorial_03_aggregate.py
```

---

## Exercises

Now it is time to practice what you have learned!

### Exercise 1: Add a Cancel Command

**Objective:** Add the ability to cancel a task.

**Requirements:**

1. Create a `TaskCancelled` event with:
   - `cancelled_by: UUID` - who cancelled the task
   - `reason: str` - why it was cancelled

2. Add "cancelled" as a valid status

3. Implement a `cancel()` command with these business rules:
   - Task must exist
   - Cannot cancel already completed tasks
   - Cannot cancel already cancelled tasks

4. Update the `_apply()` method to handle `TaskCancelled`

**Time:** 10-15 minutes

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 3 - Exercise 1 Solution: Add Cancel Command
"""
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    register_event,
)


# Existing events...
@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str
    assigned_to: UUID | None = None


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"
    completed_by: UUID


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    previous_assignee: UUID | None
    new_assignee: UUID


# NEW: TaskCancelled event
@register_event
class TaskCancelled(DomainEvent):
    """Event emitted when a task is cancelled."""
    event_type: str = "TaskCancelled"
    aggregate_type: str = "Task"
    cancelled_by: UUID
    reason: str


# Updated state model
class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"  # pending, completed, cancelled
    completed_by: UUID | None = None
    cancelled_by: UUID | None = None
    cancellation_reason: str | None = None


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
        # NEW: Handle TaskCancelled
        elif isinstance(event, TaskCancelled):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": "cancelled",
                    "cancelled_by": event.cancelled_by,
                    "cancellation_reason": event.reason,
                })

    def create(self, title: str, description: str, assigned_to: UUID | None = None) -> None:
        if self.version > 0:
            raise ValueError("Task already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            assigned_to=assigned_to,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self, completed_by: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task is already completed")
        if self.state.status == "cancelled":
            raise ValueError("Cannot complete a cancelled task")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        ))

    def reassign(self, new_assignee: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot reassign a completed task")
        if self.state.status == "cancelled":
            raise ValueError("Cannot reassign a cancelled task")
        self.apply_event(TaskReassigned(
            aggregate_id=self.aggregate_id,
            previous_assignee=self.state.assigned_to,
            new_assignee=new_assignee,
            aggregate_version=self.get_next_version(),
        ))

    # NEW: cancel command
    def cancel(self, cancelled_by: UUID, reason: str) -> None:
        """
        Cancel the task.

        Business Rules:
        - Task must exist
        - Cannot cancel completed tasks
        - Cannot cancel already cancelled tasks
        """
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot cancel a completed task")
        if self.state.status == "cancelled":
            raise ValueError("Task is already cancelled")

        self.apply_event(TaskCancelled(
            aggregate_id=self.aggregate_id,
            cancelled_by=cancelled_by,
            reason=reason,
            aggregate_version=self.get_next_version(),
        ))


def main():
    task = TaskAggregate(uuid4())
    user = uuid4()

    task.create("Important task", "Must be done", assigned_to=user)
    print(f"Created: {task.state.status}")

    task.cancel(cancelled_by=user, reason="No longer needed")
    print(f"Cancelled: {task.state.status}")
    print(f"Reason: {task.state.cancellation_reason}")

    # Try to complete (should fail)
    try:
        task.complete(completed_by=user)
    except ValueError as e:
        print(f"Business rule: {e}")


if __name__ == "__main__":
    main()
```

</details>

### Exercise 2: Convert to DeclarativeAggregate

**Objective:** Rewrite the TaskAggregate using the `DeclarativeAggregate` pattern.

**Requirements:**

1. Change the base class to `DeclarativeAggregate[TaskState]`
2. Remove the `_apply()` method
3. Add `@handles` decorated methods for each event type
4. Keep all command methods the same

**Time:** 10-15 minutes

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 3 - Exercise 2 Solution: DeclarativeAggregate
"""
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    DeclarativeAggregate,
    DomainEvent,
    handles,
    register_event,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str
    assigned_to: UUID | None = None


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"
    completed_by: UUID


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
    completed_by: UUID | None = None


class TaskAggregate(DeclarativeAggregate[TaskState]):
    """Task aggregate using the declarative pattern."""

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
            assigned_to=event.assigned_to,
            status="pending",
        )

    @handles(TaskCompleted)
    def _on_task_completed(self, event: TaskCompleted) -> None:
        if self._state:
            self._state = self._state.model_copy(update={
                "status": "completed",
                "completed_by": event.completed_by,
            })

    @handles(TaskReassigned)
    def _on_task_reassigned(self, event: TaskReassigned) -> None:
        if self._state:
            self._state = self._state.model_copy(update={
                "assigned_to": event.new_assignee,
            })

    # Command methods (unchanged)

    def create(self, title: str, description: str, assigned_to: UUID | None = None) -> None:
        if self.version > 0:
            raise ValueError("Task already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            assigned_to=assigned_to,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self, completed_by: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task is already completed")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            completed_by=completed_by,
            aggregate_version=self.get_next_version(),
        ))

    def reassign(self, new_assignee: UUID) -> None:
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot reassign a completed task")
        self.apply_event(TaskReassigned(
            aggregate_id=self.aggregate_id,
            previous_assignee=self.state.assigned_to,
            new_assignee=new_assignee,
            aggregate_version=self.get_next_version(),
        ))


def main():
    task = TaskAggregate(uuid4())
    user = uuid4()

    task.create("Learn Declarative Pattern", "Use @handles decorator")
    task.complete(completed_by=user)

    print(f"Status: {task.state.status}")
    print(f"Events: {[e.event_type for e in task.uncommitted_events]}")


if __name__ == "__main__":
    main()
```

</details>

---

## Summary

In this tutorial, you learned:

- **Aggregates are consistency boundaries** where business logic lives and invariants are enforced
- **State models** use Pydantic `BaseModel` to represent aggregate state with type safety
- **AggregateRoot** uses a single `_apply()` method with `isinstance` checks to handle events
- **DeclarativeAggregate** uses `@handles` decorators for cleaner, more organized event handling
- **Command methods** validate business rules before emitting events
- **Events are applied immediately** in memory via `apply_event()`
- **Version tracking** ensures event ordering and enables optimistic locking
- **load_from_history()** reconstructs state by replaying events

---

## Key Takeaways

!!! note "Remember"
    - Aggregates emit events, they do not store them directly
    - Business rules are enforced in command methods, not in `_apply()`
    - Events are applied immediately in memory when emitted
    - The `version` property tracks how many events have been applied
    - Use `get_next_version()` when creating events

!!! tip "Best Practice"
    Design aggregates around business invariants. If multiple entities must be consistent together (like an Order and its LineItems), they should be part of the same aggregate. If they can change independently, they should be separate aggregates.

!!! warning "Common Mistake"
    Do not put side effects (sending emails, calling APIs) in `_apply()` methods. These methods are called during replay, which would cause duplicate side effects. Handle side effects in event handlers or projections instead.

---

## Next Steps

You now know how to build aggregates that enforce business rules and emit events. However, those events are currently only in memory - they are lost when your program ends!

In the next tutorial, you will learn how to persist events using **event stores** so that your aggregate's history survives restarts.

Continue to [Tutorial 4: Event Stores Overview](04-event-stores.md) to learn how to persist your aggregate's events.

---

## Related Documentation

- [API Reference: Aggregates](../api/aggregates.md) - Complete aggregate API documentation
- [Getting Started Guide](../getting-started.md) - Quick reference for aggregate basics
- [ADR-003: Optimistic Locking](../adrs/0003-optimistic-locking.md) - Why we use version-based concurrency
