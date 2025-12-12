# Tutorial 3: Building Your First Aggregate

**Difficulty:** Beginner
**Progress:** Tutorial 3 of 21 | Phase 1: Foundations

---

## Prerequisites

- [Tutorial 1](01-introduction.md) and [Tutorial 2](02-first-event.md)
- Python 3.11+, eventsource-py installed

---

## What is an Aggregate?

An **aggregate** enforces business rules and emits events. It's a consistency boundary - all changes within an aggregate are atomic.

**Key responsibilities:**
1. Receive commands ("create task", "complete order")
2. Enforce business rules ("can't complete non-existent task")
3. Emit events when commands are valid

**State reconstruction:** Aggregates don't store current state - they replay events to rebuild it.

---

## Building the State Model

```python
from uuid import UUID
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

---

## Creating Your First Aggregate

```python
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent, register_event

# Events from Tutorial 2
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
    completed_by: UUID | None = None

# Aggregate
class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        """Return initial state."""
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
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "completed",
                        "completed_by": event.completed_by,
                    }
                )
        elif isinstance(event, TaskReassigned):
            if self._state:
                self._state = self._state.model_copy(
                    update={"assigned_to": event.new_assignee}
                )
```

**Required methods:**
- `_get_initial_state()`: Returns initial state
- `_apply(event)`: Updates state from event

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

    def reassign(self, new_assignee: UUID) -> None:
        """Reassign task to different user."""
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

**Pattern:** Validate → Emit event → State updates automatically

---

## Using the Aggregate

```python
# Create aggregate
task_id = uuid4()
task = TaskAggregate(task_id)

# Execute commands
task.create("Learn aggregates", "Complete tutorial 3")
print(f"Version: {task.version}")  # 1
print(f"Status: {task.state.status}")  # pending

user_id = uuid4()
task.complete(user_id)
print(f"Version: {task.version}")  # 2
print(f"Status: {task.state.status}")  # completed

# Check uncommitted events
print(f"Events: {len(task.uncommitted_events)}")  # 2
for event in task.uncommitted_events:
    print(f"  - {event.event_type}")
```

---

## DeclarativeAggregate Alternative

Use `@handles` decorator for cleaner event routing:

```python
from eventsource import DeclarativeAggregate, handles

class TaskAggregate(DeclarativeAggregate[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    @handles(TaskCreated)
    def _on_created(self, event: TaskCreated) -> None:
        self._state = TaskState(
            task_id=self.aggregate_id,
            title=event.title,
            description=event.description,
            status="pending",
        )

    @handles(TaskCompleted)
    def _on_completed(self, event: TaskCompleted) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={
                    "status": "completed",
                    "completed_by": event.completed_by,
                }
            )

    @handles(TaskReassigned)
    def _on_reassigned(self, event: TaskReassigned) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={"assigned_to": event.new_assignee}
            )

    # Command methods same as before
    def create(self, title: str, description: str) -> None:
        if self.version > 0:
            raise ValueError("Task already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            description=description,
            aggregate_version=self.get_next_version(),
        ))
```

**Benefits:** Type-safe event routing, clearer handler methods

---

## Replaying Events

Rebuild state from event history:

```python
# Simulate loading from event store
task_id = uuid4()
user_id = uuid4()

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

# Replay events
task = TaskAggregate(task_id)
task.load_from_history(events)

print(f"Version: {task.version}")  # 2
print(f"Status: {task.state.status}")  # completed
print(f"Uncommitted: {len(task.uncommitted_events)}")  # 0
```

---

## Complete Example

```python
"""
Tutorial 3: Building Your First Aggregate
Run with: python tutorial_03_aggregate.py
"""
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent, register_event


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


def main():
    # Create and use aggregate
    task_id = uuid4()
    task = TaskAggregate(task_id)

    task.create("Learn Event Sourcing", "Complete tutorial series")
    print(f"Created task: {task.state.title}")
    print(f"Version: {task.version}, Status: {task.state.status}")

    user_id = uuid4()
    task.complete(user_id)
    print(f"\nCompleted by: {task.state.completed_by}")
    print(f"Version: {task.version}, Status: {task.state.status}")

    # Show events
    print(f"\nUncommitted events: {len(task.uncommitted_events)}")
    for event in task.uncommitted_events:
        print(f"  - {event.event_type} (v{event.aggregate_version})")


if __name__ == "__main__":
    main()
```

---

## Next Steps

Continue to [Tutorial 4: Event Stores](04-event-stores.md).
