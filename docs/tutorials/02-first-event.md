# Tutorial 2: Your First Domain Event

**Difficulty:** Beginner
**Progress:** Tutorial 2 of 21 | Phase 1: Foundations

---

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- Python 3.11+ installed
- `pip install eventsource-py`

---

## What is a Domain Event?

A **domain event** captures something that happened in your business domain (customer placed order, payment processed, task assigned). Events are **past tense** - they represent facts that already happened.

---

## Creating Your First Event

```python
from uuid import UUID, uuid4
from eventsource import DomainEvent, register_event

@register_event
class TaskCreated(DomainEvent):
    """Event emitted when a new task is created."""
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"

    # Business data
    title: str
    description: str
    assigned_to: UUID | None = None
```

**Key components:**
- `@register_event`: Registers for serialization
- `event_type` and `aggregate_type`: Required identifiers
- Business fields: Your domain data

**Using it:**

```python
task_id = uuid4()

event = TaskCreated(
    aggregate_id=task_id,
    title="Learn Event Sourcing",
    description="Complete tutorials",
    assigned_to=uuid4(),
)

# Metadata auto-generated
print(f"Event ID: {event.event_id}")
print(f"Occurred: {event.occurred_at}")
```

---

## Event Immutability

Events cannot be modified after creation:

```python
# Raises ValidationError
event.title = "New Title"  # Error!
```

Create new instances for modifications:

```python
enriched = event.with_metadata(source="api")
```

---

## Multiple Events

```python
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
```

**Task lifecycle:**

```python
task_id = uuid4()
alice, bob = uuid4(), uuid4()

# Event sequence
events = [
    TaskCreated(
        aggregate_id=task_id,
        title="Review docs",
        description="Check accuracy",
        assigned_to=alice,
        aggregate_version=1,
    ),
    TaskReassigned(
        aggregate_id=task_id,
        previous_assignee=alice,
        new_assignee=bob,
        aggregate_version=2,
    ),
    TaskCompleted(
        aggregate_id=task_id,
        completed_by=bob,
        aggregate_version=3,
    ),
]
```

---

## Naming Conventions

**Use past tense:**

| Good | Bad |
|------|-----|
| `TaskCreated` | `CreateTask` |
| `OrderShipped` | `ShipOrder` |
| `PaymentReceived` | `ReceivePayment` |

**Be specific:**

| Specific | Vague |
|----------|-------|
| `TaskReassigned` | `TaskUpdated` |
| `PriceAdjusted` | `ProductChanged` |

---

## Pydantic Validation

```python
from pydantic import Field

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    order_total: float = Field(ge=0)  # Must be non-negative
    currency: str = Field(pattern="^[A-Z]{3}$")  # ISO code
    items: list[str] = Field(min_length=1)  # Non-empty
```

---

## Complete Example

```python
"""
Tutorial 2: Your First Domain Event
Run with: python tutorial_02_events.py
"""
from uuid import UUID, uuid4
from eventsource import DomainEvent, register_event


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


def main():
    task_id = uuid4()
    user_id = uuid4()

    # Create event
    created = TaskCreated(
        aggregate_id=task_id,
        title="Learn Event Sourcing",
        description="Complete tutorials",
        assigned_to=user_id,
        aggregate_version=1,
    )

    print(f"Event ID: {created.event_id}")
    print(f"Type: {created.event_type}")
    print(f"Title: {created.title}")
    print(f"Occurred: {created.occurred_at}")

    # Immutability demo
    try:
        created.title = "New Title"
    except Exception as e:
        print(f"\nCannot modify: {type(e).__name__}")

    # Metadata
    enriched = created.with_metadata(source="tutorial")
    print(f"\nOriginal: {created.metadata}")
    print(f"Enriched: {enriched.metadata}")

    # Serialization
    event_dict = created.to_dict()
    reconstructed = TaskCreated.from_dict(event_dict)
    print(f"\nReconstructed: {reconstructed.title}")


if __name__ == "__main__":
    main()
```

---

## Next Steps

Continue to [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md).
