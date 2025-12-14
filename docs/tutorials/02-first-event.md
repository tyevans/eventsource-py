# Tutorial 2: Your First Domain Event

**Difficulty:** Beginner

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- Basic Python programming knowledge
- Python 3.10 or higher
- Understanding of type hints and Pydantic basics

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Create domain events using the `DomainEvent` base class
2. Use the `@register_event` decorator for event registration
3. Understand event metadata (event_id, correlation_id, causation_id)
4. Work with event immutability and helper methods
5. Serialize and deserialize events
6. Apply Pydantic validation to event fields
7. Follow naming conventions for domain events

---

## What is a Domain Event?

A **domain event** is an immutable record of something that happened in your business domain. Events represent facts about the past:

- **OrderPlaced** - A customer placed an order
- **PaymentReceived** - Payment was successfully processed
- **TaskCompleted** - A task was marked as done

Events are **always named in past tense** because they represent things that have already occurred and cannot be changed.

---

## The DomainEvent Base Class

All events in eventsource-py inherit from `DomainEvent`, which is a Pydantic `BaseModel`. This gives you:

- **Type safety** with Python type hints
- **Automatic validation** using Pydantic validators
- **Immutability** through frozen models
- **JSON serialization** out of the box
- **Built-in metadata** for event tracking

### Event Metadata Fields

Every domain event automatically includes:

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | UUID | Unique identifier for this event instance (auto-generated) |
| `event_type` | str | Type name (e.g., "TaskCreated") |
| `event_version` | int | Schema version for event migrations (default: 1) |
| `occurred_at` | datetime | When the event occurred (auto-generated, UTC) |
| `aggregate_id` | UUID | ID of the aggregate this event belongs to (required) |
| `aggregate_type` | str | Type of aggregate (e.g., "Task") (required) |
| `aggregate_version` | int | Version of aggregate after this event (default: 1) |
| `tenant_id` | UUID \| None | Optional tenant ID for multi-tenancy |
| `actor_id` | str \| None | User/system that triggered the event |
| `correlation_id` | UUID | Links related events across aggregates (auto-generated) |
| `causation_id` | UUID \| None | ID of the event that caused this event |
| `metadata` | dict | Additional metadata (default: empty dict) |

---

## Creating Your First Event

Let's create a simple event for a task management system:

```python
from uuid import UUID, uuid4
from eventsource import DomainEvent, register_event

@register_event
class TaskCreated(DomainEvent):
    """Event emitted when a new task is created."""

    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"

    # Business-specific fields
    title: str
    description: str
    assigned_to: UUID | None = None
```

**Key components:**

1. **`@register_event`**: Decorator that registers the event type for serialization/deserialization
2. **`event_type`**: String identifier for this event type (used during deserialization)
3. **`aggregate_type`**: The type of aggregate this event belongs to
4. **Business fields**: Your domain-specific data (title, description, assigned_to)

---

## Using Your Event

```python
from uuid import uuid4

# Create a new task event
task_id = uuid4()
user_id = uuid4()

event = TaskCreated(
    aggregate_id=task_id,
    title="Learn Event Sourcing",
    description="Complete all tutorials in the eventsource-py documentation",
    assigned_to=user_id,
)

# Inspect the event
print(f"Event ID: {event.event_id}")           # Auto-generated UUID
print(f"Event Type: {event.event_type}")       # "TaskCreated"
print(f"Occurred At: {event.occurred_at}")     # Current timestamp (UTC)
print(f"Aggregate ID: {event.aggregate_id}")   # task_id
print(f"Title: {event.title}")                 # "Learn Event Sourcing"
```

**Output:**
```
Event ID: a1b2c3d4-e5f6-7890-abcd-ef1234567890
Event Type: TaskCreated
Occurred At: 2025-12-13 10:30:45.123456+00:00
Aggregate ID: f6e5d4c3-b2a1-0987-6543-210fedcba987
Title: Learn Event Sourcing
```

---

## Event Immutability

Events are **frozen** (immutable) after creation. This is a core principle of event sourcing - events represent historical facts that cannot be changed.

```python
event = TaskCreated(
    aggregate_id=uuid4(),
    title="Original Title",
    description="Original description",
)

# Attempting to modify raises ValidationError
try:
    event.title = "New Title"
except Exception as e:
    print(f"Error: {type(e).__name__}")  # ValidationError
```

### Creating Modified Copies

Since events are immutable, you create new instances when you need variations:

```python
# Add metadata using the with_metadata() helper
enriched_event = event.with_metadata(
    source="web-api",
    user_agent="Mozilla/5.0",
    trace_id="abc123",
)

print(event.metadata)           # {}
print(enriched_event.metadata)  # {'source': 'web-api', 'user_agent': '...', 'trace_id': 'abc123'}

# Set aggregate version
versioned_event = event.with_aggregate_version(5)
print(versioned_event.aggregate_version)  # 5
```

---

## Event Registration

The `@register_event` decorator registers events in the global event registry. This enables the event store to deserialize events from storage.

```python
from eventsource import register_event, get_event_class, list_registered_events

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str

# Later, retrieve the class by event_type
EventClass = get_event_class("TaskCreated")
print(EventClass)  # <class '__main__.TaskCreated'>

# List all registered events
print(list_registered_events())  # ['TaskCreated', ...]
```

**Alternative registration patterns:**

```python
# Option 1: With explicit event_type override
@register_event(event_type="task.created.v1")
class TaskCreated(DomainEvent):
    aggregate_type: str = "Task"
    title: str

# Option 2: Programmatic registration
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str

from eventsource import default_registry
default_registry.register(TaskCreated)
```

---

## Multiple Events for Different State Transitions

Real systems have multiple events representing different state changes:

```python
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
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"

    previous_assignee: UUID | None
    new_assignee: UUID


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"

    completed_by: UUID
    completed_at_version: int  # Which version was completed
```

### Event Sequence Example

```python
task_id = uuid4()
alice = uuid4()
bob = uuid4()

# Create a sequence of events for a task's lifecycle
events = [
    TaskCreated(
        aggregate_id=task_id,
        title="Review documentation",
        description="Check for accuracy and completeness",
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
        completed_at_version=2,
        aggregate_version=3,
    ),
]

# Print the event stream
for event in events:
    print(f"v{event.aggregate_version}: {event.event_type}")
```

**Output:**
```
v1: TaskCreated
v2: TaskReassigned
v3: TaskCompleted
```

---

## Correlation and Causation Tracking

Events can be linked together to track event chains and sagas:

### Correlation ID

The `correlation_id` links all events that are part of the same logical operation, even across different aggregates:

```python
# First event in a saga
order_created = OrderPlaced(
    aggregate_id=order_id,
    customer_id=customer_id,
    total_amount=100.0,
)

# Subsequent events use the same correlation_id
payment_event = PaymentProcessed(
    aggregate_id=payment_id,
    order_id=order_id,
    amount=100.0,
).with_causation(order_created)

# Both events share the same correlation_id
assert payment_event.correlation_id == order_created.correlation_id
assert payment_event.is_correlated_with(order_created)  # True
```

### Causation ID

The `causation_id` tracks direct cause-and-effect relationships:

```python
# Event A causes Event B
event_a = TaskCreated(
    aggregate_id=uuid4(),
    title="Parent Task",
    description="Main task",
)

# Create a related event with causation tracking
event_b = TaskCreated(
    aggregate_id=uuid4(),
    title="Subtask",
    description="Derived from parent",
).with_causation(event_a)

# event_b was caused by event_a
assert event_b.causation_id == event_a.event_id
assert event_b.is_caused_by(event_a)  # True
```

---

## Event Serialization

Events can be serialized to JSON-compatible dictionaries for storage:

```python
event = TaskCreated(
    aggregate_id=uuid4(),
    title="Test Task",
    description="For testing serialization",
)

# Serialize to dictionary
event_dict = event.to_dict()
print(event_dict)
# {
#   'event_id': 'a1b2c3d4-...',
#   'event_type': 'TaskCreated',
#   'aggregate_id': 'f6e5d4c3-...',
#   'title': 'Test Task',
#   'description': 'For testing serialization',
#   ...
# }

# Deserialize from dictionary
reconstructed = TaskCreated.from_dict(event_dict)
assert reconstructed.event_id == event.event_id
assert reconstructed.title == event.title
```

**Important:** UUIDs and datetimes are automatically converted to strings in JSON mode:

```python
import json

# Full JSON roundtrip
json_str = json.dumps(event.to_dict())
data = json.loads(json_str)
restored = TaskCreated.from_dict(data)

assert isinstance(data['event_id'], str)        # UUID as string
assert isinstance(data['occurred_at'], str)     # Datetime as ISO string
assert isinstance(restored.event_id, UUID)      # Restored as UUID
assert isinstance(restored.occurred_at, datetime)  # Restored as datetime
```

---

## Pydantic Validation

Since events are Pydantic models, you get automatic validation:

```python
from pydantic import Field, ValidationError

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    order_total: float = Field(ge=0, description="Must be non-negative")
    currency: str = Field(pattern=r"^[A-Z]{3}$", description="ISO 4217 code")
    items: list[str] = Field(min_length=1, description="At least one item required")


# Valid event
valid_order = OrderPlaced(
    aggregate_id=uuid4(),
    order_total=99.99,
    currency="USD",
    items=["item-1", "item-2"],
)

# Invalid: negative total
try:
    OrderPlaced(
        aggregate_id=uuid4(),
        order_total=-10.0,  # Fails: must be >= 0
        currency="USD",
        items=["item-1"],
    )
except ValidationError as e:
    print(e)

# Invalid: bad currency format
try:
    OrderPlaced(
        aggregate_id=uuid4(),
        order_total=50.0,
        currency="US",  # Fails: must be 3 uppercase letters
        items=["item-1"],
    )
except ValidationError as e:
    print(e)

# Invalid: empty items list
try:
    OrderPlaced(
        aggregate_id=uuid4(),
        order_total=50.0,
        currency="USD",
        items=[],  # Fails: must have at least 1 item
    )
except ValidationError as e:
    print(e)
```

---

## Naming Conventions

### Use Past Tense

Events represent things that have already happened:

| Good ✓ | Bad ✗ |
|---------|--------|
| `TaskCreated` | `CreateTask` |
| `OrderShipped` | `ShipOrder` |
| `PaymentReceived` | `ReceivePayment` |
| `UserRegistered` | `RegisterUser` |

### Be Specific

Avoid vague event names that don't clearly indicate what changed:

| Specific ✓ | Vague ✗ |
|------------|---------|
| `TaskReassigned` | `TaskUpdated` |
| `PriceAdjusted` | `ProductChanged` |
| `EmailVerified` | `UserModified` |
| `InventoryRestocked` | `InventoryChanged` |

### Use Business Language

Event names should reflect your domain's ubiquitous language:

```python
# E-commerce domain
@register_event
class OrderPlaced(DomainEvent): ...

@register_event
class OrderShipped(DomainEvent): ...

@register_event
class OrderDelivered(DomainEvent): ...


# Healthcare domain
@register_event
class PatientAdmitted(DomainEvent): ...

@register_event
class TreatmentAdministered(DomainEvent): ...

@register_event
class PatientDischarged(DomainEvent): ...
```

---

## Complete Working Example

Here's a complete runnable example demonstrating all concepts:

```python
"""
Tutorial 2: Your First Domain Event
Run with: python tutorial_02_events.py
"""

from datetime import datetime
from uuid import UUID, uuid4

from pydantic import Field

from eventsource import DomainEvent, register_event


# =============================================================================
# Define Events
# =============================================================================

@register_event
class TaskCreated(DomainEvent):
    """Event emitted when a new task is created."""

    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"

    title: str
    description: str
    assigned_to: UUID | None = None


@register_event
class TaskReassigned(DomainEvent):
    """Event emitted when a task is reassigned to a different user."""

    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"

    previous_assignee: UUID | None
    new_assignee: UUID


@register_event
class TaskCompleted(DomainEvent):
    """Event emitted when a task is marked as completed."""

    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"

    completed_by: UUID


# =============================================================================
# Main Demonstration
# =============================================================================

def main() -> None:
    """Demonstrate domain event capabilities."""

    # Create identifiers
    task_id = uuid4()
    alice = uuid4()
    bob = uuid4()

    # 1. Create an event
    print("=" * 60)
    print("1. Creating a TaskCreated event")
    print("=" * 60)

    created_event = TaskCreated(
        aggregate_id=task_id,
        title="Learn Event Sourcing",
        description="Complete all tutorials",
        assigned_to=alice,
        aggregate_version=1,
    )

    print(f"Event ID: {created_event.event_id}")
    print(f"Event Type: {created_event.event_type}")
    print(f"Aggregate ID: {created_event.aggregate_id}")
    print(f"Title: {created_event.title}")
    print(f"Occurred At: {created_event.occurred_at}")
    print(f"Aggregate Version: {created_event.aggregate_version}")

    # 2. Event immutability
    print("\n" + "=" * 60)
    print("2. Testing immutability")
    print("=" * 60)

    try:
        created_event.title = "New Title"  # type: ignore
        print("ERROR: Should have raised ValidationError!")
    except Exception as e:
        print(f"✓ Cannot modify event: {type(e).__name__}")

    # 3. Adding metadata
    print("\n" + "=" * 60)
    print("3. Adding metadata")
    print("=" * 60)

    enriched_event = created_event.with_metadata(
        source="web-ui",
        user_ip="192.168.1.1",
        trace_id="xyz789",
    )

    print(f"Original metadata: {created_event.metadata}")
    print(f"Enriched metadata: {enriched_event.metadata}")

    # 4. Event serialization
    print("\n" + "=" * 60)
    print("4. Serialization roundtrip")
    print("=" * 60)

    event_dict = created_event.to_dict()
    print(f"Serialized event_id type: {type(event_dict['event_id'])}")
    print(f"Serialized occurred_at type: {type(event_dict['occurred_at'])}")

    reconstructed_event = TaskCreated.from_dict(event_dict)
    print(f"Reconstructed event_id type: {type(reconstructed_event.event_id)}")
    print(f"Reconstructed occurred_at type: {type(reconstructed_event.occurred_at)}")
    print(f"✓ Title matches: {reconstructed_event.title == created_event.title}")

    # 5. Causation tracking
    print("\n" + "=" * 60)
    print("5. Causation tracking")
    print("=" * 60)

    reassigned_event = TaskReassigned(
        aggregate_id=task_id,
        previous_assignee=alice,
        new_assignee=bob,
        aggregate_version=2,
    ).with_causation(created_event)

    print(f"Reassigned event causation_id: {reassigned_event.causation_id}")
    print(f"Created event event_id: {created_event.event_id}")
    print(f"✓ Is caused by: {reassigned_event.is_caused_by(created_event)}")
    print(f"✓ Is correlated: {reassigned_event.is_correlated_with(created_event)}")

    # 6. Event sequence
    print("\n" + "=" * 60)
    print("6. Event sequence (task lifecycle)")
    print("=" * 60)

    completed_event = TaskCompleted(
        aggregate_id=task_id,
        completed_by=bob,
        aggregate_version=3,
    ).with_causation(reassigned_event)

    event_stream = [created_event, reassigned_event, completed_event]

    for event in event_stream:
        print(f"v{event.aggregate_version}: {event.event_type} at {event.occurred_at}")

    print("\n" + "=" * 60)
    print("✓ Tutorial complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
```

---

## Key Takeaways

1. **Events are immutable**: They represent historical facts that cannot be changed
2. **Use `@register_event`**: Register all events for serialization support
3. **Inherit from `DomainEvent`**: Get automatic metadata, validation, and serialization
4. **Name events in past tense**: `TaskCreated`, not `CreateTask`
5. **Events include rich metadata**: event_id, correlation_id, causation_id, occurred_at, etc.
6. **Pydantic validation**: Use Field validators for business rule enforcement
7. **Use helper methods**: `with_metadata()`, `with_causation()`, `with_aggregate_version()`
8. **Serialize with `to_dict()`**: Deserialize with `from_dict()`

---

## Next Steps

Now that you understand domain events, you're ready to learn about aggregates - the entities that enforce business rules and emit these events.

Continue to [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md).

For more examples, see:
- `examples/basic_usage.py` - Complete working example with events and aggregates
- `examples/aggregate_example.py` - Advanced patterns with declarative aggregates
