# Tutorial 2: Your First Domain Event

**Estimated Time:** 30-45 minutes
**Difficulty:** Beginner
**Progress:** Tutorial 2 of 21 | Phase 1: Foundations

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

If you have not installed the library yet:

```bash
pip install eventsource-py
```

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Define domain events using the `DomainEvent` base class
2. Understand the purpose of each event field (metadata and payload)
3. Register events with `@register_event` for serialization
4. Create event instances and access their fields
5. Explain why events are immutable and what that means in practice
6. Follow event naming conventions (past tense, domain language)

---

## What is a Domain Event?

In Tutorial 1, we learned that events are immutable records of things that have happened. Now let's put that into practice with code.

A **domain event** captures a fact about something that occurred in your business domain. Consider these examples:

- A customer placed an order
- A payment was processed
- A user changed their email address
- A task was assigned to someone

Notice that all of these are expressed in **past tense**. This is intentional. Events represent things that have already happened - facts that cannot be changed.

In eventsource-py, you define domain events as Python classes that inherit from the `DomainEvent` base class. The library uses [Pydantic](https://docs.pydantic.dev/) under the hood, giving you automatic validation, serialization, and type safety.

---

## The DomainEvent Base Class

The `DomainEvent` class provides the foundation for all events in your system. It includes essential metadata fields that every event needs:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `event_id` | `UUID` | No | Auto-generated | Unique identifier for this specific event |
| `event_type` | `str` | Yes | - | Type name (e.g., "TaskCreated") |
| `event_version` | `int` | No | `1` | Schema version for migrations |
| `occurred_at` | `datetime` | No | Current UTC time | When the event happened |
| `aggregate_id` | `UUID` | Yes | - | ID of the aggregate this event belongs to |
| `aggregate_type` | `str` | Yes | - | Type of aggregate (e.g., "Task") |
| `aggregate_version` | `int` | No | `1` | Aggregate version after this event |
| `tenant_id` | `UUID \| None` | No | `None` | For multi-tenancy support |
| `actor_id` | `str \| None` | No | `None` | Who triggered this event |
| `correlation_id` | `UUID` | No | Auto-generated | Links related events across aggregates |
| `causation_id` | `UUID \| None` | No | `None` | The event that caused this event |
| `metadata` | `dict` | No | `{}` | Additional context |

Do not worry about understanding every field right now. We will cover the most important ones in this tutorial and revisit the others as they become relevant in later tutorials.

---

## Creating Your First Event

Let's create our first domain event: `TaskCreated`. This event will capture the fact that a new task was created in our task management system.

### Step 1: Import the Essentials

Create a new file called `events.py` and add these imports:

```python
from uuid import UUID, uuid4
from eventsource import DomainEvent, register_event
```

We import:

- `UUID` and `uuid4` from Python's standard library for working with unique identifiers
- `DomainEvent` - the base class for all events
- `register_event` - a decorator that registers events for serialization

### Step 2: Define the TaskCreated Event

Now let's define our first event:

```python
@register_event
class TaskCreated(DomainEvent):
    """
    Event emitted when a new task is created.

    This event captures all the information needed to understand
    what task was created and its initial state.
    """
    # Required: Identify the event type and aggregate type
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"

    # Event-specific payload - the business data
    title: str
    description: str
    assigned_to: UUID | None = None
```

Let's break this down:

1. **`@register_event`**: This decorator registers the event class so the library can serialize and deserialize it. Without this, the event store will not know how to reconstruct your events from storage.

2. **`class TaskCreated(DomainEvent)`**: Our event inherits from `DomainEvent`, which gives us all the metadata fields automatically.

3. **`event_type: str = "TaskCreated"`**: This string identifies what kind of event this is. It is used when storing and retrieving events. Always set this to match your class name.

4. **`aggregate_type: str = "Task"`**: This identifies which type of aggregate this event belongs to. We will learn about aggregates in Tutorial 3.

5. **Business payload fields**: `title`, `description`, and `assigned_to` are the specific data for this event. These fields capture what actually happened - a task was created with this title, this description, and optionally assigned to someone.

### Step 3: Create an Event Instance

Now let's create an instance of our event:

```python
# Generate IDs
task_id = uuid4()  # The aggregate ID (the task's unique identifier)
user_id = uuid4()  # A user to assign the task to

# Create the event
event = TaskCreated(
    aggregate_id=task_id,
    title="Learn Event Sourcing",
    description="Complete the eventsource-py tutorial series",
    assigned_to=user_id,
)
```

Notice that we only need to provide:

- `aggregate_id` - which task this event belongs to (required)
- `title` and `description` - our business data (required)
- `assigned_to` - optional, but we're providing it

All the other fields (`event_id`, `occurred_at`, `correlation_id`, etc.) are automatically populated with sensible defaults.

### Step 4: Explore Event Fields

Let's examine the event we created:

```python
print("=== TaskCreated Event ===")
print(f"Event ID: {event.event_id}")
print(f"Event Type: {event.event_type}")
print(f"Aggregate ID: {event.aggregate_id}")
print(f"Aggregate Type: {event.aggregate_type}")
print(f"Aggregate Version: {event.aggregate_version}")
print(f"Occurred At: {event.occurred_at}")
print(f"Correlation ID: {event.correlation_id}")
print()
print("=== Business Data ===")
print(f"Title: {event.title}")
print(f"Description: {event.description}")
print(f"Assigned To: {event.assigned_to}")
```

When you run this, you will see output like:

```
=== TaskCreated Event ===
Event ID: 8f7c2a1b-3d4e-5f6a-7b8c-9d0e1f2a3b4c
Event Type: TaskCreated
Aggregate ID: a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d
Aggregate Type: Task
Aggregate Version: 1
Occurred At: 2024-01-15 14:30:00.123456+00:00
Correlation ID: 4e5f6a7b-8c9d-0e1f-2a3b-4c5d6e7f8a9b

=== Business Data ===
Title: Learn Event Sourcing
Description: Complete the eventsource-py tutorial series
Assigned To: b2c3d4e5-f6a7-8b9c-0d1e-2f3a4b5c6d7e
```

Every event gets a unique `event_id`, a timestamp for when it occurred, and a `correlation_id` for tracking related events.

---

## Event Immutability

Events are **immutable** - once created, they cannot be changed. This is a fundamental principle of event sourcing.

The `DomainEvent` base class uses Pydantic's `frozen=True` configuration, which means any attempt to modify an event after creation will raise an error:

```python
# This will raise an error!
try:
    event.title = "New Title"
except Exception as e:
    print(f"Error: {type(e).__name__}")
    print("Events are immutable - you cannot change them!")
```

Output:

```
Error: ValidationError
Events are immutable - you cannot change them!
```

### Why Immutability Matters

Immutability is essential for several reasons:

1. **Reliability**: Events represent historical facts. Just as you cannot change the past, you should not change events.

2. **Auditability**: An immutable event log provides a tamper-proof audit trail.

3. **Reproducibility**: If events could change, replaying them might produce different results each time.

4. **Concurrency Safety**: Immutable objects can be safely shared between threads without synchronization.

### Creating Modified Copies

If you need to "modify" an event (for example, to track causation), use the helper methods that create new instances:

```python
# Add metadata without modifying the original
enriched_event = event.with_metadata(source="api", request_id="abc123")

print(f"Original metadata: {event.metadata}")
print(f"Enriched metadata: {enriched_event.metadata}")
```

Output:

```
Original metadata: {}
Enriched metadata: {'source': 'api', 'request_id': 'abc123'}
```

The original event is unchanged - we created a new event with the additional metadata.

---

## The Event Registry

The `@register_event` decorator registers your event class with the library's event registry. This enables:

1. **Serialization**: Converting events to JSON for storage
2. **Deserialization**: Reconstructing the correct event class from stored data
3. **Type lookup**: Finding the right class based on the event type string

### Why Registration Matters

When events are stored in a database, they're saved as JSON with the `event_type` field identifying what kind of event it is:

```json
{
  "event_type": "TaskCreated",
  "aggregate_id": "a1b2c3d4-...",
  "title": "Learn Event Sourcing",
  ...
}
```

When the event store loads this data, it uses the registry to look up "TaskCreated" and find your `TaskCreated` class, ensuring the event is reconstructed with the correct type.

Without registration, the library would not know how to reconstruct your events from storage.

### Verifying Registration

You can verify that your events are registered:

```python
from eventsource import is_event_registered, list_registered_events

# Check if a specific event is registered
print(f"TaskCreated registered: {is_event_registered('TaskCreated')}")

# List all registered events
print(f"All registered events: {list_registered_events()}")
```

---

## Adding More Events

A task management system needs more than just creation. Let's add an event for task completion:

```python
@register_event
class TaskCompleted(DomainEvent):
    """
    Event emitted when a task is marked as complete.

    Records who completed the task and when (via occurred_at).
    """
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"

    completed_by: UUID  # Who completed the task
```

And one for reassignment:

```python
@register_event
class TaskReassigned(DomainEvent):
    """
    Event emitted when a task is reassigned to a different user.

    Captures both the previous and new assignee for audit purposes.
    """
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"

    previous_assignee: UUID | None  # Who it was assigned to (None if unassigned)
    new_assignee: UUID              # Who it's now assigned to
```

### Using Multiple Events

Now we can simulate a task's lifecycle:

```python
task_id = uuid4()
alice = uuid4()
bob = uuid4()

# Task created and assigned to Alice
created = TaskCreated(
    aggregate_id=task_id,
    title="Review documentation",
    description="Review the tutorial for accuracy",
    assigned_to=alice,
    aggregate_version=1,
)

# Task reassigned to Bob
reassigned = TaskReassigned(
    aggregate_id=task_id,
    previous_assignee=alice,
    new_assignee=bob,
    aggregate_version=2,
)

# Bob completes the task
completed = TaskCompleted(
    aggregate_id=task_id,
    completed_by=bob,
    aggregate_version=3,
)

# Print the task history
print("Task History:")
print(f"  1. {created.event_type} - '{created.title}'")
print(f"  2. {reassigned.event_type} - from {reassigned.previous_assignee} to {reassigned.new_assignee}")
print(f"  3. {completed.event_type} - by {completed.completed_by}")
```

This is the essence of event sourcing: instead of storing "Task is complete, assigned to Bob," we store the sequence of events that led to that state.

---

## Event Naming Conventions

Following consistent naming conventions makes your code more readable and your domain model clearer.

### Use Past Tense

Events represent things that have happened, so always use past tense:

| Good | Bad |
|------|-----|
| `TaskCreated` | `CreateTask` |
| `OrderShipped` | `ShipOrder` |
| `PaymentReceived` | `ReceivePayment` |
| `UserEmailChanged` | `ChangeUserEmail` |

### Use Domain Language

Name events using the language your business experts use:

| Domain-Focused | Technical |
|----------------|-----------|
| `OrderPlaced` | `OrderRecordInserted` |
| `ItemAddedToCart` | `CartItemCreated` |
| `SubscriptionRenewed` | `SubscriptionUpdated` |

### Be Specific

Events should clearly communicate what happened:

| Specific | Vague |
|----------|-------|
| `TaskReassigned` | `TaskUpdated` |
| `PriceAdjusted` | `ProductChanged` |
| `UserEmailVerified` | `UserStatusChanged` |

---

## Pydantic Validation

Since `DomainEvent` inherits from Pydantic's `BaseModel`, you get automatic validation:

```python
from pydantic import Field

@register_event
class OrderPlaced(DomainEvent):
    """Event with Pydantic validation."""
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    # Validation: must be positive
    order_total: float = Field(ge=0)

    # Validation: must match pattern
    currency: str = Field(pattern="^[A-Z]{3}$")

    # Validation: must be non-empty list
    items: list[str] = Field(min_length=1)
```

Now invalid data will be rejected:

```python
try:
    invalid_order = OrderPlaced(
        aggregate_id=uuid4(),
        order_total=-50.00,  # Negative amount!
        currency="usd",      # Lowercase!
        items=[],            # Empty list!
    )
except Exception as e:
    print(f"Validation failed: {e}")
```

This ensures your events always contain valid data.

---

## Complete Example

Here is a complete, runnable example that demonstrates everything we've covered:

```python
"""
Tutorial 2: Your First Domain Event

This example demonstrates creating domain events with eventsource-py.
Run with: python tutorial_02_events.py
"""
from uuid import UUID, uuid4

from eventsource import DomainEvent, register_event


# =============================================================================
# Event Definitions
# =============================================================================

@register_event
class TaskCreated(DomainEvent):
    """
    Event emitted when a new task is created.

    Captures all information needed to understand what task was created.
    """
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"

    # Task-specific fields
    title: str
    description: str
    assigned_to: UUID | None = None


@register_event
class TaskCompleted(DomainEvent):
    """
    Event emitted when a task is marked as complete.
    """
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"

    completed_by: UUID


@register_event
class TaskReassigned(DomainEvent):
    """
    Event emitted when a task is reassigned to a different user.
    """
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"

    previous_assignee: UUID | None
    new_assignee: UUID


# =============================================================================
# Usage Examples
# =============================================================================

def main():
    # Create a task ID (would normally come from the aggregate)
    task_id = uuid4()
    user_id = uuid4()

    # Create a TaskCreated event
    created_event = TaskCreated(
        aggregate_id=task_id,
        title="Learn Event Sourcing",
        description="Complete the eventsource-py tutorial series",
        assigned_to=user_id,
        aggregate_version=1,  # First event for this aggregate
    )

    # Explore the event
    print("=== TaskCreated Event ===")
    print(f"Event ID: {created_event.event_id}")
    print(f"Event Type: {created_event.event_type}")
    print(f"Aggregate ID: {created_event.aggregate_id}")
    print(f"Aggregate Type: {created_event.aggregate_type}")
    print(f"Version: {created_event.aggregate_version}")
    print(f"Occurred At: {created_event.occurred_at}")
    print(f"Title: {created_event.title}")
    print(f"Assigned To: {created_event.assigned_to}")
    print()

    # Create a TaskCompleted event
    completed_event = TaskCompleted(
        aggregate_id=task_id,
        completed_by=user_id,
        aggregate_version=2,  # Second event
    )

    print("=== TaskCompleted Event ===")
    print(f"Event Type: {completed_event.event_type}")
    print(f"Completed By: {completed_event.completed_by}")
    print(f"Version: {completed_event.aggregate_version}")
    print()

    # Demonstrate immutability
    print("=== Immutability Demo ===")
    try:
        created_event.title = "New Title"  # This will fail!
    except Exception as e:
        print(f"Cannot modify event: {type(e).__name__}")
        print("Events are immutable by design!")
    print()

    # Demonstrate creating a modified copy with metadata
    print("=== Metadata Demo ===")
    enriched = created_event.with_metadata(
        source="tutorial",
        step="demonstration"
    )
    print(f"Original metadata: {created_event.metadata}")
    print(f"Enriched metadata: {enriched.metadata}")
    print()

    # Demonstrate serialization
    print("=== Serialization Demo ===")
    event_dict = created_event.to_dict()
    print(f"Event as dict: {event_dict['event_type']} with title '{event_dict['title']}'")

    # Reconstruct from dict
    reconstructed = TaskCreated.from_dict(event_dict)
    print(f"Reconstructed: {reconstructed.event_type} with title '{reconstructed.title}'")


if __name__ == "__main__":
    main()
```

Save this as `tutorial_02_events.py` and run it:

```bash
python tutorial_02_events.py
```

---

## Exercises

Now it's time to practice what you've learned!

### Exercise 1: Create a TaskReassigned Event

**Objective:** Create an event that captures when a task is reassigned to a different user.

**Requirements:**

1. Event type should be "TaskReassigned"
2. Aggregate type should be "Task"
3. Must capture:
   - Who the task was previously assigned to (optional UUID - could be unassigned)
   - Who the task is now assigned to (required UUID)

**Instructions:**

1. Create a new file called `exercise_01.py`
2. Import the necessary modules
3. Define the `TaskReassigned` event class
4. Register it with `@register_event`
5. Create an instance representing a task being reassigned from Alice to Bob
6. Print the event details

**Hints:**

- Use `previous_assignee: UUID | None` for optional fields
- Use `new_assignee: UUID` for required fields
- Remember to provide `aggregate_id` when creating the event

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 2 - Exercise 1 Solution: TaskReassigned Event
"""
from uuid import UUID, uuid4

from eventsource import DomainEvent, register_event


@register_event
class TaskReassigned(DomainEvent):
    """
    Event emitted when a task is reassigned to a different user.
    """
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"

    previous_assignee: UUID | None  # Who it was assigned to (None if unassigned)
    new_assignee: UUID              # Who it's now assigned to


def main():
    task_id = uuid4()
    alice = uuid4()
    bob = uuid4()

    event = TaskReassigned(
        aggregate_id=task_id,
        previous_assignee=alice,
        new_assignee=bob,
        aggregate_version=2,  # Assuming this is the second event
    )

    print(f"Task {task_id} reassigned")
    print(f"  From: {event.previous_assignee}")
    print(f"  To: {event.new_assignee}")
    print(f"  At: {event.occurred_at}")


if __name__ == "__main__":
    main()
```

</details>

### Exercise 2: Create Events for a Shopping Cart

**Objective:** Design events for a shopping cart domain.

**Requirements:**

Create three events:

1. `ItemAddedToCart` - when an item is added to a cart
   - Item ID (UUID)
   - Quantity (int, must be positive)
   - Unit price (float, must be non-negative)

2. `ItemRemovedFromCart` - when an item is removed
   - Item ID (UUID)

3. `CartCheckedOut` - when the cart is checked out
   - Order ID (UUID) - links to the order that was created

**Instructions:**

1. Create a new file called `exercise_02.py`
2. Define all three event classes with proper validation
3. Create instances of each event for a sample cart
4. Print a summary of the cart's history

**Hints:**

- Use `aggregate_type: str = "Cart"` for all events
- Use Pydantic's `Field` for validation (`Field(gt=0)` for positive numbers)
- The aggregate ID represents the cart ID

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 2 - Exercise 2 Solution: Shopping Cart Events
"""
from uuid import UUID, uuid4

from pydantic import Field

from eventsource import DomainEvent, register_event


@register_event
class ItemAddedToCart(DomainEvent):
    """Event when an item is added to the cart."""
    event_type: str = "ItemAddedToCart"
    aggregate_type: str = "Cart"

    item_id: UUID
    quantity: int = Field(gt=0)  # Must be positive
    unit_price: float = Field(ge=0)  # Must be non-negative


@register_event
class ItemRemovedFromCart(DomainEvent):
    """Event when an item is removed from the cart."""
    event_type: str = "ItemRemovedFromCart"
    aggregate_type: str = "Cart"

    item_id: UUID


@register_event
class CartCheckedOut(DomainEvent):
    """Event when the cart is checked out."""
    event_type: str = "CartCheckedOut"
    aggregate_type: str = "Cart"

    order_id: UUID  # The resulting order


def main():
    cart_id = uuid4()
    product_a = uuid4()
    product_b = uuid4()
    order_id = uuid4()

    # Simulate cart activity
    events = [
        ItemAddedToCart(
            aggregate_id=cart_id,
            item_id=product_a,
            quantity=2,
            unit_price=29.99,
            aggregate_version=1,
        ),
        ItemAddedToCart(
            aggregate_id=cart_id,
            item_id=product_b,
            quantity=1,
            unit_price=49.99,
            aggregate_version=2,
        ),
        ItemRemovedFromCart(
            aggregate_id=cart_id,
            item_id=product_a,
            aggregate_version=3,
        ),
        CartCheckedOut(
            aggregate_id=cart_id,
            order_id=order_id,
            aggregate_version=4,
        ),
    ]

    print(f"Cart History (Cart ID: {cart_id}):")
    for i, event in enumerate(events, 1):
        if isinstance(event, ItemAddedToCart):
            print(f"  {i}. Added item {event.item_id} (qty: {event.quantity}, ${event.unit_price})")
        elif isinstance(event, ItemRemovedFromCart):
            print(f"  {i}. Removed item {event.item_id}")
        elif isinstance(event, CartCheckedOut):
            print(f"  {i}. Checked out -> Order {event.order_id}")


if __name__ == "__main__":
    main()
```

</details>

---

## Summary

In this tutorial, you learned:

- **DomainEvent base class** provides metadata fields automatically (event_id, occurred_at, correlation_id, etc.)
- **Event definition** requires setting `event_type` and `aggregate_type`, plus your business payload
- **@register_event decorator** registers events for serialization and deserialization
- **Events are immutable** - once created, they cannot be changed
- **Helper methods** like `with_metadata()` create new event instances with modifications
- **Naming conventions** - use past tense, domain language, and be specific
- **Pydantic validation** automatically validates event data

---

## Key Takeaways

!!! note "Remember"
    - Events capture **what happened**, not what should happen
    - Always use **past tense** names: Created, Updated, Completed
    - Register all events with **@register_event** for serialization
    - Events are **immutable** - create new instances to "change" them
    - Include all data needed to understand what happened in your event payload

!!! tip "Best Practice"
    Design events from the perspective of what information you would need if you were reading an audit log. If someone asked "What happened to this task?", your events should provide a clear answer.

!!! warning "Common Mistake"
    Don't store derived or calculated data in events. Store the raw facts. For example, store the items and their prices, not the calculated total - the total can always be recalculated from the events.

---

## Next Steps

You now know how to define domain events that capture business facts. In the next tutorial, you will learn how to use these events within **aggregates** - the business objects that enforce rules and emit events.

Continue to [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md) to learn how to use these events in business logic.

---

## Related Documentation

- [API Reference: Events](../api/events.md) - Complete event API documentation
- [Getting Started Guide](../getting-started.md) - Quick reference for event basics
- [ADR-002: Pydantic Event Models](../adrs/0002-pydantic-event-models.md) - Why we use Pydantic for events
