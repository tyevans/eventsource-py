# Events API Reference

This document covers the event system in eventsource, including the `DomainEvent` base class and the `EventRegistry` for serialization/deserialization.

## DomainEvent

`DomainEvent` is the base class for all domain events. Events are immutable records of things that have happened in the system.

### Import

```python
from eventsource import DomainEvent
```

### Class Definition

```python
class DomainEvent(BaseModel):
    """Base class for all domain events."""

    # Event metadata
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str = Field(...)
    event_version: int = Field(default=1, ge=1)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # Aggregate information
    aggregate_id: UUID = Field(...)
    aggregate_type: str = Field(...)
    aggregate_version: int = Field(default=1, ge=1)

    # Multi-tenancy (optional)
    tenant_id: UUID | None = Field(default=None)

    # Actor tracking
    actor_id: str | None = Field(default=None)

    # Correlation and causation
    correlation_id: UUID = Field(default_factory=uuid4)
    causation_id: UUID | None = Field(default=None)

    # Additional metadata
    metadata: dict[str, Any] = Field(default_factory=dict)
```

### Creating Events

Define your events by subclassing `DomainEvent`:

```python
from uuid import UUID
from eventsource import DomainEvent, register_event

@register_event
class UserRegistered(DomainEvent):
    """Event emitted when a user registers."""
    event_type: str = "UserRegistered"
    aggregate_type: str = "User"

    # Event-specific fields
    email: str
    username: str
```

### Key Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `event_id` | `UUID` | Unique identifier for this event instance |
| `event_type` | `str` | Type name of the event (e.g., "OrderCreated") |
| `event_version` | `int` | Schema version for this event type (for migrations) |
| `occurred_at` | `datetime` | When the event occurred (UTC timestamp) |
| `aggregate_id` | `UUID` | ID of the aggregate this event belongs to |
| `aggregate_type` | `str` | Type of aggregate (e.g., "Order") |
| `aggregate_version` | `int` | Version of aggregate after this event |
| `tenant_id` | `UUID \| None` | Optional tenant ID for multi-tenancy |
| `actor_id` | `str \| None` | User/system that triggered this event |
| `correlation_id` | `UUID` | ID linking related events across aggregates |
| `causation_id` | `UUID \| None` | ID of the event that caused this event |
| `metadata` | `dict` | Additional event metadata |

### Methods

#### `with_causation(causing_event: DomainEvent) -> Self`

Create a copy with causation tracking from another event:

```python
original_event = OrderCreated(...)
new_event = PaymentProcessed(...)
caused_event = new_event.with_causation(original_event)

assert caused_event.causation_id == original_event.event_id
assert caused_event.correlation_id == original_event.correlation_id
```

#### `with_metadata(**kwargs) -> Self`

Add additional metadata to the event:

```python
event = OrderCreated(...)
enriched = event.with_metadata(trace_id="abc123", source="api")
assert enriched.metadata["trace_id"] == "abc123"
```

#### `with_aggregate_version(version: int) -> Self`

Create a copy with a specific aggregate version:

```python
event = OrderCreated(...)
versioned = event.with_aggregate_version(5)
assert versioned.aggregate_version == 5
```

#### `to_dict() -> dict[str, Any]`

Convert event to JSON-serializable dictionary:

```python
event = OrderCreated(...)
data = event.to_dict()
# All UUIDs and datetimes are converted to strings
```

#### `from_dict(data: dict) -> Self` (classmethod)

Create event from dictionary:

```python
data = {"event_type": "OrderCreated", ...}
event = OrderCreated.from_dict(data)
```

#### `is_caused_by(event: DomainEvent) -> bool`

Check if this event was caused by another event:

```python
if caused_event.is_caused_by(original_event):
    print("Event chain detected")
```

#### `is_correlated_with(event: DomainEvent) -> bool`

Check if events are part of the same logical operation:

```python
if event1.is_correlated_with(event2):
    print("Part of same saga")
```

---

## EventRegistry

The `EventRegistry` maps event type names to event classes, enabling proper deserialization from storage.

### Import

```python
from eventsource import (
    EventRegistry,
    register_event,
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
)
```

### Using the Default Registry

The module provides a default registry for common use cases:

```python
# Register with decorator
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"

# Look up event class
event_class = get_event_class("OrderCreated")

# Check if registered
if is_event_registered("OrderCreated"):
    print("Event type is registered")

# List all registered events
event_types = list_registered_events()
```

### Registration Patterns

#### Decorator (Recommended)

```python
# Basic decorator - uses event_type field
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"

# With explicit type name
@register_event(event_type="order.created")
class OrderCreated(DomainEvent):
    event_type: str = "order.created"
    aggregate_type: str = "Order"
```

#### Explicit Registration

```python
from eventsource import EventRegistry

registry = EventRegistry()
registry.register(OrderCreated)
registry.register(OrderCreated, "order.created")  # With custom name
```

### Creating Isolated Registries

For testing or module isolation:

```python
from eventsource import EventRegistry, register_event

# Create isolated registry
test_registry = EventRegistry()

# Register to specific registry
@register_event(registry=test_registry)
class TestEvent(DomainEvent):
    event_type: str = "TestEvent"
    aggregate_type: str = "Test"

# Look up in specific registry
event_class = test_registry.get("TestEvent")
```

### Registry Methods

| Method | Description |
|--------|-------------|
| `register(event_class, event_type=None)` | Register an event class |
| `get(event_type)` | Get event class by type name (raises if not found) |
| `get_or_none(event_type)` | Get event class or None |
| `contains(event_type)` | Check if type is registered |
| `list_types()` | List all registered type names |
| `list_classes()` | List all registered classes |
| `unregister(event_type)` | Remove registration |
| `clear()` | Clear all registrations |

### Error Handling

```python
from eventsource import EventTypeNotFoundError, DuplicateEventTypeError

try:
    event_class = get_event_class("UnknownEvent")
except EventTypeNotFoundError as e:
    print(f"Unknown event: {e.event_type}")
    print(f"Available: {e.available_types}")

# Duplicate registration raises error
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"

try:
    @register_event
    class AnotherOrderCreated(DomainEvent):
        event_type: str = "OrderCreated"  # Same type name!
except DuplicateEventTypeError as e:
    print(f"Duplicate: {e.event_type}")
```

---

## Best Practices

### Event Naming

- Use past tense (OrderCreated, not CreateOrder)
- Be specific and descriptive
- Use domain language

### Event Fields

- Keep events focused on a single change
- Include all data needed to understand the change
- Use immutable field types when possible

### Versioning

- Use `event_version` for schema evolution
- Create new event types for breaking changes
- Implement upcasters for version migration

### Example: Complete Event Definition

```python
from datetime import datetime
from uuid import UUID
from eventsource import DomainEvent, register_event
from pydantic import Field

@register_event
class OrderPlaced(DomainEvent):
    """
    Event emitted when a customer places an order.

    This event captures all the information needed to process
    the order and is the initial event in the order lifecycle.
    """
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    event_version: int = 2  # Schema version for migrations

    # Business data
    customer_id: UUID
    order_total: float = Field(ge=0)
    currency: str = Field(default="USD", pattern="^[A-Z]{3}$")
    items: list[dict] = Field(default_factory=list)

    # Optional context
    promo_code: str | None = None
    notes: str | None = None
```
