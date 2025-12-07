# ADR-0006: Event Registry and Serialization

**Status:** Accepted

**Date:** 2025-12-06

**Deciders:** Tyler Evans

---

## Context

Event sourcing requires serializing events to persistent storage (databases, message queues) and deserializing them back into typed Python objects. When events are stored, they are converted to JSON with a string `event_type` field identifying the event class. When events are retrieved, the system must map this string back to the correct Python class to reconstruct the event instance.

### The Deserialization Problem

When loading events from storage, we receive:
- `event_type`: A string like `"OrderCreated"` or `"order.created.v2"`
- `event_data`: A JSON payload containing the event fields

To deserialize this into a proper Python object, we need to:
1. Map the string `event_type` to the corresponding Python class
2. Use that class to validate and instantiate the event

Without a registry, each event store would need to hardcode mappings or use error-prone reflection/import mechanisms.

### Requirements for the Registry

1. **Explicit Type Mapping**: The mapping between type names and classes should be explicit and predictable, not based on magic or convention alone.

2. **Thread Safety**: Multi-threaded applications (web servers, background workers) may register events concurrently during import time or access the registry during request handling.

3. **Testing Isolation**: Tests should be able to use isolated registries to avoid polluting the global state or encountering conflicts between test modules.

4. **Helpful Error Messages**: When deserialization fails due to an unknown type, the error should list available types to aid debugging.

5. **Flexible Registration**: Support multiple registration patterns to accommodate different coding styles and use cases (decorators, programmatic registration, custom type names).

6. **Idempotent Registration**: Registering the same class multiple times should not raise an error, enabling safe re-imports.

### Forces at Play

- Event type names may need to differ from class names (versioning: `order.created.v2`, namespacing: `com.example.OrderCreated`)
- Module imports trigger class definitions, which is when registration typically occurs
- The default/global registry is convenient for most applications but problematic for testing
- Type resolution must be deterministic and predictable

## Decision

We implement a **thread-safe `EventRegistry` class** with a **`@register_event` decorator** for declarative registration and **module-level convenience functions** for the common case of using a default global registry.

### EventRegistry Class

The registry is a simple mapping from event type names (strings) to event classes, protected by a reentrant lock:

```python
# src/eventsource/events/registry.py

class EventRegistry:
    """Registry for mapping event type names to event classes."""

    def __init__(self) -> None:
        self._registry: dict[str, type[DomainEvent]] = {}
        self._lock = threading.RLock()

    def register(
        self,
        event_class: type[TEvent],
        event_type: str | None = None,
    ) -> type[TEvent]:
        """Register an event class, returns the class (enables decorator use)."""
        resolved_type = self._resolve_event_type(event_class, event_type)

        with self._lock:
            if resolved_type in self._registry:
                existing = self._registry[resolved_type]
                if existing is not event_class:
                    raise DuplicateEventTypeError(resolved_type, existing, event_class)
                return event_class  # Idempotent: same class already registered

            self._registry[resolved_type] = event_class
            return event_class

    def get(self, event_type: str) -> type[DomainEvent]:
        """Look up class by type name, raises EventTypeNotFoundError if not found."""
        with self._lock:
            if event_type not in self._registry:
                raise EventTypeNotFoundError(event_type, list(self._registry.keys()))
            return self._registry[event_type]
```

Key design choices:
- **`threading.RLock()`**: Reentrant lock allows nested registration (e.g., a registration that triggers another registration during class initialization)
- **Identity comparison (`is not`)**: Prevents registering a different class with the same type name, but allows re-registering the same class
- **Returns the class**: Enables use as a decorator that doesn't alter the class

### Type Resolution Priority

When registering an event, the type name is resolved in this priority order:

```python
def _resolve_event_type(self, event_class, event_type):
    # 1. Explicit parameter takes precedence
    if event_type is not None:
        return event_type

    # 2. Pydantic field default value (if it's a string)
    if hasattr(event_class, "model_fields"):
        field_info = event_class.model_fields.get("event_type")
        if field_info and isinstance(field_info.default, str):
            return field_info.default

    # 3. Class name as fallback
    return event_class.__name__
```

This allows:
- Explicit overrides when needed (`@register_event(event_type="order.created.v2")`)
- Convention-based registration using the Pydantic field default (`event_type: str = "OrderCreated"`)
- Zero-configuration fallback using the class name

### Registration Patterns

The `@register_event` decorator supports multiple usage patterns:

```python
# Pattern 1: Simple decorator (uses class's event_type field or class name)
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID

# Pattern 2: Explicit type name override
@register_event(event_type="order.created.v2")
class OrderCreatedV2(DomainEvent):
    event_type: str = "order.created.v2"
    aggregate_type: str = "Order"
    ...

# Pattern 3: Custom registry for testing isolation
test_registry = EventRegistry()

@register_event(registry=test_registry)
class TestOnlyEvent(DomainEvent):
    ...

# Pattern 4: Programmatic registration (no decorator)
registry.register(OrderCreated)
registry.register(OrderCreatedV2, "order.created.v2")
```

The decorator implementation handles both `@register_event` and `@register_event()` syntax:

```python
def register_event(
    event_class: type[TEvent] | None = None,
    *,
    event_type: str | None = None,
    registry: EventRegistry | None = None,
) -> type[TEvent] | Callable[[type[TEvent]], type[TEvent]]:
    target_registry = registry or default_registry

    def decorator(cls: type[TEvent]) -> type[TEvent]:
        return target_registry.register(cls, event_type)

    if event_class is not None:
        return decorator(event_class)
    return decorator
```

### Default Registry and Convenience Functions

A module-level default registry provides a singleton for typical applications:

```python
# Module-level singleton
default_registry = EventRegistry()

# Convenience functions using the default registry
def get_event_class(event_type: str) -> type[DomainEvent]:
    return default_registry.get(event_type)

def is_event_registered(event_type: str) -> bool:
    return default_registry.contains(event_type)

def list_registered_events() -> list[str]:
    return default_registry.list_types()
```

### Serialization Flow (Storing Events)

When an event is stored, it is serialized using Pydantic's JSON-compatible serialization:

```
Event Instance (OrderCreated)
        |
        v
event.to_dict()  -->  event.model_dump(mode="json")
        |
        v
{"event_type": "OrderCreated", "aggregate_id": "...", "event_id": "...", ...}
        |
        v
JSON stored in database (PostgreSQL JSONB) or message queue
```

The `event_type` field is stored alongside the payload, enabling later deserialization.

### Deserialization Flow (Loading Events)

When events are loaded from storage, the registry maps the type string to the class:

```
Load from storage
        |
        v
{"event_type": "OrderCreated", "aggregate_id": "...", ...}
        |
        v
registry.get("OrderCreated")  -->  OrderCreated class
        |
        v
OrderCreated.from_dict(data)  -->  OrderCreated.model_validate(data)
        |
        v
Event Instance (OrderCreated)
```

The PostgreSQL store implementation demonstrates this:

```python
# src/eventsource/stores/postgresql.py

def _deserialize_event(self, event_type: str, payload: str | dict, ...) -> DomainEvent:
    # Get class from registry
    event_class = self._event_registry.get(event_type)

    # Parse JSON payload
    event_data = payload if isinstance(payload, dict) else json.loads(payload)

    # Create validated instance
    return event_class.model_validate(event_data, strict=False)
```

### Error Handling

Two custom exceptions provide clear error messages:

```python
class EventTypeNotFoundError(KeyError):
    """Raised when event type is not in registry."""

    def __init__(self, event_type: str, available_types: list[str]) -> None:
        available = ", ".join(sorted(available_types)) if available_types else "none"
        super().__init__(
            f"Unknown event type: '{event_type}'. "
            f"Available types: {available}. "
            f"Did you forget to register this event type?"
        )

class DuplicateEventTypeError(ValueError):
    """Raised when registering a different class with an existing type name."""

    def __init__(self, event_type: str, existing_class, new_class) -> None:
        super().__init__(
            f"Event type '{event_type}' is already registered to {existing_class.__name__}. "
            f"Cannot register {new_class.__name__} with the same type name."
        )
```

## Consequences

### Positive

1. **Explicit registration is predictable**: No magic or implicit discovery. Developers know exactly which events are registered by looking at the decorators or registration calls.

2. **Thread-safe operations**: The `RLock` protects against race conditions during concurrent registration or lookup, safe for multi-threaded web servers.

3. **Testing isolation via custom registries**: Tests can create isolated registries that don't affect the global state:
   ```python
   test_registry = EventRegistry()
   @register_event(registry=test_registry)
   class TestEvent(DomainEvent): ...
   ```

4. **Helpful error messages**: When an unknown event type is encountered, the error message lists all available types, making debugging straightforward.

5. **Flexible registration patterns**: The decorator works with or without parentheses and supports explicit type names, accommodating different coding styles.

6. **Idempotent re-registration**: Re-importing a module that registers events does not raise errors, preventing issues with Python's module caching.

7. **Support for versioned type names**: Explicit type names like `"order.created.v2"` enable schema evolution without renaming classes.

### Negative

1. **Every event type must be registered**: If an event is not registered before deserialization, it will fail. This requires ensuring all event modules are imported at startup.

2. **Registration order matters**: Events must be registered before any code attempts to deserialize them. This typically means importing event modules early in the application startup.

3. **Default registry is global state**: The module-level `default_registry` is a singleton, which some consider an anti-pattern. However, this is mitigated by the ability to use custom registries.

4. **Type name collisions**: If two different classes are accidentally registered with the same type name, the second registration raises an error rather than silently overwriting.

### Neutral

1. **Module imports trigger registration**: Using the `@register_event` decorator means registration happens at import time. This is the standard Python pattern and works well with most application structures.

2. **Type resolution has defined priority**: The priority order (explicit > field default > class name) is deterministic, but developers must understand it to predict behavior.

3. **Registry is not persisted**: The registry is rebuilt on each application start by importing event modules. This is intentional, as the registry reflects the current codebase.

## References

### Code References

- `src/eventsource/events/registry.py` - Complete `EventRegistry` implementation
- `src/eventsource/events/base.py` - `DomainEvent` base class with serialization methods
- `src/eventsource/stores/postgresql.py` - Registry usage in deserialization (see `_deserialize_event` method)
- `src/eventsource/events/__init__.py` - Public API exports

### External Documentation

- [Event Sourcing - Martin Fowler](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Event Type Registry Pattern](https://enterprisecraftsmanship.com/posts/cqrs-vs-specification-pattern/)
- [Python Threading RLock](https://docs.python.org/3/library/threading.html#rlock-objects)

### Related ADRs

- [ADR-0002: Pydantic Event Models](0002-pydantic-event-models.md) - Events use Pydantic for serialization via `model_dump()` and `model_validate()`
- [ADR-0001: Async-First Design](0001-async-first-design.md) - Event stores are async but registry operations are synchronous (thread-safe)

## Notes

### Alternatives Considered

1. **Convention-based auto-discovery**
   - Scan modules for `DomainEvent` subclasses automatically
   - **Why rejected**: Implicit and unpredictable. Depends on import order. Hard to understand which events are available. Doesn't support custom type names for versioning.

2. **Configuration file mapping**
   - Define type-to-class mappings in YAML/JSON configuration
   - **Why rejected**: Creates coupling between code and configuration. Easy to get out of sync. Harder to refactor. Doesn't benefit from Python's type checking.

3. **Metaclass-based auto-registration**
   - Use `__init_subclass__` or a metaclass to automatically register all event subclasses
   - **Why rejected**: Too magical. Hard to control which registry is used. Doesn't support explicit type name overrides. Testing isolation becomes difficult.

4. **Global dictionary without locking**
   - Simple `dict[str, type]` without thread safety
   - **Why rejected**: Race conditions in multi-threaded applications. Unsafe for web servers with multiple workers/threads.

5. **Duck typing without registry**
   - Infer the class from the payload structure
   - **Why rejected**: Fragile and slow. Requires scanning all known classes. Ambiguous when multiple classes have similar structures. No clear error handling for unknown types.

### Schema Evolution Strategy

The registry supports schema evolution through type name versioning:

```python
# Original event
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    total_amount: float  # Version 1: dollars as float

# New version with different schema
@register_event(event_type="OrderCreated.v2")
class OrderCreatedV2(DomainEvent):
    event_type: str = "OrderCreated.v2"
    event_version: int = 2
    total_amount_cents: int  # Version 2: cents as integer
```

Both versions can coexist in the registry, and the event store will deserialize each to the correct class based on the stored `event_type`.

### Startup Recommendations

To ensure all events are registered before deserialization:

```python
# app/events/__init__.py
from app.events.orders import OrderCreated, OrderShipped, OrderCancelled
from app.events.payments import PaymentReceived, PaymentRefunded
from app.events.inventory import StockReserved, StockReleased

# app/main.py
import app.events  # Ensure all events are imported at startup
```

This pattern ensures that by the time any code attempts to load events from storage, all event classes have been registered.
