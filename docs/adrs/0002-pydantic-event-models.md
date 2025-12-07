# ADR-0002: Pydantic Event Models

**Status:** Accepted

**Date:** 2025-12-06

**Deciders:** Tyler Evans

---

## Context

Domain events are the fundamental data structure in event sourcing. They represent immutable facts about things that have happened in the system and serve as the authoritative source of truth for reconstructing aggregate state. Given their critical role, events must satisfy several requirements:

### Requirements for Event Models

1. **Immutability**: Events represent historical facts and must not be modifiable after creation. Once an event is recorded, its content is fixed forever.

2. **Validation**: Events must be structurally valid at creation time. Invalid events should never enter the event store, as they would corrupt the historical record.

3. **Serialization**: Events must be persistable to databases (PostgreSQL JSONB) and transmittable via message buses (Redis Pub/Sub). This requires reliable JSON serialization with proper handling of complex types (UUIDs, datetimes).

4. **Type Safety**: Developers should benefit from IDE autocomplete, static type checking, and clear error messages when working with events.

5. **Schema Evolution**: As systems evolve, event schemas may need to change. The event model should support versioning to enable forward and backward compatibility.

6. **Rich Metadata**: Events benefit from descriptive field definitions that serve as documentation and enable JSON Schema generation.

7. **Extensibility**: Defining new event types should be straightforward, requiring minimal boilerplate while inheriting common functionality.

### Python Data Model Options (2024-2025)

The Python ecosystem offers several options for structured data models:

| Option | Validation | Serialization | Immutability | Type Safety | Ecosystem |
|--------|------------|---------------|--------------|-------------|-----------|
| **dataclasses** | None built-in | Manual | `frozen=True` | Good | Standard library |
| **attrs** | Validators | Manual | `frozen=True` | Good | Third-party |
| **NamedTuple** | None | None | Inherent | Good | Standard library |
| **Pydantic** | Automatic | Built-in | `frozen=True` | Excellent | Third-party |
| **msgspec** | Automatic | Built-in | Limited | Good | Third-party |

### Forces at Play

- Event sourcing systems are I/O-bound; serialization performance matters but is not the primary bottleneck
- Events are created frequently; the validation and instantiation cost should be reasonable
- Events are read from storage constantly; deserialization must be reliable and type-aware
- The library should integrate well with modern Python async frameworks (FastAPI, Starlette)
- Pydantic v2 is already widely adopted in the Python ecosystem for data validation

## Decision

We use **Pydantic v2 BaseModel as the foundation for all domain events**. All event classes inherit from a `DomainEvent` base class that configures Pydantic for immutability and provides standard event fields.

### Core Implementation

The `DomainEvent` base class uses Pydantic's `ConfigDict(frozen=True)` to enforce immutability:

```python
# src/eventsource/events/base.py
from pydantic import BaseModel, ConfigDict, Field
from uuid import UUID, uuid4
from datetime import datetime, UTC

class DomainEvent(BaseModel):
    """Base class for all domain events."""

    model_config = ConfigDict(frozen=True)

    # Event identification
    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique event identifier",
    )
    event_type: str = Field(
        ...,
        description="Type of event (e.g., 'OrderCreated')",
    )
    event_version: int = Field(
        default=1,
        ge=1,
        description="Event schema version for migrations",
    )
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When event occurred (UTC)",
    )

    # Aggregate information
    aggregate_id: UUID = Field(
        ...,
        description="ID of the aggregate this event belongs to",
    )
    aggregate_type: str = Field(
        ...,
        description="Type of aggregate (e.g., 'Order')",
    )
    aggregate_version: int = Field(
        default=1,
        ge=1,
        description="Version of aggregate after this event",
    )

    # Multi-tenancy support
    tenant_id: UUID | None = Field(
        default=None,
        description="Tenant this event belongs to (optional)",
    )

    # Actor tracking
    actor_id: str | None = Field(
        default=None,
        description="User/system that triggered this event",
    )

    # Event correlation
    correlation_id: UUID = Field(
        default_factory=uuid4,
        description="ID linking related events across aggregates",
    )
    causation_id: UUID | None = Field(
        default=None,
        description="ID of the event that caused this event",
    )

    # Extensible metadata
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional event metadata",
    )
```

### Defining Custom Events

Subclasses define their event-specific payload by adding fields and overriding `event_type` and `aggregate_type`:

```python
class OrderCreated(DomainEvent):
    """Event representing order creation."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"

    # Event-specific fields
    order_number: str = Field(..., description="Unique order number")
    customer_id: UUID = Field(..., description="Customer who placed the order")
    total_amount: float = Field(..., ge=0, description="Order total")
```

### Immutability Pattern

Events are frozen (immutable). Any attempt to modify a field after creation raises a `ValidationError`:

```python
event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001", ...)
event.order_number = "ORD-002"  # Raises ValidationError
```

To create modified versions of events (for adding metadata or causation tracking), we use Pydantic's `model_copy()` method via wrapper methods:

```python
# Create event with causation tracking
caused_event = payment_event.with_causation(order_event)
# caused_event.causation_id == order_event.event_id
# caused_event.correlation_id == order_event.correlation_id

# Add metadata to event
enriched_event = event.with_metadata(trace_id="abc123", source="api")

# Set aggregate version
versioned_event = event.with_aggregate_version(5)
```

These methods return new event instances, preserving immutability of the original.

### Serialization

Pydantic provides built-in JSON-compatible serialization via `model_dump(mode="json")`:

```python
class DomainEvent(BaseModel):
    def to_dict(self) -> dict[str, Any]:
        """Convert event to JSON-serializable dictionary."""
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create event from dictionary with validation."""
        return cls.model_validate(data)
```

The `mode="json"` parameter ensures all values are JSON-serializable:
- UUIDs become strings
- Datetimes become ISO 8601 strings
- Nested models are recursively converted

### Schema Versioning

The `event_version` field enables schema evolution:

```python
class OrderCreatedV1(DomainEvent):
    event_type: str = "OrderCreated"
    event_version: int = 1
    total_amount: float  # Dollars as float

class OrderCreatedV2(DomainEvent):
    event_type: str = "OrderCreated"
    event_version: int = 2
    total_amount_cents: int  # Cents as integer (more precise)
```

Event handlers and projections can use `event_version` to apply appropriate transformations.

### Field Validation

Pydantic's Field constraints provide declarative validation:

```python
event_version: int = Field(default=1, ge=1)  # Must be >= 1
aggregate_version: int = Field(default=1, ge=1)  # Must be >= 1
total_amount: float = Field(..., ge=0)  # Must be non-negative
```

Validation occurs automatically at instantiation, ensuring invalid events are rejected immediately.

## Consequences

### Positive

1. **Automatic validation on event creation**: Invalid events are rejected immediately with clear error messages, preventing corruption of the event store.

2. **JSON serialization built-in**: `model_dump(mode="json")` handles UUIDs, datetimes, and nested objects correctly without custom serializers.

3. **IDE autocomplete and type checking**: Pydantic models work excellently with static type checkers (mypy) and IDE autocomplete.

4. **Rich field descriptions**: Field descriptions serve as inline documentation and enable JSON Schema generation for API documentation.

5. **Immutability enforcement**: `frozen=True` prevents accidental modification of events, ensuring data integrity.

6. **Schema versioning support**: The `event_version` field provides a standard mechanism for schema evolution.

7. **Ecosystem alignment**: Pydantic v2 is widely used in the Python ecosystem (FastAPI, SQLModel, etc.), reducing friction for adoption.

8. **Causation and correlation tracking**: Built-in support for tracing event chains via `with_causation()` method.

### Negative

1. **Pydantic v2 is a required dependency**: The library cannot be used without Pydantic, increasing the dependency footprint.

2. **Custom types require Pydantic-compatible serializers**: Complex custom types need `__get_pydantic_core_schema__` implementations or annotated validators.

3. **Frozen models require `model_copy()` for modifications**: Creating derived events requires understanding Pydantic's copy semantics.

4. **Serialization has some overhead**: Pydantic's validation and serialization is not as fast as msgspec, though this rarely matters for I/O-bound event sourcing workloads.

5. **Learning curve for advanced customization**: Custom validators, serializers, and model configuration require Pydantic-specific knowledge.

### Neutral

1. **Tied to Pydantic v2 (not v1)**: The library uses Pydantic v2 APIs (`model_config`, `model_dump`, `model_validate`) and is not compatible with Pydantic v1.

2. **Events are classes, not plain dicts**: Event instances are full Pydantic models with methods and metadata, which is more heavyweight than plain dictionaries but provides richer functionality.

3. **Metadata dict prevents hashing**: Events with non-empty metadata cannot be used in sets or as dict keys (the metadata dict is mutable even though the model is frozen).

## References

### Code References

- `src/eventsource/events/base.py` - `DomainEvent` base class implementation
- `src/eventsource/events/registry.py` - Event type registry for deserialization
- `tests/unit/test_domain_event.py` - Comprehensive tests demonstrating event usage

### External Documentation

- [Pydantic v2 Documentation](https://docs.pydantic.dev/latest/)
- [Pydantic Frozen Models](https://docs.pydantic.dev/latest/concepts/models/#frozen-models)
- [Pydantic Field Constraints](https://docs.pydantic.dev/latest/concepts/fields/)
- [Pydantic Serialization](https://docs.pydantic.dev/latest/concepts/serialization/)

### Related ADRs

- [ADR-0001: Async-First Design](0001-async-first-design.md) - Pydantic v2 is async-friendly
- [ADR-0006: Event Registry and Serialization](0006-event-registry-serialization.md) - Details the type registry pattern for event deserialization

## Notes

### Alternatives Considered

1. **Standard dataclasses**
   - **Why rejected**: No built-in validation beyond type hints. Requires manual implementation of JSON serialization for UUIDs and datetimes. Would need third-party libraries (e.g., `dacite`, `dataclasses-json`) for proper serialization, fragmenting the solution across multiple dependencies.

2. **attrs**
   - **Why rejected**: While attrs provides validators and `frozen=True`, it lacks Pydantic's integrated JSON serialization and ecosystem presence. Would require additional work for JSON schema generation and integration with FastAPI/OpenAPI.

3. **NamedTuple**
   - **Why rejected**: Immutable by design, but no validation support. Fields are positional, making evolution difficult. Limited to simple types without custom serialization.

4. **msgspec**
   - **Why rejected**: Faster serialization but smaller ecosystem. Less mature than Pydantic with fewer resources and community support. Immutability support is limited compared to Pydantic's frozen models.

5. **Custom implementation**
   - **Why rejected**: Reinventing the wheel. Would need to implement validation, serialization, immutability, and type checking from scratch. Pydantic is mature, well-tested, and widely understood.

### Implementation Notes

- All events should inherit from `DomainEvent` directly or indirectly
- The `event_type` field should match the class name by convention (e.g., `OrderCreated`)
- Use `with_causation()` to establish event chains in sagas and process managers
- The `metadata` field is intentionally flexible for cross-cutting concerns (tracing, request IDs)
- Events are registered in the `EventRegistry` for deserialization (see `src/eventsource/events/registry.py`)

### Future Considerations

- Consider adding support for Pydantic's computed fields for derived values
- Explore discriminated unions for polymorphic event handling
- May add helper methods for common metadata patterns (tracing context, request correlation)
