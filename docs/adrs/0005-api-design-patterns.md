# ADR-0005: API Design Patterns

**Status:** Accepted

**Date:** 2025-12-06

**Deciders:** Tyler Evans

---

## Context

A well-designed API is essential for library usability, maintainability, and developer experience. The eventsource-py library provides complex functionality (event sourcing, aggregates, projections) that must be accessible to developers without deep expertise in event sourcing patterns.

### Requirements

1. **Aggregate Persistence**: Clean interface for loading and saving aggregates without exposing event store internals
2. **Event Streaming**: Memory-efficient access to potentially large event streams
3. **Projection Handlers**: Declarative event routing with minimal boilerplate
4. **Testability**: Interfaces that enable easy mocking and testing
5. **Type Safety**: Strong typing throughout with IDE support and compile-time checking
6. **Consistency**: Predictable patterns across all library components

### Forces

- Users should not need to understand implementation details to use the library effectively
- Memory usage must be controlled when processing large event streams
- Boilerplate should be minimized without sacrificing clarity
- The API should follow Python conventions and feel natural to Python developers
- Async-first design (see ADR-0001) must be reflected in all public APIs

## Decision

We adopt several key API design patterns that provide clean abstractions while maintaining flexibility and performance.

### 1. Repository Pattern for Aggregates

The `AggregateRepository` provides a clean abstraction over event store operations, handling the complexity of loading, saving, and versioning aggregates.

```python
from eventsource import AggregateRepository, InMemoryEventStore

store = InMemoryEventStore()
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=event_bus,  # optional
)

# Create new aggregate
order = repo.create_new(uuid4())
order.create(customer_id=uuid4())
await repo.save(order)

# Load existing aggregate
order = await repo.load(order_id)
order.ship(tracking_number="TRACK123")
await repo.save(order)

# Check existence
if await repo.exists(order_id):
    order = await repo.load(order_id)
```

**Benefits:**
- Encapsulates event store operations (append, get_events)
- Handles optimistic locking automatically via `expected_version`
- Supports optional event publishing after successful save
- Type-safe with generics (`AggregateRepository[TAggregate]`)
- Clean factory pattern for aggregate instantiation

### 2. AsyncIterator for Event Streaming

Event store read operations return `AsyncIterator[StoredEvent]` for memory-efficient streaming of potentially large event streams.

```python
from eventsource import ReadOptions, ReadDirection

# Stream events from a specific aggregate
async for stored_event in store.read_stream("order-123:Order"):
    print(f"Event at position {stored_event.stream_position}")
    projection.handle(stored_event.event)

# Stream with options
options = ReadOptions(
    direction=ReadDirection.FORWARD,
    from_position=100,
    limit=50,
)
async for stored_event in store.read_stream(stream_id, options):
    await process_event(stored_event)

# Stream all events (for projections)
async for stored_event in store.read_all():
    # Process event without loading all into memory
    await projection.handle(stored_event.event)
```

**Benefits:**
- Memory-efficient for large streams (no full list in memory)
- Natural async/await integration
- Supports backpressure through async iteration
- Works with Python async iteration protocols (`async for`)
- Configurable via `ReadOptions` dataclass

### 3. @handles Decorator for Declarative Projections

The `@handles` decorator enables declarative event routing in projections, eliminating manual routing boilerplate and providing automatic handler discovery.

```python
from eventsource.handlers import handles
from eventsource.projections.base import DeclarativeProjection

class OrderProjection(DeclarativeProjection):
    @handles(OrderCreated)
    async def _on_created(self, conn, event: OrderCreated) -> None:
        await conn.execute(
            "INSERT INTO orders (id, status) VALUES ($1, $2)",
            event.aggregate_id, "created"
        )

    @handles(OrderShipped)
    async def _on_shipped(self, conn, event: OrderShipped) -> None:
        await conn.execute(
            "UPDATE orders SET status = $1 WHERE id = $2",
            "shipped", event.aggregate_id
        )

    async def _truncate_read_models(self) -> None:
        # Reset logic for rebuild
        pass
```

**Benefits:**
- Event type explicitly visible at method definition
- Automatic `subscribed_to()` generation from decorators
- Handler signature validation at initialization time
- Eliminates manual event routing switch statements
- Supports both sync and async handlers

### 4. Protocol Classes for Interface Contracts

We use Python's `Protocol` for structural subtyping where duck typing is preferred, and ABC for nominal inheritance where explicit contracts are required.

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class EventHandler(Protocol):
    """Protocol for handling events - any class with handle() works."""
    async def handle(self, event: DomainEvent) -> None: ...

class EventPublisher(Protocol):
    """Protocol for publishing events to external systems."""
    async def publish(self, events: list[DomainEvent]) -> None: ...
```

**Benefits:**
- `Protocol` enables duck typing with type safety
- `@runtime_checkable` allows `isinstance()` checks
- ABCs used for inheritance hierarchies (EventStore, Projection)
- Clear separation between contracts and implementations

### 5. Immutable Data Classes for Configuration and Results

Configuration objects and results use frozen dataclasses for immutability and clarity.

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class ReadOptions:
    direction: ReadDirection = ReadDirection.FORWARD
    from_position: int = 0
    limit: int | None = None
    from_timestamp: datetime | None = None
    to_timestamp: datetime | None = None

@dataclass(frozen=True)
class AppendResult:
    success: bool
    new_version: int
    global_position: int = 0
    conflict: bool = False

    @classmethod
    def successful(cls, new_version: int, global_position: int = 0) -> "AppendResult":
        return cls(success=True, new_version=new_version, global_position=global_position)
```

**Benefits:**
- Immutability prevents accidental modification
- Clear documentation of all configuration options
- Factory classmethods for common cases (`AppendResult.successful()`)
- Validation in `__post_init__` where needed

### 6. Pydantic BaseModel for Domain Events

Domain events use Pydantic for validation, serialization, and immutability (see ADR-0002).

```python
class DomainEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str = Field(...)
    aggregate_id: UUID = Field(...)
    # ... other fields

    def with_causation(self, causing_event: "DomainEvent") -> Self:
        """Fluent API for event enrichment."""
        return self.model_copy(update={
            "causation_id": causing_event.event_id,
            "correlation_id": causing_event.correlation_id,
        })
```

**Benefits:**
- Automatic validation on instantiation
- JSON serialization via `model_dump(mode="json")`
- Fluent API with `with_*` methods for immutable updates
- Strong typing with IDE autocompletion

### 7. Generics for Type-Safe Collections

Generic types enable type-safe APIs while maintaining flexibility.

```python
from typing import Generic, TypeVar

TAggregate = TypeVar("TAggregate", bound="AggregateRoot[Any]")
TState = TypeVar("TState", bound="BaseModel")

class AggregateRepository(Generic[TAggregate]):
    def __init__(
        self,
        event_store: EventStore,
        aggregate_factory: type[TAggregate],
        aggregate_type: str,
    ) -> None: ...

    async def load(self, aggregate_id: UUID) -> TAggregate: ...

class AggregateRoot(Generic[TState], ABC):
    @property
    def state(self) -> TState | None: ...
```

**Benefits:**
- Type checker validates correct aggregate types
- IDE provides accurate autocompletion for state properties
- Factory patterns work correctly with type inference

### 8. Consistent Naming Conventions

The library follows consistent naming patterns:

| Pattern | Example | Usage |
|---------|---------|-------|
| `async def action()` | `load()`, `save()`, `handle()` | Main operations |
| `def sync_action()` | N/A - we prefer async | Sync alternatives when needed |
| `async def get_*()` | `get_events()`, `get_checkpoint()` | Retrieval operations |
| `async def *_exists()` | `event_exists()`, `exists()` | Boolean existence checks |
| `_private_method()` | `_apply()`, `_process_event()` | Internal methods |
| `@property` | `version`, `state`, `uncommitted_events` | Read-only accessors |
| `create_*()` | `create_new()` | Factory methods |
| `with_*()` | `with_causation()`, `with_metadata()` | Fluent immutable updates |

## Consequences

### Positive

- **Clean separation of concerns**: Repository hides event store complexity from domain logic
- **Memory-efficient streaming**: AsyncIterator prevents memory issues with large event streams
- **Reduced boilerplate**: Declarative projections eliminate manual event routing
- **Strong typing**: Generics and Protocols provide IDE support and catch errors early
- **Testability**: Protocols and ABCs enable easy mocking
- **Pythonic feel**: Follows Python conventions (async/await, dataclasses, type hints)
- **Discoverability**: Consistent naming makes APIs predictable

### Negative

- **Repository indirection**: Adds a layer between domain and event store (acceptable trade-off for cleaner domain code)
- **Decorator-based discovery uses reflection**: DeclarativeProjection inspects methods at initialization (minor performance impact, one-time cost)
- **Learning curve**: Developers must learn async iteration patterns
- **Generics complexity**: Type variable bounds can be confusing for newcomers

### Neutral

- **Generic types require explicit factory parameter**: `aggregate_factory: type[TAggregate]` is needed for proper type inference
- **Handler signatures must follow convention**: `(self, conn, event)` or `(self, event)` pattern is required
- **Frozen dataclasses require copy-on-modify**: `model_copy()` instead of direct mutation

## References

### Code References

- `src/eventsource/aggregates/repository.py` - AggregateRepository implementation
- `src/eventsource/stores/interface.py` - EventStore ABC, read_stream(), read_all()
- `src/eventsource/projections/decorators.py` - @handles decorator
- `src/eventsource/projections/base.py` - DeclarativeProjection
- `src/eventsource/projections/protocols.py` - EventHandler Protocol
- `src/eventsource/events/base.py` - DomainEvent with fluent API

### External Documentation

- [Martin Fowler - Repository Pattern](https://martinfowler.com/eaaCatalog/repository.html)
- [PEP 544 - Protocols: Structural Subtyping](https://peps.python.org/pep-0544/)
- [ADR-0001: Async-First Design](0001-async-first-design.md)
- [ADR-0002: Pydantic Event Models](0002-pydantic-event-models.md)
- [ADR-0003: Optimistic Locking](0003-optimistic-locking.md)

## Notes

### Alternatives Considered

1. **Direct event store access (no repository):**
   - Rejected: Would couple domain logic to event store implementation details
   - Every aggregate operation would need to handle versioning, stream naming, and event publishing
   - Repository pattern provides a clean abstraction worth the indirection

2. **List-based streaming (`async def read_stream() -> list[StoredEvent]`):**
   - Rejected: Would load entire event stream into memory
   - Problematic for large aggregates or global event streams
   - AsyncIterator provides memory-efficient streaming with backpressure

3. **Manual handler routing (switch/match statements):**
   - Rejected: Requires duplicating event types in subscribed_to() and handle()
   - Error-prone: easy to forget to add new event types
   - @handles decorator eliminates duplication and provides validation

4. **Configuration-based handlers (YAML/JSON configuration):**
   - Rejected: Separates handler definition from implementation
   - Loses IDE support and type checking
   - Decorator pattern keeps code and metadata together

5. **Callback-based event handling (event_store.on(EventType, handler)):**
   - Rejected: Harder to manage lifecycle and testing
   - Projection pattern with subscribed_to() is more explicit
   - Class-based projections enable dependency injection and state
