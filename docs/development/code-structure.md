# Code Structure Guide

This guide explains the eventsource package organization, module responsibilities, and how the components interact.

## Overview

The eventsource library follows a modular architecture designed around the core concepts of event sourcing:

- **Events** are the source of truth
- **Aggregates** maintain consistency boundaries
- **Stores** persist events durably
- **Projections** build read models from events
- **Buses** distribute events to interested consumers

The package uses the `src` layout pattern with all source code under `src/eventsource/`.

## Package Structure

```
src/eventsource/
├── __init__.py          # Public API exports (version 0.1.0)
├── types.py             # Shared type definitions
├── exceptions.py        # Exception hierarchy
├── config.py            # Configuration utilities
├── py.typed             # PEP 561 marker for type hints
│
├── events/              # Event foundation layer
│   ├── __init__.py
│   ├── base.py          # DomainEvent base class
│   └── registry.py      # Event type registration
│
├── stores/              # Event persistence layer
│   ├── __init__.py
│   ├── interface.py     # EventStore ABC and data classes
│   ├── in_memory.py     # InMemoryEventStore
│   └── postgresql.py    # PostgreSQLEventStore
│
├── aggregates/          # Aggregate pattern implementation
│   ├── __init__.py
│   ├── base.py          # AggregateRoot, DeclarativeAggregate
│   └── repository.py    # AggregateRepository
│
├── projections/         # Read model building
│   ├── __init__.py
│   ├── base.py          # Projection base classes
│   ├── decorators.py    # @handles decorator
│   ├── protocols.py     # EventSubscriber protocol
│   └── coordinator.py   # ProjectionCoordinator
│
├── repositories/        # Infrastructure repositories
│   ├── __init__.py
│   ├── _json.py         # JSON utilities (internal)
│   ├── checkpoint.py    # CheckpointRepository
│   ├── dlq.py           # DLQRepository
│   └── outbox.py        # OutboxRepository
│
├── bus/                 # Event distribution
│   ├── __init__.py
│   ├── interface.py     # EventBus ABC and protocols
│   ├── memory.py        # InMemoryEventBus
│   └── redis.py         # RedisEventBus
│
└── migrations/          # Database schema
    ├── __init__.py
    ├── schemas/         # SQL schema files
    └── templates/       # Alembic templates
```

## Module Dependency Graph

The package follows a layered architecture where higher layers depend on lower layers:

```
                      ┌─────────────┐
                      │   types.py  │
                      │ exceptions  │
                      └──────┬──────┘
                             │
                      ┌──────▼──────┐
                      │   events/   │
                      │  DomainEvent│
                      │  Registry   │
                      └──────┬──────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
  ┌──────▼──────┐     ┌──────▼──────┐     ┌──────▼──────┐
  │   stores/   │     │    bus/     │     │projections/ │
  │  EventStore │     │  EventBus   │     │ Projection  │
  └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
         │                   │                   │
         │                   │           ┌───────▼───────┐
         │                   │           │ repositories/ │
         │                   │           │  Checkpoint   │
         │                   │           │  DLQ, Outbox  │
         │                   │           └───────────────┘
  ┌──────▼──────┐
  │ aggregates/ │
  │AggregateRoot│
  │ Repository  │
  └─────────────┘
         │
         ▼
  (Application Code)
```

## Core Module Details

### types.py

Shared type definitions used throughout the library.

| Type | Description |
|------|-------------|
| `TState` | TypeVar for aggregate state (bound to `BaseModel`) |
| `AggregateId` | UUID alias for aggregate identifiers |
| `EventId` | UUID alias for event identifiers |
| `TenantId` | UUID or None for multi-tenancy |
| `CorrelationId` | UUID for linking related events |
| `CausationId` | UUID or None for event causation tracking |
| `Version` | int alias for optimistic locking |
| `StreamPosition` | int for position within an aggregate stream |
| `GlobalPosition` | int for position across all events |

**Dependencies:** pydantic (for BaseModel bound)

### exceptions.py

Exception hierarchy for the library:

| Exception | Description |
|-----------|-------------|
| `EventSourceError` | Base exception for all library errors |
| `OptimisticLockError` | Version conflict during event append |
| `EventNotFoundError` | Event lookup failed |
| `AggregateNotFoundError` | Aggregate has no events |
| `ProjectionError` | Projection failed to process event |
| `EventStoreError` | Event store operation failed |
| `EventBusError` | Event bus operation failed |
| `CheckpointError` | Checkpoint operation failed |
| `SerializationError` | Event serialization/deserialization failed |

**Dependencies:** None (stdlib only)

---

### events/

The foundation of event sourcing - all other modules depend on this.

#### base.py - DomainEvent

The immutable base class for all domain events, built on Pydantic:

```python
from eventsource import DomainEvent

class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    order_total: Decimal
```

**Key Features:**
- Immutable (Pydantic `frozen=True`)
- Auto-generated `event_id`, `occurred_at`, `correlation_id`
- Causation tracking via `with_causation()`
- Metadata enrichment via `with_metadata()`
- JSON serialization via `to_dict()` / `from_dict()`

**Dependencies:** pydantic, datetime, uuid

#### registry.py - EventRegistry

Maps event type names to classes for deserialization:

```python
from eventsource import register_event, get_event_class

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    ...

# Later, deserialize from storage
event_class = get_event_class("OrderCreated")
event = event_class.from_dict(stored_data)
```

**Key Features:**
- Thread-safe registration
- Decorator-based or explicit registration
- Multiple registry support for testing isolation
- Helpful error messages listing available types

**Dependencies:** events/base.py, threading

---

### stores/

The persistence layer for events.

#### interface.py - EventStore ABC

Defines the contract for event persistence:

| Class/Type | Purpose |
|------------|---------|
| `EventStore` | Abstract base for async event stores |
| `EventStream` | Container for an aggregate's events |
| `StoredEvent` | Event with position metadata |
| `AppendResult` | Result of append operation |
| `ReadOptions` | Configuration for reading events |
| `ReadDirection` | Forward or backward reading |
| `ExpectedVersion` | Constants for optimistic locking |
| `EventPublisher` | Protocol for publishing events |

**Key Operations:**
- `append_events()` - Atomic append with optimistic locking
- `get_events()` - Retrieve events for an aggregate
- `get_events_by_type()` - Query events by aggregate type
- `event_exists()` - Idempotency check
- `read_stream()` / `read_all()` - Streaming reads

**Dependencies:** events/base.py, abc

#### in_memory.py - InMemoryEventStore

Testing and development implementation:

```python
from eventsource import InMemoryEventStore

store = InMemoryEventStore()
result = await store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[order_created],
    expected_version=0,
)
```

**Suitable for:** Unit tests, prototyping, single-process apps
**Not suitable for:** Production, distributed systems, data persistence

**Dependencies:** stores/interface.py, collections, threading

#### postgresql.py - PostgreSQLEventStore

Production implementation using SQLAlchemy async:

```python
from eventsource import PostgreSQLEventStore
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

engine = create_async_engine("postgresql+asyncpg://...")
session_factory = async_sessionmaker(engine, expire_on_commit=False)
store = PostgreSQLEventStore(
    session_factory,
    outbox_enabled=True,
    enable_tracing=True,
)
```

**Features:**
- Optimistic locking via database constraints
- Optional outbox pattern integration
- Optional OpenTelemetry tracing
- Partition-aware timestamp filtering

**Dependencies:** stores/interface.py, events/registry.py, sqlalchemy, json

---

### aggregates/

The aggregate pattern for consistency boundaries.

#### base.py - AggregateRoot

Base class for event-sourced aggregates:

```python
from eventsource import AggregateRoot, handles

class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(...)
```

**Alternative:** `DeclarativeAggregate` with `@handles` decorator:

```python
class OrderAggregate(DeclarativeAggregate[OrderState]):
    aggregate_type = "Order"

    @handles(OrderCreated)
    def _on_order_created(self, event: OrderCreated) -> None:
        self._state = OrderState(...)
```

**Key Features:**
- Generic state type parameter
- Uncommitted event tracking
- Event replay for reconstitution
- Version tracking for optimistic locking

**Dependencies:** events/base.py, types.py, abc

#### repository.py - AggregateRepository

Repository pattern for aggregate persistence:

```python
from eventsource import AggregateRepository

repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=event_bus,  # Optional
)

# Load existing
order = await repo.load(order_id)

# Save changes
order.ship(tracking_number="TRACK123")
await repo.save(order)
```

**Key Features:**
- Load aggregates from event history
- Save uncommitted events atomically
- Optional event publishing after save
- `load_or_create()` for new aggregates

**Dependencies:** aggregates/base.py, stores/interface.py, exceptions.py

---

### projections/

Read model building from events.

#### base.py - Projection Base Classes

| Class | Purpose |
|-------|---------|
| `Projection` | Abstract base for async projections |
| `EventHandlerBase` | Base for event handlers |
| `CheckpointTrackingProjection` | Adds checkpoint, retry, DLQ |
| `DeclarativeProjection` | Uses `@handles` decorator |

**Dependencies:** events/base.py, abc

#### decorators.py - @handles

Decorator for declarative event handling:

```python
from eventsource.projections import DeclarativeProjection, handles

class OrderProjection(DeclarativeProjection):
    @handles(OrderCreated)
    async def _handle_order_created(self, conn, event: OrderCreated):
        await conn.execute(...)

    @handles(OrderShipped)
    async def _handle_order_shipped(self, conn, event: OrderShipped):
        await conn.execute(...)
```

**Dependencies:** events/base.py

#### protocols.py - EventSubscriber

Protocol for event subscription:

```python
class OrderProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        ...
```

**Dependencies:** events/base.py, typing

#### coordinator.py - ProjectionCoordinator

Manages multiple projections:

```python
from eventsource.projections import ProjectionRegistry

registry = ProjectionRegistry()
registry.register_projection(OrderProjection())
registry.register_projection(InventoryProjection())

# Dispatch event to all interested projections
await registry.dispatch(event)
```

**Dependencies:** projections/base.py, projections/protocols.py

---

### repositories/

Infrastructure support repositories.

#### checkpoint.py - CheckpointRepository

Tracks projection processing position:

```python
from eventsource import CheckpointRepository, PostgreSQLCheckpointRepository

repo = PostgreSQLCheckpointRepository(conn)

# Get last processed event
last_event_id = await repo.get_checkpoint("MyProjection")

# Update after processing
await repo.update_checkpoint("MyProjection", event.event_id, event.event_type)

# Get lag metrics
metrics = await repo.get_lag_metrics("MyProjection", ["OrderCreated", "OrderShipped"])
```

**Implementations:**
- `PostgreSQLCheckpointRepository` - Production
- `InMemoryCheckpointRepository` - Testing

**Dependencies:** sqlalchemy, datetime, uuid

#### dlq.py - DLQRepository

Dead letter queue for failed events:

```python
from eventsource import DLQRepository, PostgreSQLDLQRepository

repo = PostgreSQLDLQRepository(conn)

# Add failed event
await repo.add_failed_event(
    event_id=event.event_id,
    projection_name="MyProjection",
    event_type=event.event_type,
    event_data=event.to_dict(),
    error=exception,
)

# Get failures for investigation
failed = await repo.get_failed_events(projection_name="MyProjection")

# Mark resolved after fix
await repo.mark_resolved(dlq_id, resolved_by="admin")
```

**Implementations:**
- `PostgreSQLDLQRepository` - Production
- `InMemoryDLQRepository` - Testing

**Dependencies:** sqlalchemy, datetime, uuid, traceback

#### outbox.py - OutboxRepository

Transactional outbox pattern:

```python
from eventsource import OutboxRepository, PostgreSQLOutboxRepository

repo = PostgreSQLOutboxRepository(conn)

# Add event to outbox (in same transaction as event store)
outbox_id = await repo.add_event(event)

# Background worker publishes and marks complete
pending = await repo.get_pending_events(limit=100)
for entry in pending:
    await event_bus.publish(...)
    await repo.mark_published(entry.id)
```

**Implementations:**
- `PostgreSQLOutboxRepository` - Production
- `InMemoryOutboxRepository` - Testing

**Dependencies:** events/base.py, sqlalchemy, datetime, uuid

---

### bus/

Event distribution to consumers.

#### interface.py - EventBus ABC

Defines the contract for event distribution:

| Type | Purpose |
|------|---------|
| `EventBus` | Abstract base for event buses |
| `EventHandler` | Protocol for handlers with `handle()` method |
| `EventSubscriber` | Protocol declaring subscribed event types |
| `EventHandlerFunc` | Type alias for function handlers |
| `AsyncEventHandler` | Base class for async handlers |

**Key Operations:**
- `publish()` - Send events to subscribers
- `subscribe()` - Register handler for event type
- `subscribe_all()` - Register EventSubscriber for all its types
- `subscribe_to_all_events()` - Wildcard subscription

**Dependencies:** events/base.py, abc

#### memory.py - InMemoryEventBus

In-process event distribution:

```python
from eventsource import InMemoryEventBus

bus = InMemoryEventBus()
bus.subscribe(OrderCreated, order_handler)
bus.subscribe_to_all_events(audit_logger)

# Synchronous (wait for handlers)
await bus.publish([event])

# Fire and forget
await bus.publish([event], background=True)

# Graceful shutdown
await bus.shutdown(timeout=30.0)
```

**Features:**
- Thread-safe subscription
- Sync and async handler support
- Error isolation between handlers
- Optional OpenTelemetry tracing
- Background task management

**Suitable for:** Single-instance apps, testing
**Not suitable for:** Distributed deployments

**Dependencies:** bus/interface.py, asyncio, threading

#### redis.py - RedisEventBus

Distributed event streaming via Redis Streams:

```python
from eventsource.bus import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="myapp",
    consumer_group="projections",
)
bus = RedisEventBus(config=config, event_registry=registry)

await bus.connect()
bus.subscribe(OrderCreated, order_handler)
await bus.publish([event])
await bus.start_consuming()  # Blocks, processing events
```

**Features:**
- At-least-once delivery via consumer groups
- Horizontal scaling with multiple consumers
- Automatic pending message recovery
- Dead letter queue for unrecoverable failures
- Pipeline optimization for batch publishing
- Optional OpenTelemetry tracing

**Dependencies:** bus/interface.py, events/registry.py, redis.asyncio

---

## Public API Surface

The main `__init__.py` exports the public API. Everything listed in `__all__` is considered stable:

### Core Types
```python
from eventsource import (
    # Type aliases
    TState, AggregateId, EventId, TenantId, CorrelationId, CausationId,
)
```

### Events
```python
from eventsource import (
    DomainEvent,
    EventRegistry, default_registry,
    register_event, get_event_class, get_event_class_or_none,
    is_event_registered, list_registered_events,
    EventTypeNotFoundError, DuplicateEventTypeError,
)
```

### Event Stores
```python
from eventsource import (
    EventStore, EventPublisher,
    EventStream, AppendResult, StoredEvent,
    ReadOptions, ReadDirection, ExpectedVersion,
    InMemoryEventStore, PostgreSQLEventStore,
)
```

### Aggregates
```python
from eventsource import (
    AggregateRoot, DeclarativeAggregate, handles,
    AggregateRepository,
)
```

### Event Bus
```python
from eventsource import (
    EventBus, EventHandler, EventSubscriber,
    EventHandlerFunc, AsyncEventHandler,
    InMemoryEventBus,
    RedisEventBus, RedisEventBusConfig, RedisEventBusStats,
    RedisNotAvailableError, REDIS_AVAILABLE,
)
```

### Repositories
```python
from eventsource import (
    CheckpointRepository, PostgreSQLCheckpointRepository,
    InMemoryCheckpointRepository, CheckpointData, LagMetrics,
    DLQRepository, PostgreSQLDLQRepository, InMemoryDLQRepository,
    DLQEntry, DLQStats, ProjectionFailureCount,
    OutboxRepository, PostgreSQLOutboxRepository, InMemoryOutboxRepository,
    OutboxEntry, OutboxStats,
    EventSourceJSONEncoder,
)
```

### Exceptions
```python
from eventsource import (
    EventSourceError,
    OptimisticLockError,
    AggregateNotFoundError,
    EventNotFoundError,
    ProjectionError,
)
```

---

## Internal vs Public

**Public (stable API):** Everything exported from the top-level `__init__.py`

**Internal (may change):**
- Modules starting with `_` (e.g., `repositories/_json.py`)
- Classes/functions not in `__all__`
- Helper utilities within modules
- Anything accessed by direct submodule import that's not re-exported

**Example of internal usage (avoid in application code):**
```python
# Internal - may change without notice
from eventsource.repositories._json import json_dumps

# Public - stable API
from eventsource import EventSourceJSONEncoder
```

---

## Extension Points

### Custom Event Store

Implement `EventStore` ABC for new storage backends:

```python
from eventsource import EventStore, AppendResult, EventStream

class MongoDBEventStore(EventStore):
    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        # Implement MongoDB-specific logic
        ...

    async def get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
    ) -> EventStream:
        ...

    # ... other abstract methods
```

### Custom Projection

Extend `DeclarativeProjection` for read models:

```python
from eventsource.projections import DeclarativeProjection, handles

class OrderDashboardProjection(DeclarativeProjection):
    @handles(OrderCreated)
    async def _on_order_created(self, conn, event: OrderCreated) -> None:
        await conn.execute(
            "INSERT INTO order_dashboard (order_id, status) VALUES ($1, $2)",
            event.aggregate_id, "created"
        )

    @handles(OrderShipped)
    async def _on_order_shipped(self, conn, event: OrderShipped) -> None:
        await conn.execute(
            "UPDATE order_dashboard SET status = $1 WHERE order_id = $2",
            "shipped", event.aggregate_id
        )
```

### Custom Event Bus

Implement `EventBus` ABC for new distribution mechanisms:

```python
from eventsource import EventBus, EventHandler, EventHandlerFunc

class KafkaEventBus(EventBus):
    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        # Implement Kafka publishing
        ...

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: EventHandler | EventHandlerFunc,
    ) -> None:
        ...

    # ... other abstract methods
```

---

## Type Safety

The package is fully typed with a `py.typed` marker (PEP 561):

- All public APIs have type hints
- Compatible with mypy strict mode
- IDE autocomplete supported
- Generic types for aggregates (`AggregateRoot[TState]`)

Run type checking:
```bash
mypy src/eventsource/
```

---

## Related Documentation

### Architecture Decision Records

| ADR | Relevance |
|-----|-----------|
| [ADR-0001: Async-First Design](../adrs/0001-async-first-design.md) | Why all stores/buses are async |
| [ADR-0002: Pydantic Event Models](../adrs/0002-pydantic-event-models.md) | DomainEvent design choices |
| [ADR-0003: Optimistic Locking](../adrs/0003-optimistic-locking.md) | Version-based concurrency |
| [ADR-0004: Projection Error Handling](../adrs/0004-projection-error-handling.md) | DLQ and retry strategy |
| [ADR-0005: API Design Patterns](../adrs/0005-api-design-patterns.md) | Public API conventions |
| [ADR-0006: Event Registry](../adrs/0006-event-registry-serialization.md) | Event type registration |

### Other Documentation

- [Architecture Overview](../architecture.md) - High-level system design
- [Getting Started Guide](../getting-started.md) - Quick start tutorial
- [API Reference](../api/index.md) - Detailed API documentation
- [Testing Guide](testing.md) - Testing strategies and patterns
- [Production Guide](../guides/production.md) - Deployment recommendations
