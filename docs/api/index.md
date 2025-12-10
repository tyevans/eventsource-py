# API Reference

This section provides comprehensive API documentation for the eventsource library. The library implements the event sourcing pattern with a focus on type safety, async-first design, and developer ergonomics.

## Quick Reference

### All Public Classes and Functions

| Module | Class/Function | Description |
|--------|----------------|-------------|
| **Events** | [`DomainEvent`](events.md#domainevent) | Base class for all domain events |
| | [`EventRegistry`](events.md#eventregistry) | Maps event type names to classes for deserialization |
| | [`@register_event`](events.md#using-the-default-registry) | Decorator to register event types |
| | [`get_event_class`](events.md#using-the-default-registry) | Look up event class by type name |
| | [`is_event_registered`](events.md#using-the-default-registry) | Check if event type is registered |
| | [`list_registered_events`](events.md#using-the-default-registry) | List all registered event types |
| **Stores** | [`EventStore`](stores.md#eventstore-interface) | Abstract interface for event persistence |
| | [`InMemoryEventStore`](stores.md#inmemoryeventstore) | In-memory store for testing |
| | [`PostgreSQLEventStore`](stores.md#postgresqleventstore) | Production PostgreSQL store |
| | [`SQLiteEventStore`](stores.md#sqliteeventstore) | SQLite store for dev/testing/embedded |
| | [`EventStream`](stores.md#eventstream) | Container for aggregate events |
| | [`AppendResult`](stores.md#appendresult) | Result of appending events |
| | [`StoredEvent`](stores.md#storedevent) | Wrapper for persisted events |
| | [`ExpectedVersion`](stores.md#expectedversion) | Constants for version expectations |
| **Aggregates** | [`AggregateRoot`](aggregates.md#aggregateroot) | Base class for event-sourced aggregates |
| | [`DeclarativeAggregate`](aggregates.md#declarativeaggregate) | Aggregate with decorator-based handlers |
| | [`AggregateRepository`](aggregates.md#aggregaterepository) | Repository pattern for aggregates |
| **Projections** | [`Projection`](projections.md#projection) | Simple async projection base class |
| | [`DeclarativeProjection`](projections.md#declarativeprojection) | Projection with `@handles` decorators |
| | [`CheckpointTrackingProjection`](projections.md#checkpointtrackingprojection) | Projection with checkpoints and DLQ |
| | [`@handles`](projections.md#declarativeprojection) | Decorator for event handlers |
| | [`ProjectionCoordinator`](projections.md#projectioncoordinator) | Coordinates multiple projections |
| **Bus** | [`EventBus`](bus.md#eventbus-interface) | Abstract interface for event distribution |
| | [`InMemoryEventBus`](bus.md#inmemoryeventbus) | In-memory bus for single-process apps |
| | [`RedisEventBus`](bus.md#rediseventbus) | Distributed bus using Redis Streams |
| | [`RedisEventBusConfig`](bus.md#configuration) | Configuration for Redis bus |
| | [`EventHandler`](bus.md#eventhandler) | Protocol for event handlers |
| | [`EventSubscriber`](bus.md#eventsubscriber) | Protocol for subscribers with declared types |
| **Snapshots** | [`Snapshot`](snapshots.md#snapshot) | Point-in-time aggregate state capture |
| | [`SnapshotStore`](snapshots.md#snapshotstore-interface) | Abstract interface for snapshot storage |
| | [`InMemorySnapshotStore`](snapshots.md#inmemorysnapshotstore) | In-memory store for testing |
| | [`PostgreSQLSnapshotStore`](snapshots.md#postgresqlsnapshotstore) | Production PostgreSQL store |
| | [`SQLiteSnapshotStore`](snapshots.md#sqlitesnapshotstore) | SQLite store for dev/embedded |
| **Observability** | [`OTEL_AVAILABLE`](observability.md#otel_available) | OpenTelemetry availability constant |
| | [`get_tracer`](observability.md#get_tracer) | Get OpenTelemetry tracer if available |
| | [`should_trace`](observability.md#should_trace) | Check if tracing should be active |
| | [`@traced`](observability.md#traced-decorator) | Decorator for method-level tracing |
| | [`TracingMixin`](observability.md#tracingmixin-class) | Mixin class for tracing support |
| **Subscriptions** | [`SubscriptionManager`](subscriptions.md#subscriptionmanager) | Main entry point for subscription management |
| | [`Subscription`](subscriptions.md#subscription) | Individual subscription state machine |
| | [`SubscriptionConfig`](subscriptions.md#subscriptionconfig) | Configuration for subscriptions |
| | [`CatchUpRunner`](subscriptions.md#catchuprunner) | Historical event processing |
| | [`LiveRunner`](subscriptions.md#liverunner) | Real-time event processing |
| | [`TransitionCoordinator`](subscriptions.md#transitioncoordinator) | Catch-up to live transition |
| | [`FlowController`](subscriptions.md#flowcontroller) | Backpressure management |
| | [`ShutdownCoordinator`](subscriptions.md#shutdowncoordinator) | Graceful shutdown coordination |
| | [`CircuitBreaker`](subscriptions.md#circuitbreaker) | Circuit breaker for resilience |
| | [`EventFilter`](subscriptions.md#eventfilter) | Event filtering support |

---

## Module Overview

### Events Module

The foundation of event sourcing - immutable records of state changes.

```python
from eventsource import (
    DomainEvent,
    register_event,
    EventRegistry,
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
)
```

**Key Components:**

- **`DomainEvent`**: Base class for all domain events. Provides metadata fields (event_id, occurred_at, correlation_id), aggregate tracking, and serialization methods.

- **`EventRegistry`**: Maps event type names to event classes, enabling proper deserialization from storage.

- **`@register_event`**: Decorator that registers an event class with the default registry.

**Example:**

```python
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    total: float
```

[Read the full Events API documentation](events.md)

---

### Event Stores Module

Persistent storage for events with optimistic locking and streaming support.

```python
from eventsource import (
    # Interface
    EventStore,

    # Implementations
    InMemoryEventStore,
    PostgreSQLEventStore,
    SQLiteEventStore,

    # Data structures
    EventStream,
    AppendResult,
    StoredEvent,
    ReadOptions,
    ReadDirection,
    ExpectedVersion,
)
```

**Key Components:**

- **`EventStore`**: Abstract interface defining append_events, get_events, and streaming methods.

- **`InMemoryEventStore`**: Thread-safe in-memory implementation for testing and development.

- **`SQLiteEventStore`**: Lightweight SQLite implementation for development, testing, and embedded applications.

- **`PostgreSQLEventStore`**: Production-ready store with optimistic locking, transactional outbox, and OpenTelemetry tracing.

**Example:**

```python
# Development (no persistence)
store = InMemoryEventStore()

# Development/Testing (SQLite)
async with SQLiteEventStore("./events.db") as store:
    await store.initialize()
    # ... use store

# Production (PostgreSQL)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
session_factory = async_sessionmaker(engine, expire_on_commit=False)
store = PostgreSQLEventStore(session_factory, outbox_enabled=True)
```

[Read the full Event Stores API documentation](stores.md)

---

### Aggregates Module

Consistency boundaries that enforce business rules and emit events.

```python
from eventsource import (
    AggregateRoot,
    DeclarativeAggregate,
    AggregateRepository,
    handles,  # For DeclarativeAggregate
)
```

**Key Components:**

- **`AggregateRoot`**: Generic base class for aggregates. Manages state, tracks uncommitted events, and handles version management.

- **`DeclarativeAggregate`**: Alternative to AggregateRoot using `@handles` decorators for cleaner event application.

- **`AggregateRepository`**: Repository pattern for loading aggregates from event streams and saving new events. Supports event publishing, snapshot integration, and optimistic locking.

> **See the [Repository Pattern Guide](../guides/repository-pattern.md) for comprehensive usage patterns.**

**Example:**

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                status="created",
            )

    def create(self, customer_id: UUID) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            aggregate_version=self.get_next_version(),
        ))
```

[Read the full Aggregates API documentation](aggregates.md)

---

### Projections Module

Build read-optimized views from event streams.

```python
from eventsource.projections import (
    # Base classes
    Projection,
    DeclarativeProjection,
    CheckpointTrackingProjection,

    # Decorators
    handles,

    # Coordinator
    ProjectionCoordinator,
)
```

**Key Components:**

- **`Projection`**: Simple abstract base class for async projections.

- **`DeclarativeProjection`**: Uses `@handles` decorators for clean event routing.

- **`CheckpointTrackingProjection`**: Full-featured projection with automatic checkpointing, retry logic, and dead letter queue support.

- **`ProjectionCoordinator`**: Coordinates multiple projections for batch event processing.

**Example:**

```python
class OrderSummaryProjection(DeclarativeProjection):
    @handles(OrderCreated)
    async def _handle_created(self, event: OrderCreated) -> None:
        await self._db.execute(
            "INSERT INTO orders (id, customer_id, status) VALUES ($1, $2, $3)",
            event.aggregate_id, event.customer_id, "created"
        )

    @handles(OrderShipped)
    async def _handle_shipped(self, event: OrderShipped) -> None:
        await self._db.execute(
            "UPDATE orders SET status = 'shipped' WHERE id = $1",
            event.aggregate_id
        )

    async def _truncate_read_models(self) -> None:
        await self._db.execute("TRUNCATE TABLE orders")
```

[Read the full Projections API documentation](projections.md)

---

### Event Bus Module

Distribute events to subscribers in real-time.

```python
from eventsource import (
    # Interface
    EventBus,
    EventHandler,
    EventSubscriber,
    EventHandlerFunc,
    AsyncEventHandler,

    # Implementations
    InMemoryEventBus,
    RedisEventBus,
    RedisEventBusConfig,
)
```

**Key Components:**

- **`EventBus`**: Abstract interface for event publishing and subscription.

- **`InMemoryEventBus`**: Single-process event bus with wildcard subscriptions and background task management.

- **`RedisEventBus`**: Distributed event bus using Redis Streams with consumer groups and reliable delivery.

**Example:**

```python
# Setup
bus = InMemoryEventBus()

# Subscribe handlers
bus.subscribe(OrderCreated, notification_handler)
bus.subscribe_all(analytics_projection)  # Subscribes to all declared types
bus.subscribe_to_all_events(audit_logger)  # Wildcard subscription

# Connect to repository for automatic publishing
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=bus,  # Events published after save
)
```

> **💡 Tip:** For production projections, use [SubscriptionManager](subscriptions.md) instead of direct `subscribe_all()` to get historical catch-up, checkpoint tracking, and health monitoring.

[Read the full Event Bus API documentation](bus.md)

---

### Snapshots Module

Optimize aggregate loading with point-in-time state snapshots.

```python
from eventsource.snapshots import (
    # Core types
    Snapshot,
    SnapshotStore,

    # Implementations
    InMemorySnapshotStore,
    PostgreSQLSnapshotStore,
    SQLiteSnapshotStore,

    # Exceptions
    SnapshotError,
    SnapshotDeserializationError,
    SnapshotSchemaVersionError,
)
```

**Key Components:**

- **`Snapshot`**: Immutable data structure representing captured aggregate state at a point in time.

- **`SnapshotStore`**: Abstract interface for snapshot persistence with upsert semantics.

- **`InMemorySnapshotStore`**: Thread-safe in-memory implementation for testing.

- **`PostgreSQLSnapshotStore`**: Production-ready store with OpenTelemetry tracing.

- **`SQLiteSnapshotStore`**: Lightweight implementation for embedded deployments.

**Example:**

```python
from eventsource import AggregateRepository
from eventsource.snapshots import PostgreSQLSnapshotStore

snapshot_store = PostgreSQLSnapshotStore(session_factory)

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    snapshot_store=snapshot_store,
    snapshot_threshold=100,  # Snapshot every 100 events
    snapshot_mode="background",  # Non-blocking
)

# Load uses snapshot if available
order = await repo.load(order_id)
```

[Read the full Snapshots API documentation](snapshots.md)

---

### Observability Module

Reusable OpenTelemetry tracing utilities for consistent observability.

```python
from eventsource.observability import (
    # Constants
    OTEL_AVAILABLE,

    # Helpers
    get_tracer,
    should_trace,

    # Decorator
    traced,

    # Mixin
    TracingMixin,
)
```

**Key Components:**

- **`OTEL_AVAILABLE`**: Boolean constant indicating OpenTelemetry availability (single source of truth).

- **`get_tracer()`**: Safely obtain an OpenTelemetry tracer, returning None if unavailable.

- **`@traced`**: Decorator for adding tracing to methods with minimal boilerplate.

- **`TracingMixin`**: Mixin class providing `_init_tracing()` and `_create_span_context()` methods for standardized tracing support.

**Example:**

```python
from eventsource.observability import traced, TracingMixin

class MyStore(TracingMixin):
    def __init__(self, enable_tracing: bool = True):
        self._init_tracing(__name__, enable_tracing)

    @traced("my_store.save")
    async def save(self, item_id: str) -> None:
        # Automatically traced
        await self._do_save(item_id)

    async def load(self, item_id: str) -> dict:
        # Dynamic attributes
        with self._create_span_context(
            "my_store.load",
            {"item.id": item_id},
        ):
            return await self._do_load(item_id)
```

[Read the full Observability API documentation](observability.md)

---

### Subscriptions Module

Unified catch-up and live event subscription management with resilience features.

```python
from eventsource.subscriptions import (
    # Core classes
    SubscriptionManager,
    SubscriptionConfig,
    Subscription,
    SubscriptionState,
    SubscriptionStatus,

    # Runners
    CatchUpRunner,
    LiveRunner,
    TransitionCoordinator,

    # Resilience
    FlowController,
    ShutdownCoordinator,
    CircuitBreaker,
    RetryConfig,

    # Filtering
    EventFilter,

    # Error handling
    SubscriptionErrorHandler,
    ErrorHandlerRegistry,

    # Health
    ManagerHealth,
    ReadinessStatus,
    LivenessStatus,
)
```

**Key Components:**

- **`SubscriptionManager`**: Main entry point for managing catch-up and live event subscriptions. Handles subscription lifecycle, graceful shutdown, and health monitoring.

- **`SubscriptionConfig`**: Configuration for subscriptions including start position, batch size, backpressure settings, checkpoint strategy, and retry/circuit breaker settings.

- **`CatchUpRunner`**: Reads historical events from the event store in batches with checkpointing.

- **`LiveRunner`**: Receives real-time events from the event bus with duplicate detection.

- **`TransitionCoordinator`**: Coordinates the transition from catch-up to live using a watermark approach to ensure no events are lost.

- **`FlowController`**: Backpressure management using semaphore-based flow control.

- **`ShutdownCoordinator`**: Signal-aware graceful shutdown with phased draining.

**Example:**

```python
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

# Create manager
manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)

# Register subscribers
await manager.subscribe(
    my_projection,
    SubscriptionConfig(start_from="beginning", batch_size=500)
)

# Run until shutdown signal (SIGTERM/SIGINT)
result = await manager.run_until_shutdown()
```

[Read the full Subscriptions API documentation](subscriptions.md)

---

## Common Import Patterns

### Minimal Import (Most Common Use Case)

```python
from eventsource import (
    # Events
    DomainEvent,
    register_event,

    # Aggregates
    AggregateRoot,
    AggregateRepository,

    # Stores
    InMemoryEventStore,
)
```

### Production Setup

```python
from eventsource import (
    # Events
    DomainEvent,
    register_event,

    # Aggregates
    AggregateRoot,
    AggregateRepository,

    # Stores
    PostgreSQLEventStore,
    ExpectedVersion,
    OptimisticLockError,

    # Bus
    InMemoryEventBus,  # or RedisEventBus
)

from eventsource.projections import (
    DeclarativeProjection,
    handles,
)
```

### Full-Featured Setup

```python
from eventsource import (
    # Events
    DomainEvent,
    register_event,
    EventRegistry,
    get_event_class,
    EventTypeNotFoundError,
    DuplicateEventTypeError,

    # Aggregates
    AggregateRoot,
    DeclarativeAggregate,
    AggregateRepository,
    AggregateNotFoundError,

    # Stores
    EventStore,
    InMemoryEventStore,
    PostgreSQLEventStore,
    EventStream,
    AppendResult,
    StoredEvent,
    ReadOptions,
    ExpectedVersion,
    OptimisticLockError,

    # Bus
    EventBus,
    InMemoryEventBus,
    RedisEventBus,
    RedisEventBusConfig,
    EventHandler,
    EventSubscriber,
)

from eventsource.projections import (
    Projection,
    SyncProjection,
    DeclarativeProjection,
    CheckpointTrackingProjection,
    handles,
    ProjectionCoordinator,
)

from eventsource.repositories import (
    CheckpointRepository,
    DLQRepository,
    PostgreSQLCheckpointRepository,
    PostgreSQLDLQRepository,
    SQLiteCheckpointRepository,
    SQLiteOutboxRepository,
    SQLiteDLQRepository,
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
)

from eventsource.subscriptions import (
    SubscriptionManager,
    SubscriptionConfig,
    Subscription,
    SubscriptionState,
    FlowController,
    ShutdownCoordinator,
    CircuitBreaker,
    RetryConfig,
    EventFilter,
)
```

---

## Quick Start Example

Here is a minimal working example demonstrating the core API:

```python
import asyncio
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
)

# 1. Define events
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID

@register_event
class OrderCompleted(DomainEvent):
    event_type: str = "OrderCompleted"
    aggregate_type: str = "Order"

# 2. Define state
class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    status: str = "draft"

# 3. Define aggregate
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                status="created",
            )
        elif isinstance(event, OrderCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, customer_id: UUID) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Cannot complete order")
        self.apply_event(OrderCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))

# 4. Use it
async def main():
    store = InMemoryEventStore()
    repo = AggregateRepository(store, OrderAggregate, "Order")

    # Create and save
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(customer_id=uuid4())
    await repo.save(order)

    # Load, modify, save
    order = await repo.load(order_id)
    order.complete()
    await repo.save(order)

    print(f"Order {order_id}: {order.state.status}")

asyncio.run(main())
```

---

## Error Handling

The library provides specific exception types for common error cases:

| Exception | Module | Description |
|-----------|--------|-------------|
| `EventTypeNotFoundError` | Events | Event type not registered |
| `DuplicateEventTypeError` | Events | Event type already registered |
| `OptimisticLockError` | Stores | Concurrent modification detected |
| `AggregateNotFoundError` | Aggregates | No events found for aggregate |
| `RedisNotAvailableError` | Bus | Redis package not installed |

```python
from eventsource import (
    EventTypeNotFoundError,
    DuplicateEventTypeError,
    OptimisticLockError,
    AggregateNotFoundError,
)

# Handle optimistic lock conflicts
try:
    await repo.save(order)
except OptimisticLockError as e:
    # Reload and retry
    order = await repo.load(e.aggregate_id)
    order.complete()
    await repo.save(order)
```

---

## Type Safety

The library is fully typed and works with mypy in strict mode. Key type patterns:

```python
# Generic aggregate with typed state
class OrderAggregate(AggregateRoot[OrderState]):
    ...

# Typed event handlers
@handles(OrderCreated)
async def _handle_created(self, event: OrderCreated) -> None:
    ...

# Protocol-based handlers
class MyHandler(EventHandler):
    async def handle(self, event: DomainEvent) -> None:
        ...
```

---

## See Also

### Guides

- [Getting Started Guide](../getting-started.md) - Step-by-step tutorial for new users
- [Architecture Overview](../architecture.md) - System design and data flow
- [Repository Pattern Guide](../guides/repository-pattern.md) - Comprehensive repository usage
- [Snapshotting Guide](../guides/snapshotting.md) - Optimize aggregate load performance
- [Snapshotting Migration Guide](../guides/snapshotting-migration.md) - Add snapshotting to existing projects
- [Multi-Tenant Guide](../guides/multi-tenant.md) - Multi-tenancy support
- [Error Handling Guide](../guides/error-handling.md) - Error handling patterns
- [Production Guide](../guides/production.md) - Production deployment

### Examples

- [Basic Order Example](../examples/basic-order.md) - Complete order management example
- [Snapshotting Example](../examples/snapshotting.md) - Snapshot configuration and usage
- [Projections Example](../examples/projections.md) - Building read models
- [Multi-Tenant Example](../examples/multi-tenant.md) - Tenant isolation
- [Testing Example](../examples/testing.md) - Testing strategies

### Architecture Decision Records

- [ADR-0001: Async-First Design](../adrs/0001-async-first-design.md) - Why all I/O is async
- [ADR-0002: Pydantic Event Models](../adrs/0002-pydantic-event-models.md) - Event serialization approach
- [ADR-0003: Optimistic Locking](../adrs/0003-optimistic-locking.md) - Concurrency control strategy
- [ADR-0004: Projection Error Handling](../adrs/0004-projection-error-handling.md) - Retry and DLQ patterns
- [ADR-0005: API Design Patterns](../adrs/0005-api-design-patterns.md) - Public API conventions
- [ADR-0006: Event Registry](../adrs/0006-event-registry-serialization.md) - Event type registration
