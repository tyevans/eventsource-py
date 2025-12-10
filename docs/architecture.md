# Architecture Overview

This document explains the event sourcing architecture implemented in eventsource.

## What is Event Sourcing?

Event sourcing is an architectural pattern where:

1. **State is derived from events**: Instead of storing current state, we store a sequence of events that led to the current state.

2. **Events are immutable**: Once stored, events are never modified or deleted.

3. **Events are the source of truth**: All queries and projections are derived from the event stream.

## Key Benefits

- **Complete audit trail**: Every state change is recorded
- **Time travel**: Reconstruct state at any point in time
- **Debug-friendly**: Replay events to understand issues
- **Scalability**: Event streams enable eventual consistency
- **Flexibility**: Add new projections without migration

## System Architecture

```
                            Commands
                               |
                               v
+------------------+    +------------------+    +------------------+
|                  |    |                  |    |                  |
|  Application     |--->|   Aggregates     |--->|   Event Store    |
|  Layer (API)     |    |                  |    |                  |
|                  |    +------------------+    +--------+---------+
+------------------+           |                         |
                              |                         |
                        Events |                         | Events
                              v                         v
                    +------------------+    +------------------+
                    |                  |    |                  |
                    |  Repository      |    |   Event Bus      |
                    |                  |    |                  |
                    +------------------+    +--------+---------+
                                                     |
                                         +-----------+-----------+
                                         |           |           |
                                         v           v           v
                                   +---------+ +---------+ +---------+
                                   |Projection| |Projection| | Handler |
                                   |   A     | |   B     | |         |
                                   +---------+ +---------+ +---------+
                                         |           |
                                         v           v
                                   +---------+ +---------+
                                   |  Read   | |  Read   |
                                   | Model A | | Model B |
                                   +---------+ +---------+
                                         ^           ^
                                         |           |
                                   +-----+-----------+-----+
                                   |                       |
                                   |       Queries         |
                                   |                       |
                                   +-----------------------+
```

## Core Components

### 1. Domain Events

Events are immutable records of things that happened in the system.

```python
@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: UUID
    total: float
    items: list[dict]
```

**Design principles:**
- Use past tense (OrderPlaced, not PlaceOrder)
- Include all data needed to understand the change
- Be domain-focused (business language)
- Never change existing event schemas (create new versions)

### 2. Event Store

The event store is the append-only database for events.

```
+-------------------------------------------------------------+
|                        Event Store                          |
|-------------------------------------------------------------|
| Stream: Order-123                                           |
|   [1] OrderPlaced { customer_id, total, items }             |
|   [2] OrderShipped { tracking_number }                      |
|   [3] OrderDelivered { delivered_at }                       |
|-------------------------------------------------------------|
| Stream: Order-456                                           |
|   [1] OrderPlaced { customer_id, total, items }             |
|   [2] OrderCancelled { reason }                             |
+-------------------------------------------------------------+
```

**Implementations:**
- `InMemoryEventStore`: Development and testing
- `PostgreSQLEventStore`: Production use

**Key features:**
- Optimistic locking prevents concurrent writes
- Global ordering for projections
- Idempotent appending

### 3. Aggregates

Aggregates are consistency boundaries that encapsulate business logic.

```
+------------------+
|   OrderAggregate |
|------------------|
| - aggregate_id   |
| - version        |
| - state          |
|------------------|
| + create()       |  Commands
| + addItem()      |  (Business Logic)
| + ship()         |
|------------------|
| - _apply()       |  State Transitions
| - _getInitial()  |
+------------------+
        |
        | emits
        v
   [DomainEvent]
```

**Responsibilities:**
- Validate business rules
- Emit events for state changes
- Reconstruct state from events

### 4. Repository

Repositories abstract the event store interaction, providing a clean interface for loading and saving aggregates.

```python
from eventsource import AggregateRepository, InMemoryEventStore

repo = AggregateRepository(
    event_store=InMemoryEventStore(),
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=event_bus,      # Optional: publish after save
    snapshot_store=snapshot_store,  # Optional: enable snapshots
)

# Load aggregate (reconstruct from events)
order = await repo.load(order_id)

# Execute command
order.ship(tracking_number="TRACK-123")

# Save (persist new events with optimistic locking)
await repo.save(order)
```

**Key Features:**
- **Clean abstraction**: Simple load/save API hides event store complexity
- **Optimistic locking**: Detects concurrent modifications via version checking
- **Snapshot integration**: Load from snapshot + recent events for performance
- **Event publishing**: Automatically publish events to bus after save
- **OpenTelemetry tracing**: Built-in observability support

See the [Repository Pattern Guide](guides/repository-pattern.md) for comprehensive documentation.

### 5. Projections

Projections build read models from event streams.

```
                   Event Stream
                        |
     +------------------+------------------+
     |                  |                  |
     v                  v                  v
+-----------+    +-----------+    +-----------+
|  Orders   |    | Customer  |    | Inventory |
| Projection|    | Stats     |    | Levels    |
+-----------+    +-----------+    +-----------+
     |                  |                  |
     v                  v                  v
+-----------+    +-----------+    +-----------+
|  orders   |    | customers |    | inventory |
|  table    |    | table     |    | table     |
+-----------+    +-----------+    +-----------+
```

**Types:**
- `Projection`: Simple async base class
- `CheckpointTrackingProjection`: With checkpoint and DLQ
- `DeclarativeProjection`: Using @handles decorators

### 6. Event Bus

The event bus distributes events to subscribers, decoupling producers from consumers.

```
                     Event Bus
                        |
         +--------------+--------------+
         |              |              |
         v              v              v
   +-----------+  +-----------+  +-----------+
   |  Email    |  | Analytics |  |  Search   |
   |  Handler  |  |  Handler  |  |  Indexer  |
   +-----------+  +-----------+  +-----------+
```

**Implementations:**

| Implementation | Use Case | Scaling Model |
|----------------|----------|---------------|
| `InMemoryEventBus` | Development, testing, single-process | None |
| `RedisEventBus` | Real-time distributed systems | Consumer groups |
| `RabbitMQEventBus` | Enterprise messaging with routing | Queue bindings |
| `KafkaEventBus` | High-throughput event streaming | Partitions |

See the [Event Bus Guide](guides/event-bus.md) for detailed usage.

## Data Flow

### Write Path (Commands)

```
1. API receives command
2. Load aggregate from repository
3. Execute command method on aggregate
4. Aggregate validates and emits event(s)
5. Repository saves events to store
6. Events published to event bus
7. Response returned to client
```

### Read Path (Queries)

```
1. API receives query
2. Query read model directly (not aggregate)
3. Return data to client
```

### Projection Path

```
1. Projection subscribes to events
2. Event store/bus delivers events
3. Projection updates read model
4. Checkpoint saved (for recovery)
```

## Design Decisions

### Why Pydantic for Events?

- **Validation**: Automatic field validation
- **Serialization**: JSON serialization built-in
- **Type Safety**: Full type hint support
- **Immutability**: `frozen=True` configuration

### Why Separate Event Registry?

- **Decoupling**: Events defined anywhere
- **Deserialization**: Lookup by type name
- **Thread Safety**: Safe concurrent access

### Why Repository Pattern?

- **Clean API**: Simple load/save interface
- **Testability**: Easy to mock
- **Encapsulation**: Hides event store details
- **Publishing**: Automatic event distribution

### Why Abstract Event Store?

- **Flexibility**: Swap implementations
- **Testing**: In-memory for tests
- **Production**: PostgreSQL for durability

## Consistency Model

### Strong Consistency (Aggregates)

Within a single aggregate, consistency is guaranteed:
- Optimistic locking prevents conflicts
- Events applied in order
- State always valid

### Eventual Consistency (Projections)

Across aggregates and read models:
- Projections may lag behind events
- Read models eventually consistent
- Design for idempotency

## Error Handling

### Optimistic Locking

```python
try:
    order = await repo.load(order_id)
    order.ship(tracking)
    await repo.save(order)
except OptimisticLockError:
    # Reload and retry
    order = await repo.load(order_id)
    order.ship(tracking)
    await repo.save(order)
```

### Projection Failures

```python
class MyProjection(CheckpointTrackingProjection):
    MAX_RETRIES = 3  # Retry transient failures
    # After max retries, event goes to DLQ
```

## Scaling Considerations

### Event Store Scaling

- **Partitioning**: By aggregate type or tenant
- **Archiving**: Move old events to cold storage
- **Trimming**: Keep only recent events in hot path

### Projection Scaling

- **Parallel Processing**: Multiple projection instances
- **Consumer Groups**: Redis Streams for load balancing
- **Checkpoints**: Resume from last position

### Event Bus Scaling

- **Redis Streams**: Distributed processing with consumer groups
- **RabbitMQ**: Exchange-based routing with competing consumers
- **Kafka**: Partition-based scaling with consumer groups
- **Consumer Groups**: Horizontal scaling across all distributed bus implementations
- **Backpressure**: Handle slow consumers through acknowledgment mechanisms

## Multi-Tenancy

Built-in support via `tenant_id` field:

```python
event = OrderPlaced(
    aggregate_id=order_id,
    tenant_id=tenant_id,  # Isolates data
    ...
)

# Query by tenant
events = await store.get_events_by_type("Order", tenant_id=tenant_id)
```

## Observability

eventsource provides comprehensive observability support through the `eventsource.observability` module.

### Logging

- Structured logging throughout
- Event IDs for correlation
- Handler success/failure tracking

### Tracing (OpenTelemetry)

OpenTelemetry integration is provided through reusable utilities that ensure consistent tracing across all components:

```python
from eventsource.observability import (
    OTEL_AVAILABLE,   # Single source of truth for availability
    get_tracer,       # Safe tracer acquisition
    traced,           # Decorator for method tracing
    TracingMixin,     # Mixin for tracing support
)
```

**Components with Tracing Support:**

| Component | Configuration | Spans Created |
|-----------|---------------|---------------|
| `PostgreSQLEventStore` | `enable_tracing=True` | `event_store.append_events`, `event_store.get_events` |
| `SQLiteEventStore` | `enable_tracing=True` | `event_store.append_events`, `event_store.get_events` |
| `InMemoryEventBus` | `enable_tracing=True` | `event.dispatch.{EventType}`, `event_handler.{HandlerName}` |
| `RedisEventBus` | `config.enable_tracing=True` | `event_bus.publish`, `event_bus.process_message` |
| `RabbitMQEventBus` | `config.enable_tracing=True` | `event_bus.publish`, `event_bus.consume` |
| `PostgreSQLSnapshotStore` | `enable_tracing=True` | `snapshot_store.save`, `snapshot_store.get` |

**Tracing Patterns:**

```python
# Pattern 1: @traced decorator (for static attributes)
class MyStore(TracingMixin):
    @traced("my_store.operation")
    async def operation(self) -> None:
        pass

# Pattern 2: _create_span_context (for dynamic attributes)
class MyStore(TracingMixin):
    async def save(self, item_id: str) -> None:
        with self._create_span_context(
            "my_store.save",
            {"item.id": item_id},
        ):
            await self._do_save(item_id)
```

**Disabling Tracing:**

Tracing can be disabled per-component for testing or performance reasons:

```python
store = SQLiteEventStore(":memory:", enable_tracing=False)
bus = InMemoryEventBus(enable_tracing=False)
```

### Metrics

- Events published/processed counts
- Projection lag monitoring
- Error rates

## Best Practices

1. **Keep aggregates small**: One aggregate per consistency boundary

2. **Design events carefully**: Events are forever

3. **Use projections for queries**: Never query aggregates

4. **Plan for evolution**: Version events, use upcasters

5. **Test with in-memory stores**: Fast, deterministic tests

6. **Monitor projection lag**: Alert on growing lag

7. **Handle failures gracefully**: Use DLQ for permanent failures
