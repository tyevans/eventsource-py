# Frequently Asked Questions

Common questions about using eventsource-py.

---

## General Questions

### What is event sourcing?

Event sourcing is an architectural pattern where the state of an application is determined by a sequence of events rather than just the current state. Instead of storing the current state directly, you store all the events that led to that state.

```
Traditional: Store current state (Order: status="shipped")
Event Sourcing: Store events (OrderCreated -> OrderPaid -> OrderShipped)
```

The current state is derived by replaying events in order. This provides:

- **Complete audit trail**: Every change is recorded
- **Time travel**: Reconstruct state at any point in time
- **Debugging**: Replay events to understand issues
- **Flexibility**: Add new projections without migrations

See the [Architecture Overview](architecture.md) for more details.

### Why use eventsource-py over other libraries?

eventsource-py is designed specifically for Python 3.11+ with these key advantages:

| Feature | eventsource-py | Other Libraries |
|---------|-------------|-----------------|
| Async-first | Native async/await | Often sync with async wrappers |
| Type safety | Full type hints, Pydantic validation | Varies |
| Production-ready | PostgreSQL, retry logic, DLQ | Often dev-focused |
| Multi-tenancy | Built-in tenant isolation | Usually requires custom code |
| Observability | OpenTelemetry integration | Limited or none |

The library follows Python best practices with clean abstractions that work well with FastAPI, SQLAlchemy 2.0, and modern async patterns.

### What databases are supported?

**Event Stores:**

| Backend | Use Case | Installation |
|---------|----------|--------------|
| `InMemoryEventStore` | Development, testing | Included |
| `SQLiteEventStore` | Lightweight deployments | `pip install eventsource-py[sqlite]` |
| `PostgreSQLEventStore` | Production | `pip install eventsource-py[postgresql]` |

**Event Buses:**

| Backend | Use Case | Installation |
|---------|----------|--------------|
| `InMemoryEventBus` | Development, testing, single-process | Included |
| `RedisEventBus` | Distributed systems | `pip install eventsource-py[redis]` |
| `RabbitMQEventBus` | Enterprise messaging | `pip install eventsource-py[rabbitmq]` |
| `KafkaEventBus` | High-throughput streaming | `pip install eventsource-py[kafka]` |

PostgreSQL is the recommended production event store. For distributed deployments, choose an event bus based on your infrastructure:
- **Redis**: Simple setup, good for Redis-native environments
- **RabbitMQ**: Traditional message broker with advanced routing
- **Kafka**: Highest throughput, best for event streaming at scale

### Is eventsource-py production-ready?

Yes. eventsource-py is designed for production use with:

- **PostgreSQL backend** with proper indexing and partitioning support
- **Optimistic locking** for consistent concurrent access
- **Retry logic** with exponential backoff for projection failures
- **Dead letter queue** for permanent failures
- **OpenTelemetry integration** for observability
- **Multi-tenancy** support built-in

See the [Production Deployment Guide](guides/production.md) for configuration details.

---

## Getting Started

### How do I install the library?

```bash
# Basic installation (in-memory only)
pip install eventsource-py

# With PostgreSQL support (production)
pip install eventsource-py[postgresql]

# With SQLite support
pip install eventsource-py[sqlite]

# Event bus options
pip install eventsource-py[redis]      # Redis Streams
pip install eventsource-py[rabbitmq]   # RabbitMQ/AMQP
pip install eventsource-py[kafka]      # Apache Kafka

# With OpenTelemetry tracing
pip install eventsource-py[telemetry]

# Kafka with Schema Registry support
pip install eventsource-py[kafka-schema-registry]

# All optional dependencies
pip install eventsource-py[all]
```

### What Python versions are supported?

eventsource-py requires **Python 3.11+**. We recommend Python 3.12+ for best performance.

Key dependencies:
- pydantic >= 2.0
- sqlalchemy >= 2.0 (for PostgreSQL)
- asyncpg (for PostgreSQL)

### How do I set up PostgreSQL?

1. **Install PostgreSQL** (version 14+ recommended)

2. **Create the database:**
   ```bash
   createdb eventsource
   ```

3. **Apply the schema:**
   ```bash
   psql -d eventsource -f src/eventsource/migrations/schemas/all.sql
   ```

4. **Configure the connection:**
   ```python
   from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
   from eventsource import PostgreSQLEventStore

   engine = create_async_engine(
       "postgresql+asyncpg://user:password@localhost:5432/eventsource",
       pool_size=10,
       max_overflow=20,
   )
   session_factory = async_sessionmaker(engine, expire_on_commit=False)
   store = PostgreSQLEventStore(session_factory)
   ```

See [Production Deployment](guides/production.md) for detailed configuration.

---

## Events

### How do I define custom events?

Events inherit from `DomainEvent` and must be registered for serialization:

```python
from uuid import UUID
from eventsource import DomainEvent, register_event

@register_event
class OrderCreated(DomainEvent):
    """Event emitted when an order is created."""
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"

    # Your custom fields
    customer_id: UUID
    total_amount: float
    items: list[dict]
```

**Key points:**
- Use `@register_event` decorator for serialization support
- Set `event_type` and `aggregate_type` class attributes
- Use past tense naming (OrderCreated, not CreateOrder)
- All fields are validated by Pydantic

### How does event serialization work?

Events are serialized to JSON using Pydantic's `model_dump()`:

```python
# Serialization (automatic when saving)
event_dict = event.model_dump(mode="json")
# {"event_id": "...", "event_type": "OrderCreated", ...}

# Deserialization (automatic when loading)
event = OrderCreated.model_validate(event_dict)
```

The event registry maps `event_type` strings to Python classes:

```python
from eventsource.events.registry import default_registry

# Register manually (or use @register_event decorator)
default_registry.register(OrderCreated)

# Look up by name
event_class = default_registry.get("OrderCreated")
```

**Important:** Ensure all event classes are imported before deserialization:
```python
# In your app startup
from myapp.events import orders, payments, shipping
```

### How do I version events?

Use the `event_version` field for schema evolution:

```python
@register_event
class OrderCreatedV1(DomainEvent):
    event_type: str = "OrderCreated"
    event_version: int = 1  # Original version
    customer_id: UUID
    total: float

@register_event
class OrderCreatedV2(DomainEvent):
    event_type: str = "OrderCreated"
    event_version: int = 2  # New version
    customer_id: UUID
    total_amount: float  # Renamed field
    currency: str = "USD"  # New field with default
```

Handle both versions in your aggregate or projection:

```python
def _apply(self, event: DomainEvent) -> None:
    if isinstance(event, (OrderCreatedV1, OrderCreatedV2)):
        # Handle both versions
        total = getattr(event, 'total_amount', getattr(event, 'total', 0))
        self._state = OrderState(total_amount=total)
```

**Best practice:** Always add new fields with defaults for backward compatibility.

### Why does eventsource use integers for global ordering instead of UUIDs?

Events have two identifiers with different purposes:

| Field | Type | Purpose |
|-------|------|---------|
| `event_id` | UUID | **Identity** — Unique identifier for deduplication and idempotency |
| `global_position` | Integer | **Ordering** — Strict ordering for subscriptions and projections |

**Why not use UUIDs for ordering?**

Standard UUIDs (v1-v4) are not naturally sortable. While UUID v7 adds timestamp-based ordering, we use database-assigned integers because:

1. **Database guarantees strict ordering**: Auto-increment is atomic and sequential
2. **No clock skew issues**: Multiple writers on different machines won't produce conflicting orderings
3. **Works with any UUID version**: Your `event_id` can be any UUID version you prefer
4. **Simple and proven**: This is the standard pattern used by EventStoreDB, Marten, and other event stores

**How it works in the database:**

```sql
-- PostgreSQL
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,        -- Global position (auto-increment)
    event_id UUID NOT NULL UNIQUE,   -- Unique event identifier
    ...
);

-- SQLite
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Global position
    event_id TEXT NOT NULL UNIQUE,         -- Unique event identifier (UUID as text)
    ...
);
```

When you append an event, the database assigns the next sequential `id`. Subscriptions use this `global_position` to track progress and ensure gap-free event delivery.

See the [Event Stores API Reference](api/stores.md#stream-position-vs-global-position) for more details.

---

### What if I need to fix incorrect events?

Events are immutable and cannot be modified. Instead, create compensating events:

```python
# Wrong: Trying to modify existing event
# event.total_amount = 150.00  # This won't work!

# Correct: Create a correction event
@register_event
class OrderAmountCorrected(DomainEvent):
    event_type: str = "OrderAmountCorrected"
    aggregate_type: str = "Order"
    previous_amount: float
    corrected_amount: float
    correction_reason: str

# Apply the correction
order.correct_amount(
    corrected_amount=150.00,
    reason="Customer applied coupon"
)
```

This preserves the audit trail while fixing the business state.

---

## Aggregates

### What's the difference between AggregateRoot and DeclarativeAggregate?

Both are base classes for event-sourced aggregates:

**AggregateRoot** - Manual event routing via `_apply()`:
```python
class OrderAggregate(AggregateRoot[OrderState]):
    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(...)
        elif isinstance(event, OrderShipped):
            self._state = self._state.model_copy(update={"status": "shipped"})
```

**DeclarativeAggregate** - Decorator-based routing with `@handles`:
```python
class OrderAggregate(DeclarativeAggregate[OrderState]):
    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        self._state = OrderState(...)

    @handles(OrderShipped)
    def _on_shipped(self, event: OrderShipped) -> None:
        self._state = self._state.model_copy(update={"status": "shipped"})
```

**When to use each:**
- Use `AggregateRoot` for simple aggregates with few event types
- Use `DeclarativeAggregate` for complex aggregates with many event types (cleaner code)

### How do I handle concurrency conflicts?

When two processes modify the same aggregate simultaneously, one will receive an `OptimisticLockError`. Handle this with retry logic:

```python
from eventsource.exceptions import OptimisticLockError

async def ship_order(repo, order_id: UUID, tracking: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            order = await repo.load(order_id)
            order.ship(tracking_number=tracking)
            await repo.save(order)
            return  # Success!
        except OptimisticLockError as e:
            if attempt == max_retries - 1:
                raise  # Exhausted retries
            # Log and retry with fresh load
            logger.warning(
                f"Version conflict (expected {e.expected_version}, "
                f"actual {e.actual_version}), retrying..."
            )
```

**Tips to reduce conflicts:**
1. Keep aggregates small (fewer events = fewer conflicts)
2. Process commands quickly (shorter transactions)
3. Consider command queuing for high-contention aggregates

See [Error Handling Guide](guides/error-handling.md) and [ADR-0003](adrs/0003-optimistic-locking.md) for more details.

### How do I load an aggregate by ID?

Use the `AggregateRepository`:

```python
from eventsource import AggregateRepository, InMemoryEventStore

# Set up repository
store = InMemoryEventStore()
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
)

# Load existing aggregate
order = await repo.load(order_id)
print(f"Order status: {order.state.status}")
print(f"Order version: {order.version}")

# Load or create new aggregate
order = repo.create_new(order_id)  # Doesn't hit database
if order.version == 0:
    order.create(customer_id=customer_id, total=99.99)
await repo.save(order)
```

If the aggregate doesn't exist, `load()` raises `AggregateNotFoundError`:

```python
from eventsource.exceptions import AggregateNotFoundError

try:
    order = await repo.load(order_id)
except AggregateNotFoundError:
    raise HTTPException(status_code=404, detail="Order not found")
```

---

## Projections

### What's the difference between projection types?

eventsource provides three projection base classes:

| Class | Use Case | Features |
|-------|----------|----------|
| `Projection` | Simple projections | Basic async handle/reset |
| `CheckpointTrackingProjection` | Production | Checkpoints, retry, DLQ |
| `DeclarativeProjection` | Recommended | All above + `@handles` decorator |

**Projection** - Minimal base class:
```python
class SimpleProjection(Projection):
    async def handle(self, event: DomainEvent) -> None:
        # Process event
        pass

    async def reset(self) -> None:
        # Clear read model
        pass
```

**DeclarativeProjection** - Recommended for production:
```python
class OrderListProjection(DeclarativeProjection):
    MAX_RETRIES = 3  # Retry config

    @handles(OrderCreated)
    async def _on_created(self, event: OrderCreated) -> None:
        await self._db.insert_order(event)

    @handles(OrderShipped)
    async def _on_shipped(self, event: OrderShipped) -> None:
        await self._db.update_order_status(event.aggregate_id, "shipped")

    async def _truncate_read_models(self) -> None:
        await self._db.execute("TRUNCATE orders")
```

### How do I rebuild a projection?

Rebuilding replays all events through the projection to regenerate the read model:

```python
# 1. Reset the projection (clears checkpoint and read model)
await projection.reset()

# 2. Replay all events
async for stored_event in event_store.read_all():
    event = stored_event.event
    if type(event) in projection.subscribed_to():
        await projection.handle(event)
```

For declarative projections:
```python
# subscribed_to() is auto-generated from @handles decorators
print(projection.subscribed_to())  # [OrderCreated, OrderShipped]
```

**When to rebuild:**
- Schema changes in read model
- Bug fixes in projection logic
- Adding a new projection

See [Production Guide](guides/production.md#projection-rebuilding) for production procedures.

### How do I handle projection errors?

`CheckpointTrackingProjection` (and `DeclarativeProjection`) automatically handles errors:

1. **Automatic retry** with exponential backoff:
   ```python
   class MyProjection(DeclarativeProjection):
       MAX_RETRIES = 3           # Default: 3 attempts
       RETRY_BACKOFF_BASE = 2    # 2s, 4s, 8s backoff
   ```

2. **Dead letter queue** for permanent failures:
   ```python
   # Events that fail all retries go to DLQ
   failed_events = await dlq_repo.get_failed_events(
       projection_name="MyProjection",
       status="failed"
   )
   ```

3. **Checkpoint tracking** - only advances on success:
   ```python
   # Get current checkpoint
   checkpoint = await projection.get_checkpoint()

   # Get lag metrics
   metrics = await projection.get_lag_metrics()
   # {"lag_seconds": 2.5, "events_processed": 1000}
   ```

See [Error Handling Guide](guides/error-handling.md#projectionerror) for DLQ processing procedures.

---

## Production

### How do I scale horizontally?

**Application servers** can scale horizontally since they're stateless:

```
Load Balancer --> App Server 1 --> PostgreSQL (shared)
              --> App Server 2 -->
              --> App Server N -->
```

**Projection workers** can be partitioned:

```python
class PartitionedProjection(DeclarativeProjection):
    def __init__(self, partition_id: int, total_partitions: int):
        super().__init__()
        self._partition = partition_id
        self._total = total_partitions
        self._projection_name = f"{self.__class__.__name__}_{partition_id}"

    async def _process_event(self, event: DomainEvent) -> None:
        # Only process events for this partition
        event_partition = hash(str(event.aggregate_id)) % self._total
        if event_partition != self._partition:
            return  # Skip events for other partitions
        await super()._process_event(event)

# Deploy 4 workers, each handling 1/4 of aggregates
# Worker 0: partition_id=0, total_partitions=4
# Worker 1: partition_id=1, total_partitions=4
```

**Redis event bus** with consumer groups:

```python
from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379/0",
    consumer_group="projection-workers",
    consumer_name="worker-1",  # Unique per instance
)
bus = RedisEventBus(config)
```

**RabbitMQ event bus** with consumer groups:

```python
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",
    exchange_type="topic",  # or "direct", "fanout", "headers"
    consumer_group="projection-workers",
    prefetch_count=10,
    enable_dlq=True,
)
bus = RabbitMQEventBus(config=config)
```

**Kafka event bus** with consumer groups:

```python
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="events",
    consumer_group="projection-workers",
    enable_metrics=True,  # OpenTelemetry metrics
)
bus = KafkaEventBus(config=config)
```

### Which event bus should I choose?

| Scenario | Recommended Bus | Why |
|----------|-----------------|-----|
| Development/Testing | `InMemoryEventBus` | No external dependencies |
| Simple distributed | `RedisEventBus` | Minimal setup if you already use Redis |
| Enterprise messaging | `RabbitMQEventBus` | Advanced routing, mature ecosystem |
| High throughput | `KafkaEventBus` | Designed for event streaming at scale |

**Key differences:**

- **InMemoryEventBus**: Events lost on process restart. No distribution.
- **RedisEventBus**: Uses Redis Streams with consumer groups. At-least-once delivery.
- **RabbitMQEventBus**: AMQP protocol with exchange types (topic, direct, fanout, headers).
- **KafkaEventBus**: Partition-based ordering by aggregate_id, comprehensive metrics.

All distributed buses support:
- Dead letter queues (DLQ)
- At-least-once delivery guarantees
- OpenTelemetry tracing
- Horizontal scaling via consumer groups

### What delivery guarantees do event buses provide?

All distributed event buses (Redis, RabbitMQ, Kafka) provide **at-least-once** delivery semantics:

- Events may be processed multiple times in failure scenarios
- Handlers should be **idempotent** to handle duplicate deliveries
- **Exactly-once semantics are NOT supported** out of the box

For stronger consistency, consider:
1. **Transactional Outbox Pattern**: Store events atomically with domain changes
2. **Idempotency keys**: Use `event_id` to deduplicate in handlers
3. **Saga/Compensation patterns**: Design for eventual consistency

```python
# Example idempotent handler
@handles(OrderCreated)
async def _on_order_created(self, event: OrderCreated) -> None:
    # Check if already processed using event_id
    if await self._db.event_processed(event.event_id):
        return  # Skip duplicate

    await self._db.create_order(event)
    await self._db.mark_event_processed(event.event_id)
```

### What monitoring should I set up?

**Key metrics to track:**

| Metric | Alert Threshold | Description |
|--------|-----------------|-------------|
| Event append latency (p99) | > 100ms | Write performance |
| Projection lag | > 60s warning, > 300s critical | Read model freshness |
| DLQ depth | > 0 | Failed events needing attention |
| Connection pool utilization | > 80% | Database pressure |
| Optimistic lock conflicts/min | High rate | Aggregate contention |

**Projection lag monitoring:**

```python
async def monitor_projections(projections: list[DeclarativeProjection]):
    for projection in projections:
        metrics = await projection.get_lag_metrics()
        if metrics and metrics["lag_seconds"] > 60:
            logger.warning(
                f"Projection {projection.projection_name} lag: "
                f"{metrics['lag_seconds']:.1f}s"
            )
```

**Health check endpoint:**

```python
@app.get("/health")
async def health():
    # Check database
    db_health = await check_database_health(session_factory)

    # Check projections
    projection_lag = {}
    for p in projections:
        metrics = await p.get_lag_metrics()
        projection_lag[p.projection_name] = metrics

    # Check DLQ
    dlq_count = await dlq_repo.get_failure_count()

    return {
        "database": db_health,
        "projections": projection_lag,
        "dlq_depth": dlq_count,
    }
```

See [Production Guide](guides/production.md#monitoring-and-observability) for complete examples.

### How do I backup the event store?

Events are immutable, making backup straightforward:

**PostgreSQL backup:**
```bash
# Full backup
pg_dump -Fc -d eventsource > backup.dump

# Restore
pg_restore -d eventsource backup.dump
```

**Point-in-time recovery** with WAL archiving:
```ini
# postgresql.conf
archive_mode = on
archive_command = 'cp %p /archive/%f'
```

**Projection rebuilding** as disaster recovery:
```python
# Projections can always be rebuilt from events
await projection.reset()
async for stored_event in event_store.read_all():
    if type(stored_event.event) in projection.subscribed_to():
        await projection.handle(stored_event.event)
```

See [Production Guide](guides/production.md#backup-and-recovery) for detailed procedures.

---

## Troubleshooting

### Why am I getting OptimisticLockError?

This happens when two processes try to modify the same aggregate simultaneously:

```
Process A: Load Order (v5) --> Modify --> Save (v5->v6) --> Success
Process B: Load Order (v5) --> Modify --> Save (v5->?) --> CONFLICT!
```

**Solutions:**

1. **Implement retry logic** (most common):
   ```python
   for attempt in range(3):
       try:
           order = await repo.load(order_id)
           order.ship(tracking)
           await repo.save(order)
           return
       except OptimisticLockError:
           if attempt == 2:
               raise
   ```

2. **Reduce aggregate scope** - smaller aggregates have fewer conflicts

3. **Use command queuing** for high-contention aggregates

4. **Check for bugs** - infinite loops or duplicate command handlers

See [Error Handling Guide](guides/error-handling.md#optimisticlockerror) for detailed patterns.

### Why is my projection falling behind?

Check these common causes:

1. **Slow handler logic:**
   ```python
   # Profile your handler
   import time

   @handles(OrderCreated)
   async def _on_created(self, event: OrderCreated) -> None:
       start = time.time()
       await self._heavy_operation()
       logger.debug(f"Handler took {time.time() - start:.2f}s")
   ```

2. **Database bottleneck:**
   - Check index usage with `EXPLAIN ANALYZE`
   - Increase connection pool size
   - Consider batch inserts

3. **Errors in handler** causing retries:
   ```python
   # Check DLQ for errors
   failed = await dlq_repo.get_failed_events(
       projection_name="MyProjection"
   )
   for entry in failed:
       logger.error(f"Failed: {entry.error_message}")
   ```

4. **Single-threaded processing:**
   - Consider partitioned projections for parallelism
   - Use consumer groups with Redis event bus

**Monitoring lag:**
```python
metrics = await projection.get_lag_metrics()
print(f"Lag: {metrics['lag_seconds']:.1f}s")
print(f"Events processed: {metrics['events_processed']}")
```

### How do I debug event handlers?

1. **Enable debug logging:**
   ```python
   import logging
   logging.getLogger("eventsource").setLevel(logging.DEBUG)
   ```

2. **Add logging to handlers:**
   ```python
   @handles(OrderCreated)
   async def _on_created(self, event: OrderCreated) -> None:
       logger.debug(
           f"Handling OrderCreated: {event.event_id} "
           f"for aggregate {event.aggregate_id}"
       )
       try:
           await self._process(event)
       except Exception as e:
           logger.error(f"Handler failed: {e}", exc_info=True)
           raise
   ```

3. **Inspect event data:**
   ```python
   # View raw event data
   print(event.model_dump(mode="json"))

   # Check event metadata
   print(f"Occurred at: {event.occurred_at}")
   print(f"Version: {event.aggregate_version}")
   print(f"Correlation: {event.correlation_id}")
   ```

4. **Replay specific events:**
   ```python
   # Get events for debugging
   stream = await store.get_events(aggregate_id, "Order")
   for event in stream.events:
       print(f"v{event.aggregate_version}: {event.event_type}")
   ```

5. **Check DLQ for details:**
   ```python
   entries = await dlq_repo.get_failed_events(
       projection_name="MyProjection",
       limit=10
   )
   for entry in entries:
       print(f"Event: {entry.event_id}")
       print(f"Error: {entry.error_message}")
       print(f"Traceback: {entry.error_stacktrace}")
   ```

### Events aren't being deserialized correctly

Common causes:

1. **Event class not registered:**
   ```python
   # Ensure @register_event decorator is present
   @register_event  # Required!
   class OrderCreated(DomainEvent):
       event_type: str = "OrderCreated"
   ```

2. **Event class not imported before deserialization:**
   ```python
   # In your app startup (before loading events)
   from myapp.events import orders, payments, shipping
   ```

3. **event_type mismatch:**
   ```python
   # Class name vs event_type
   class OrderCreatedEvent(DomainEvent):
       event_type: str = "OrderCreated"  # This is what's stored
   ```

4. **Schema incompatibility:**
   ```python
   # Add defaults for new fields
   @register_event
   class OrderCreatedV2(DomainEvent):
       event_type: str = "OrderCreated"
       new_field: str = "default_value"  # Has default for old events
   ```

**Debugging:**
```python
from eventsource.events.registry import default_registry

# List all registered events
print(default_registry.list_event_types())

# Check if specific type is registered
try:
    cls = default_registry.get("OrderCreated")
    print(f"Found: {cls}")
except Exception as e:
    print(f"Not registered: {e}")
```

---

## See Also

- [Getting Started](getting-started.md) - Tutorial
- [Architecture](architecture.md) - System design
- [API Reference](api/index.md) - Detailed API docs
- [Error Handling](guides/error-handling.md) - Exception patterns
- [Production Deployment](guides/production.md) - Production configuration
