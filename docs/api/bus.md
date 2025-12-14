# Event Bus API Reference

This document covers the event bus system for publishing and subscribing to domain events.

## Overview

The event bus decouples event producers from consumers, allowing projections and other handlers to react to events independently.

**Implementations:**

| Implementation | Backend | Use Case |
|----------------|---------|----------|
| `InMemoryEventBus` | None | Development, testing, single-process |
| `RedisEventBus` | Redis Streams | Real-time distributed systems |
| `RabbitMQEventBus` | RabbitMQ AMQP | Enterprise messaging with routing |
| `KafkaEventBus` | Apache Kafka | High-throughput event streaming |

For a comprehensive guide, see [Event Bus Guide](../guides/event-bus.md).

```python
from eventsource import (
    # Interface and base classes
    EventBus,
    EventHandler,
    EventSubscriber,
    EventHandlerFunc,
    AsyncEventHandler,

    # In-memory (always available)
    InMemoryEventBus,

    # Redis (requires: pip install eventsource[redis])
    RedisEventBus,
    RedisEventBusConfig,
    RedisEventBusStats,
    REDIS_AVAILABLE,

    # RabbitMQ (requires: pip install eventsource[rabbitmq])
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
    RABBITMQ_AVAILABLE,

    # Kafka (requires: pip install eventsource[kafka])
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusStats,
    KAFKA_AVAILABLE,
)
```

---

## EventBus Interface

Abstract base class defining the event bus contract.

### Core Methods

```python
class EventBus(ABC):
    @abstractmethod
    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to all registered subscribers."""
        pass

    @abstractmethod
    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: EventHandler | EventHandlerFunc,
    ) -> None:
        """Subscribe a handler to a specific event type."""
        pass

    @abstractmethod
    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: EventHandler | EventHandlerFunc,
    ) -> bool:
        """Unsubscribe a handler from a specific event type."""
        pass

    @abstractmethod
    def subscribe_all(self, subscriber: EventSubscriber) -> None:
        """Subscribe an EventSubscriber to all its declared event types."""
        pass

    @abstractmethod
    def subscribe_to_all_events(
        self,
        handler: EventHandler | EventHandlerFunc,
    ) -> None:
        """Subscribe a handler to all event types (wildcard)."""
        pass

    @abstractmethod
    def unsubscribe_from_all_events(
        self,
        handler: EventHandler | EventHandlerFunc,
    ) -> bool:
        """Unsubscribe from wildcard subscription."""
        pass
```

---

## Handler Protocols

### EventHandler

Protocol for objects that handle events:

```python
@runtime_checkable
class EventHandler(Protocol):
    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """Handle a domain event (sync or async)."""
        ...
```

**Usage:**

```python
class MyHandler:
    async def handle(self, event: DomainEvent) -> None:
        print(f"Handling {event.event_type}")

handler = MyHandler()
event_bus.subscribe(OrderCreated, handler)
```

### EventSubscriber

Protocol for subscribers that declare their event types:

```python
@runtime_checkable
class EventSubscriber(Protocol):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return list of event types this subscriber handles."""
        ...

    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """Handle a domain event."""
        ...
```

**Usage:**

```python
class OrderProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            await self.create_order(event)
        # ...

projection = OrderProjection()
event_bus.subscribe_all(projection)  # Subscribes to all declared types
```

> **💡 Recommended:** For production projections, use [`SubscriptionManager`](subscriptions.md) instead of direct `subscribe_all()`. It provides:
> - Historical event catch-up from the event store
> - Checkpoint tracking for resumable processing
> - Health monitoring and graceful shutdown
>
> See the [Subscriptions Guide](../guides/subscriptions.md) for details.

### EventHandlerFunc

Type alias for function-based handlers:

```python
EventHandlerFunc = Callable[[DomainEvent], Awaitable[None] | None]

# Usage
async def my_handler(event: DomainEvent) -> None:
    print(f"Got event: {event.event_type}")

event_bus.subscribe(OrderCreated, my_handler)
```

---

## InMemoryEventBus

In-memory implementation for single-process deployments.

### Features

- Thread-safe subscription management
- Support for sync and async handlers
- Wildcard subscriptions
- Error isolation (handler failures don't stop other handlers)
- Optional OpenTelemetry tracing (via composition-based `Tracer`)
- Background task management

### Constructor

```python
from eventsource import InMemoryEventBus

# Default - tracing enabled when OpenTelemetry is available
bus = InMemoryEventBus()

# Explicitly disable tracing (useful for testing)
bus = InMemoryEventBus(enable_tracing=False)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing when available |

### Usage

```python
from eventsource import InMemoryEventBus

bus = InMemoryEventBus()

# Subscribe to specific event type
bus.subscribe(OrderCreated, order_handler)

# Subscribe using lambda
bus.subscribe(OrderCreated, lambda e: print(e))

# Subscribe to all events (audit logging, metrics)
bus.subscribe_to_all_events(audit_logger)

# Subscribe projection to multiple event types
bus.subscribe_all(order_projection)

# Publish events
await bus.publish([order_created_event])

# Publish in background (fire-and-forget)
await bus.publish([event], background=True)
```

### Methods

| Method | Description |
|--------|-------------|
| `subscribe(event_type, handler)` | Subscribe to specific event type |
| `unsubscribe(event_type, handler)` | Remove subscription |
| `subscribe_all(subscriber)` | Subscribe to all declared types |
| `subscribe_to_all_events(handler)` | Wildcard subscription |
| `unsubscribe_from_all_events(handler)` | Remove wildcard subscription |
| `publish(events, background)` | Publish events to subscribers |
| `clear_subscribers()` | Remove all subscriptions |
| `get_subscriber_count(event_type)` | Count subscribers |
| `get_wildcard_subscriber_count()` | Count wildcard subscribers |
| `get_stats()` | Get operation statistics |
| `get_background_task_count()` | Count active background tasks |
| `shutdown(timeout)` | Wait for background tasks to complete |

### Statistics

```python
stats = bus.get_stats()
print(f"Events published: {stats['events_published']}")
print(f"Handlers invoked: {stats['handlers_invoked']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Background tasks: {stats['background_tasks_created']}")
```

### Shutdown

Ensure all background tasks complete during application shutdown:

```python
async def shutdown():
    await bus.shutdown(timeout=30.0)
```

---

## RedisEventBus

Distributed event bus using Redis Streams.

### Features

- Distributed event publishing
- Consumer groups for load balancing
- Reliable delivery with acknowledgments
- Dead letter queue support
- Automatic reconnection
- Stream trimming

### Setup

```python
from eventsource.bus import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="myapp",
    consumer_group="my-app",
    consumer_name="worker-1",
    block_ms=5000,
    batch_size=100,
)

bus = RedisEventBus(config=config)
await bus.connect()
await bus.start_consuming()
```

### Configuration

```python
@dataclass
class RedisEventBusConfig:
    redis_url: str = "redis://localhost:6379"
    stream_prefix: str = "events"             # Stream name: {prefix}:stream
    consumer_group: str = "default"
    consumer_name: str | None = None          # Auto-generated if None
    batch_size: int = 100                     # Events per batch
    block_ms: int = 5000                      # Block time for reads (ms)
    max_retries: int = 3                      # Max processing attempts
    pending_idle_ms: int = 60000              # Claim idle messages (ms)
    enable_dlq: bool = True                   # Enable DLQ
    dlq_stream_suffix: str = "_dlq"           # DLQ stream: {prefix}:stream_dlq
    socket_timeout: float = 5.0               # Socket timeout (seconds)
    socket_connect_timeout: float = 5.0       # Connection timeout (seconds)
    enable_tracing: bool = True               # OpenTelemetry tracing
    retry_key_prefix: str = "retry"           # Retry count key prefix
    retry_key_expiry_seconds: int = 86400     # Retry key expiry (24h)
```

### Usage

```python
bus = RedisEventBus(config=config)
await bus.connect()

# Subscribe handlers (same as InMemoryEventBus)
bus.subscribe(OrderCreated, order_handler)
bus.subscribe_all(order_projection)

# Start consuming (blocks until stopped)
await bus.start_consuming()

# Publish events
await bus.publish([order_created_event])

# Get statistics
stats = bus.get_stats_dict()
print(f"Published: {stats['events_published']}")
print(f"Consumed: {stats['events_consumed']}")
print(f"Success: {stats['events_processed_success']}")
print(f"Failed: {stats['events_processed_failed']}")
print(f"DLQ: {stats['messages_sent_to_dlq']}")

# Shutdown
await bus.shutdown()
```

### Consumer Groups

Multiple consumers can process events in parallel:

```python
# Worker 1
config1 = RedisEventBusConfig(
    consumer_group="order-processors",
    consumer_name="worker-1",
)
bus1 = RedisEventBus(config1)

# Worker 2
config2 = RedisEventBusConfig(
    consumer_group="order-processors",
    consumer_name="worker-2",
)
bus2 = RedisEventBus(config2)

# Events are distributed between workers
```

### Dead Letter Queue

```python
# Get DLQ messages
messages = await bus.get_dlq_messages(count=100)
for msg in messages:
    print(f"ID: {msg['message_id']}")
    print(f"Data: {msg['data']}")

# Replay a DLQ message back to main stream
success = await bus.replay_dlq_message(message_id)
```

### Pending Message Recovery

Recover messages that were read but never acknowledged:

```python
# Manually trigger recovery (also runs automatically)
stats = await bus.recover_pending_messages(
    min_idle_time_ms=60000,  # Messages idle for 1 minute
    max_retries=3,            # Send to DLQ after 3 attempts
)
print(f"Checked: {stats['checked']}")
print(f"Reprocessed: {stats['reprocessed']}")
print(f"Sent to DLQ: {stats['dlq']}")
```

### Stream Information

```python
info = await bus.get_stream_info()
# {
#     "connected": True,
#     "stream": {
#         "name": "myapp:stream",
#         "length": 1000,
#         "first_entry_id": "1234567890-0",
#         "last_entry_id": "1234567899-0",
#     },
#     "consumer_groups": [...],
#     "pending_messages": 5,
#     "dlq_messages": 2,
#     "active_consumers": 3,
#     "stats": {...},
# }
```

### Availability Check

```python
from eventsource.bus import REDIS_AVAILABLE, RedisNotAvailableError

if not REDIS_AVAILABLE:
    print("Redis support not installed")
    # pip install eventsource[redis]

try:
    bus = RedisEventBus(config=config)
except RedisNotAvailableError:
    print("Redis package not available")
```

---

## AsyncEventHandler

Base class for structured async handlers.

```python
from eventsource import AsyncEventHandler

class OrderNotificationHandler(AsyncEventHandler):
    def event_types(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            await self.send_confirmation_email(event)
        elif isinstance(event, OrderShipped):
            await self.send_shipping_notification(event)

    def can_handle(self, event: DomainEvent) -> bool:
        # Custom filtering logic (optional override)
        return type(event) in self.event_types()
```

---

## Complete Example

```python
import asyncio
from uuid import uuid4
from eventsource import (
    DomainEvent,
    register_event,
    InMemoryEventBus,
    InMemoryEventStore,
    AggregateRepository,
)

# Events
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str

# Handler classes
class NotificationService:
    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            print(f"Sending order confirmation for {event.aggregate_id}")
        elif isinstance(event, OrderShipped):
            print(f"Sending shipping notification for {event.aggregate_id}")

class AnalyticsService:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        print(f"Analytics: Recording {event.event_type}")

class AuditLogger:
    async def handle(self, event: DomainEvent) -> None:
        print(f"Audit: {event.event_type} at {event.occurred_at}")

async def main():
    # Set up event bus
    bus = InMemoryEventBus()

    # Subscribe handlers
    notification_service = NotificationService()
    bus.subscribe(OrderCreated, notification_service)
    bus.subscribe(OrderShipped, notification_service)

    # Subscribe projection (subscribes to all declared types)
    analytics = AnalyticsService()
    bus.subscribe_all(analytics)

    # Subscribe audit logger to ALL events
    audit = AuditLogger()
    bus.subscribe_to_all_events(audit)

    # Set up repository with event publishing
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=bus,  # Events published after save
    )

    # Create and save order (events auto-published)
    order = repo.create_new(uuid4())
    order.create(customer_id=uuid4())
    await repo.save(order)

    # Or publish directly
    await bus.publish([
        OrderShipped(
            aggregate_id=order.aggregate_id,
            tracking_number="TRACK-123",
            aggregate_version=2,
        )
    ])

    # Check stats
    stats = bus.get_stats()
    print(f"\nStats: {stats}")

    # Cleanup
    await bus.shutdown()

asyncio.run(main())
```

---

## Best Practices

### Handler Isolation

Handlers should be independent and not assume order:

```python
class MyHandler:
    async def handle(self, event: DomainEvent) -> None:
        # Don't assume other handlers have run
        # Don't modify shared state without synchronization
        await self._process_independently(event)
```

### Error Handling

Handler errors are logged but don't stop other handlers:

```python
class ResilientHandler:
    async def handle(self, event: DomainEvent) -> None:
        try:
            await self._do_work(event)
        except TemporaryError:
            # Let it propagate - will be logged
            raise
        except PermanentError as e:
            # Log and continue - don't block other handlers
            logger.error(f"Permanent failure: {e}")
```

### Background Publishing

Use background publishing for better response times:

```python
# Synchronous (wait for handlers)
await bus.publish([event], background=False)

# Asynchronous (fire-and-forget)
await bus.publish([event], background=True)
# Response returns immediately, handlers run in background
```

### Graceful Shutdown

Always shutdown properly:

```python
try:
    # Application runs
    pass
finally:
    await bus.shutdown(timeout=30.0)
```

### Wildcard Subscriptions

Use sparingly for cross-cutting concerns:

```python
# Good uses for wildcard
bus.subscribe_to_all_events(audit_logger)     # Audit trail
bus.subscribe_to_all_events(metrics_handler)  # Metrics collection
bus.subscribe_to_all_events(debug_handler)    # Development debugging

# Bad: Using wildcard when specific subscription is better
# bus.subscribe_to_all_events(order_handler)  # Only handles OrderCreated anyway
bus.subscribe(OrderCreated, order_handler)     # Better
```

---

## RabbitMQEventBus

Distributed event bus using RabbitMQ with AMQP protocol.

### Features

- Durable event storage (events survive restarts)
- Consumer groups via queue bindings
- At-least-once delivery guarantees
- Multiple exchange types (topic, direct, fanout, headers)
- Dead letter queue for failed messages
- Automatic reconnection
- Optional OpenTelemetry tracing

### Setup

```python
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",
    exchange_type="topic",  # or "direct", "fanout", "headers"
    consumer_group="my-service",
    prefetch_count=10,
    enable_dlq=True,
)

bus = RabbitMQEventBus(config=config)
await bus.connect()
```

### Configuration

```python
@dataclass
class RabbitMQEventBusConfig:
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    exchange_name: str = "events"
    exchange_type: str = "topic"  # topic, direct, fanout, headers
    consumer_group: str = "default"
    consumer_name: str | None = None  # Auto-generated if not set
    prefetch_count: int = 10
    max_retries: int = 3
    enable_dlq: bool = True
    durable: bool = True
    auto_delete: bool = False
    routing_key_pattern: str | None = None  # Auto-determined by exchange type
```

### Exchange Types

RabbitMQ supports multiple exchange types for different routing patterns:

#### Topic Exchange (Default)

Routes messages based on routing key patterns with wildcards.

```python
config = RabbitMQEventBusConfig(
    exchange_type="topic",
    # Default: receives all messages with routing_key_pattern="#"
)

# Messages are published with routing key: {aggregate_type}.{event_type}
# e.g., "Order.OrderCreated", "User.UserRegistered"

# Consumers can bind with patterns:
# "Order.*" - all Order events
# "*.OrderCreated" - all OrderCreated events from any aggregate
# "#" - all events (default)
```

#### Direct Exchange

Routes messages to queues with exact routing key match. Ideal for:
- Work queue pattern (competing consumers)
- Point-to-point messaging
- Load balancing between workers

```python
config = RabbitMQEventBusConfig(
    exchange_name="tasks",
    exchange_type="direct",
    consumer_group="workers",
    # Default: binds with queue_name as routing key
)

# Multiple workers with same config share the queue
# RabbitMQ load-balances messages between them
```

See [Work Queue Pattern](#work-queue-pattern-competing-consumers) below.

#### Fanout Exchange

Broadcasts messages to all bound queues (ignores routing key).

```python
config = RabbitMQEventBusConfig(
    exchange_type="fanout",
    # All bound queues receive all messages
)
```

### Work Queue Pattern (Competing Consumers)

The work queue pattern distributes tasks among multiple workers. Messages are
load-balanced between consumers on the same queue.

```python
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

# All workers use the same configuration (same consumer_group)
# Only consumer_name differs between workers

def create_worker_config(worker_id: str) -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="tasks",
        exchange_type="direct",
        consumer_group="task-processors",  # Same for all workers
        consumer_name=f"worker-{worker_id}",  # Unique per worker
        prefetch_count=1,  # Process one task at a time for fair dispatch
    )

# Worker 1
config1 = create_worker_config("1")
worker1 = RabbitMQEventBus(config=config1)
await worker1.connect()

# Worker 2
config2 = create_worker_config("2")
worker2 = RabbitMQEventBus(config=config2)
await worker2.connect()

# Both workers share queue "tasks.task-processors"
# Messages are distributed round-robin between them
```

### Custom Routing Key Binding

For fine-grained control over which events a consumer receives:

```python
config = RabbitMQEventBusConfig(
    exchange_type="direct",
    routing_key_pattern="",  # No default binding
)

bus = RabbitMQEventBus(config=config)
await bus.connect()

# Bind only to specific event types
await bus.bind_event_type(OrderCreated)
await bus.bind_event_type(OrderShipped)

# Or bind with custom routing keys
await bus.bind_routing_key("Order.OrderCreated")
await bus.bind_routing_key("Order.OrderCancelled")
```

### Usage

```python
bus = RabbitMQEventBus(config=config)

# Subscribe handlers (same as other event bus implementations)
bus.subscribe(OrderCreated, order_handler)
bus.subscribe_all(order_projection)

# Connect and start consuming
await bus.connect()
await bus.start_consuming()

# Publish events
await bus.publish([order_created_event])

# Get statistics
stats = bus.get_stats()
print(f"Published: {stats.events_published}")
print(f"Consumed: {stats.events_consumed}")

# Graceful shutdown
await bus.shutdown(timeout=30.0)
```

### Context Manager

```python
async with RabbitMQEventBus(config=config) as bus:
    bus.subscribe(OrderCreated, handler)
    await bus.start_consuming()
    # ... use the bus
# Automatic cleanup on exit
```

### Routing Key Generation

When publishing events, routing keys are generated as:

```
{aggregate_type}.{event_type}
```

For example:
- `Order.OrderCreated`
- `User.UserRegistered`
- `Payment.PaymentProcessed`

This allows subscribers to:
- Receive all events: bind with `#` (topic exchange default)
- Receive aggregate events: bind with `Order.*`
- Receive specific event type: bind with `*.OrderCreated`
- Receive exact event: bind with `Order.OrderCreated`

### Availability Check

```python
from eventsource.bus.rabbitmq import (
    RABBITMQ_AVAILABLE,
    RabbitMQNotAvailableError,
)

if not RABBITMQ_AVAILABLE:
    print("RabbitMQ support not installed")
    # pip install eventsource[rabbitmq]

try:
    bus = RabbitMQEventBus(config)
except RabbitMQNotAvailableError:
    print("aio-pika package not available")
```

---

## KafkaEventBus

Distributed event bus using Apache Kafka for high-throughput event streaming.

### Features

- High-throughput event publishing (10,000+ events/second)
- Consumer groups for horizontal scaling
- Partition-based ordering by aggregate_id
- At-least-once delivery guarantees
- Dead letter queue with replay capability
- Configurable retry policies with exponential backoff
- TLS/SSL and SASL authentication support
- Optional OpenTelemetry tracing integration

### Setup

```python
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="myapp.events",
    consumer_group="projections",
    consumer_name="worker-1",
)

bus = KafkaEventBus(config=config)
await bus.connect()
```

### Configuration

```python
@dataclass
class KafkaEventBusConfig:
    # Connection
    bootstrap_servers: str = "localhost:9092"      # Kafka broker addresses
    topic_prefix: str = "events"                   # Prefix for topic names
    consumer_group: str = "default"                # Consumer group ID
    consumer_name: str | None = None               # Auto-generated if not set

    # Producer settings
    acks: str = "all"                              # "0", "1", or "all"
    compression_type: str | None = "gzip"          # gzip, snappy, lz4, zstd
    batch_size: int = 16384                        # Batch size in bytes
    linger_ms: int = 5                             # Wait time for batching

    # Consumer settings
    auto_offset_reset: str = "earliest"            # "earliest" or "latest"
    session_timeout_ms: int = 30000                # Session timeout
    heartbeat_interval_ms: int = 10000             # Heartbeat interval
    max_poll_interval_ms: int = 300000             # Max time between polls

    # Error handling
    max_retries: int = 3                           # Max retry attempts
    retry_base_delay: float = 1.0                  # Base delay (seconds)
    retry_max_delay: float = 60.0                  # Max delay (seconds)
    retry_jitter: float = 0.1                      # Jitter fraction (0-1)

    # Dead letter queue
    enable_dlq: bool = True                        # Enable DLQ
    dlq_topic_suffix: str = ".dlq"                 # DLQ topic suffix

    # Security
    security_protocol: str = "PLAINTEXT"           # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    sasl_mechanism: str | None = None              # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    sasl_username: str | None = None               # SASL username
    sasl_password: str | None = None               # SASL password
    ssl_cafile: str | None = None                  # CA certificate file
    ssl_certfile: str | None = None                # Client certificate (mTLS)
    ssl_keyfile: str | None = None                 # Client key (mTLS)
    ssl_check_hostname: bool = True                # Verify hostname

    # Observability
    enable_tracing: bool = True                    # Enable OpenTelemetry

    # Shutdown
    shutdown_timeout: float = 30.0                 # Graceful shutdown timeout
```

### Topic Naming

Topics are named using the following pattern:

| Topic Type | Pattern | Example |
|------------|---------|---------|
| Main events | `{topic_prefix}.stream` | `myapp.events.stream` |
| Dead letter queue | `{topic_prefix}.stream.dlq` | `myapp.events.stream.dlq` |

### Usage

```python
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="order-projections",
)

bus = KafkaEventBus(config=config)

# Subscribe handlers (before connecting)
bus.subscribe(OrderCreated, order_handler)
bus.subscribe_all(order_projection)

# Connect and start consuming
await bus.connect()
task = bus.start_consuming_in_background()

# Publish events
await bus.publish([order_created_event])

# Get statistics
stats = bus.get_stats_dict()
print(f"Published: {stats['events_published']}")
print(f"Consumed: {stats['events_consumed']}")
print(f"DLQ: {stats['messages_sent_to_dlq']}")

# Graceful shutdown
await bus.shutdown(timeout=30.0)
```

### Context Manager

```python
async with KafkaEventBus(config=config) as bus:
    bus.subscribe(OrderCreated, handler)
    task = bus.start_consuming_in_background()
    await bus.publish([event])
    # ... use the bus
# Automatic cleanup on exit
```

### Consumer Groups

Multiple consumers in the same group share partitions for load balancing:

```python
# Worker 1
config1 = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    consumer_group="order-processors",  # Same group
    consumer_name="worker-1",           # Unique name
)
worker1 = KafkaEventBus(config=config1)

# Worker 2
config2 = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    consumer_group="order-processors",  # Same group
    consumer_name="worker-2",           # Unique name
)
worker2 = KafkaEventBus(config=config2)

# Events are distributed between workers (one event = one worker)
```

### Partition Ordering

Events are partitioned by `aggregate_id`, ensuring:

- Events for the same aggregate are always processed in order
- Different aggregates can be processed in parallel
- Horizontal scaling respects aggregate consistency

```python
# All events for Order-123 go to the same partition
await bus.publish([
    OrderCreated(aggregate_id="Order-123", ...),
    OrderShipped(aggregate_id="Order-123", ...),  # Same partition
    OrderDelivered(aggregate_id="Order-123", ...), # Same partition
])

# Events for Order-456 may go to a different partition
await bus.publish([
    OrderCreated(aggregate_id="Order-456", ...),  # May be different partition
])
```

### Dead Letter Queue

Failed messages are sent to DLQ after max retries:

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="orders",
    max_retries=3,              # Retry 3 times
    retry_base_delay=1.0,       # 1s, 2s, 4s delays
    enable_dlq=True,            # Enable DLQ
    dlq_topic_suffix=".dlq",    # orders.stream.dlq
)

bus = KafkaEventBus(config=config)

# Inspect DLQ messages
dlq_messages = await bus.get_dlq_messages(limit=100)
for msg in dlq_messages:
    print(f"Event: {msg['headers']['event_type']}")
    print(f"Error: {msg['headers']['dlq_error_message']}")
    print(f"Retries: {msg['headers']['dlq_retry_count']}")

# Get DLQ message count
count = await bus.get_dlq_message_count()
print(f"DLQ contains {count} messages")

# Replay a specific message
await bus.replay_dlq_message(partition=0, offset=42)
```

### Security Configuration

#### Development (No Security)

```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    security_protocol="PLAINTEXT",
)
```

#### TLS Only (Encryption)

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    security_protocol="SSL",
    ssl_cafile="/path/to/ca.crt",
)
```

#### SASL/PLAIN with TLS

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="myuser",
    sasl_password="mypassword",
    ssl_cafile="/path/to/ca.crt",
)
```

#### SASL/SCRAM-SHA-512 with TLS (Recommended)

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_username="myuser",
    sasl_password="mypassword",
    ssl_cafile="/path/to/ca.crt",
)
```

#### Mutual TLS (mTLS)

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    security_protocol="SSL",
    ssl_cafile="/path/to/ca.crt",
    ssl_certfile="/path/to/client.crt",
    ssl_keyfile="/path/to/client.key",
)
```

### OpenTelemetry Tracing

When OpenTelemetry is installed and `enable_tracing=True`, the bus creates spans for:

- **Publish operations**: `kafka.publish {event_type}`
- **Consume operations**: `kafka.consume {event_type}`
- **Handler invocations**: `handle {handler_name}`

Trace context is propagated through Kafka message headers.

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Set up OpenTelemetry
trace.set_tracer_provider(TracerProvider())
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter())
)

# Kafka bus with tracing
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="events",
    enable_tracing=True,  # Default when OpenTelemetry is available
)

bus = KafkaEventBus(config=config)
# Traces automatically created for all operations
```

### Statistics

```python
stats = bus.get_stats_dict()
# Returns:
{
    "events_published": 1000,
    "events_consumed": 950,
    "events_processed_success": 940,
    "events_processed_failed": 10,
    "messages_sent_to_dlq": 3,
    "handler_errors": 10,
    "reconnections": 0,
    "rebalance_count": 2,
    "last_publish_at": "2025-12-07T10:30:00+00:00",
    "last_consume_at": "2025-12-07T10:30:01+00:00",
    "last_error_at": "2025-12-07T09:15:00+00:00",
    "connected_at": "2025-12-07T08:00:00+00:00",
}
```

### Availability Check

```python
from eventsource.bus.kafka import (
    KAFKA_AVAILABLE,
    KafkaNotAvailableError,
)

if not KAFKA_AVAILABLE:
    print("Kafka support not installed")
    # pip install eventsource[kafka]

try:
    bus = KafkaEventBus(config)
except KafkaNotAvailableError:
    print("aiokafka package not available")
```

### Migration from Redis/RabbitMQ

KafkaEventBus implements the same `EventBus` interface, making migration straightforward:

```python
# Before (Redis)
from eventsource.bus import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="events",
    consumer_group="projections",
)
bus = RedisEventBus(config=config)

# After (Kafka) - only config changes
from eventsource.bus import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="events",
    consumer_group="projections",
)
bus = KafkaEventBus(config=config)

# Same interface - handlers work unchanged
bus.subscribe(OrderCreated, my_handler)
bus.subscribe_all(my_projection)
await bus.publish([event])
```

### When to Choose Kafka

Choose KafkaEventBus when:

- **High throughput**: Need 10,000+ events/second
- **Enterprise infrastructure**: Organization standardized on Kafka
- **Long-term retention**: Need to replay events from weeks/months ago
- **Multi-datacenter**: Need cross-datacenter replication
- **Ecosystem integration**: Need Kafka Connect, Kafka Streams, or KSQL

Choose Redis/RabbitMQ when:

- **Simpler operations**: Lower operational complexity needed
- **Real-time focus**: Sub-millisecond latency more important than throughput
- **Existing infrastructure**: Already running Redis or RabbitMQ
