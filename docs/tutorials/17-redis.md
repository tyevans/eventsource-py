# Tutorial 17: Redis Event Bus - Distributed Event Distribution

**Difficulty:** Advanced

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 7: Event Bus - Distributing Events Across Your System](07-event-bus.md)
- [Tutorial 11: PostgreSQL Event Store](11-postgresql.md)
- [Tutorial 13: Subscription Management](13-subscriptions.md)
- Python 3.10 or higher
- Understanding of async/await
- Basic Redis knowledge helpful but not required
- Docker for running Redis

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain why Redis Streams are ideal for distributed event buses
2. Install and configure Redis for event distribution
3. Set up RedisEventBus with proper configuration
4. Understand consumer groups and load balancing
5. Implement pending message recovery for fault tolerance
6. Use dead letter queues for failed messages
7. Monitor Redis event bus health and performance
8. Deploy multiple consumers for horizontal scaling

---

## What is Redis Event Bus?

The **RedisEventBus** is a distributed event bus implementation that uses **Redis Streams** for durable event distribution across multiple processes and servers. While `InMemoryEventBus` (from Tutorial 7) works only within a single process, RedisEventBus enables:

- **Cross-process communication**: Events published by one service reach handlers in other services
- **Durability**: Events persist in Redis, surviving application restarts
- **Load balancing**: Multiple consumers share event processing workload
- **Fault tolerance**: Pending messages automatically recovered after crashes
- **At-least-once delivery**: Events acknowledged only after successful processing

---

## Why Redis Streams?

Redis Streams combine the best features of message queues and pub/sub systems:

| Feature | Benefit |
|---------|---------|
| **Durability** | Events persist in Redis memory/disk |
| **Consumer Groups** | Multiple consumers share workload automatically |
| **Ordering Guarantees** | Events processed in order within a stream |
| **Pending Message List** | Track unacknowledged messages for recovery |
| **At-Least-Once Delivery** | Messages acknowledged only after processing |
| **Low Latency** | In-memory operations with microsecond access times |
| **Dead Letter Queue** | Automatically handle poison messages |
| **Horizontal Scaling** | Add consumers dynamically without reconfiguration |

### When to Use Redis Event Bus

**Good for:**
- Multi-instance deployments (horizontal scaling)
- Microservices architectures
- Event-driven systems requiring durability
- Applications needing load balancing across workers
- Systems requiring fault tolerance and recovery

**Less suitable for:**
- Single-process applications (use InMemoryEventBus)
- Ultra-high throughput (>100k events/sec - consider Kafka)
- Complex routing requirements (consider RabbitMQ)

---

## Installation

Install Redis support for eventsource-py:

```bash
pip install eventsource-py[redis]
```

This installs the `redis` package with async support (aioredis).

---

## Setting Up Redis

### Using Docker

Start Redis in a Docker container:

```bash
# Start Redis
docker run -d \
  --name redis \
  -p 6379:6379 \
  redis:7-alpine

# Verify Redis is running
docker exec redis redis-cli ping
# Output: PONG
```

### Verify Connection

Test your Redis connection:

```python
import asyncio
import redis.asyncio as aioredis

async def test_redis():
    client = await aioredis.from_url("redis://localhost:6379")
    try:
        result = await client.ping()
        print(f"Redis connection: {'OK' if result else 'FAILED'}")
    finally:
        await client.aclose()

asyncio.run(test_redis())
# Output: Redis connection: OK
```

---

## Basic RedisEventBus Setup

### Minimal Configuration

```python
from eventsource import EventRegistry, DomainEvent, register_event
from eventsource.bus import RedisEventBus, RedisEventBusConfig

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"

    customer_id: str
    total_amount: float

# Create event registry
registry = EventRegistry()
registry.register(OrderCreated)

# Configure Redis event bus
config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="myapp",
    consumer_group="default",
)

# Create bus
bus = RedisEventBus(config=config, event_registry=registry)

# Connect to Redis
await bus.connect()
```

### Publishing Events

```python
from uuid import uuid4

# Subscribe handler
async def on_order_created(event: OrderCreated):
    print(f"Order created: {event.aggregate_id} - ${event.total_amount}")

bus.subscribe(OrderCreated, on_order_created)

# Publish event
event = OrderCreated(
    aggregate_id=uuid4(),
    customer_id="cust-123",
    total_amount=99.99,
    aggregate_version=1,
)
await bus.publish([event])

# Start consuming (runs continuously)
consumer_task = asyncio.create_task(bus.start_consuming())

# ... application runs ...

# Shutdown
await bus.stop_consuming()
await consumer_task
await bus.disconnect()
```

---

## RedisEventBusConfig Options

All configuration options with their defaults:

```python
config = RedisEventBusConfig(
    # Connection
    redis_url="redis://localhost:6379",          # Redis connection URL
    socket_timeout=5.0,                          # Socket read/write timeout
    socket_connect_timeout=5.0,                  # Connection timeout
    single_connection_client=False,              # Use single connection (testing)

    # Stream naming
    stream_prefix="events",                      # Prefix for stream names

    # Consumer group
    consumer_group="default",                    # Consumer group name
    consumer_name=None,                          # Auto-generated: hostname-uuid

    # Consumption settings
    batch_size=100,                              # Events per read batch
    block_ms=5000,                               # Milliseconds to block for new events

    # Error handling
    max_retries=3,                               # Retries before DLQ
    enable_dlq=True,                             # Enable dead letter queue
    dlq_stream_suffix="_dlq",                    # DLQ stream suffix

    # Pending message recovery
    pending_idle_ms=60000,                       # Min idle time before claiming (1 min)
    retry_key_prefix="retry",                    # Retry counter key prefix
    retry_key_expiry_seconds=86400,              # Retry counter TTL (24h)

    # Observability
    enable_tracing=True,                         # OpenTelemetry tracing
)
```

### Stream Naming

RedisEventBus creates these Redis streams:

```python
config = RedisEventBusConfig(stream_prefix="orders")

# Main stream: orders:stream
# DLQ stream:  orders:stream_dlq
```

Access stream names programmatically:

```python
print(config.stream_name)      # orders:stream
print(config.dlq_stream_name)  # orders:stream_dlq
```

---

## Consumer Groups and Load Balancing

Consumer groups enable automatic load balancing: each event is delivered to exactly **one consumer** within the group.

### Single Consumer Group (Load Balancing)

```python
# Worker 1
config1 = RedisEventBusConfig(
    stream_prefix="orders",
    consumer_group="order-processors",
    consumer_name="worker-1",
)
bus1 = RedisEventBus(config=config1, event_registry=registry)
await bus1.connect()

# Worker 2 (same group)
config2 = RedisEventBusConfig(
    stream_prefix="orders",
    consumer_group="order-processors",
    consumer_name="worker-2",
)
bus2 = RedisEventBus(config=config2, event_registry=registry)
await bus2.connect()

# Subscribe both workers
bus1.subscribe(OrderCreated, worker1_handler)
bus2.subscribe(OrderCreated, worker2_handler)

# Start consuming
task1 = asyncio.create_task(bus1.start_consuming())
task2 = asyncio.create_task(bus2.start_consuming())

# Publish 10 events
for i in range(10):
    await bus1.publish([OrderCreated(...)])

# Events automatically distributed between worker-1 and worker-2
# Each worker processes ~5 events
```

### Multiple Consumer Groups (Fan-Out)

Different consumer groups all receive the same events:

```python
# Projection consumer group
projection_config = RedisEventBusConfig(
    stream_prefix="orders",
    consumer_group="projections",
)
projection_bus = RedisEventBus(config=projection_config, event_registry=registry)

# Notification consumer group
notification_config = RedisEventBusConfig(
    stream_prefix="orders",
    consumer_group="notifications",
)
notification_bus = RedisEventBus(config=notification_config, event_registry=registry)

# Both receive ALL events from orders:stream
projection_bus.subscribe(OrderCreated, update_projection)
notification_bus.subscribe(OrderCreated, send_notification)
```

**Result:** Each group processes every event independently.

---

## Pending Message Recovery

When a consumer crashes before acknowledging a message, it becomes **pending**. RedisEventBus can automatically recover these messages.

### Manual Recovery

```python
# Recover messages idle for 60+ seconds
stats = await bus.recover_pending_messages(
    min_idle_time_ms=60000,
    max_retries=3,
    batch_size=100,
)

print(f"Checked: {stats['checked']}")
print(f"Claimed: {stats['claimed']}")
print(f"Reprocessed: {stats['reprocessed']}")
print(f"Sent to DLQ: {stats['dlq']}")
print(f"Failed: {stats['failed']}")
```

### Automatic Recovery Worker

For production, run recovery continuously:

```python
async def recovery_worker(bus: RedisEventBus):
    """Background task to recover pending messages."""
    while True:
        try:
            stats = await bus.recover_pending_messages()

            if stats['reprocessed'] > 0 or stats['dlq'] > 0:
                print(f"Recovery: {stats['reprocessed']} reprocessed, "
                      f"{stats['dlq']} to DLQ")

        except Exception as e:
            print(f"Recovery error: {e}")

        # Check every minute
        await asyncio.sleep(60)

# Start recovery worker
recovery_task = asyncio.create_task(recovery_worker(bus))
```

### How Pending Recovery Works

1. **Consumer reads message**: Message marked as pending
2. **Consumer crashes**: Message remains pending (not acknowledged)
3. **Recovery detects idle message**: After `pending_idle_ms` (default: 60s)
4. **Recovery claims message**: XCLAIM transfers ownership
5. **Message reprocessed**: Handled by recovery worker
6. **Success**: Message acknowledged, removed from pending
7. **Failure**: Retry counter incremented
8. **Max retries exceeded**: Message sent to DLQ

---

## Dead Letter Queue (DLQ)

Messages that fail after `max_retries` are automatically sent to the DLQ for later analysis.

### DLQ Configuration

```python
config = RedisEventBusConfig(
    enable_dlq=True,              # Enable DLQ
    max_retries=3,                # Retries before DLQ
    dlq_stream_suffix="_dlq",     # DLQ stream name suffix
)
```

### Inspecting DLQ Messages

```python
# Get DLQ messages
dlq_messages = await bus.get_dlq_messages(count=100)

for msg in dlq_messages:
    message_id = msg['message_id']
    data = msg['data']

    print(f"Message ID: {message_id}")
    print(f"Event Type: {data.get('event_type')}")
    print(f"Original ID: {data.get('original_message_id')}")
    print(f"Retry Count: {data.get('retry_count')}")
    print(f"DLQ Timestamp: {data.get('dlq_timestamp')}")
    print("---")
```

### Replaying DLQ Messages

After fixing the underlying issue, replay messages from DLQ:

```python
# Replay specific message
success = await bus.replay_dlq_message(message_id="1234567890-0")

if success:
    print("Message replayed successfully")
else:
    print("Message not found in DLQ")
```

Replayed messages are:
1. Removed from DLQ stream
2. Added back to main stream with new message ID
3. Processed by consumers normally

### Monitoring DLQ

```python
# Check DLQ status
info = await bus.get_stream_info()

if info.get('dlq_messages', 0) > 0:
    print(f"WARNING: {info['dlq_messages']} messages in DLQ!")

    # Alert operations team
    # Investigate root cause
    # Replay after fixing issue
```

---

## Monitoring and Statistics

### Event Bus Statistics

```python
# Get statistics dictionary
stats = bus.get_stats_dict()

print(f"Events published: {stats['events_published']}")
print(f"Events consumed: {stats['events_consumed']}")
print(f"Successfully processed: {stats['events_processed_success']}")
print(f"Failed processing: {stats['events_processed_failed']}")
print(f"Messages recovered: {stats['messages_recovered']}")
print(f"Messages in DLQ: {stats['messages_sent_to_dlq']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Reconnections: {stats['reconnections']}")
```

### Stream Information

```python
# Get detailed stream info
info = await bus.get_stream_info()

# Connection status
print(f"Connected: {info['connected']}")

# Stream details
stream = info['stream']
print(f"Stream name: {stream['name']}")
print(f"Stream length: {stream['length']}")
print(f"First entry: {stream['first_entry_id']}")
print(f"Last entry: {stream['last_entry_id']}")

# Consumer groups
for group in info['consumer_groups']:
    print(f"Group: {group['name']}")
    print(f"  Consumers: {group['consumers']}")
    print(f"  Pending: {group['pending']}")

# Overall stats
print(f"Pending messages: {info['pending_messages']}")
print(f"DLQ messages: {info['dlq_messages']}")
print(f"Active consumers: {info['active_consumers']}")
```

### Subscriber Counts

```python
# Count subscribers
total = bus.get_subscriber_count()
order_created_count = bus.get_subscriber_count(OrderCreated)
wildcard_count = bus.get_wildcard_subscriber_count()

print(f"Total subscribers: {total}")
print(f"OrderCreated subscribers: {order_created_count}")
print(f"Wildcard subscribers: {wildcard_count}")
```

---

## Integration with Repository

Connect RedisEventBus to your repository for automatic event publishing:

```python
from eventsource import AggregateRepository, PostgreSQLEventStore
from sqlalchemy.ext.asyncio import async_sessionmaker

# Create infrastructure
session_factory = async_sessionmaker(...)
event_store = PostgreSQLEventStore(session_factory)

# Create Redis event bus
config = RedisEventBusConfig(
    stream_prefix="orders",
    consumer_group="projections",
)
bus = RedisEventBus(config=config, event_registry=registry)
await bus.connect()

# Create repository with event publishing
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=bus,  # Events auto-published to Redis
)

# Subscribe handlers
async def update_order_projection(event: DomainEvent):
    # Update read model
    pass

bus.subscribe(OrderCreated, update_order_projection)

# Start consuming
consume_task = asyncio.create_task(bus.start_consuming())

# Create and save aggregate
order = repo.create_new(uuid4())
order.create(customer_id="cust-123", total_amount=99.99)
await repo.save(order)  # Events automatically flow through Redis

# Handler receives event automatically
```

**Publishing flow:**
1. Repository saves events to PostgreSQL event store
2. Repository publishes events to RedisEventBus
3. Redis distributes events to all consumers in consumer group
4. Handlers process events and update projections

---

## Complete Working Example

```python
"""
Tutorial 17: Redis Event Bus - Complete Example
Demonstrates publishing, consuming, consumer groups, and monitoring.

Requirements:
- pip install eventsource-py[redis]
- docker run -d --name redis -p 6379:6379 redis:7-alpine
"""

import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    EventRegistry,
    register_event,
)
from eventsource.bus import (
    REDIS_AVAILABLE,
    RedisEventBus,
    RedisEventBusConfig,
)

if not REDIS_AVAILABLE:
    print("ERROR: Redis support not installed.")
    print("Install with: pip install eventsource-py[redis]")
    exit(1)


# =============================================================================
# Events
# =============================================================================


@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"

    customer_id: str
    total_amount: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str


# =============================================================================
# Aggregate
# =============================================================================


class OrderState(BaseModel):
    order_id: UUID
    customer_id: str | None = None
    total_amount: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                total_amount=event.total_amount,
                status="created",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                    }
                )

    def create(self, customer_id: str, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(
            OrderCreated(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                total_amount=total_amount,
                aggregate_version=self.get_next_version(),
            )
        )

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                tracking_number=tracking_number,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Projections
# =============================================================================


class OrderStatisticsProjection:
    """Maintains order statistics."""

    def __init__(self):
        self.total_orders = 0
        self.total_revenue = 0.0
        self.shipped_count = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self.total_orders += 1
            self.total_revenue += event.total_amount
            print(f"  [Stats] Order created: ${event.total_amount:.2f}")
        elif isinstance(event, OrderShipped):
            self.shipped_count += 1
            print(f"  [Stats] Order shipped: {event.tracking_number}")


class AuditLogger:
    """Logs all events for audit trail."""

    def __init__(self):
        self.log: list[dict] = []

    async def handle(self, event: DomainEvent) -> None:
        entry = {
            "event_type": event.event_type,
            "aggregate_id": str(event.aggregate_id),
            "timestamp": event.occurred_at.isoformat(),
        }
        self.log.append(entry)


# =============================================================================
# Demo Functions
# =============================================================================


async def demo_basic_publish_subscribe():
    """Demo 1: Basic publishing and subscribing."""
    print("\n" + "=" * 60)
    print("Demo 1: Basic Publish/Subscribe")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderCreated)
    registry.register(OrderShipped)

    config = RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="demo1",
        consumer_group="demo-consumers",
        block_ms=1000,
        single_connection_client=True,
    )
    bus = RedisEventBus(config=config, event_registry=registry)

    try:
        await bus.connect()
        print("Connected to Redis")

        received_events: list[DomainEvent] = []

        async def event_handler(event: DomainEvent) -> None:
            received_events.append(event)
            print(f"  Received: {event.event_type}")

        bus.subscribe(OrderCreated, event_handler)
        print("Subscribed handler to OrderCreated")

        # Publish event
        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            customer_id="cust-123",
            total_amount=99.99,
            aggregate_version=1,
        )
        await bus.publish([event])
        print(f"Published OrderCreated for {order_id}")

        # Start consuming
        consume_task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(2.0)

        # Stop consuming
        await bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        print(f"Received {len(received_events)} event(s)")

    finally:
        await bus.disconnect()


async def demo_consumer_groups():
    """Demo 2: Consumer groups for load balancing."""
    print("\n" + "=" * 60)
    print("Demo 2: Consumer Groups and Load Balancing")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderCreated)

    base_config = {
        "redis_url": "redis://localhost:6379",
        "stream_prefix": "demo2",
        "consumer_group": "load-balanced-consumers",
        "block_ms": 500,
        "single_connection_client": True,
    }

    config1 = RedisEventBusConfig(**base_config, consumer_name="worker-1")
    config2 = RedisEventBusConfig(**base_config, consumer_name="worker-2")

    bus1 = RedisEventBus(config=config1, event_registry=registry)
    bus2 = RedisEventBus(config=config2, event_registry=registry)

    worker1_events: list[str] = []
    worker2_events: list[str] = []

    async def worker1_handler(event: DomainEvent) -> None:
        worker1_events.append(str(event.aggregate_id))
        print(f"  Worker 1 processed: {event.aggregate_id}")

    async def worker2_handler(event: DomainEvent) -> None:
        worker2_events.append(str(event.aggregate_id))
        print(f"  Worker 2 processed: {event.aggregate_id}")

    try:
        await bus1.connect()
        await bus2.connect()
        print("Both workers connected")

        bus1.subscribe(OrderCreated, worker1_handler)
        bus2.subscribe(OrderCreated, worker2_handler)

        # Publish events
        print("\nPublishing 6 events...")
        for i in range(6):
            event = OrderCreated(
                aggregate_id=uuid4(),
                customer_id=f"cust-{i}",
                total_amount=10.0 * i,
                aggregate_version=1,
            )
            await bus1.publish([event])

        # Both workers consume
        task1 = asyncio.create_task(bus1.start_consuming())
        task2 = asyncio.create_task(bus2.start_consuming())
        await asyncio.sleep(3.0)

        # Stop consuming
        await bus1.stop_consuming()
        await bus2.stop_consuming()

        for task in [task1, task2]:
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except TimeoutError:
                task.cancel()

        print(f"\nResults:")
        print(f"  Worker 1 processed: {len(worker1_events)} events")
        print(f"  Worker 2 processed: {len(worker2_events)} events")
        print(f"  Total: {len(worker1_events) + len(worker2_events)} events")

    finally:
        await bus1.disconnect()
        await bus2.disconnect()


async def demo_statistics_and_monitoring():
    """Demo 3: Statistics and monitoring."""
    print("\n" + "=" * 60)
    print("Demo 3: Statistics and Monitoring")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderCreated)
    registry.register(OrderShipped)

    config = RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="demo3",
        consumer_group="monitoring-demo",
        single_connection_client=True,
    )
    bus = RedisEventBus(config=config, event_registry=registry)

    try:
        await bus.connect()

        # Publish events
        for i in range(10):
            event = OrderCreated(
                aggregate_id=uuid4(),
                customer_id=f"cust-{i}",
                total_amount=100.0 + i,
                aggregate_version=1,
            )
            await bus.publish([event])

        # Get stream info
        info = await bus.get_stream_info()
        print(f"Published 10 events")
        print(f"Stream length: {info['stream']['length']}")
        print(f"Consumer groups: {len(info['consumer_groups'])}")
        print(f"Pending messages: {info['pending_messages']}")
        print(f"DLQ messages: {info['dlq_messages']}")

        # Get statistics
        stats = bus.get_stats_dict()
        print(f"\nStatistics:")
        print(f"  Published: {stats['events_published']}")
        print(f"  Consumed: {stats['events_consumed']}")
        print(f"  Success: {stats['events_processed_success']}")
        print(f"  Failed: {stats['events_processed_failed']}")

        # Subscriber counts
        print(f"\nSubscribers:")
        print(f"  Total: {bus.get_subscriber_count()}")
        print(f"  Wildcard: {bus.get_wildcard_subscriber_count()}")

    finally:
        await bus.disconnect()


async def demo_with_projections():
    """Demo 4: Integration with projections."""
    print("\n" + "=" * 60)
    print("Demo 4: Integration with Projections")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderCreated)
    registry.register(OrderShipped)

    config = RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="demo4",
        consumer_group="projection-consumers",
        block_ms=500,
        single_connection_client=True,
    )
    bus = RedisEventBus(config=config, event_registry=registry)

    stats_projection = OrderStatisticsProjection()
    auditor = AuditLogger()

    try:
        await bus.connect()

        # Subscribe projection to its declared events
        bus.subscribe_all(stats_projection)
        print("Subscribed statistics projection")

        # Subscribe auditor to all events
        bus.subscribe_to_all_events(auditor)
        print("Subscribed audit logger to all events")

        # Create and process order
        order_id = uuid4()
        print(f"\nProcessing order: {order_id}")

        await bus.publish([
            OrderCreated(
                aggregate_id=order_id,
                customer_id="premium-customer",
                total_amount=499.99,
                aggregate_version=1,
            )
        ])

        await bus.publish([
            OrderShipped(
                aggregate_id=order_id,
                tracking_number="TRACK-12345",
                aggregate_version=2,
            )
        ])

        # Start consuming
        consume_task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(2.0)
        await bus.stop_consuming()

        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        # Show results
        print(f"\nProjection Results:")
        print(f"  Total orders: {stats_projection.total_orders}")
        print(f"  Total revenue: ${stats_projection.total_revenue:.2f}")
        print(f"  Shipped count: {stats_projection.shipped_count}")
        print(f"\nAudit log entries: {len(auditor.log)}")

    finally:
        await bus.disconnect()


async def main():
    """Run all demos."""
    print("=" * 60)
    print("Tutorial 17: Redis Event Bus")
    print("=" * 60)

    try:
        await demo_basic_publish_subscribe()
        await demo_consumer_groups()
        await demo_statistics_and_monitoring()
        await demo_with_projections()

        print("\n" + "=" * 60)
        print("Tutorial 17 Complete!")
        print("=" * 60)

    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure Redis is running:")
        print("  docker run -d --name redis -p 6379:6379 redis:7-alpine")
        raise


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
============================================================
Tutorial 17: Redis Event Bus
============================================================

============================================================
Demo 1: Basic Publish/Subscribe
============================================================
Connected to Redis
Subscribed handler to OrderCreated
Published OrderCreated for <uuid>
  Received: OrderCreated
Received 1 event(s)

============================================================
Demo 2: Consumer Groups and Load Balancing
============================================================
Both workers connected

Publishing 6 events...
  Worker 1 processed: <uuid>
  Worker 2 processed: <uuid>
  Worker 1 processed: <uuid>
  Worker 2 processed: <uuid>
  Worker 1 processed: <uuid>
  Worker 2 processed: <uuid>

Results:
  Worker 1 processed: 3 events
  Worker 2 processed: 3 events
  Total: 6 events

============================================================
Demo 3: Statistics and Monitoring
============================================================
Published 10 events
Stream length: 10
Consumer groups: 1
Pending messages: 0
DLQ messages: 0

Statistics:
  Published: 10
  Consumed: 0
  Success: 0
  Failed: 0

Subscribers:
  Total: 0
  Wildcard: 0

============================================================
Demo 4: Integration with Projections
============================================================
Subscribed statistics projection
Subscribed audit logger to all events

Processing order: <uuid>
  [Stats] Order created: $499.99
  [Stats] Order shipped: TRACK-12345

Projection Results:
  Total orders: 1
  Total revenue: $499.99
  Shipped count: 1

Audit log entries: 2

============================================================
Tutorial 17 Complete!
============================================================
```

---

## Production Deployment Patterns

### Graceful Shutdown

```python
import signal

class RedisEventBusService:
    def __init__(self, bus: RedisEventBus):
        self.bus = bus
        self.consume_task: asyncio.Task | None = None
        self.recovery_task: asyncio.Task | None = None
        self.shutdown_event = asyncio.Event()

    async def start(self):
        """Start event bus service."""
        await self.bus.connect()

        # Start consumer
        self.consume_task = asyncio.create_task(
            self.bus.start_consuming()
        )

        # Start recovery worker
        self.recovery_task = asyncio.create_task(
            self._recovery_worker()
        )

        # Wait for shutdown signal
        await self.shutdown_event.wait()

    async def _recovery_worker(self):
        """Background recovery worker."""
        while not self.shutdown_event.is_set():
            try:
                stats = await self.bus.recover_pending_messages()
                if stats['dlq'] > 0:
                    print(f"Sent {stats['dlq']} messages to DLQ")
            except Exception as e:
                print(f"Recovery error: {e}")

            await asyncio.sleep(60)

    async def shutdown(self):
        """Graceful shutdown."""
        print("Shutting down...")
        self.shutdown_event.set()

        # Stop consuming
        await self.bus.stop_consuming()

        # Wait for tasks to complete
        if self.consume_task:
            await self.consume_task
        if self.recovery_task:
            self.recovery_task.cancel()

        # Disconnect
        await self.bus.disconnect()
        print("Shutdown complete")

# Setup signal handlers
async def main():
    service = RedisEventBusService(bus)

    loop = asyncio.get_running_loop()

    def signal_handler():
        asyncio.create_task(service.shutdown())

    loop.add_signal_handler(signal.SIGTERM, signal_handler)
    loop.add_signal_handler(signal.SIGINT, signal_handler)

    await service.start()

asyncio.run(main())
```

### Health Check Endpoint

```python
async def health_check(bus: RedisEventBus) -> dict:
    """
    Health check for Kubernetes liveness/readiness probes.

    Returns:
        Health status dictionary
    """
    try:
        info = await bus.get_stream_info()

        is_healthy = (
            info.get('connected', False) and
            bus.is_consuming
        )

        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "connected": info.get('connected', False),
            "consuming": bus.is_consuming,
            "pending_messages": info.get('pending_messages', 0),
            "dlq_messages": info.get('dlq_messages', 0),
        }

    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }
```

### Connection Pool Sizing

When running multiple instances, configure Redis connection limits:

```python
import os

# Calculate connections per instance
instance_count = int(os.getenv("INSTANCE_COUNT", "1"))

# Redis default max connections: 10000
# Reserve some for admin/monitoring
max_connections_per_instance = 9000 // instance_count

config = RedisEventBusConfig(
    redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
    # Single connection mode recommended for most deployments
    single_connection_client=False,
)
```

---

## Key Takeaways

1. **RedisEventBus enables distributed event distribution** across multiple processes and servers
2. **Redis Streams provide durability** - events survive application restarts
3. **Consumer groups enable load balancing** - events distributed across workers automatically
4. **Pending message recovery provides fault tolerance** - crashed consumers don't lose events
5. **Dead letter queues capture poison messages** - prevent blocking on unprocessable events
6. **Pipeline batching optimizes throughput** - multiple events published in single network call
7. **Event registry required for deserialization** - register all events before consuming
8. **Monitor stream health and statistics** - track pending messages, DLQ, and consumer status
9. **Same EventBus interface as InMemoryEventBus** - easy migration between implementations
10. **Horizontal scaling via multiple consumers** - add instances without reconfiguration

---

## Common Patterns

### Pattern 1: Multiple Consumer Groups

Different services each get all events:

```python
# Service A: Updates projections
projection_bus = RedisEventBus(
    config=RedisEventBusConfig(consumer_group="projections"),
    event_registry=registry,
)

# Service B: Sends notifications
notification_bus = RedisEventBus(
    config=RedisEventBusConfig(consumer_group="notifications"),
    event_registry=registry,
)

# Service C: Captures analytics
analytics_bus = RedisEventBus(
    config=RedisEventBusConfig(consumer_group="analytics"),
    event_registry=registry,
)
```

### Pattern 2: Scaled Workers

Multiple instances of same service share load:

```python
# Worker instance 1
worker1 = RedisEventBus(
    config=RedisEventBusConfig(
        consumer_group="order-processors",
        consumer_name="worker-1",
    ),
    event_registry=registry,
)

# Worker instance 2
worker2 = RedisEventBus(
    config=RedisEventBusConfig(
        consumer_group="order-processors",
        consumer_name="worker-2",
    ),
    event_registry=registry,
)

# Events automatically distributed between worker-1 and worker-2
```

### Pattern 3: Event Replay

Replay historical events to new projections:

```python
# Read events from main stream starting from beginning
messages = await redis_client.xrange(
    name=config.stream_name,
    min="-",      # Oldest
    max="+",      # Newest
    count=1000,
)

# Process each event
for message_id, data in messages:
    event = deserialize_event(data)
    await new_projection.handle(event)
```

---

## Troubleshooting

### Consumer Not Receiving Events

**Check consumer group exists:**
```python
info = await bus.get_stream_info()
groups = [g['name'] for g in info['consumer_groups']]
print(f"Consumer groups: {groups}")
```

**Verify consumer is running:**
```python
assert bus.is_consuming, "Consumer not running"
```

### High Pending Message Count

**Recover pending messages:**
```python
stats = await bus.recover_pending_messages(min_idle_time_ms=30000)
print(f"Recovered: {stats['reprocessed']}")
```

**Check for slow handlers:**
```python
# Add timing to handlers
import time

async def slow_handler(event: DomainEvent):
    start = time.time()
    # ... process event ...
    duration = time.time() - start
    if duration > 1.0:
        print(f"Slow handler: {duration:.2f}s")
```

### Messages in DLQ

**Inspect DLQ messages:**
```python
dlq_messages = await bus.get_dlq_messages(count=10)
for msg in dlq_messages:
    print(f"Failed: {msg['data']['event_type']}")
    print(f"Reason: Check application logs")
```

**Fix and replay:**
```python
# Fix the underlying bug
# Then replay all DLQ messages
for msg in dlq_messages:
    await bus.replay_dlq_message(msg['message_id'])
```

---

## Next Steps

Now that you understand Redis event distribution, explore other distributed event bus implementations:

- [Tutorial 18: Kafka Event Bus](18-kafka.md) - Ultra-high throughput event streaming
- [Tutorial 19: RabbitMQ Event Bus](19-rabbitmq.md) - Enterprise messaging with complex routing

For production deployment:
- [Tutorial 15: Production Deployment](15-production.md) - Health checks, monitoring, graceful shutdown

---

## Additional Resources

- [Redis Streams Documentation](https://redis.io/docs/latest/develop/data-types/streams/)
- [Consumer Groups Guide](https://redis.io/docs/latest/develop/data-types/streams/#consumer-groups)
- `tests/integration/bus/test_redis.py` - Comprehensive integration tests
- `src/eventsource/bus/redis.py` - RedisEventBus implementation
