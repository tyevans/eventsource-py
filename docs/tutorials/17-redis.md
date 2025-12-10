# Tutorial 17: Using Redis Event Bus

**Estimated Time:** 60 minutes
**Difficulty:** Advanced
**Progress:** Tutorial 17 of 21 | Phase 4: Advanced Patterns

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 15: Production Deployment Guide](15-production.md)
- Completed [Tutorial 7: Distributing Events with Event Bus](07-event-bus.md)
- Understanding of event bus patterns from Tutorial 7
- Python 3.11+ installed
- Docker installed (for running Redis locally)
- eventsource-py with Redis support installed:

```bash
pip install eventsource-py[redis]
```

This tutorial covers distributed event distribution using Redis Streams. If you are building a multi-process or multi-server application that needs to share events across instances, the Redis event bus is an excellent choice.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain why Redis is valuable for distributed event distribution
2. Configure and connect the `RedisEventBus`
3. Implement pub/sub patterns using Redis Streams
4. Manage consumer groups for load balancing
5. Handle failures with dead letter queues (DLQ)
6. Recover pending messages after failures
7. Apply best practices for production Redis event bus usage

---

## Why Redis for Event Distribution?

In Tutorial 7, you learned how `InMemoryEventBus` distributes events within a single process. But what happens when you need to scale horizontally across multiple servers?

### The Problem: Single-Process Limitations

`InMemoryEventBus` has inherent limitations:

```
                 Server 1                    Server 2
              +-------------+              +-------------+
              |   App       |              |   App       |
              |   +-------+ |              |   +-------+ |
              |   | Bus   | |              |   | Bus   | |
              |   +-------+ |              |   +-------+ |
              | Events stay |              | Events stay |
              |   local!    |              |   local!    |
              +-------------+              +-------------+
```

- Events published on Server 1 never reach Server 2
- Projections running on different servers see different events
- No fault tolerance - if a server crashes, events are lost

### The Solution: Redis Streams

Redis Streams provide a durable, distributed log that multiple consumers can read from:

```
              Server 1          Redis           Server 2
            +-----------+    +--------+       +-----------+
            | Publisher |    |        |       | Consumer  |
            |    |      |--->| Stream |<------|    A      |
            +-----------+    |        |       +-----------+
                             |        |
              Server 3       |        |         Server 4
            +-----------+    |        |       +-----------+
            | Publisher |--->|        |<------| Consumer  |
            |    &      |    +--------+       |    B      |
            | Consumer  |                     +-----------+
            +-----------+
```

### Redis Streams Features

Redis Streams provide enterprise-grade event distribution:

| Feature | Benefit |
|---------|---------|
| **Durability** | Events persist in Redis, surviving restarts |
| **Consumer Groups** | Multiple consumers can share the workload |
| **At-Least-Once Delivery** | Messages acknowledged after successful processing |
| **Message Recovery** | Pending messages can be recovered after crashes |
| **Dead Letter Queue** | Failed messages captured for later analysis |
| **Horizontal Scaling** | Add more consumers to handle increased load |
| **Pipeline Optimization** | Batch publishing for high throughput |

### When to Use RedisEventBus

Use `RedisEventBus` when you need:

- Multiple processes or servers processing events
- Event persistence beyond process lifetime
- Load balancing across multiple consumers
- Fault tolerance and message recovery
- Horizontal scaling capability

Continue using `InMemoryEventBus` when:

- All event processing happens in a single process
- You are writing tests that need fast, isolated execution
- You do not need event persistence in the bus

---

## Setting Up Redis

Before using `RedisEventBus`, you need a running Redis instance.

### Using Docker (Recommended for Development)

Start Redis with Docker:

```bash
# Start Redis
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Verify it's running
docker exec redis redis-cli ping
# Should output: PONG
```

### Verifying the Connection

Test your Redis connection:

```python
import asyncio
import redis.asyncio as aioredis

async def test_redis():
    """Test Redis connection."""
    client = await aioredis.from_url("redis://localhost:6379")
    try:
        result = await client.ping()
        print(f"Redis connection: {'OK' if result else 'FAILED'}")
    finally:
        await client.aclose()

asyncio.run(test_redis())
```

---

## RedisEventBus Configuration

The `RedisEventBusConfig` dataclass provides comprehensive configuration options.

### Basic Configuration

```python
from eventsource.bus import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="myapp",
    consumer_group="projections",
)

bus = RedisEventBus(config=config)
```

### Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `redis_url` | `"redis://localhost:6379"` | Redis connection URL |
| `stream_prefix` | `"events"` | Prefix for stream names |
| `consumer_group` | `"default"` | Consumer group name |
| `consumer_name` | Auto-generated | Unique consumer identifier |
| `batch_size` | `100` | Max events per read |
| `block_ms` | `5000` | Blocking read timeout (ms) |
| `max_retries` | `3` | Retries before DLQ |
| `pending_idle_ms` | `60000` | Idle time before claiming pending |
| `enable_dlq` | `True` | Enable dead letter queue |
| `dlq_stream_suffix` | `"_dlq"` | DLQ stream name suffix |
| `socket_timeout` | `5.0` | Socket timeout (seconds) |
| `socket_connect_timeout` | `5.0` | Connection timeout (seconds) |
| `enable_tracing` | `True` | Enable OpenTelemetry tracing |
| `retry_key_prefix` | `"retry"` | Prefix for retry count keys |
| `retry_key_expiry_seconds` | `86400` | Retry key expiry (24h) |

### Stream Naming

The configuration determines stream names:

```python
config = RedisEventBusConfig(
    stream_prefix="orders",
    dlq_stream_suffix="_dlq",
)

# Main stream: orders:stream
# DLQ stream: orders:stream_dlq
```

### Consumer Naming

Each consumer needs a unique name within its group:

```python
# Auto-generated (hostname + UUID)
config = RedisEventBusConfig(
    consumer_group="projections",
    # consumer_name auto-generated: "server1-abc12345"
)

# Explicit naming (recommended for production)
config = RedisEventBusConfig(
    consumer_group="projections",
    consumer_name="worker-1",
)
```

---

## Basic Usage

Let's build a complete example of publishing and consuming events with Redis.

### Step 1: Define Events

```python
from uuid import UUID, uuid4
from eventsource import DomainEvent, register_event

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str
    total: float

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str
```

### Step 2: Create and Connect the Bus

```python
from eventsource.bus import RedisEventBus, RedisEventBusConfig
from eventsource import EventRegistry

# Create event registry (required for deserialization)
registry = EventRegistry()
registry.register(OrderCreated)
registry.register(OrderShipped)

# Configure the bus
config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="orders",
    consumer_group="order-processors",
)

# Create and connect
bus = RedisEventBus(config=config, event_registry=registry)
await bus.connect()
```

### Step 3: Subscribe Handlers

Subscription works the same as `InMemoryEventBus`:

```python
from eventsource import DomainEvent

# Function-based handler
async def log_order_created(event: DomainEvent) -> None:
    print(f"Order created: {event.aggregate_id}")

bus.subscribe(OrderCreated, log_order_created)

# Class-based handler
class OrderNotificationHandler:
    async def handle(self, event: DomainEvent) -> None:
        print(f"Sending notification for order: {event.aggregate_id}")

notification_handler = OrderNotificationHandler()
bus.subscribe(OrderCreated, notification_handler)

# Wildcard subscription (all events)
async def audit_all_events(event: DomainEvent) -> None:
    print(f"[AUDIT] {event.event_type}: {event.aggregate_id}")

bus.subscribe_to_all_events(audit_all_events)
```

### Step 4: Publish Events

```python
from uuid import uuid4

# Create and publish an event
order_id = uuid4()
event = OrderCreated(
    aggregate_id=order_id,
    customer_id="cust-123",
    total=99.99,
    aggregate_version=1,
)

await bus.publish([event])
print(f"Published OrderCreated for order {order_id}")
```

### Step 5: Start Consuming

```python
import asyncio

# Start consuming in background
async def run_consumer():
    """Run the consumer loop."""
    await bus.start_consuming()

# Option 1: Run as main task (blocking)
await bus.start_consuming()

# Option 2: Run in background task
consumer_task = asyncio.create_task(run_consumer())

# ... do other work ...

# Stop consuming when done
await bus.stop_consuming()
await consumer_task
```

### Step 6: Graceful Shutdown

```python
async def shutdown():
    """Gracefully shut down the bus."""
    await bus.shutdown(timeout=30.0)
    print("Redis event bus shutdown complete")
```

---

## Consumer Groups and Load Balancing

Consumer groups allow multiple consumers to share the workload of processing events.

### How Consumer Groups Work

```
                     Redis Stream
                    +-------------+
                    |  Event 1    |  --> Consumer A (processes)
                    |  Event 2    |  --> Consumer B (processes)
                    |  Event 3    |  --> Consumer A (processes)
                    |  Event 4    |  --> Consumer B (processes)
                    +-------------+
```

Each event is delivered to exactly one consumer within the group. This enables:

- **Load Balancing**: Events distributed across available consumers
- **Horizontal Scaling**: Add more consumers to handle more events
- **Fault Tolerance**: If one consumer fails, others continue processing

### Creating Multiple Consumers

```python
# Worker 1 configuration
config_worker1 = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="orders",
    consumer_group="order-processors",  # Same group
    consumer_name="worker-1",           # Unique name
)
bus_worker1 = RedisEventBus(config=config_worker1, event_registry=registry)

# Worker 2 configuration
config_worker2 = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="orders",
    consumer_group="order-processors",  # Same group
    consumer_name="worker-2",           # Unique name
)
bus_worker2 = RedisEventBus(config=config_worker2, event_registry=registry)
```

### Consumer Group Isolation

Different consumer groups receive all events independently:

```python
# Group A: Processes events for projections
config_projections = RedisEventBusConfig(
    consumer_group="projections",
)

# Group B: Processes events for notifications
config_notifications = RedisEventBusConfig(
    consumer_group="notifications",
)

# Both groups receive every event!
```

This is useful when:

- Different systems need to process all events
- You want independent scaling for different workloads
- Different groups have different processing requirements

### Checking Consumer Group Status

```python
info = await bus.get_stream_info()

print(f"Stream: {info['stream']['name']}")
print(f"Stream length: {info['stream']['length']}")
print(f"Active consumers: {info['active_consumers']}")

for group in info['consumer_groups']:
    print(f"  Group '{group['name']}': {group['consumers']} consumers, {group['pending']} pending")
```

---

## Dead Letter Queue (DLQ)

Failed messages that cannot be processed after max retries are sent to the DLQ.

### How DLQ Works

```
                     Main Stream
                    +-------------+
                    |  Event A    | --> Consumer --> Success! --> ACK
                    |  Event B    | --> Consumer --> FAIL! (retry 1)
                    |  Event B    | --> Consumer --> FAIL! (retry 2)
                    |  Event B    | --> Consumer --> FAIL! (retry 3)
                    +-------------+          |
                                             v
                                      +-------------+
                                      |  DLQ Stream |
                                      |  Event B    |
                                      +-------------+
```

### Configuring DLQ Behavior

```python
config = RedisEventBusConfig(
    enable_dlq=True,              # Enable DLQ (default: True)
    max_retries=3,                # Retries before DLQ (default: 3)
    dlq_stream_suffix="_dlq",     # DLQ stream suffix
)
```

### Inspecting DLQ Messages

```python
# Get messages from DLQ
dlq_messages = await bus.get_dlq_messages(count=100)

for msg in dlq_messages:
    print(f"Message ID: {msg['message_id']}")
    print(f"Event Type: {msg['data'].get('event_type')}")
    print(f"Original ID: {msg['data'].get('original_message_id')}")
    print(f"Retry Count: {msg['data'].get('retry_count')}")
    print(f"DLQ Time: {msg['data'].get('dlq_timestamp')}")
    print("---")
```

### Replaying DLQ Messages

Once you fix the underlying issue, you can replay messages:

```python
# Replay a single message back to the main stream
success = await bus.replay_dlq_message(message_id="1234567890-0")

if success:
    print("Message replayed successfully")
else:
    print("Failed to replay message")
```

### DLQ Monitoring

Monitor DLQ health as part of your operations:

```python
info = await bus.get_stream_info()
dlq_count = info.get("dlq_messages", 0)

if dlq_count > 0:
    print(f"WARNING: {dlq_count} messages in DLQ need attention!")
```

---

## Pending Message Recovery

Messages that are read but not acknowledged (e.g., due to crash) can be recovered.

### How Pending Recovery Works

```
Timeline:
=========

t=0:  Consumer reads Event A
t=1:  Consumer crashes (Event A not ACKed)
...
t=60: Recovery worker claims Event A (idle > 60s)
t=61: Recovery worker processes Event A
t=62: Event A acknowledged
```

### Manual Recovery

Trigger manual recovery of pending messages:

```python
# Recover pending messages
recovery_stats = await bus.recover_pending_messages(
    min_idle_time_ms=60000,  # Messages idle for 1+ minute
    max_retries=3,           # Max retries before DLQ
    batch_size=100,          # Messages per batch
)

print(f"Checked: {recovery_stats['checked']}")
print(f"Claimed: {recovery_stats['claimed']}")
print(f"Reprocessed: {recovery_stats['reprocessed']}")
print(f"Sent to DLQ: {recovery_stats['dlq']}")
print(f"Failed: {recovery_stats['failed']}")
```

### Automatic Recovery

For production, set up periodic recovery:

```python
import asyncio

async def recovery_worker(bus: RedisEventBus):
    """Background worker for pending message recovery."""
    while True:
        try:
            stats = await bus.recover_pending_messages()
            if stats['reprocessed'] > 0 or stats['dlq'] > 0:
                print(f"Recovery: {stats['reprocessed']} reprocessed, {stats['dlq']} to DLQ")
        except Exception as e:
            print(f"Recovery error: {e}")

        await asyncio.sleep(60)  # Run every minute

# Start as background task
recovery_task = asyncio.create_task(recovery_worker(bus))
```

---

## Statistics and Monitoring

`RedisEventBus` tracks detailed statistics for monitoring and debugging.

### Available Statistics

```python
# Get statistics dictionary
stats = bus.get_stats_dict()

print(f"Events published: {stats['events_published']}")
print(f"Events consumed: {stats['events_consumed']}")
print(f"Successfully processed: {stats['events_processed_success']}")
print(f"Failed processing: {stats['events_processed_failed']}")
print(f"Messages recovered: {stats['messages_recovered']}")
print(f"Sent to DLQ: {stats['messages_sent_to_dlq']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Reconnections: {stats['reconnections']}")
```

### Using RedisEventBusStats

Access the stats object directly:

```python
from eventsource.bus import RedisEventBusStats

stats: RedisEventBusStats = bus.stats

# Calculate success rate
if stats.events_consumed > 0:
    success_rate = stats.events_processed_success / stats.events_consumed
    print(f"Success rate: {success_rate:.2%}")
```

### Stream Information

Get detailed information about the Redis stream:

```python
info = await bus.get_stream_info()

if info['connected']:
    stream = info['stream']
    print(f"Stream: {stream['name']}")
    print(f"Length: {stream['length']} messages")
    print(f"First entry: {stream['first_entry_id']}")
    print(f"Last entry: {stream['last_entry_id']}")

    print(f"\nConsumer groups: {len(info['consumer_groups'])}")
    for group in info['consumer_groups']:
        print(f"  - {group['name']}: {group['consumers']} consumers, {group['pending']} pending")

    print(f"\nPending messages: {info['pending_messages']}")
    print(f"DLQ messages: {info['dlq_messages']}")
else:
    print("Not connected to Redis")
```

---

## Integration with Repository

Like `InMemoryEventBus`, you can connect `RedisEventBus` to repositories for automatic event publishing.

### Setting Up Repository Integration

```python
from eventsource import (
    AggregateRepository,
    PostgreSQLEventStore,
)
from eventsource.bus import RedisEventBus, RedisEventBusConfig

# Create Redis event bus
config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="orders",
)
bus = RedisEventBus(config=config, event_registry=registry)
await bus.connect()

# Create event store
event_store = PostgreSQLEventStore(session_factory)

# Create repository with event bus
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=bus,  # Events published to Redis after save
)

# Subscribe projection
bus.subscribe_all(order_projection)

# Now when you save aggregates, events flow through Redis
order = repo.create_new(uuid4())
order.create(customer_id="cust-123", total=99.99)
await repo.save(order)  # Events published to Redis!
```

### Multi-Process Architecture

With Redis event bus, you can separate publishers and consumers:

```
                    Publisher Process
                   +------------------+
                   |  API Server      |
                   |  +------------+  |
                   |  | Repository |--+---> Redis ---> Consumer Process 1
                   |  +------------+  |              +------------------+
                   +------------------+              |  Projection A    |
                                                    |  Projection B    |
                                                    +------------------+

                                                    Consumer Process 2
                                                    +------------------+
                                                    |  Notification    |
                                                    |  Handler         |
                                                    +------------------+
```

---

## Complete Example

Here is a complete example demonstrating all Redis event bus concepts:

```python
"""
Tutorial 17: Using Redis Event Bus

This example demonstrates distributed event distribution with Redis Streams.
Run with: python tutorial_17_redis.py

Requirements:
- Redis running on localhost:6379
- pip install eventsource-py[redis]
"""
import asyncio
import signal
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    EventRegistry,
    InMemoryEventStore,
    register_event,
)
from eventsource.bus import (
    REDIS_AVAILABLE,
    RedisEventBus,
    RedisEventBusConfig,
)


# =============================================================================
# Check Redis availability
# =============================================================================

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
    total: float


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
    total: float = 0.0
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
                total=event.total,
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

    def create(self, customer_id: str, total: float) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(
            OrderCreated(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                total=total,
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
# Event Handlers
# =============================================================================

class OrderCountProjection:
    """Simple projection that counts orders."""

    def __init__(self):
        self.created_count = 0
        self.shipped_count = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self.created_count += 1
            print(f"  [Projection] Order created: {event.aggregate_id}")
        elif isinstance(event, OrderShipped):
            self.shipped_count += 1
            print(f"  [Projection] Order shipped: {event.aggregate_id}")


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
        print(f"  [Audit] {event.event_type} at {event.occurred_at}")


# =============================================================================
# Demo Functions
# =============================================================================

async def demo_basic_publish_subscribe():
    """Demonstrate basic publishing and subscribing."""
    print("=" * 60)
    print("Demo 1: Basic Publish/Subscribe")
    print("=" * 60)
    print()

    # Create event registry
    registry = EventRegistry()
    registry.register(OrderCreated)
    registry.register(OrderShipped)

    # Configure and create bus
    config = RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="demo1",
        consumer_group="demo-consumers",
        block_ms=1000,  # Short timeout for demo
        single_connection_client=True,  # Better for demos
    )
    bus = RedisEventBus(config=config, event_registry=registry)

    try:
        # Connect
        await bus.connect()
        print("Connected to Redis")

        # Create handlers
        received_events: list[DomainEvent] = []

        async def event_handler(event: DomainEvent) -> None:
            received_events.append(event)
            print(f"  Received: {event.event_type}")

        # Subscribe
        bus.subscribe(OrderCreated, event_handler)
        print("Subscribed handler to OrderCreated")

        # Publish events
        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            customer_id="cust-123",
            total=99.99,
            aggregate_version=1,
        )
        await bus.publish([event])
        print(f"Published OrderCreated for {order_id}")

        # Start consuming in background
        consume_task = asyncio.create_task(bus.start_consuming())

        # Wait for events to be processed
        await asyncio.sleep(2.0)

        # Stop consuming
        await bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        print(f"\nReceived {len(received_events)} event(s)")

        # Show statistics
        stats = bus.get_stats_dict()
        print(f"\nStatistics:")
        print(f"  Published: {stats['events_published']}")
        print(f"  Consumed: {stats['events_consumed']}")

    finally:
        await bus.disconnect()
        print("\nDisconnected from Redis")


async def demo_consumer_groups():
    """Demonstrate consumer groups for load balancing."""
    print()
    print("=" * 60)
    print("Demo 2: Consumer Groups")
    print("=" * 60)
    print()

    # Create event registry
    registry = EventRegistry()
    registry.register(OrderCreated)

    # Create base config
    base_config = {
        "redis_url": "redis://localhost:6379",
        "stream_prefix": "demo2",
        "consumer_group": "load-balanced-consumers",
        "block_ms": 500,
        "single_connection_client": True,
    }

    # Create two consumers in the same group
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
        # Connect both
        await bus1.connect()
        await bus2.connect()
        print("Both workers connected")

        # Subscribe handlers
        bus1.subscribe(OrderCreated, worker1_handler)
        bus2.subscribe(OrderCreated, worker2_handler)

        # Publish multiple events
        print("\nPublishing 6 events...")
        for i in range(6):
            event = OrderCreated(
                aggregate_id=uuid4(),
                customer_id=f"cust-{i}",
                total=10.0 * i,
                aggregate_version=1,
            )
            await bus1.publish([event])

        # Start both consumers
        task1 = asyncio.create_task(bus1.start_consuming())
        task2 = asyncio.create_task(bus2.start_consuming())

        # Wait for processing
        await asyncio.sleep(3.0)

        # Stop consumers
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
        print(f"  Total processed: {len(worker1_events) + len(worker2_events)} events")

    finally:
        await bus1.disconnect()
        await bus2.disconnect()


async def demo_statistics_and_monitoring():
    """Demonstrate statistics and monitoring capabilities."""
    print()
    print("=" * 60)
    print("Demo 3: Statistics and Monitoring")
    print("=" * 60)
    print()

    # Create event registry
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

        # Publish some events
        for i in range(10):
            event = OrderCreated(
                aggregate_id=uuid4(),
                customer_id=f"cust-{i}",
                total=100.0 + i,
                aggregate_version=1,
            )
            await bus.publish([event])

        print("Published 10 events")

        # Get stream info
        info = await bus.get_stream_info()

        print(f"\nStream Information:")
        print(f"  Connected: {info['connected']}")
        if info['connected']:
            print(f"  Stream name: {info['stream']['name']}")
            print(f"  Stream length: {info['stream']['length']}")
            print(f"  Consumer groups: {len(info['consumer_groups'])}")
            print(f"  Pending messages: {info['pending_messages']}")
            print(f"  DLQ messages: {info['dlq_messages']}")

        # Get statistics
        stats = bus.get_stats_dict()
        print(f"\nBus Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # Check subscriber counts
        print(f"\nSubscriber Counts:")
        print(f"  Total: {bus.get_subscriber_count()}")
        print(f"  Wildcard: {bus.get_wildcard_subscriber_count()}")

    finally:
        await bus.disconnect()


async def demo_with_projection():
    """Demonstrate integration with projections."""
    print()
    print("=" * 60)
    print("Demo 4: Integration with Projections")
    print("=" * 60)
    print()

    # Create event registry
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

    # Create projection and audit logger
    projection = OrderCountProjection()
    auditor = AuditLogger()

    try:
        await bus.connect()

        # Subscribe projection to its event types
        bus.subscribe_all(projection)
        print("Subscribed projection to OrderCreated and OrderShipped")

        # Subscribe auditor to all events
        bus.subscribe_to_all_events(auditor)
        print("Subscribed auditor to all events")

        # Simulate order workflow
        order_id = uuid4()
        print(f"\nProcessing order: {order_id}")

        # Publish order created
        await bus.publish([
            OrderCreated(
                aggregate_id=order_id,
                customer_id="premium-customer",
                total=499.99,
                aggregate_version=1,
            )
        ])

        # Publish order shipped
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
        print(f"  Orders created: {projection.created_count}")
        print(f"  Orders shipped: {projection.shipped_count}")

        print(f"\nAudit Log ({len(auditor.log)} entries):")
        for entry in auditor.log:
            print(f"  - {entry['event_type']}: {entry['aggregate_id'][:8]}...")

    finally:
        await bus.disconnect()


async def main():
    """Run all demos."""
    print()
    print("=" * 60)
    print("Tutorial 17: Using Redis Event Bus")
    print("=" * 60)
    print()

    try:
        await demo_basic_publish_subscribe()
        await demo_consumer_groups()
        await demo_statistics_and_monitoring()
        await demo_with_projection()

        print()
        print("=" * 60)
        print("Tutorial 17 Complete!")
        print("=" * 60)
        print()
        print("You've learned:")
        print("  - Connecting to Redis and configuring the event bus")
        print("  - Publishing and subscribing to events")
        print("  - Using consumer groups for load balancing")
        print("  - Monitoring statistics and stream information")
        print("  - Integrating with projections")
        print()

    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure Redis is running:")
        print("  docker run -d --name redis -p 6379:6379 redis:7-alpine")
        raise


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_17_redis.py` and run it:

```bash
# Start Redis if not running
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Run the tutorial
python tutorial_17_redis.py
```

---

## Exercises

### Exercise 1: Multi-Worker Setup

**Objective:** Set up multiple workers that share the event processing load.

**Time:** 20-25 minutes

**Requirements:**

1. Create two separate Python scripts (worker1.py and worker2.py)
2. Both workers should connect to the same Redis stream
3. Both workers should be in the same consumer group
4. Have a third script publish 100 events
5. Verify that events are distributed between workers

**Starter Code:**

```python
"""
Tutorial 17 - Exercise 1: Multi-Worker Setup

Create worker1.py, worker2.py, and publisher.py to demonstrate
load balancing across multiple workers.
"""

# publisher.py
import asyncio
from uuid import uuid4
from eventsource import DomainEvent, register_event, EventRegistry
from eventsource.bus import RedisEventBus, RedisEventBusConfig

@register_event
class TaskAssigned(DomainEvent):
    event_type: str = "TaskAssigned"
    aggregate_type: str = "Task"
    task_name: str

async def publish_tasks():
    registry = EventRegistry()
    registry.register(TaskAssigned)

    config = RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="exercise1",
    )
    bus = RedisEventBus(config=config, event_registry=registry)

    await bus.connect()

    # TODO: Publish 100 TaskAssigned events

    await bus.disconnect()

if __name__ == "__main__":
    asyncio.run(publish_tasks())
```

**Hints:**

- Use the same `stream_prefix` and `consumer_group` for all workers
- Give each worker a unique `consumer_name`
- Track how many events each worker processes

---

### Exercise 2: Implementing DLQ Monitoring

**Objective:** Create a monitoring script that checks the DLQ and alerts on issues.

**Time:** 20-25 minutes

**Requirements:**

1. Create a handler that intentionally fails for certain events
2. Publish events that will fail
3. Create a monitoring script that checks DLQ status
4. Alert when DLQ has more than 5 messages
5. Implement a "replay" command that moves messages back to the main stream

**Starter Code:**

```python
"""
Tutorial 17 - Exercise 2: DLQ Monitoring

Your task: Create a monitoring system for the dead letter queue.
"""
import asyncio
from eventsource.bus import RedisEventBus, RedisEventBusConfig

async def check_dlq_health(bus: RedisEventBus):
    """Check DLQ health and alert if needed."""
    # TODO: Get DLQ messages
    # TODO: Alert if count > 5
    # TODO: Return health status
    pass

async def replay_all_dlq_messages(bus: RedisEventBus):
    """Replay all messages from DLQ."""
    # TODO: Get all DLQ messages
    # TODO: Replay each one
    # TODO: Return count of replayed messages
    pass

async def main():
    config = RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="exercise2",
        enable_dlq=True,
        max_retries=2,
    )
    bus = RedisEventBus(config=config)
    await bus.connect()

    try:
        # TODO: Implement health check and replay logic
        pass
    finally:
        await bus.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

**Hints:**

- Use `bus.get_dlq_messages()` to retrieve DLQ messages
- Use `bus.replay_dlq_message(message_id)` to replay
- Use `bus.get_stream_info()` to get overall status

---

## Best Practices

### Connection Management

```python
# Always use try/finally for cleanup
bus = RedisEventBus(config=config)
try:
    await bus.connect()
    # ... use the bus
finally:
    await bus.disconnect()

# Or use graceful shutdown
async def shutdown_handler():
    await bus.shutdown(timeout=30.0)
```

### Consumer Naming

```python
# Use meaningful, unique names
config = RedisEventBusConfig(
    consumer_group="order-projections",
    consumer_name=f"worker-{os.getenv('POD_NAME', 'local')}-{os.getpid()}",
)
```

### Error Handling

```python
class ResilientHandler:
    async def handle(self, event: DomainEvent) -> None:
        try:
            await self._process(event)
        except TransientError:
            # Re-raise to trigger retry
            raise
        except PermanentError as e:
            # Log but don't retry
            logger.error(f"Permanent failure: {e}")
            # Optionally: send to custom DLQ with more context
```

### Monitoring

```python
# Set up periodic health checks
async def health_check_loop(bus: RedisEventBus):
    while True:
        info = await bus.get_stream_info()

        # Alert on high pending count
        if info.get('pending_messages', 0) > 1000:
            logger.warning(f"High pending count: {info['pending_messages']}")

        # Alert on DLQ growth
        if info.get('dlq_messages', 0) > 10:
            logger.warning(f"DLQ has {info['dlq_messages']} messages")

        await asyncio.sleep(60)
```

### Testing

```python
# Use single_connection_client for tests
config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix=f"test_{uuid4().hex[:8]}",  # Unique per test
    single_connection_client=True,  # Avoids event loop issues
)
```

---

## Summary

In this tutorial, you learned:

- **Redis Streams** provide durable, distributed event storage
- **RedisEventBusConfig** offers comprehensive configuration options
- **Consumer groups** enable load balancing across multiple workers
- **Dead letter queues** capture failed messages for later analysis
- **Pending message recovery** handles crashed consumers
- **Statistics and monitoring** help you understand system health
- **Best practices** ensure reliable production deployments

---

## Key Takeaways

!!! note "Remember"
    - Redis event bus enables horizontal scaling across processes and servers
    - Consumer groups allow multiple workers to share the event processing load
    - Each consumer in a group receives different events (load balancing)
    - Different consumer groups receive all events independently (isolation)

!!! tip "Best Practice"
    Use meaningful consumer names that include hostname and process ID. This makes debugging much easier when investigating pending messages or consumer group status.

!!! warning "Common Mistake"
    Do not forget to call `bus.connect()` before using the bus. Unlike `InMemoryEventBus`, `RedisEventBus` requires explicit connection setup.

---

## Next Steps

You now understand how to use Redis for distributed event distribution. In the next tutorial, you will learn about Apache Kafka for high-throughput event streaming.

Continue to [Tutorial 18: Using Kafka Event Bus](18-kafka.md) to learn about Kafka integration for even higher throughput scenarios.

---

## Related Documentation

- [Event Bus API Reference](../api/bus.md) - Complete API documentation
- [Tutorial 7: Event Bus](07-event-bus.md) - Event bus fundamentals
- [Tutorial 15: Production](15-production.md) - Production deployment patterns
- [Tutorial 18: Kafka Event Bus](18-kafka.md) - High-throughput streaming
- [Tutorial 19: RabbitMQ Event Bus](19-rabbitmq.md) - AMQP-based messaging
