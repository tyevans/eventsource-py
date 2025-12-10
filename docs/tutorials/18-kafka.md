# Tutorial 18: Using Kafka Event Bus

**Estimated Time:** 60-75 minutes
**Difficulty:** Advanced
**Progress:** Tutorial 18 of 21 | Phase 4: Advanced Patterns

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 7: Distributing Events with Event Bus](07-event-bus.md)
- Completed [Tutorial 17: Using Redis Event Bus](17-redis.md)
- Python 3.11+ installed
- Apache Kafka running locally (Docker recommended)
- Basic understanding of Kafka concepts (topics, partitions, consumer groups)

This tutorial builds on the event bus concepts from Tutorial 7 and extends them to high-throughput distributed scenarios using Apache Kafka.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Configure `KafkaEventBus` for high-throughput event distribution
2. Understand partition-based event ordering and its implications
3. Use consumer groups for horizontal scaling of event processors
4. Handle consumer group rebalancing gracefully
5. Monitor Kafka event bus operations with OpenTelemetry metrics
6. Implement reliable at-least-once delivery patterns

---

## When to Use Kafka Event Bus

Apache Kafka excels at high-throughput, durable event streaming. Consider Kafka when:

- **High Volume**: Processing thousands to millions of events per second
- **Long Retention**: Need to retain events for days, weeks, or longer
- **Multiple Consumers**: Many independent consumer groups reading the same events
- **Strong Ordering**: Need per-aggregate ordering guarantees at scale
- **Enterprise Integration**: Connecting with other Kafka-based systems

### Kafka vs Redis vs RabbitMQ

| Feature | Kafka | Redis | RabbitMQ |
|---------|-------|-------|----------|
| **Throughput** | Very High (10,000+ events/sec) | High (1,000+ events/sec) | Moderate (hundreds/sec) |
| **Ordering** | Per-partition | Per-stream | Per-queue |
| **Retention** | Configurable (days/weeks) | Limited | Limited |
| **Durability** | Excellent (replication) | Good (AOF) | Good (persistence) |
| **Scaling** | Partition-based | Stream-based | Queue-based |
| **Complexity** | High | Low | Medium |
| **Best For** | Enterprise streaming | Moderate scale, simple setup | Complex routing, RPC |

### Decision Criteria

Use **Kafka** when you need:

- The highest throughput and lowest latency
- Strong durability and replay capabilities
- Integration with a Kafka ecosystem
- Per-key ordering at massive scale

Use **Redis** when you need:

- Simpler setup and operations
- Good performance without Kafka complexity
- Existing Redis infrastructure

Use **RabbitMQ** when you need:

- Complex routing patterns (topic exchanges)
- Request-reply patterns
- Message priority queues

---

## Installation

Install eventsource-py with Kafka support:

```bash
pip install eventsource-py[kafka]
```

This installs `aiokafka`, an async Kafka client for Python.

### Verifying Installation

Check that Kafka support is available:

```python
from eventsource.bus.kafka import KAFKA_AVAILABLE, KafkaNotAvailableError

if KAFKA_AVAILABLE:
    print("Kafka support is available!")
else:
    print("Kafka not available. Install with: pip install eventsource-py[kafka]")
```

---

## Kafka Setup with Docker

The easiest way to run Kafka locally is with Docker Compose.

### Docker Compose File

Create a file named `docker-compose.kafka.yml`:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 30s
      timeout: 10s
      retries: 5

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
```

### Starting Kafka

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Wait for Kafka to be ready:

```bash
docker compose -f docker-compose.kafka.yml logs kafka | grep "started"
```

You can access the Kafka UI at http://localhost:8080 to monitor topics and messages.

---

## Configuring Kafka Event Bus

The `KafkaEventBusConfig` class provides all settings for connecting to Kafka and tuning producer/consumer behavior.

### Basic Configuration

```python
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    # Connection settings
    bootstrap_servers="localhost:9092",  # Kafka broker addresses
    topic_prefix="myapp.events",         # Prefix for topic names
    consumer_group="projections",        # Consumer group ID
)
```

The configuration creates:

- **Main topic**: `{topic_prefix}.stream` (e.g., `myapp.events.stream`)
- **DLQ topic**: `{topic_prefix}.stream.dlq` (e.g., `myapp.events.stream.dlq`)

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `bootstrap_servers` | `str` | `"localhost:9092"` | Kafka broker addresses (comma-separated) |
| `topic_prefix` | `str` | `"events"` | Prefix for topic names |
| `consumer_group` | `str` | `"default"` | Consumer group ID |
| `consumer_name` | `str` | Auto-generated | Unique consumer identifier |
| `acks` | `str` | `"all"` | Producer acknowledgment level |
| `compression_type` | `str` | `"gzip"` | Message compression |
| `batch_size` | `int` | `16384` | Max batch size in bytes |
| `linger_ms` | `int` | `5` | Time to wait for batching |
| `auto_offset_reset` | `str` | `"earliest"` | Where to start consuming |
| `max_retries` | `int` | `3` | Retries before DLQ |
| `enable_dlq` | `bool` | `True` | Enable dead letter queue |
| `enable_metrics` | `bool` | `True` | OpenTelemetry metrics |
| `enable_tracing` | `bool` | `True` | OpenTelemetry tracing |

### Producer Configuration

Tune the producer for your throughput needs:

```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="highvolume",

    # Acknowledgment level
    # "0" = fire-and-forget (fastest, least reliable)
    # "1" = leader only (balanced)
    # "all" = all replicas (slowest, most reliable)
    acks="all",

    # Batching for throughput
    batch_size=32768,     # 32KB batches
    linger_ms=10,         # Wait 10ms for more messages

    # Compression
    compression_type="gzip",  # Options: None, "gzip", "snappy", "lz4", "zstd"
)
```

### Consumer Configuration

Tune the consumer for your processing patterns:

```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="order-processors",

    # Where to start when no offset exists
    auto_offset_reset="earliest",  # or "latest"

    # Session management
    session_timeout_ms=30000,      # 30s session timeout
    heartbeat_interval_ms=10000,   # 10s heartbeat
    max_poll_interval_ms=300000,   # 5 min max between polls
)
```

### Security Configuration

For production deployments, configure TLS and authentication:

```python
# SASL/SCRAM with TLS (recommended for production)
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    topic_prefix="production",
    consumer_group="prod-projections",

    # Security
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_username="myapp",
    sasl_password="secret",
    ssl_cafile="/etc/kafka/ssl/ca.crt",
)
```

Available security protocols:

- `PLAINTEXT`: No security (development only)
- `SSL`: TLS encryption
- `SASL_PLAINTEXT`: SASL auth without encryption
- `SASL_SSL`: SASL auth with TLS (production recommended)

---

## Creating and Connecting KafkaEventBus

### Basic Setup

```python
import asyncio
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_id: str
    total: float

async def main():
    # Create configuration
    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="orders",
        consumer_group="order-handlers",
    )

    # Create bus with event registry
    bus = KafkaEventBus(config=config, event_registry=default_registry)

    # Connect to Kafka
    await bus.connect()
    print(f"Connected: {bus.is_connected}")

    # ... use the bus ...

    # Disconnect when done
    await bus.disconnect()

asyncio.run(main())
```

### Using Context Manager

For automatic connection management, use the async context manager:

```python
async def main():
    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="orders",
        consumer_group="handlers",
    )

    async with KafkaEventBus(config=config, event_registry=default_registry) as bus:
        # Bus is connected here
        await bus.publish([event])
        # Bus is automatically disconnected on exit
```

### Connection Properties

Check the connection state:

```python
bus = KafkaEventBus(config=config, event_registry=default_registry)

print(f"Connected: {bus.is_connected}")    # False before connect()
print(f"Consuming: {bus.is_consuming}")    # False before start_consuming()

await bus.connect()
print(f"Connected: {bus.is_connected}")    # True after connect()
```

---

## Publishing Events

Once connected, publish events using the `publish()` method.

### Basic Publishing

```python
from uuid import uuid4

# Create an event
event = OrderPlaced(
    aggregate_id=uuid4(),
    customer_id="CUST-001",
    total=99.99,
    aggregate_version=1,
)

# Publish to Kafka
await bus.publish([event])
```

### Batch Publishing

For higher throughput, publish events in batches:

```python
# Create multiple events
events = [
    OrderPlaced(
        aggregate_id=uuid4(),
        customer_id=f"CUST-{i:04d}",
        total=float(i * 10),
        aggregate_version=1,
    )
    for i in range(100)
]

# Publish as a batch - more efficient than individual publishes
await bus.publish(events)
```

### Background Publishing

For fire-and-forget scenarios:

```python
# Returns immediately, doesn't wait for broker acknowledgment
await bus.publish([event], background=True)
```

**Warning**: Background publishing is faster but provides weaker delivery guarantees. Use with caution and only when you can tolerate some message loss.

### Partition Routing

Events are automatically routed to partitions based on `aggregate_id`. This ensures:

- Events for the same aggregate go to the same partition
- Events within an aggregate are processed in order
- Different aggregates can be processed in parallel

```
Order-1 events -> Partition 0 (ordered within partition)
Order-2 events -> Partition 2 (ordered within partition)
Order-3 events -> Partition 0 (ordered within partition)

Events across different partitions may be processed out of order,
but events within a partition (same aggregate) are always ordered.
```

---

## Subscribing Handlers

Subscribe handlers to receive events when consuming.

### Function Handlers

```python
async def handle_order_placed(event: DomainEvent) -> None:
    if isinstance(event, OrderPlaced):
        print(f"Processing order {event.aggregate_id}: ${event.total}")

# Subscribe to specific event type
bus.subscribe(OrderPlaced, handle_order_placed)
```

### Class-Based Handlers

```python
class OrderNotificationHandler:
    def __init__(self):
        self.notifications_sent = 0

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            await self.send_confirmation_email(event)
            self.notifications_sent += 1

    async def send_confirmation_email(self, event: OrderPlaced):
        print(f"Sending email for order {event.aggregate_id}")

handler = OrderNotificationHandler()
bus.subscribe(OrderPlaced, handler)
```

### Projection Handlers

Use `subscribe_all()` for projections that handle multiple event types:

```python
from eventsource import DeclarativeProjection, handles

class OrderProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.orders: dict = {}

    @handles(OrderPlaced)
    async def _on_placed(self, event: OrderPlaced) -> None:
        self.orders[str(event.aggregate_id)] = {
            "customer": event.customer_id,
            "total": event.total,
            "status": "placed",
        }

    @handles(OrderShipped)
    async def _on_shipped(self, event: OrderShipped) -> None:
        order_id = str(event.aggregate_id)
        if order_id in self.orders:
            self.orders[order_id]["status"] = "shipped"

    async def _truncate_read_models(self) -> None:
        self.orders.clear()

projection = OrderProjection()
bus.subscribe_all(projection)
```

### Wildcard Handlers

For cross-cutting concerns (audit logging, metrics):

```python
async def audit_all_events(event: DomainEvent) -> None:
    print(f"[AUDIT] {event.event_type} at {event.occurred_at}")

bus.subscribe_to_all_events(audit_all_events)
```

---

## Consuming Events

### Starting the Consumer

Start consuming events after subscribing handlers:

```python
# Connect and subscribe
await bus.connect()
bus.subscribe(OrderPlaced, handle_order)

# Start consuming (blocks until stopped)
await bus.start_consuming()
```

### Background Consumption

For non-blocking consumption:

```python
# Start consuming in a background task
task = bus.start_consuming_in_background()

# ... do other work ...

# Later, stop consuming
await bus.stop_consuming()
await task  # Wait for task to complete
```

### Consuming with Timeout

For controlled consumption periods:

```python
async def consume_for_duration(bus: KafkaEventBus, seconds: float):
    """Consume events for a specified duration."""
    task = bus.start_consuming_in_background()

    await asyncio.sleep(seconds)

    await bus.stop_consuming()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
```

### Auto-Reconnection

The consumer automatically reconnects on errors with exponential backoff:

```python
# Auto-reconnect is enabled by default
await bus.start_consuming(auto_reconnect=True)

# Disable auto-reconnect if you want to handle errors yourself
await bus.start_consuming(auto_reconnect=False)
```

---

## Consumer Groups

Consumer groups enable horizontal scaling by distributing partitions among multiple consumers.

### How Consumer Groups Work

```
Topic: orders.stream (4 partitions)

Consumer Group: "order-processors"
  - Worker 1: handles Partition 0, 1
  - Worker 2: handles Partition 2, 3

Each message is delivered to exactly ONE consumer in the group.
```

### Setting Up Multiple Workers

```python
# Worker 1 configuration
config_1 = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="order-processors",  # Same group
    consumer_name="worker-1",           # Unique name
)

# Worker 2 configuration
config_2 = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="order-processors",  # Same group
    consumer_name="worker-2",           # Unique name
)

# Each worker creates its own bus instance
bus_1 = KafkaEventBus(config=config_1, event_registry=default_registry)
bus_2 = KafkaEventBus(config=config_2, event_registry=default_registry)
```

### Multiple Consumer Groups

Different consumer groups receive all events independently:

```python
# Projections group - builds read models
projection_config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="order-projections",  # Independent group
)

# Notifications group - sends emails
notification_config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="order-notifications",  # Independent group
)

# Both groups receive ALL events independently
```

### Partition Assignment

Kafka distributes partitions across consumers in a group:

- More consumers than partitions: Some consumers sit idle
- More partitions than consumers: Some consumers handle multiple partitions
- Equal numbers: One partition per consumer

**Tip**: Size your partition count based on expected parallelism. You can increase partitions later, but not decrease them.

---

## Handling Rebalances

When consumers join or leave a group, Kafka rebalances partition assignments. The `KafkaEventBus` handles this automatically, but understanding rebalances helps you design robust handlers.

### What Happens During Rebalance

1. Kafka pauses message consumption
2. Current offsets are committed for revoked partitions
3. Partitions are reassigned across consumers
4. Consumption resumes with new assignments

### Designing for Rebalances

Make your handlers idempotent since rebalances can cause duplicate processing:

```python
class IdempotentOrderHandler:
    def __init__(self):
        self.processed_events: set[str] = set()

    async def handle(self, event: DomainEvent) -> None:
        event_id = str(event.event_id)

        # Check if already processed
        if event_id in self.processed_events:
            print(f"Skipping duplicate: {event_id}")
            return

        # Process the event
        await self.process_order(event)

        # Mark as processed
        self.processed_events.add(event_id)
```

### Monitoring Rebalances

Check rebalance statistics:

```python
stats = bus.get_stats_dict()
print(f"Rebalances: {stats['rebalance_count']}")
```

Frequent rebalances may indicate:

- Unstable consumers (crashing/restarting)
- Long processing times exceeding `max_poll_interval_ms`
- Network issues between consumers and brokers

---

## Dead Letter Queue

Failed messages are automatically sent to the DLQ after max retries.

### DLQ Configuration

```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="handlers",

    # Error handling
    max_retries=3,        # Retry 3 times before DLQ
    enable_dlq=True,      # Enable dead letter queue

    # Retry timing
    retry_base_delay=1.0,   # Start with 1 second delay
    retry_max_delay=60.0,   # Cap at 60 seconds
    retry_jitter=0.1,       # 10% randomization
)
```

### DLQ Flow

```
Message Processing Flow:
1. Message received
2. Handler fails -> Retry 1 (1s delay)
3. Handler fails -> Retry 2 (2s delay)
4. Handler fails -> Retry 3 (4s delay)
5. Handler fails -> Send to DLQ
```

### Inspecting DLQ Messages

```python
# Get messages from DLQ
dlq_messages = await bus.get_dlq_messages(limit=100)

for msg in dlq_messages:
    print(f"Event ID: {msg['headers']['event_id']}")
    print(f"Event Type: {msg['headers']['event_type']}")
    print(f"Error: {msg['headers']['dlq_error_message']}")
    print(f"Retry Count: {msg['headers']['dlq_retry_count']}")
    print(f"Original Topic: {msg['headers']['dlq_original_topic']}")
    print("---")

# Get DLQ message count
count = await bus.get_dlq_message_count()
print(f"Total DLQ messages: {count}")
```

### Replaying DLQ Messages

```python
# Replay a specific message back to the main topic
success = await bus.replay_dlq_message(partition=0, offset=42)

if success:
    print("Message republished to main topic")
else:
    print("Replay failed")
```

---

## Monitoring with OpenTelemetry Metrics

The Kafka event bus emits OpenTelemetry metrics for comprehensive monitoring.

### Available Metrics

**Counter Metrics:**

| Metric | Description |
|--------|-------------|
| `kafka.eventbus.messages.published` | Total messages published |
| `kafka.eventbus.messages.consumed` | Total messages consumed |
| `kafka.eventbus.handler.invocations` | Handler invocation count |
| `kafka.eventbus.handler.errors` | Handler error count |
| `kafka.eventbus.messages.dlq` | Messages sent to DLQ |
| `kafka.eventbus.publish.errors` | Publish error count |
| `kafka.eventbus.reconnections` | Reconnection attempts |
| `kafka.eventbus.rebalances` | Consumer rebalances |

**Histogram Metrics:**

| Metric | Description |
|--------|-------------|
| `kafka.eventbus.publish.duration` | Publish latency (ms) |
| `kafka.eventbus.consume.duration` | Message processing latency (ms) |
| `kafka.eventbus.handler.duration` | Handler execution time (ms) |
| `kafka.eventbus.batch.size` | Publish batch sizes |

**Gauge Metrics:**

| Metric | Description |
|--------|-------------|
| `kafka.eventbus.connections.active` | Connection status (1=connected, 0=disconnected) |
| `kafka.eventbus.consumer.lag` | Messages behind per partition |

### Setting Up Metrics Export

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

# Start Prometheus metrics endpoint
start_http_server(port=8000, addr="0.0.0.0")

# Configure OpenTelemetry
reader = PrometheusMetricReader()
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

# Create bus with metrics enabled (default)
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="projections",
    enable_metrics=True,  # Default
)

bus = KafkaEventBus(config=config, event_registry=default_registry)
```

### Internal Statistics

Even without OpenTelemetry, you can access internal statistics:

```python
stats = bus.get_stats_dict()

print(f"Events published: {stats['events_published']}")
print(f"Events consumed: {stats['events_consumed']}")
print(f"Successfully processed: {stats['events_processed_success']}")
print(f"Failed processing: {stats['events_processed_failed']}")
print(f"Sent to DLQ: {stats['messages_sent_to_dlq']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Reconnections: {stats['reconnections']}")
print(f"Rebalances: {stats['rebalance_count']}")
print(f"Connected since: {stats['connected_at']}")
```

---

## At-Least-Once Delivery

Kafka provides **at-least-once** delivery guarantees. Events may be processed multiple times in failure scenarios.

### Why Duplicates Happen

- Consumer crashes after processing but before committing offset
- Rebalance occurs during processing
- Network issues cause commit failures

### Designing Idempotent Handlers

```python
class IdempotentProjection:
    """
    Projection that handles duplicate events safely.
    """
    def __init__(self, db):
        self.db = db

    async def handle(self, event: DomainEvent) -> None:
        # Use event_id as idempotency key
        event_id = str(event.event_id)

        # Check if already processed using database
        if await self.db.event_processed(event_id):
            return  # Skip duplicate

        # Process in transaction with idempotency record
        async with self.db.transaction():
            await self.apply_event(event)
            await self.db.mark_event_processed(event_id)
```

### Idempotency Patterns

1. **Deduplication by Event ID**: Store processed event IDs
2. **Natural Idempotency**: Design operations to be naturally idempotent (e.g., `SET` instead of `INCREMENT`)
3. **Version Checks**: Only apply if aggregate version matches expected

---

## Complete Example

Here is a comprehensive example demonstrating Kafka event bus usage:

```python
"""
Tutorial 18: Using Kafka Event Bus

This example demonstrates high-throughput event distribution with Kafka.
Run with: python tutorial_18_kafka.py

Prerequisites:
- Kafka running on localhost:9092
- pip install eventsource-py[kafka]
"""
import asyncio
import time
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry

# Check Kafka availability
from eventsource.bus.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
)

if not KAFKA_AVAILABLE:
    print("Kafka not available. Install with: pip install eventsource-py[kafka]")
    exit(1)


# =============================================================================
# Events
# =============================================================================

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_id: str
    total: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


# =============================================================================
# Event Handlers
# =============================================================================

class OrderProjection:
    """Builds a read model of orders."""

    def __init__(self):
        self.orders: dict[str, dict] = {}

    async def handle(self, event: DomainEvent) -> None:
        order_id = str(event.aggregate_id)

        if isinstance(event, OrderPlaced):
            self.orders[order_id] = {
                "customer": event.customer_id,
                "total": event.total,
                "status": "placed",
            }
        elif isinstance(event, OrderShipped):
            if order_id in self.orders:
                self.orders[order_id]["status"] = "shipped"
                self.orders[order_id]["tracking"] = event.tracking_number


class MetricsHandler:
    """Collects simple metrics."""

    def __init__(self):
        self.event_count = 0
        self.total_revenue = 0.0

    async def handle(self, event: DomainEvent) -> None:
        self.event_count += 1
        if isinstance(event, OrderPlaced):
            self.total_revenue += event.total


# =============================================================================
# Examples
# =============================================================================

async def basic_kafka_example():
    """Demonstrate basic Kafka event bus usage."""
    print("=" * 60)
    print("Basic Kafka Event Bus")
    print("=" * 60)
    print()

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="tutorial18",
        consumer_group="demo-handlers",
        enable_metrics=True,
        enable_tracing=True,
    )

    print(f"Bootstrap servers: {config.bootstrap_servers}")
    print(f"Topic: {config.topic_name}")
    print(f"Consumer group: {config.consumer_group}")

    bus = KafkaEventBus(config=config, event_registry=default_registry)

    # Create handlers
    projection = OrderProjection()
    metrics = MetricsHandler()

    # Subscribe handlers
    bus.subscribe(OrderPlaced, projection)
    bus.subscribe(OrderPlaced, metrics)
    bus.subscribe(OrderShipped, projection)

    try:
        print("\nConnecting to Kafka...")
        await bus.connect()
        print("Connected!")

        # Publish events
        print("\nPublishing events...")
        order_id = uuid4()

        await bus.publish([
            OrderPlaced(
                aggregate_id=order_id,
                customer_id="CUST-001",
                total=99.99,
                aggregate_version=1,
            ),
        ])
        print("  Published: OrderPlaced")

        await bus.publish([
            OrderShipped(
                aggregate_id=order_id,
                tracking_number="TRACK-12345",
                aggregate_version=2,
            ),
        ])
        print("  Published: OrderShipped")

        # Consume events
        print("\nConsuming events...")
        task = bus.start_consuming_in_background()
        await asyncio.sleep(3)
        await bus.stop_consuming()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Show results
        print(f"\nProjection orders: {len(projection.orders)}")
        if projection.orders:
            for oid, data in projection.orders.items():
                print(f"  {oid[:8]}...: {data}")

        print(f"\nMetrics:")
        print(f"  Events processed: {metrics.event_count}")
        print(f"  Total revenue: ${metrics.total_revenue:.2f}")

    finally:
        await bus.disconnect()
        print("\nDisconnected from Kafka")


async def high_throughput_example():
    """Demonstrate high-throughput publishing."""
    print()
    print("=" * 60)
    print("High Throughput Example")
    print("=" * 60)
    print()

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="throughput",
        consumer_group="throughput-test",
        batch_size=32768,
        linger_ms=10,
    )

    bus = KafkaEventBus(config=config, event_registry=default_registry)
    events_received = {"count": 0}

    async def count_handler(event: DomainEvent):
        events_received["count"] += 1

    bus.subscribe(OrderPlaced, count_handler)

    try:
        await bus.connect()

        num_events = 100
        print(f"Publishing {num_events} events...")

        start = time.perf_counter()

        events = [
            OrderPlaced(
                aggregate_id=uuid4(),
                customer_id=f"CUST-{i:04d}",
                total=float(i * 10),
                aggregate_version=1,
            )
            for i in range(num_events)
        ]

        # Publish in batches for efficiency
        batch_size = 50
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            await bus.publish(batch)

        publish_time = time.perf_counter() - start
        print(f"Published {num_events} events in {publish_time:.3f}s")
        print(f"Throughput: {num_events / publish_time:.0f} events/sec")

        # Consume events
        print("\nConsuming events...")
        task = bus.start_consuming_in_background()
        await asyncio.sleep(5)
        await bus.stop_consuming()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        print(f"Events received: {events_received['count']}")

    finally:
        await bus.disconnect()


async def consumer_groups_example():
    """Explain consumer groups."""
    print()
    print("=" * 60)
    print("Consumer Groups")
    print("=" * 60)
    print()

    print("Consumer groups enable horizontal scaling:")
    print()
    print("  Topic: orders.stream (4 partitions)")
    print()
    print("  Consumer Group: 'order-processors'")
    print("    Worker-1 -> Partition 0, 1")
    print("    Worker-2 -> Partition 2, 3")
    print()
    print("  Each message is delivered to exactly ONE consumer in the group.")
    print("  Different groups receive ALL messages independently.")
    print()
    print("Configuration:")
    print()
    print("  config = KafkaEventBusConfig(")
    print("      bootstrap_servers='localhost:9092',")
    print("      topic_prefix='orders',")
    print("      consumer_group='order-processors',  # Same group = share load")
    print("      consumer_name='worker-1',           # Unique per worker")
    print("  )")


async def statistics_example():
    """Show bus statistics."""
    print()
    print("=" * 60)
    print("Event Bus Statistics")
    print("=" * 60)
    print()

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="stats-demo",
        consumer_group="stats-handlers",
    )

    bus = KafkaEventBus(config=config, event_registry=default_registry)

    async def handler(event): pass
    bus.subscribe(OrderPlaced, handler)

    try:
        await bus.connect()

        # Publish some events
        for i in range(10):
            await bus.publish([
                OrderPlaced(
                    aggregate_id=uuid4(),
                    customer_id=f"CUST-{i}",
                    total=float(i * 10),
                    aggregate_version=1,
                )
            ])

        # Get statistics
        stats = bus.get_stats_dict()

        print("Statistics after publishing 10 events:")
        print()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # Get topic info
        info = await bus.get_topic_info()
        print()
        print("Topic information:")
        print(f"  Topic: {info['topic']}")
        print(f"  Partitions: {info['partitions']}")
        print(f"  Consumer group: {info['consumer_group']}")
        print(f"  Connected: {info['connected']}")

    finally:
        await bus.disconnect()


async def main():
    print("Tutorial 18: Kafka Event Bus")
    print()
    print("=" * 60)

    try:
        await basic_kafka_example()
        await consumer_groups_example()
        await statistics_example()
        # Uncomment to test throughput:
        # await high_throughput_example()
    except Exception as e:
        print(f"\nError: {e}")
        print("Make sure Kafka is running. See docker-compose.kafka.yml")

    print()
    print("=" * 60)
    print("Tutorial complete!")


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_18_kafka.py` and run it:

```bash
python tutorial_18_kafka.py
```

---

## Exercises

Now it is time to practice what you have learned!

### Exercise 1: High-Throughput Event Pipeline

**Objective:** Build a high-throughput event pipeline with Kafka.

**Time:** 25-30 minutes

**Requirements:**

1. Configure `KafkaEventBus` with metrics enabled
2. Create a `SensorReading` event with temperature and humidity fields
3. Create a `MetricsAggregator` handler that tracks average temperature
4. Publish 1000 sensor reading events
5. Consume events and display throughput metrics

**Starter Code:**

```python
"""
Tutorial 18 - Exercise 1: High-Throughput Event Pipeline

Your task: Build a sensor data pipeline with throughput monitoring.
"""
import asyncio
import time
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig, KAFKA_AVAILABLE

if not KAFKA_AVAILABLE:
    print("Install kafka: pip install eventsource-py[kafka]")
    exit(1)


# Step 1: Define a SensorReading event
# TODO: Create a SensorReading event with temperature and humidity fields


# Step 2: Create MetricsAggregator handler
# TODO: Track count and average temperature


async def main():
    print("=== Exercise 18-1: High-Throughput Pipeline ===\n")

    # Step 3: Configure Kafka bus with metrics enabled
    # TODO: Create KafkaEventBusConfig

    # Step 4: Create bus and subscribe handler
    # TODO: Create KafkaEventBus and subscribe MetricsAggregator

    # Step 5: Publish 1000 events
    # TODO: Generate and publish events, measure time

    # Step 6: Consume and display results
    # TODO: Start consuming, wait, then display metrics


if __name__ == "__main__":
    asyncio.run(main())
```

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 18 - Exercise 1 Solution: High-Throughput Event Pipeline
"""
import asyncio
import time
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig, KAFKA_AVAILABLE

if not KAFKA_AVAILABLE:
    print("Install kafka: pip install eventsource-py[kafka]")
    exit(1)


# Step 1: Define a SensorReading event
@register_event
class SensorReading(DomainEvent):
    event_type: str = "SensorReading"
    aggregate_type: str = "Sensor"
    temperature: float
    humidity: float


# Step 2: Create MetricsAggregator handler
class MetricsAggregator:
    def __init__(self):
        self.count = 0
        self.total_temp = 0.0

    async def handle(self, event: DomainEvent):
        if isinstance(event, SensorReading):
            self.count += 1
            self.total_temp += event.temperature

    @property
    def avg_temperature(self) -> float:
        return self.total_temp / max(self.count, 1)


async def main():
    print("=== Exercise 18-1: High-Throughput Pipeline ===\n")

    # Step 3: Configure Kafka bus with metrics enabled
    print("Step 1: Configuring Kafka bus with metrics...")
    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="exercise18",
        consumer_group="sensor-aggregator",
        batch_size=32768,
        linger_ms=10,
        enable_metrics=True,
    )
    bus = KafkaEventBus(config=config, event_registry=default_registry)

    # Step 4: Create bus and subscribe handler
    aggregator = MetricsAggregator()
    bus.subscribe(SensorReading, aggregator.handle)
    print("   Metrics enabled, handler subscribed!")

    await bus.connect()

    # Step 5: Publish 1000 events
    print("\nStep 2: Publishing 1000 events...")
    start = time.perf_counter()

    events = [
        SensorReading(
            aggregate_id=uuid4(),
            temperature=20.0 + (i % 10),
            humidity=50.0 + (i % 20),
            aggregate_version=1,
        )
        for i in range(1000)
    ]

    # Publish in batches
    for i in range(0, 1000, 100):
        await bus.publish(events[i:i+100])

    publish_time = time.perf_counter() - start
    print(f"   Published in {publish_time:.3f}s ({1000/publish_time:.0f} events/sec)")

    # Step 6: Consume and display results
    print("\nStep 3: Consuming events...")
    task = bus.start_consuming_in_background()
    await asyncio.sleep(5)
    await bus.stop_consuming()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    print("\nStep 4: Throughput metrics:")
    print(f"   Events received: {aggregator.count}")
    print(f"   Avg temperature: {aggregator.avg_temperature:.1f}C")

    stats = bus.get_stats_dict()
    print(f"   Events published: {stats['events_published']}")
    print(f"   Events consumed: {stats['events_consumed']}")

    await bus.disconnect()
    print("\n=== Exercise Complete! ===")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

The solution file is available at: `docs/tutorials/exercises/solutions/18-1.py`

---

## Summary

In this tutorial, you learned:

- **Kafka provides the highest throughput** for event distribution among the supported backends
- **Partition-based routing** ensures per-aggregate ordering using `aggregate_id` as the partition key
- **Consumer groups** enable horizontal scaling by distributing partitions across workers
- **Rebalancing** happens automatically when consumers join or leave, requiring idempotent handlers
- **Built-in metrics** via OpenTelemetry simplify monitoring throughput, latency, and errors
- **Dead letter queues** capture failed messages for later investigation and replay
- **At-least-once delivery** means handlers must be designed to handle duplicate events

---

## Key Takeaways

!!! note "Remember"
    - Always `connect()` before publishing or consuming
    - Make handlers idempotent to handle at-least-once delivery
    - Size partitions based on expected parallelism
    - Use consumer groups for load distribution, not isolation
    - Monitor consumer lag to detect processing bottlenecks

!!! tip "Best Practice"
    For production deployments:
    - Pre-create topics with appropriate partition counts
    - Use `acks="all"` for durability
    - Enable metrics and set up alerting
    - Design handlers to be idempotent from the start

!!! warning "Common Mistakes"
    - Creating too few partitions (limits scaling later)
    - Not handling duplicate events (at-least-once delivery)
    - Ignoring rebalance implications in long-running handlers
    - Not monitoring consumer lag

---

## Next Steps

You now understand how to use Kafka for high-throughput event distribution. In the next tutorial, you will learn about RabbitMQ for different messaging patterns.

Continue to [Tutorial 19: Using RabbitMQ Event Bus](19-rabbitmq.md) to learn about traditional message queue patterns.

---

## Related Documentation

- [Kafka Event Bus Guide](../guides/kafka-event-bus.md) - Comprehensive Kafka reference
- [Kafka Metrics Guide](../guides/kafka-metrics.md) - OpenTelemetry metrics and alerting
- [Event Bus API Reference](../api/bus.md) - Complete API documentation
- [Tutorial 7: Event Bus](07-event-bus.md) - Event bus basics
- [Tutorial 17: Redis Event Bus](17-redis.md) - Alternative distributed bus
