# Tutorial 18: Kafka Event Bus - High-Throughput Event Streaming

**Difficulty:** Advanced

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 7: Event Bus - Distributing Events Across Your System](07-event-bus.md)
- [Tutorial 17: Redis Event Bus - Distributed Event Distribution](17-redis.md)
- [Tutorial 11: PostgreSQL Event Store](11-postgresql.md)
- [Tutorial 13: Subscription Management](13-subscriptions.md)
- Python 3.10 or higher
- Understanding of async/await
- Basic Apache Kafka knowledge helpful but not required
- Docker for running Kafka locally

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain why Apache Kafka is ideal for high-throughput event streaming
2. Install and configure Kafka for event distribution
3. Set up KafkaEventBus with proper configuration
4. Understand partitions and how they provide ordering guarantees
5. Implement consumer groups for horizontal scaling
6. Use dead letter queues for failed messages
7. Configure retry policies with exponential backoff
8. Monitor Kafka event bus with OpenTelemetry metrics
9. Deploy multiple consumers for load balancing
10. Tune Kafka for production workloads

---

## What is Kafka Event Bus?

The **KafkaEventBus** is a distributed event bus implementation that uses **Apache Kafka** for ultra-high-throughput event distribution across multiple processes, services, and data centers. While `RedisEventBus` (from Tutorial 17) handles moderate-scale event distribution, KafkaEventBus is designed for enterprise-scale event streaming with:

- **Extreme throughput**: 100,000+ events/sec on commodity hardware
- **Horizontal scalability**: Add brokers and consumers dynamically
- **Partition-based ordering**: Events for the same aggregate maintain order
- **Durable persistence**: Events replicated across multiple brokers
- **Consumer groups**: Automatic load balancing across workers
- **Long-term retention**: Store events for days, weeks, or indefinitely
- **At-least-once delivery**: Events acknowledged only after successful processing
- **Fault tolerance**: Automatic failover and recovery

---

## Why Apache Kafka?

Kafka is a distributed streaming platform originally built at LinkedIn for high-throughput data pipelines. It combines features from traditional message queues with distributed log storage:

| Feature | Benefit |
|---------|---------|
| **Partitioned Topics** | Parallel processing while maintaining order per partition |
| **Consumer Groups** | Multiple consumers share workload automatically |
| **Replication** | Data replicated across brokers for fault tolerance |
| **Persistence** | Events stored on disk, survive broker restarts |
| **Batching** | Efficient batch processing reduces network overhead |
| **Zero-Copy I/O** | Optimized data transfer for maximum throughput |
| **Backpressure Handling** | Consumers control processing rate |
| **Long Retention** | Events available for replay hours/days/weeks later |

### Comparing Event Bus Implementations

| Implementation | Throughput | Latency | Durability | Complexity | Best For |
|----------------|------------|---------|------------|------------|----------|
| **InMemoryEventBus** | Low | Microseconds | None | Simple | Single instance, testing |
| **RedisEventBus** | Medium (10k/s) | Milliseconds | Good | Medium | Multi-instance, moderate scale |
| **KafkaEventBus** | Very High (100k+/s) | Milliseconds | Excellent | High | Enterprise streaming, analytics |
| **RabbitMQEventBus** | Medium (10k/s) | Milliseconds | Good | Medium | Complex routing, RPC patterns |

### When to Use Kafka Event Bus

**Good for:**
- High-throughput event processing (10,000+ events/sec)
- Enterprise microservices architectures
- Event streaming and data pipelines
- Long-term event retention for analytics
- Multi-datacenter deployments
- Systems requiring strong durability guarantees
- Event replay from historical data

**Less suitable for:**
- Single-instance applications (use InMemoryEventBus)
- Low-volume applications (<100 events/sec - Redis may be simpler)
- Request/reply patterns (use RabbitMQ)
- When operational complexity is a concern

---

## Installation

Install Kafka support for eventsource-py:

```bash
pip install eventsource-py[kafka]
```

This installs the `aiokafka` package with async support for Apache Kafka.

### Verify Installation

```python
from eventsource.bus import KAFKA_AVAILABLE

if KAFKA_AVAILABLE:
    print("Kafka support is available!")
else:
    print("Kafka not available. Install with: pip install eventsource-py[kafka]")
```

---

## Setting Up Kafka

### Using Docker Compose

Create `docker-compose.yml` for local development:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
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
      KAFKA_LOG_RETENTION_HOURS: 168  # 7 days

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
```

Start Kafka:

```bash
docker compose up -d

# Verify Kafka is ready
docker logs kafka | grep "started (kafka.server.KafkaServer)"
```

Access Kafka UI at http://localhost:8080 to view topics, messages, and consumer groups.

---

## Basic KafkaEventBus Setup

### Minimal Configuration

```python
from eventsource import EventRegistry, DomainEvent, register_event
from eventsource.bus import KafkaEventBus, KafkaEventBusConfig

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: str
    total_amount: float

# Create event registry
registry = EventRegistry()
registry.register(OrderPlaced)

# Configure Kafka event bus
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="order-processors",
)

# Create bus
bus = KafkaEventBus(config=config, event_registry=registry)

# Connect to Kafka
await bus.connect()
```

### Publishing Events

```python
from uuid import uuid4

# Subscribe handler
async def on_order_placed(event: OrderPlaced):
    print(f"Order placed: {event.aggregate_id} - ${event.total_amount}")

bus.subscribe(OrderPlaced, on_order_placed)

# Publish event
event = OrderPlaced(
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

## KafkaEventBusConfig Options

### Connection Settings

```python
config = KafkaEventBusConfig(
    # Kafka connection
    bootstrap_servers="localhost:9092",          # Comma-separated broker list

    # Topic naming
    topic_prefix="myapp.events",                 # Topic prefix (topic: myapp.events.stream)

    # Consumer settings
    consumer_group="projections",                # Consumer group name
    consumer_name=None,                          # Auto-generated: hostname-uuid
)
```

### Producer Settings

```python
config = KafkaEventBusConfig(
    # Acknowledgment level
    acks="all",                                  # "0" | "1" | "all" (most durable)

    # Compression
    compression_type="gzip",                     # None | "gzip" | "snappy" | "lz4" | "zstd"

    # Batching
    batch_size=16384,                            # Bytes (16KB default)
    linger_ms=0,                                 # Wait time for batching (ms)
)
```

**Acknowledgment Levels:**
- `acks="0"`: Fire-and-forget (fastest, no guarantees)
- `acks="1"`: Leader acknowledges (balanced)
- `acks="all"`: All in-sync replicas acknowledge (safest)

**Compression:**
- `None`: No compression (fastest, largest network usage)
- `"gzip"`: Good compression ratio (default)
- `"snappy"`: Fast compression
- `"lz4"`: Balanced compression and speed
- `"zstd"`: Best compression ratio

### Consumer Settings

```python
config = KafkaEventBusConfig(
    # Offset behavior
    auto_offset_reset="earliest",                # "earliest" | "latest"

    # Timeouts
    session_timeout_ms=30000,                    # 30 seconds
    heartbeat_interval_ms=3000,                  # 3 seconds
    max_poll_interval_ms=300000,                 # 5 minutes
)
```

**auto_offset_reset:**
- `"earliest"`: Start from beginning when no offset exists (default)
- `"latest"`: Start from end (skip historical events)

### Retry and DLQ Settings

```python
config = KafkaEventBusConfig(
    # Retry policy
    max_retries=3,                               # Retries before DLQ
    retry_base_delay=1.0,                        # Base delay (seconds)
    retry_max_delay=60.0,                        # Max delay (seconds)
    retry_jitter=0.1,                            # Jitter fraction (0.0-1.0)

    # Dead letter queue
    enable_dlq=True,                             # Enable DLQ
    dlq_topic_suffix=".dlq",                     # DLQ topic suffix
)
```

**Exponential Backoff Formula:**
```python
delay = min(retry_base_delay * (2 ** attempt), retry_max_delay)
delay *= (1 + random.uniform(-retry_jitter, retry_jitter))
```

### Security Settings

```python
# Development (no security - NOT for production)
config = KafkaEventBusConfig(
    security_protocol="PLAINTEXT",
)

# TLS encryption only
config = KafkaEventBusConfig(
    security_protocol="SSL",
    ssl_cafile="/path/to/ca.crt",
)

# SASL authentication with TLS (recommended for production)
config = KafkaEventBusConfig(
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",              # "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"
    sasl_username="myapp",
    sasl_password="secret",
    ssl_cafile="/path/to/ca.crt",
)

# Mutual TLS (mTLS) - client certificate authentication
config = KafkaEventBusConfig(
    security_protocol="SSL",
    ssl_cafile="/path/to/ca.crt",
    ssl_certfile="/path/to/client.crt",
    ssl_keyfile="/path/to/client.key",
    ssl_check_hostname=True,
)
```

### Observability Settings

```python
config = KafkaEventBusConfig(
    enable_tracing=True,                         # OpenTelemetry tracing
    enable_metrics=True,                         # OpenTelemetry metrics (default)
    shutdown_timeout=30.0,                       # Graceful shutdown timeout
)
```

### Topic Names

KafkaEventBus creates topics using the prefix:

```python
config = KafkaEventBusConfig(topic_prefix="orders")

# Main topic: orders.stream
# DLQ topic:  orders.stream.dlq
```

Access topic names programmatically:

```python
print(config.topic_name)      # orders.stream
print(config.dlq_topic_name)  # orders.stream.dlq
```

---

## Partitions and Ordering

Kafka topics are divided into **partitions** for parallel processing. KafkaEventBus uses the **aggregate_id** as the partition key to ensure all events for the same aggregate go to the same partition.

### How Partitioning Works

```python
# Order 1 events → Partition 0
OrderPlaced(aggregate_id="order-1", ...)    # Partition 0
OrderPaid(aggregate_id="order-1", ...)      # Partition 0
OrderShipped(aggregate_id="order-1", ...)   # Partition 0

# Order 2 events → Partition 1
OrderPlaced(aggregate_id="order-2", ...)    # Partition 1
OrderPaid(aggregate_id="order-2", ...)      # Partition 1

# Order 3 events → Partition 0 (same hash as order-1)
OrderPlaced(aggregate_id="order-3", ...)    # Partition 0
```

**Guarantees:**
- ✅ Events for same aggregate_id are ordered (same partition)
- ✅ Events in same partition processed in order
- ❌ No global ordering across partitions

### Viewing Partition Distribution

```python
# Get topic info (requires admin access)
from aiokafka import AIOKafkaAdminClient

admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
await admin.start()

metadata = await admin.describe_topics([config.topic_name])
topic_metadata = metadata[config.topic_name]

print(f"Topic: {config.topic_name}")
print(f"Partitions: {len(topic_metadata.partitions)}")

for partition in topic_metadata.partitions:
    print(f"  Partition {partition.partition_id}: Leader {partition.leader}")

await admin.close()
```

---

## Consumer Groups and Load Balancing

Consumer groups enable automatic load balancing: Kafka assigns partitions to consumers within the group.

### Single Consumer Group (Load Balancing)

```python
# Worker 1
config1 = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="order-processors",
    consumer_name="worker-1",
)
bus1 = KafkaEventBus(config=config1, event_registry=registry)
await bus1.connect()

# Worker 2 (same group)
config2 = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="order-processors",
    consumer_name="worker-2",
)
bus2 = KafkaEventBus(config=config2, event_registry=registry)
await bus2.connect()

# Subscribe both workers
bus1.subscribe(OrderPlaced, worker1_handler)
bus2.subscribe(OrderPlaced, worker2_handler)

# Start consuming
task1 = asyncio.create_task(bus1.start_consuming())
task2 = asyncio.create_task(bus2.start_consuming())

# Kafka automatically assigns partitions:
# - Worker 1: Partitions 0, 1
# - Worker 2: Partitions 2, 3
# Each partition assigned to exactly one consumer in the group
```

### Multiple Consumer Groups (Fan-Out)

Different consumer groups all receive the same events:

```python
# Projection consumer group
projection_config = KafkaEventBusConfig(
    topic_prefix="orders",
    consumer_group="projections",
)
projection_bus = KafkaEventBus(config=projection_config, event_registry=registry)

# Notification consumer group
notification_config = KafkaEventBusConfig(
    topic_prefix="orders",
    consumer_group="notifications",
)
notification_bus = KafkaEventBus(config=notification_config, event_registry=registry)

# Analytics consumer group
analytics_config = KafkaEventBusConfig(
    topic_prefix="orders",
    consumer_group="analytics",
)
analytics_bus = KafkaEventBus(config=analytics_config, event_registry=registry)

# Each group processes ALL events independently
projection_bus.subscribe(OrderPlaced, update_projection)
notification_bus.subscribe(OrderPlaced, send_notification)
analytics_bus.subscribe(OrderPlaced, track_metrics)
```

**Result:** Each consumer group processes every event from the topic.

---

## Consumer Rebalancing

When consumers join or leave a consumer group, Kafka **rebalances** partition assignments. KafkaEventBus handles rebalancing gracefully:

### Rebalance Process

1. **Consumer joins/leaves**: Triggers rebalance
2. **Partitions revoked**: Current consumer commits offsets
3. **Partitions reassigned**: New consumer receives partitions
4. **Processing continues**: No events lost

### Rebalance Listener

KafkaEventBus includes a built-in rebalance listener that:

- Commits offsets before partitions are revoked
- Records rebalance count in statistics
- Emits metrics for monitoring
- Logs partition assignments

```python
# Rebalancing is automatic - no code needed
# Check statistics to monitor rebalances
stats = bus.get_stats_dict()
print(f"Rebalances: {stats['rebalance_count']}")
```

### Designing for Rebalances

Make handlers **idempotent** to handle duplicate processing during rebalances:

```python
class IdempotentOrderHandler:
    """Handler with deduplication."""

    def __init__(self):
        self.processed_events: set[str] = set()

    async def handle(self, event: DomainEvent) -> None:
        event_id = str(event.event_id)

        # Check if already processed
        if event_id in self.processed_events:
            logger.info(f"Skipping duplicate event: {event_id}")
            return

        # Process event
        await self.process_order(event)

        # Mark as processed
        self.processed_events.add(event_id)
```

**Best practices:**
- Use event_id for deduplication
- Store processed IDs in database (persistent)
- Design operations to be naturally idempotent (SET vs INCREMENT)

---

## Dead Letter Queue (DLQ)

Messages that fail after `max_retries` are automatically sent to the DLQ topic for later analysis.

### DLQ Configuration

```python
config = KafkaEventBusConfig(
    enable_dlq=True,                             # Enable DLQ
    max_retries=3,                               # Retries before DLQ
    dlq_topic_suffix=".dlq",                     # DLQ topic name suffix
)
```

### Inspecting DLQ Messages

```python
# Get DLQ message count
count = await bus.get_dlq_message_count()
print(f"DLQ messages: {count}")

# Get DLQ messages
dlq_messages = await bus.get_dlq_messages(limit=100)

for msg in dlq_messages:
    print(f"Partition: {msg['partition']}")
    print(f"Offset: {msg['offset']}")
    print(f"Event Type: {msg['event_type']}")
    print(f"Error: {msg['error']}")
    print(f"Retry Count: {msg['retry_count']}")
    print("---")
```

### Replaying DLQ Messages

After fixing the underlying issue, replay messages from DLQ:

```python
# Replay specific message by partition and offset
success = await bus.replay_dlq_message(partition=0, offset=42)

if success:
    print("Message replayed successfully")
else:
    print("Message not found in DLQ")
```

Replayed messages are:
1. Read from DLQ topic
2. Published to main topic with new offset
3. Processed by consumers normally

### Monitoring DLQ

```python
# Set up alerts for DLQ
count = await bus.get_dlq_message_count()

if count > 0:
    logger.warning(f"WARNING: {count} messages in DLQ!")
    # Alert operations team
    # Investigate root cause
    # Replay after fixing issue
```

---

## Retry Policies

KafkaEventBus implements **exponential backoff with jitter** for retries:

### Default Retry Policy

```python
config = KafkaEventBusConfig(
    max_retries=3,                               # Max retry attempts
    retry_base_delay=1.0,                        # 1 second base delay
    retry_max_delay=60.0,                        # 60 second max delay
    retry_jitter=0.1,                            # 10% jitter
)
```

### Retry Timeline Example

```
Attempt 1 fails → Retry after ~1 second (1.0 * 2^0 ± 10%)
Attempt 2 fails → Retry after ~2 seconds (1.0 * 2^1 ± 10%)
Attempt 3 fails → Retry after ~4 seconds (1.0 * 2^2 ± 10%)
Attempt 4 fails → Sent to DLQ
```

### Custom Retry Policy

```python
# Aggressive retries with long delays
config = KafkaEventBusConfig(
    max_retries=5,                               # More retry attempts
    retry_base_delay=2.0,                        # Higher base delay
    retry_max_delay=120.0,                       # 2 minute max
    retry_jitter=0.2,                            # 20% jitter
)

# Timeline:
# Attempt 1 fails → ~2s
# Attempt 2 fails → ~4s
# Attempt 3 fails → ~8s
# Attempt 4 fails → ~16s
# Attempt 5 fails → ~32s
# Attempt 6 fails → DLQ
```

**Jitter prevents thundering herd:** Multiple failing consumers don't retry simultaneously.

---

## OpenTelemetry Metrics

When `enable_metrics=True` (default), KafkaEventBus emits comprehensive metrics for monitoring.

### Available Metrics

**Counters:**
- `kafka.eventbus.messages.published`: Total messages published
- `kafka.eventbus.messages.consumed`: Total messages consumed
- `kafka.eventbus.handler.invocations`: Handler invocation count
- `kafka.eventbus.handler.errors`: Handler error count
- `kafka.eventbus.messages.dlq`: Messages sent to DLQ
- `kafka.eventbus.connection.errors`: Connection errors
- `kafka.eventbus.reconnections`: Reconnection attempts
- `kafka.eventbus.rebalances`: Consumer group rebalances
- `kafka.eventbus.publish.errors`: Publish errors

**Histograms:**
- `kafka.eventbus.publish.duration`: Publish latency (ms)
- `kafka.eventbus.consume.duration`: Consume/process latency (ms)
- `kafka.eventbus.handler.duration`: Handler execution time (ms)
- `kafka.eventbus.batch.size`: Publish batch sizes

**Observable Gauges:**
- `kafka.eventbus.connections.active`: Connection status (1=connected, 0=disconnected)
- `kafka.eventbus.consumer.lag`: Messages behind per partition

### Setting Up Metrics Export

```python
# Prometheus exporter
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

# Start Prometheus HTTP server
start_http_server(port=8000)

# Configure OpenTelemetry
metrics.set_meter_provider(
    MeterProvider(metric_readers=[PrometheusMetricReader()])
)

# Create bus with metrics enabled (default)
config = KafkaEventBusConfig(
    enable_metrics=True,
)
bus = KafkaEventBus(config=config, event_registry=registry)
```

Access metrics at http://localhost:8000/metrics

### Example PromQL Queries

```promql
# Publish rate (events/sec)
rate(kafka_eventbus_messages_published_total[1m])

# Consumer lag (messages behind)
kafka_eventbus_consumer_lag

# Error rate
rate(kafka_eventbus_handler_errors_total[5m])

# Average handler latency (ms)
rate(kafka_eventbus_handler_duration_sum[5m]) /
rate(kafka_eventbus_handler_duration_count[5m])

# Rebalance frequency
rate(kafka_eventbus_rebalances_total[10m])
```

---

## Internal Statistics

In addition to OpenTelemetry metrics, KafkaEventBus tracks internal statistics:

```python
# Get statistics dictionary
stats = bus.get_stats_dict()

print(f"Events published: {stats['events_published']}")
print(f"Events consumed: {stats['events_consumed']}")
print(f"Successfully processed: {stats['events_processed_success']}")
print(f"Failed processing: {stats['events_processed_failed']}")
print(f"Messages in DLQ: {stats['messages_sent_to_dlq']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Reconnections: {stats['reconnections']}")
print(f"Rebalances: {stats['rebalance_count']}")
print(f"Last publish: {stats['last_publish_at']}")
print(f"Last consume: {stats['last_consume_at']}")
print(f"Connected at: {stats['connected_at']}")
```

---

## Integration with Repository

Connect KafkaEventBus to your repository for automatic event publishing:

```python
from eventsource import AggregateRepository, PostgreSQLEventStore
from sqlalchemy.ext.asyncio import async_sessionmaker

# Create infrastructure
session_factory = async_sessionmaker(...)
event_store = PostgreSQLEventStore(session_factory)

# Create Kafka event bus
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="projections",
)
bus = KafkaEventBus(config=config, event_registry=registry)
await bus.connect()

# Create repository with event publishing
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=bus,  # Events auto-published to Kafka
)

# Subscribe handlers
async def update_order_projection(event: DomainEvent):
    # Update read model
    pass

bus.subscribe(OrderPlaced, update_order_projection)

# Start consuming
consume_task = asyncio.create_task(bus.start_consuming())

# Create and save aggregate
order = repo.create_new(uuid4())
order.place(customer_id="cust-123", total_amount=99.99)
await repo.save(order)  # Events automatically flow through Kafka

# Handler receives event automatically
```

**Publishing flow:**
1. Repository saves events to PostgreSQL event store
2. Repository publishes events to KafkaEventBus
3. Kafka distributes events to all consumer groups
4. Consumers in each group share partitions
5. Handlers process events and update projections

---

## Complete Working Example

```python
"""
Tutorial 18: Kafka Event Bus - Complete Example
Demonstrates publishing, consuming, consumer groups, and monitoring.

Requirements:
- pip install eventsource-py[kafka]
- docker compose up -d (with Kafka configuration)
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
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
)

if not KAFKA_AVAILABLE:
    print("ERROR: Kafka support not installed.")
    print("Install with: pip install eventsource-py[kafka]")
    exit(1)


# =============================================================================
# Events
# =============================================================================


@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
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
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                total_amount=event.total_amount,
                status="placed",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                    }
                )

    def place(self, customer_id: str, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        self.apply_event(
            OrderPlaced(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                total_amount=total_amount,
                aggregate_version=self.get_next_version(),
            )
        )

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "placed":
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
        return [OrderPlaced, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self.total_orders += 1
            self.total_revenue += event.total_amount
            print(f"  [Stats] Order placed: ${event.total_amount:.2f}")
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
    registry.register(OrderPlaced)
    registry.register(OrderShipped)

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="demo1",
        consumer_group="demo-consumers",
    )
    bus = KafkaEventBus(config=config, event_registry=registry)

    try:
        await bus.connect()
        print("Connected to Kafka")

        received_events: list[DomainEvent] = []

        async def event_handler(event: DomainEvent) -> None:
            received_events.append(event)
            print(f"  Received: {event.event_type}")

        bus.subscribe(OrderPlaced, event_handler)
        print("Subscribed handler to OrderPlaced")

        # Publish event
        order_id = uuid4()
        event = OrderPlaced(
            aggregate_id=order_id,
            customer_id="cust-123",
            total_amount=99.99,
            aggregate_version=1,
        )
        await bus.publish([event])
        print(f"Published OrderPlaced for {order_id}")

        # Start consuming
        consume_task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(3.0)

        # Stop consuming
        await bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except asyncio.TimeoutError:
            consume_task.cancel()
            try:
                await consume_task
            except asyncio.CancelledError:
                pass

        print(f"Received {len(received_events)} event(s)")

    finally:
        await bus.disconnect()


async def demo_consumer_groups():
    """Demo 2: Consumer groups for load balancing."""
    print("\n" + "=" * 60)
    print("Demo 2: Consumer Groups and Load Balancing")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderPlaced)

    base_config = {
        "bootstrap_servers": "localhost:9092",
        "topic_prefix": "demo2",
        "consumer_group": "load-balanced-consumers",
    }

    config1 = KafkaEventBusConfig(**base_config, consumer_name="worker-1")
    config2 = KafkaEventBusConfig(**base_config, consumer_name="worker-2")

    bus1 = KafkaEventBus(config=config1, event_registry=registry)
    bus2 = KafkaEventBus(config=config2, event_registry=registry)

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

        bus1.subscribe(OrderPlaced, worker1_handler)
        bus2.subscribe(OrderPlaced, worker2_handler)

        # Publish events
        print("\nPublishing 10 events...")
        for i in range(10):
            event = OrderPlaced(
                aggregate_id=uuid4(),
                customer_id=f"cust-{i}",
                total_amount=10.0 * (i + 1),
                aggregate_version=1,
            )
            await bus1.publish([event])

        # Both workers consume
        task1 = asyncio.create_task(bus1.start_consuming())
        task2 = asyncio.create_task(bus2.start_consuming())
        await asyncio.sleep(5.0)

        # Stop consuming
        await bus1.stop_consuming()
        await bus2.stop_consuming()

        for task in [task1, task2]:
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except asyncio.TimeoutError:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        print(f"\nResults:")
        print(f"  Worker 1 processed: {len(worker1_events)} events")
        print(f"  Worker 2 processed: {len(worker2_events)} events")
        print(f"  Total: {len(worker1_events) + len(worker2_events)} events")

    finally:
        await bus1.disconnect()
        await bus2.disconnect()


async def demo_high_throughput():
    """Demo 3: High-throughput publishing."""
    print("\n" + "=" * 60)
    print("Demo 3: High-Throughput Publishing")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderPlaced)

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="demo3",
        consumer_group="throughput-consumers",
        batch_size=32768,                        # 32KB batches
        linger_ms=10,                            # 10ms batching
        compression_type="gzip",                 # Compress batches
    )
    bus = KafkaEventBus(config=config, event_registry=registry)

    try:
        await bus.connect()
        print("Connected to Kafka")

        # Publish 1000 events
        import time
        num_events = 1000
        start = time.perf_counter()

        events = [
            OrderPlaced(
                aggregate_id=uuid4(),
                customer_id=f"cust-{i}",
                total_amount=float(i * 10),
                aggregate_version=1,
            )
            for i in range(num_events)
        ]

        # Publish in batches of 100
        for i in range(0, len(events), 100):
            await bus.publish(events[i:i + 100])

        elapsed = time.perf_counter() - start
        throughput = num_events / elapsed

        print(f"Published {num_events} events in {elapsed:.3f}s")
        print(f"Throughput: {throughput:.0f} events/sec")

        # Get statistics
        stats = bus.get_stats_dict()
        print(f"\nStatistics:")
        print(f"  Events published: {stats['events_published']}")

    finally:
        await bus.disconnect()


async def demo_with_projections():
    """Demo 4: Integration with projections."""
    print("\n" + "=" * 60)
    print("Demo 4: Integration with Projections")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderPlaced)
    registry.register(OrderShipped)

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="demo4",
        consumer_group="projection-consumers",
    )
    bus = KafkaEventBus(config=config, event_registry=registry)

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
            OrderPlaced(
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
        await asyncio.sleep(3.0)
        await bus.stop_consuming()

        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except asyncio.TimeoutError:
            consume_task.cancel()
            try:
                await consume_task
            except asyncio.CancelledError:
                pass

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
    print("Tutorial 18: Kafka Event Bus")
    print("=" * 60)

    try:
        await demo_basic_publish_subscribe()
        await demo_consumer_groups()
        await demo_high_throughput()
        await demo_with_projections()

        print("\n" + "=" * 60)
        print("Tutorial 18 Complete!")
        print("=" * 60)

    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure Kafka is running:")
        print("  docker compose up -d")
        raise


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
============================================================
Tutorial 18: Kafka Event Bus
============================================================

============================================================
Demo 1: Basic Publish/Subscribe
============================================================
Connected to Kafka
Subscribed handler to OrderPlaced
Published OrderPlaced for <uuid>
  Received: OrderPlaced
Received 1 event(s)

============================================================
Demo 2: Consumer Groups and Load Balancing
============================================================
Both workers connected

Publishing 10 events...
  Worker 1 processed: <uuid>
  Worker 2 processed: <uuid>
  Worker 1 processed: <uuid>
  Worker 2 processed: <uuid>
  ...

Results:
  Worker 1 processed: 5 events
  Worker 2 processed: 5 events
  Total: 10 events

============================================================
Demo 3: High-Throughput Publishing
============================================================
Connected to Kafka
Published 1000 events in 0.842s
Throughput: 1188 events/sec

Statistics:
  Events published: 1000

============================================================
Demo 4: Integration with Projections
============================================================
Subscribed statistics projection
Subscribed audit logger to all events

Processing order: <uuid>
  [Stats] Order placed: $499.99
  [Stats] Order shipped: TRACK-12345

Projection Results:
  Total orders: 1
  Total revenue: $499.99
  Shipped count: 1

Audit log entries: 2

============================================================
Tutorial 18 Complete!
============================================================
```

---

## Production Deployment Patterns

### Graceful Shutdown

```python
import signal

class KafkaEventBusService:
    def __init__(self, bus: KafkaEventBus):
        self.bus = bus
        self.consume_task: asyncio.Task | None = None
        self.shutdown_event = asyncio.Event()

    async def start(self):
        """Start event bus service."""
        await self.bus.connect()

        # Start consumer
        self.consume_task = asyncio.create_task(
            self.bus.start_consuming()
        )

        # Wait for shutdown signal
        await self.shutdown_event.wait()

    async def shutdown(self):
        """Graceful shutdown."""
        print("Shutting down...")
        self.shutdown_event.set()

        # Stop consuming
        await self.bus.stop_consuming()

        # Wait for consumer task
        if self.consume_task:
            try:
                await asyncio.wait_for(self.consume_task, timeout=30.0)
            except asyncio.TimeoutError:
                self.consume_task.cancel()

        # Disconnect
        await self.bus.disconnect()
        print("Shutdown complete")

# Setup signal handlers
async def main():
    service = KafkaEventBusService(bus)

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
async def health_check(bus: KafkaEventBus) -> dict:
    """
    Health check for Kubernetes liveness/readiness probes.

    Returns:
        Health status dictionary
    """
    try:
        stats = bus.get_stats_dict()

        is_healthy = (
            bus._connected and
            bus._consuming
        )

        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "connected": bus._connected,
            "consuming": bus._consuming,
            "events_published": stats['events_published'],
            "events_consumed": stats['events_consumed'],
            "handler_errors": stats['handler_errors'],
            "dlq_messages": stats['messages_sent_to_dlq'],
        }

    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }
```

### Production Configuration

```python
import os

config = KafkaEventBusConfig(
    # Connection
    bootstrap_servers=os.getenv("KAFKA_BROKERS", "localhost:9092"),

    # Topic
    topic_prefix=os.getenv("KAFKA_TOPIC_PREFIX", "myapp.events"),

    # Consumer
    consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "default"),
    consumer_name=os.getenv("HOSTNAME"),  # Kubernetes pod name

    # Durability (production settings)
    acks="all",                              # Wait for all replicas
    compression_type="gzip",                 # Compress for network efficiency

    # Batching
    batch_size=32768,                        # 32KB batches
    linger_ms=10,                            # 10ms batching window

    # Security
    security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
    sasl_username=os.getenv("KAFKA_USERNAME"),
    sasl_password=os.getenv("KAFKA_PASSWORD"),
    ssl_cafile=os.getenv("KAFKA_CA_FILE", "/etc/kafka/ssl/ca.crt"),

    # Error handling
    max_retries=3,
    enable_dlq=True,

    # Observability
    enable_metrics=True,
    enable_tracing=True,
)
```

---

## Performance Tuning

### Producer Tuning

```python
# Maximum throughput (some latency)
config = KafkaEventBusConfig(
    batch_size=1048576,                      # 1MB batches
    linger_ms=100,                           # Wait up to 100ms for full batch
    compression_type="lz4",                  # Fast compression
    acks="1",                                # Leader only (faster)
)

# Minimum latency (lower throughput)
config = KafkaEventBusConfig(
    batch_size=16384,                        # 16KB batches
    linger_ms=0,                             # Send immediately
    compression_type=None,                   # No compression overhead
    acks="1",                                # Leader only
)

# Balanced (recommended)
config = KafkaEventBusConfig(
    batch_size=32768,                        # 32KB batches
    linger_ms=10,                            # 10ms batching
    compression_type="gzip",                 # Good compression
    acks="all",                              # Durability
)
```

### Consumer Tuning

```python
config = KafkaEventBusConfig(
    # Adjust based on handler processing time
    session_timeout_ms=30000,                # 30 seconds
    max_poll_interval_ms=300000,             # 5 minutes (long handlers)

    # Start from beginning for new consumer groups
    auto_offset_reset="earliest",
)
```

**Guidelines:**
- `session_timeout_ms`: How long consumer can be silent before evicted
- `max_poll_interval_ms`: Max time between poll() calls (must exceed handler time)
- Set `max_poll_interval_ms` > longest handler duration

---

## Key Takeaways

1. **KafkaEventBus enables ultra-high-throughput event streaming** across distributed systems
2. **Partitions provide ordering guarantees per aggregate_id** while enabling parallel processing
3. **Consumer groups enable automatic load balancing** across workers
4. **At-least-once delivery requires idempotent handlers** to handle duplicates during rebalances
5. **Dead letter queues capture poison messages** preventing processing blocks
6. **Exponential backoff with jitter prevents thundering herd** during retries
7. **OpenTelemetry metrics provide comprehensive observability** into event pipeline health
8. **Kafka excels at high-volume event streaming** but adds operational complexity
9. **Same EventBus interface as InMemory/Redis** enables easy migration between implementations
10. **Production deployments require careful tuning** of batching, compression, and acknowledgments

---

## Common Patterns

### Pattern 1: Multiple Consumer Groups

Different services each get all events:

```python
# Service A: Updates projections
projection_bus = KafkaEventBus(
    config=KafkaEventBusConfig(consumer_group="projections"),
    event_registry=registry,
)

# Service B: Sends notifications
notification_bus = KafkaEventBus(
    config=KafkaEventBusConfig(consumer_group="notifications"),
    event_registry=registry,
)

# Service C: Captures analytics
analytics_bus = KafkaEventBus(
    config=KafkaEventBusConfig(consumer_group="analytics"),
    event_registry=registry,
)
```

### Pattern 2: Scaled Workers

Multiple instances of same service share load:

```python
# Worker instance 1
worker1 = KafkaEventBus(
    config=KafkaEventBusConfig(
        consumer_group="order-processors",
        consumer_name="worker-1",
    ),
    event_registry=registry,
)

# Worker instance 2
worker2 = KafkaEventBus(
    config=KafkaEventBusConfig(
        consumer_group="order-processors",
        consumer_name="worker-2",
    ),
    event_registry=registry,
)

# Partitions automatically distributed between worker-1 and worker-2
```

### Pattern 3: Event Replay

Replay historical events to new projections:

```python
# Create new consumer group starting from beginning
replay_config = KafkaEventBusConfig(
    consumer_group="new-projection",         # New group
    auto_offset_reset="earliest",            # Start from beginning
)
replay_bus = KafkaEventBus(config=replay_config, event_registry=registry)

# Subscribe new projection
replay_bus.subscribe_all(new_projection)

# Start consuming - replays all historical events
await replay_bus.start_consuming()
```

---

## Troubleshooting

### Consumer Not Receiving Events

**Check connection:**
```python
assert bus._connected, "Not connected to Kafka"
```

**Verify consumer is running:**
```python
assert bus._consuming, "Consumer not running"
```

**Check consumer group:**
```python
stats = bus.get_stats_dict()
print(f"Events consumed: {stats['events_consumed']}")
```

### High Consumer Lag

**Monitor lag with metrics:**
```promql
kafka_eventbus_consumer_lag > 1000
```

**Scale horizontally:**
```python
# Add more consumers to the same group
# Kafka automatically rebalances partitions
```

**Check handler performance:**
```python
import time

async def slow_handler(event: DomainEvent):
    start = time.time()
    # ... process event ...
    duration = time.time() - start
    if duration > 1.0:
        print(f"Slow handler: {duration:.2f}s for {event.event_type}")
```

### Messages in DLQ

**Inspect DLQ:**
```python
count = await bus.get_dlq_message_count()
print(f"DLQ messages: {count}")

dlq_messages = await bus.get_dlq_messages(limit=10)
for msg in dlq_messages:
    print(f"Failed: {msg['event_type']}")
    print(f"Error: {msg['error']}")
```

**Fix and replay:**
```python
# Fix the underlying bug
# Then replay all DLQ messages
dlq_messages = await bus.get_dlq_messages(limit=1000)
for msg in dlq_messages:
    await bus.replay_dlq_message(
        partition=msg['partition'],
        offset=msg['offset']
    )
```

### Frequent Rebalances

**Monitor rebalances:**
```python
stats = bus.get_stats_dict()
print(f"Rebalances: {stats['rebalance_count']}")
```

**Increase session timeout:**
```python
config = KafkaEventBusConfig(
    session_timeout_ms=60000,                # 60 seconds (was 30)
    max_poll_interval_ms=600000,             # 10 minutes (was 5)
)
```

**Make handlers faster:**
- Use batch processing
- Move slow operations to background tasks
- Scale horizontally

---

## Next Steps

Now that you understand Kafka event distribution, explore other messaging patterns:

- [Tutorial 19: RabbitMQ Event Bus](19-rabbitmq.md) - Traditional message queue with complex routing
- [Tutorial 20: Observability](20-observability.md) - Distributed tracing, metrics, and logging

For production deployment:
- [Tutorial 15: Production Deployment](15-production.md) - Health checks, monitoring, graceful shutdown

---

## Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Consumer Groups](https://kafka.apache.org/documentation/#consumergroups)
- [Kafka Partitions and Ordering](https://kafka.apache.org/documentation/#intro_topics)
- `tests/integration/bus/test_kafka.py` - Comprehensive integration tests
- `src/eventsource/bus/kafka.py` - KafkaEventBus implementation
