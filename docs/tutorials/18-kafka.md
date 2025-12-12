# Tutorial 18: Using Kafka Event Bus

**Difficulty:** Advanced
**Progress:** Tutorial 18 of 21 | Phase 4: Advanced Patterns

---

## Prerequisites

- Completed [Tutorial 7: Distributing Events with Event Bus](07-event-bus.md) and [Tutorial 17: Using Redis Event Bus](17-redis.md)
- Python 3.11+ installed, Apache Kafka running locally (Docker recommended)
- Basic understanding of Kafka concepts (topics, partitions, consumer groups)

---

## When to Use Kafka Event Bus

Use Kafka for high-throughput, durable event streaming at scale with partition-based ordering.

| Feature | Kafka | Redis | RabbitMQ |
|---------|-------|-------|----------|
| **Throughput** | Very High (10,000+ events/sec) | High (1,000+ events/sec) | Moderate (hundreds/sec) |
| **Ordering** | Per-partition | Per-stream | Per-queue |
| **Retention** | Configurable (days/weeks) | Limited | Limited |
| **Durability** | Excellent (replication) | Good (AOF) | Good (persistence) |
| **Scaling** | Partition-based | Stream-based | Queue-based |
| **Complexity** | High | Low | Medium |
| **Best For** | Enterprise streaming | Moderate scale, simple setup | Complex routing, RPC |

---

## Installation

```bash
pip install eventsource-py[kafka]
```

```python
from eventsource.bus.kafka import KAFKA_AVAILABLE, KafkaNotAvailableError

if KAFKA_AVAILABLE:
    print("Kafka support is available!")
else:
    print("Kafka not available. Install with: pip install eventsource-py[kafka]")
```

---

## Kafka Setup with Docker

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

```bash
docker compose -f docker-compose.kafka.yml up -d
```

---

## Configuring Kafka Event Bus

```python
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",  # Kafka broker addresses
    topic_prefix="myapp.events",         # Prefix for topic names
    consumer_group="projections",        # Consumer group ID
)
```

Key parameters: `bootstrap_servers`, `topic_prefix`, `consumer_group`, `acks`, `compression_type`, `batch_size`, `linger_ms`, `auto_offset_reset`, `max_retries`, `enable_dlq`, `enable_metrics`

### Tuning Configuration

```python
# Producer tuning
config = KafkaEventBusConfig(
    acks="all",           # "0" | "1" | "all" (most reliable)
    batch_size=32768,     # 32KB batches
    linger_ms=10,         # Wait 10ms for batching
    compression_type="gzip",  # None, "gzip", "snappy", "lz4", "zstd"
)

# Consumer tuning
config = KafkaEventBusConfig(
    auto_offset_reset="earliest",  # or "latest"
    session_timeout_ms=30000,
    max_poll_interval_ms=300000,
)

# Security (production)
config = KafkaEventBusConfig(
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_username="myapp",
    sasl_password="secret",
    ssl_cafile="/etc/kafka/ssl/ca.crt",
)
```

---

## Creating and Connecting KafkaEventBus

```python
from eventsource import DomainEvent, register_event, default_registry
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_id: str
    total: float

config = KafkaEventBusConfig(bootstrap_servers="localhost:9092", topic_prefix="orders", consumer_group="handlers")
bus = KafkaEventBus(config=config, event_registry=default_registry)

await bus.connect()
# ... use the bus ...
await bus.disconnect()

# Or use context manager (auto-connect/disconnect)
async with KafkaEventBus(config=config, event_registry=default_registry) as bus:
    await bus.publish([event])
```

---

## Publishing Events

```python
from uuid import uuid4

event = OrderPlaced(aggregate_id=uuid4(), customer_id="CUST-001", total=99.99, aggregate_version=1)
await bus.publish([event])

# Batch publishing (more efficient)
events = [OrderPlaced(aggregate_id=uuid4(), customer_id=f"CUST-{i:04d}", total=float(i * 10), aggregate_version=1) for i in range(100)]
await bus.publish(events)

# Background (fire-and-forget)
await bus.publish([event], background=True)
```

Events route to partitions by `aggregate_id`, ensuring per-aggregate ordering.

---

## Subscribing Handlers

```python
async def handle_order_placed(event: DomainEvent) -> None:
    if isinstance(event, OrderPlaced):
        print(f"Processing order {event.aggregate_id}: ${event.total}")

bus.subscribe(OrderPlaced, handle_order_placed)
```

### Class-Based and Projection Handlers

```python
class OrderNotificationHandler:
    def __init__(self):
        self.notifications_sent = 0

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self.notifications_sent += 1

bus.subscribe(OrderPlaced, OrderNotificationHandler())

# Projection with DeclarativeProjection
from eventsource import DeclarativeProjection, handles

class OrderProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.orders: dict = {}

    @handles(OrderPlaced)
    async def _on_placed(self, event: OrderPlaced) -> None:
        self.orders[str(event.aggregate_id)] = {"customer": event.customer_id, "total": event.total, "status": "placed"}

bus.subscribe_all(OrderProjection())
bus.subscribe_to_all_events(lambda e: print(f"[AUDIT] {e.event_type}"))  # Wildcard
```

---

## Consuming Events

```python
await bus.connect()
bus.subscribe(OrderPlaced, handle_order)
await bus.start_consuming()  # Blocks until stopped
```

### Background Consumption

```python
task = bus.start_consuming_in_background()
# ... do other work ...
await bus.stop_consuming()
await task
```

---

## Consumer Groups

Consumer groups distribute partitions across workers for horizontal scaling. Each message is delivered to exactly one consumer in the group.

```python
# Multiple workers in same group share load
config_1 = KafkaEventBusConfig(consumer_group="order-processors", consumer_name="worker-1")
config_2 = KafkaEventBusConfig(consumer_group="order-processors", consumer_name="worker-2")

# Different groups receive ALL events independently
projection_config = KafkaEventBusConfig(consumer_group="order-projections")
notification_config = KafkaEventBusConfig(consumer_group="order-notifications")
```

---

## Handling Rebalances

Kafka rebalances partitions when consumers join or leave. Design idempotent handlers to handle potential duplicate processing:

```python
class IdempotentOrderHandler:
    def __init__(self):
        self.processed_events: set[str] = set()

    async def handle(self, event: DomainEvent) -> None:
        event_id = str(event.event_id)

        if event_id in self.processed_events:
            return  # Skip duplicate

        await self.process_order(event)
        self.processed_events.add(event_id)
```

---

## Dead Letter Queue

```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="orders",
    consumer_group="handlers",
    max_retries=3,
    enable_dlq=True,
    retry_base_delay=1.0,
    retry_max_delay=60.0,
    retry_jitter=0.1,
)
```

```python
# Inspect DLQ
dlq_messages = await bus.get_dlq_messages(limit=100)
count = await bus.get_dlq_message_count()

# Replay DLQ message
await bus.replay_dlq_message(partition=0, offset=42)
```

---

## Monitoring with OpenTelemetry Metrics

### Available Metrics

**Counters:** `messages.published`, `messages.consumed`, `handler.invocations`, `handler.errors`, `messages.dlq`, `publish.errors`, `reconnections`, `rebalances`

**Histograms:** `publish.duration`, `consume.duration`, `handler.duration`, `batch.size`

**Gauges:** `connections.active`, `consumer.lag`

```python
# Setup OpenTelemetry export
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

start_http_server(port=8000)
metrics.set_meter_provider(MeterProvider(metric_readers=[PrometheusMetricReader()]))

config = KafkaEventBusConfig(enable_metrics=True)
bus = KafkaEventBus(config=config, event_registry=default_registry)
```

### Internal Statistics

```python
stats = bus.get_stats_dict()
print(f"Events published: {stats['events_published']}")
print(f"Events consumed: {stats['events_consumed']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Sent to DLQ: {stats['messages_sent_to_dlq']}")
print(f"Rebalances: {stats['rebalance_count']}")
```

---

## At-Least-Once Delivery

Kafka provides at-least-once delivery guarantees. Events may be processed multiple times due to consumer crashes, rebalances, or network issues.

### Designing Idempotent Handlers

```python
class IdempotentProjection:
    def __init__(self, db):
        self.db = db

    async def handle(self, event: DomainEvent) -> None:
        event_id = str(event.event_id)

        if await self.db.event_processed(event_id):
            return  # Skip duplicate

        async with self.db.transaction():
            await self.apply_event(event)
            await self.db.mark_event_processed(event_id)
```

**Idempotency Patterns:** Deduplication by event ID, natural idempotency (SET vs INCREMENT), version checks

---

## Complete Example

```python
"""Tutorial 18: Using Kafka Event Bus"""
import asyncio
import time
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry
from eventsource.bus.kafka import KAFKA_AVAILABLE, KafkaEventBus, KafkaEventBusConfig

if not KAFKA_AVAILABLE:
    print("Kafka not available. Install with: pip install eventsource-py[kafka]")
    exit(1)

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

class OrderProjection:
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
    def __init__(self):
        self.event_count = 0
        self.total_revenue = 0.0

    async def handle(self, event: DomainEvent) -> None:
        self.event_count += 1
        if isinstance(event, OrderPlaced):
            self.total_revenue += event.total

async def basic_kafka_example():
    print("=" * 60)
    print("Basic Kafka Event Bus")
    print("=" * 60)

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="tutorial18",
        consumer_group="demo-handlers",
        enable_metrics=True,
    )

    bus = KafkaEventBus(config=config, event_registry=default_registry)
    projection = OrderProjection()
    metrics = MetricsHandler()

    bus.subscribe(OrderPlaced, projection)
    bus.subscribe(OrderPlaced, metrics)
    bus.subscribe(OrderShipped, projection)

    try:
        await bus.connect()
        print("Connected!")

        order_id = uuid4()
        await bus.publish([
            OrderPlaced(
                aggregate_id=order_id,
                customer_id="CUST-001",
                total=99.99,
                aggregate_version=1,
            ),
        ])
        await bus.publish([
            OrderShipped(
                aggregate_id=order_id,
                tracking_number="TRACK-12345",
                aggregate_version=2,
            ),
        ])
        print("Published 2 events")

        task = bus.start_consuming_in_background()
        await asyncio.sleep(3)
        await bus.stop_consuming()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        print(f"\nProjection orders: {len(projection.orders)}")
        for oid, data in projection.orders.items():
            print(f"  {oid[:8]}...: {data}")

        print(f"\nMetrics: {metrics.event_count} events, ${metrics.total_revenue:.2f} revenue")

    finally:
        await bus.disconnect()

async def high_throughput_example():
    print("\n" + "=" * 60)
    print("High Throughput Example")
    print("=" * 60)

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

        for i in range(0, len(events), 50):
            await bus.publish(events[i:i + 50])

        publish_time = time.perf_counter() - start
        print(f"Published {num_events} in {publish_time:.3f}s ({num_events / publish_time:.0f} events/sec)")

        task = bus.start_consuming_in_background()
        await asyncio.sleep(5)
        await bus.stop_consuming()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        print(f"Received: {events_received['count']} events")

    finally:
        await bus.disconnect()

async def main():
    print("Tutorial 18: Kafka Event Bus\n" + "=" * 60)

    try:
        await basic_kafka_example()
        await high_throughput_example()
    except Exception as e:
        print(f"\nError: {e}")
        print("Make sure Kafka is running. See docker-compose.kafka.yml")

    print("\n" + "=" * 60)
    print("Tutorial complete!")

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Exercises

### Exercise 1: High-Throughput Event Pipeline

Build a sensor data pipeline with Kafka. Configure `KafkaEventBus`, create a `SensorReading` event, publish 1000 events, and display throughput metrics.

**Starter Code:**

```python
"""
Tutorial 18 - Exercise 1: High-Throughput Event Pipeline
"""
import asyncio
import time
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig, KAFKA_AVAILABLE

if not KAFKA_AVAILABLE:
    print("Install kafka: pip install eventsource-py[kafka]")
    exit(1)

# TODO: Define SensorReading event with temperature and humidity fields
# TODO: Create MetricsAggregator handler that tracks count and average temperature
# TODO: Configure KafkaEventBus, publish 1000 events, consume and display metrics

async def main():
    print("=== Exercise 18-1: High-Throughput Pipeline ===\n")
    # Your code here

if __name__ == "__main__":
    asyncio.run(main())
```

**Solution:** See `docs/tutorials/exercises/solutions/18-1.py`

---

## Next Steps

Continue to [Tutorial 19: Using RabbitMQ Event Bus](19-rabbitmq.md) to learn about traditional message queue patterns.
