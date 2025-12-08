# Kafka Event Bus Guide

This guide covers using the KafkaEventBus for distributed event streaming with Apache Kafka.

## Overview

The `KafkaEventBus` provides high-throughput event distribution using Apache Kafka. It's designed for enterprise environments requiring:

- High-throughput event streaming (10,000+ events/second)
- Consumer groups for horizontal scaling
- Long-term event retention
- Partition-based ordering guarantees
- TLS/SSL and SASL authentication

## Installation

```bash
pip install eventsource-py[kafka]
```

This installs the `aiokafka` async Kafka client.

## Quick Start

```python
import asyncio
from uuid import uuid4
from eventsource import DomainEvent, register_event
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str

async def main():
    # Configure the bus
    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="orders",
        consumer_group="order-processor",
    )

    # Create and connect
    bus = KafkaEventBus(config=config)
    await bus.connect()

    # Subscribe handlers
    async def handle_order_created(event: DomainEvent) -> None:
        print(f"Order created: {event.aggregate_id}")

    bus.subscribe(OrderCreated, handle_order_created)

    # Start consuming in background
    task = bus.start_consuming_in_background()

    # Publish an event
    event = OrderCreated(
        aggregate_id=uuid4(),
        customer_id="customer-123",
        aggregate_version=1,
    )
    await bus.publish([event])

    # Wait a bit for processing
    await asyncio.sleep(2)

    # Shutdown
    await bus.shutdown()

asyncio.run(main())
```

## Configuration Reference

### Connection Settings

```python
config = KafkaEventBusConfig(
    # Required: Kafka broker addresses (comma-separated for multiple)
    bootstrap_servers="broker1:9092,broker2:9092,broker3:9092",

    # Topic prefix - main topic will be {prefix}.stream
    topic_prefix="myapp.events",

    # Consumer group ID for coordinated consumption
    consumer_group="my-service",

    # Unique consumer name (auto-generated if not provided)
    consumer_name="worker-1",
)
```

### Producer Settings

```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",

    # Acknowledgment level: "0", "1", or "all"
    # "all" = most durable, waits for all replicas
    acks="all",

    # Compression: None, "gzip", "snappy", "lz4", "zstd"
    compression_type="gzip",

    # Batch settings for throughput optimization
    batch_size=16384,  # bytes
    linger_ms=5,       # milliseconds to wait for more messages
)
```

### Consumer Settings

```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",

    # Where to start when no offset exists: "earliest" or "latest"
    auto_offset_reset="earliest",

    # Consumer heartbeat and session timing
    session_timeout_ms=30000,      # Session timeout
    heartbeat_interval_ms=10000,   # Heartbeat frequency
    max_poll_interval_ms=300000,   # Max time between polls
)
```

### Error Handling

```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",

    # Retry configuration
    max_retries=3,           # Max attempts before DLQ
    retry_base_delay=1.0,    # Initial delay (seconds)
    retry_max_delay=60.0,    # Maximum delay (seconds)
    retry_jitter=0.1,        # Randomization factor (0-1)

    # Dead letter queue
    enable_dlq=True,
    dlq_topic_suffix=".dlq",  # Results in {prefix}.stream.dlq
)
```

## Consumer Groups

Consumer groups enable horizontal scaling by distributing partitions among multiple consumers.

### Single Consumer Group

All consumers in the same group share the workload:

```python
# Configuration shared by all workers
def create_config(worker_id: str) -> KafkaEventBusConfig:
    return KafkaEventBusConfig(
        bootstrap_servers="kafka:9092",
        topic_prefix="orders",
        consumer_group="order-processors",  # Same for all
        consumer_name=f"worker-{worker_id}",  # Unique per worker
    )

# Worker 1
async def run_worker_1():
    config = create_config("1")
    bus = KafkaEventBus(config=config)
    bus.subscribe(OrderCreated, handle_order)
    await bus.connect()
    await bus.start_consuming()

# Worker 2
async def run_worker_2():
    config = create_config("2")
    bus = KafkaEventBus(config=config)
    bus.subscribe(OrderCreated, handle_order)
    await bus.connect()
    await bus.start_consuming()
```

### Multiple Consumer Groups

Different consumer groups receive all events independently:

```python
# Projections group - builds read models
projection_config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="orders",
    consumer_group="order-projections",  # Independent group
)

# Notifications group - sends emails
notification_config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="orders",
    consumer_group="order-notifications",  # Independent group
)

# Both groups receive all events
```

## Dead Letter Queue

Failed messages are automatically sent to DLQ after max retries.

### Automatic DLQ Routing

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="orders",
    max_retries=3,
    enable_dlq=True,
)

bus = KafkaEventBus(config=config)

# Handler that might fail
async def flaky_handler(event: DomainEvent) -> None:
    if random.random() < 0.1:  # 10% failure rate
        raise Exception("Transient error")
    await process_event(event)

bus.subscribe(OrderCreated, flaky_handler)
# After 3 failures, event goes to orders.stream.dlq
```

### Inspecting DLQ

```python
# Get messages from DLQ
dlq_messages = await bus.get_dlq_messages(limit=100)

for msg in dlq_messages:
    print(f"Event ID: {msg['headers']['event_id']}")
    print(f"Event Type: {msg['headers']['event_type']}")
    print(f"Error: {msg['headers']['dlq_error_message']}")
    print(f"Retry Count: {msg['headers']['dlq_retry_count']}")
    print(f"Original Topic: {msg['headers']['dlq_original_topic']}")
    print(f"Timestamp: {msg['headers']['dlq_timestamp']}")
    print(f"Payload: {msg['payload']}")
    print("---")

# Get count
count = await bus.get_dlq_message_count()
print(f"Total DLQ messages: {count}")
```

### Replaying DLQ Messages

```python
# Replay a specific message by partition and offset
success = await bus.replay_dlq_message(partition=0, offset=42)

if success:
    print("Message republished to main topic")
else:
    print("Replay failed")
```

## Security Configuration

### Development (No Security)

```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    security_protocol="PLAINTEXT",
)
```

### TLS Encryption

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    security_protocol="SSL",
    ssl_cafile="/etc/kafka/ssl/ca.crt",
)
```

### SASL Authentication

#### SASL/PLAIN

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="myapp",
    sasl_password="secret",
    ssl_cafile="/etc/kafka/ssl/ca.crt",
)
```

#### SASL/SCRAM-SHA-512 (Recommended)

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_username="myapp",
    sasl_password="secret",
    ssl_cafile="/etc/kafka/ssl/ca.crt",
)
```

### Mutual TLS (mTLS)

```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    security_protocol="SSL",
    ssl_cafile="/etc/kafka/ssl/ca.crt",
    ssl_certfile="/etc/kafka/ssl/client.crt",
    ssl_keyfile="/etc/kafka/ssl/client.key",
)
```

### Environment-Based Configuration

```python
import os

config = KafkaEventBusConfig(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    topic_prefix=os.environ.get("KAFKA_TOPIC_PREFIX", "events"),
    consumer_group=os.environ.get("KAFKA_CONSUMER_GROUP", "default"),
    security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM"),
    sasl_username=os.environ.get("KAFKA_SASL_USERNAME"),
    sasl_password=os.environ.get("KAFKA_SASL_PASSWORD"),
    ssl_cafile=os.environ.get("KAFKA_SSL_CAFILE"),
)
```

## OpenTelemetry Tracing

The Kafka event bus integrates with OpenTelemetry for distributed tracing.

### Setup

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Configure OpenTelemetry
trace.set_tracer_provider(TracerProvider())
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317"))
)

# Kafka bus with tracing enabled (default when OpenTelemetry is available)
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="orders",
    enable_tracing=True,  # Default
)

bus = KafkaEventBus(config=config)
```

### Span Attributes

Publish spans include:

- `messaging.system`: "kafka"
- `messaging.destination`: topic name
- `messaging.operation`: "publish"
- `event.type`: event type name
- `event.id`: event UUID
- `aggregate.id`: aggregate UUID
- `messaging.kafka.partition`: partition number
- `messaging.kafka.offset`: message offset

Consume spans include:

- `messaging.system`: "kafka"
- `messaging.source`: topic name
- `messaging.operation`: "receive"
- `messaging.kafka.partition`: partition number
- `messaging.kafka.offset`: message offset
- `messaging.kafka.consumer_group`: consumer group ID

## OpenTelemetry Metrics

The Kafka event bus emits comprehensive OpenTelemetry metrics for production monitoring when `enable_metrics=True` (default).

### Quick Setup

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# Configure metrics export
reader = PeriodicExportingMetricReader(OTLPMetricExporter())
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

# Kafka bus with metrics enabled
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9092",
    topic_prefix="orders",
    enable_metrics=True,  # Default
)

bus = KafkaEventBus(config=config)
```

### Available Metrics

| Category | Metrics |
|----------|---------|
| **Counters** | `messages.published`, `messages.consumed`, `handler.invocations`, `handler.errors`, `messages.dlq`, `publish.errors`, `reconnections`, `rebalances` |
| **Histograms** | `publish.duration`, `consume.duration`, `handler.duration`, `batch.size` |
| **Gauges** | `connections.active`, `consumer.lag` |

All metrics use the `kafka.eventbus.` prefix (e.g., `kafka.eventbus.messages.published`).

For detailed PromQL queries, alerting recommendations, and Grafana dashboard examples, see the [Kafka Metrics Guide](kafka-metrics.md).

## Monitoring and Statistics

### Getting Statistics

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
print(f"Last publish: {stats['last_publish_at']}")
print(f"Last consume: {stats['last_consume_at']}")
print(f"Last error: {stats['last_error_at']}")
```

### Topic Information

```python
info = await bus.get_topic_info()

print(f"Topic: {info['topic']}")
print(f"Partitions: {info['partitions']}")
print(f"Consumer group: {info['consumer_group']}")
print(f"Connected: {info['connected']}")
print(f"Consuming: {info['consuming']}")
```

## Migration Guide

### From RedisEventBus

```python
# Before
from eventsource.bus import RedisEventBus, RedisEventBusConfig

redis_config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="myapp",
    consumer_group="projections",
)
bus = RedisEventBus(config=redis_config)

# After
from eventsource.bus import KafkaEventBus, KafkaEventBusConfig

kafka_config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="myapp",
    consumer_group="projections",
)
bus = KafkaEventBus(config=kafka_config)

# Handler code remains unchanged
bus.subscribe(OrderCreated, my_handler)
bus.subscribe_all(my_projection)
await bus.publish([event])
```

### From RabbitMQEventBus

```python
# Before
from eventsource.bus import RabbitMQEventBus, RabbitMQEventBusConfig

rabbitmq_config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",
    consumer_group="projections",
)
bus = RabbitMQEventBus(config=rabbitmq_config)

# After
from eventsource.bus import KafkaEventBus, KafkaEventBusConfig

kafka_config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="events",
    consumer_group="projections",
)
bus = KafkaEventBus(config=kafka_config)

# Handler code remains unchanged
```

### Key Differences

| Aspect | Redis/RabbitMQ | Kafka |
|--------|----------------|-------|
| Topic/Stream Creation | Auto-created | Pre-create topics |
| Retention | Limited | Configurable (days/weeks) |
| Throughput | Good | Excellent |
| Ordering | Stream-level | Partition-level |
| Consumer Scaling | Up to stream count | Up to partition count |

## Best Practices

### Topic Management

Kafka topics should be pre-created in production:

```bash
# Create main topic with 12 partitions
kafka-topics.sh --create \
  --topic myapp.events.stream \
  --partitions 12 \
  --replication-factor 3 \
  --bootstrap-server kafka:9092

# Create DLQ topic
kafka-topics.sh --create \
  --topic myapp.events.stream.dlq \
  --partitions 3 \
  --replication-factor 3 \
  --bootstrap-server kafka:9092
```

### Partition Count

- Set partition count based on expected throughput
- More partitions = more parallelism
- Cannot reduce partitions after creation
- Consumer count limited to partition count

### Consumer Configuration

```python
# For low-latency processing
config = KafkaEventBusConfig(
    session_timeout_ms=10000,     # Faster failure detection
    heartbeat_interval_ms=3000,   # More frequent heartbeats
    max_poll_interval_ms=60000,   # Shorter processing window
)

# For batch processing
config = KafkaEventBusConfig(
    session_timeout_ms=60000,      # More tolerance
    heartbeat_interval_ms=20000,
    max_poll_interval_ms=600000,   # 10 minute processing window
)
```

### Graceful Shutdown

```python
async def main():
    bus = KafkaEventBus(config=config)
    await bus.connect()

    try:
        await bus.start_consuming()
    except asyncio.CancelledError:
        pass
    finally:
        await bus.shutdown(timeout=30.0)

# Or use context manager
async with KafkaEventBus(config=config) as bus:
    await bus.start_consuming()
```

### Error Handling

```python
class ResilientHandler:
    async def handle(self, event: DomainEvent) -> None:
        try:
            await self.process(event)
        except TransientError:
            # Re-raise to trigger retry
            raise
        except PermanentError as e:
            # Log but don't retry - will eventually go to DLQ
            logger.error(f"Permanent failure: {e}")
            raise
```

## Docker Compose Example

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

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

  app:
    build: .
    depends_on:
      - kafka
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC_PREFIX: myapp.events
      KAFKA_CONSUMER_GROUP: my-service
```

## See Also

- [Kafka Metrics Guide](kafka-metrics.md) - OpenTelemetry metrics, PromQL queries, and alerting
- [Event Bus API Reference](../api/bus.md#kafkaeventbus)
- [Installation Guide](../installation.md)
- [Architecture Overview](../architecture.md)
- [Production Deployment](production.md)
