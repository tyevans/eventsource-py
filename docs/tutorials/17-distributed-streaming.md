# Tutorial 17: Distributed Event Streaming with Kafka, RabbitMQ, and Redis

In [Tutorial 7: Event Bus](07-event-bus.md), you published events through `InMemoryEventBus`.
That bus was fast and required zero configuration, but it was confined to a single Python
process: if your process crashed, unhandled events vanished; if you ran two web servers
behind a load balancer, consumers on server B never saw events published on server A.

Real distributed systems cannot run on in-memory buses alone. Projections, email workers,
search indexers, and analytics pipelines typically run in dedicated worker pools, separate
from API instances. To bridge process boundaries without losing events, you need a
**distributed event bus** backed by durable message brokers.

`eventsource-py` ships three first-class distributed bus adapters, all conforming to the same
core `EventBus` port:

1. **Apache Kafka** (`KafkaEventBus`): High-throughput log-based streaming with partition-level
   ordering and durable replay for high-scale event distribution.
2. **RabbitMQ** (`RabbitMQEventBus`): AMQP message broker with rich topic routing, flexible
   queues, and consumer prefetch controls.
3. **Redis Streams** (`RedisEventBus`): Lightweight, durable append-only log built directly on
   Redis, ideal for small-to-medium deployments without standalone broker infrastructure.

In this tutorial, you will examine how distributed buses work, explore Kafka consumer groups
and partition assignment, compare Kafka with RabbitMQ and Redis Streams, and learn the critical
operational patterns required for distributed event distribution.

---

## What You'll Build and Learn

In this tutorial, you will:

1. **Understand Distributed Streaming Mechanics**: Move beyond single-process event dispatch to
   broker-backed distribution with at-least-once delivery semantics.
2. **Configure and Operate `KafkaEventBus`**: Set up `KafkaEventBusConfig`, configure partition
   keys based on `aggregate_id`, and wire consumer groups for horizontal scaling.
3. **Inspect Consumer Groups and Partition Assignment**: See how Kafka assigns partitions across
   competing consumers and handles rebalances dynamically.
4. **Compare Broker Topologies**:
   - Kafka's partitioned log architecture.
   - RabbitMQ's topic exchange and binding topology.
   - Redis Streams' consumer group and pending message recovery model.
5. **Handle Failures and Dead Letter Queues (DLQ)**: Route unprocessable events into DLQ
   destinations without stalling the event stream.
6. **Avoid the Dual-Write Trap**: Understand why an event store and an event bus must never be
   written independently without the Transactional Outbox pattern.

---

## Prerequisites

- **Tutorial 2 (First Event)** and **Tutorial 7 (Event Bus)**: You should understand `DomainEvent`,
  subscribers, and handler registration.
- **Tutorial 15 (Outbox Pattern)**: Distributed buses are typically driven by an outbox relay
  in production.
- **Python 3.13+** with optional broker extras:

```bash
# Install the broker extras you need
uv add --optional kafka "aiokafka>=0.11.0"
uv add --optional rabbitmq "aio-pika>=9.4.0"
uv add --optional redis "redis>=5.0.0"

# Or install all extras for local exploration
uv sync --all-extras
```

---

## The Distributed Bus Architecture

In `InMemoryEventBus`, `publish()` iterates over in-memory callbacks and awaits them in the
same process. In a distributed bus, `publish()` serializes the domain events into JSON payloads
and transmits them over the network to a central message broker.

```mermaid
flowchart LR
    subgraph API["API Service (Node 1)"]
        OrderAgg[Order Aggregate] -->|Append| Store[(Event Store)]
        OrderAgg -->|Publish| BusClient[KafkaEventBus Producer]
    end

    subgraph Broker["Apache Kafka Cluster"]
        Topic["Topic: myapp.events"]
        P0["Partition 0 (Orders A-M)"]
        P1["Partition 1 (Orders N-Z)"]
        Topic --> P0
        Topic --> P1
    end

    subgraph Workers["Projection Workers (Consumer Group)"]
        P0 -->|Consume| Worker1["Worker 1 (Consumer A)"]
        P1 -->|Consume| Worker2["Worker 2 (Consumer B)"]
    end

    BusClient -->|Key = aggregate_id| Topic
```

This architecture introduces three fundamental shifts from in-memory processing:

1. **Serialization**: Events must be serialized to JSON with standard metadata envelopes.
2. **At-Least-Once Delivery**: Network disconnects, consumer crashes, and rebalances mean a message
   can be delivered more than once. Consumer handlers **must be idempotent**.
3. **Partition-Based Ordering**: Total global ordering across all events does not scale horizontally.
   Distributed brokers maintain ordering within a *partition* or *stream*, keyed by `aggregate_id`.

---

## 1. Deep Dive: Apache Kafka (`KafkaEventBus`)

Apache Kafka treats topics as distributed, partitioned commit logs. Each event appended to a
partition receives a sequential, monotonically increasing offset.

### Partition Keys and Causality

In event sourcing, you do **not** need total global ordering across all orders in your system.
You only need strict ordering for events belonging to the **same aggregate instance** (e.g.,
Order `101` cannot be shipped before it is created).

`KafkaEventBus` automatically extracts `str(event.aggregate_id)` as the Kafka message key:

```python
# From eventsource.adapters.kafka.publisher:
key = str(event.aggregate_id).encode("utf-8")
```

Kafka hashes this key to determine the target partition. All events for Order `101` are guaranteed
to land on the exact same partition in the exact sequence they were published, ensuring FIFO
processing at the consumer.

### Configuring `KafkaEventBus`

The configuration object `KafkaEventBusConfig` controls producer buffering, consumer groups,
deserialization, and reliability settings:

```python
from eventsource.adapters.kafka import KafkaEventBus, KafkaEventBusConfig
from eventsource.domain import EventRegistry

# Initialize registry with your domain events
registry = EventRegistry()

# Configure Kafka bus
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="ordering_service",       # Results in: ordering_service.events
    consumer_group="order_projections",    # Shared group for workers
    consumer_name="worker-instance-01",    # Unique worker instance name
    acks="all",                            # Strongest durability guarantee
    linger_ms=5,                           # Batching window for producer throughput
    auto_offset_reset="earliest",          # Replay from start if no committed offset
    enable_dlq=True,                       # Send poison pills to DLQ topic
)

bus = KafkaEventBus(config=config, event_registry=registry)
```

### The Consumer Group Model

When multiple workers start with the same `consumer_group` (e.g., `order_projections`), Kafka
distributes the topic's partitions evenly among them:

- If a topic has **4 partitions** and you run **2 worker instances**, each worker receives
  **2 partitions**.
- If you scale up to **4 worker instances**, Kafka initiates a **rebalance**, assigning **1 partition**
  to each worker.
- If you scale to **8 workers**, 4 workers consume events while 4 remain idle standby replicas.

```mermaid
flowchart TD
    subgraph Topic["Kafka Topic: 4 Partitions"]
        Part0["Partition 0"]
        Part1["Partition 1"]
        Part2["Partition 2"]
        Part3["Partition 3"]
    end

    subgraph Group["Consumer Group: order_projections"]
        W1["Worker Instance 1"]
        W2["Worker Instance 2"]
    end

    Part0 --> W1
    Part1 --> W1
    Part2 --> W2
    Part3 --> W2
```

### Publishing and Consuming: A Complete Script

Here is a runnable example demonstrating how to register handlers, connect the bus, publish
events, and consume them across a group:

```python
import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4
from pydantic import Field

from eventsource.domain import DomainEvent, EventRegistry
from eventsource.adapters.kafka import KafkaEventBus, KafkaEventBusConfig


# 1. Define Domain Events
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    total_amount: float


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str


async def main() -> None:
    # 2. Register events in the registry
    registry = EventRegistry()
    registry.register(OrderCreated)
    registry.register(OrderShipped)

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        topic_prefix="ecommerce",
        consumer_group="projection_service",
        auto_offset_reset="earliest",
    )

    bus = KafkaEventBus(config=config, event_registry=registry)

    # 3. Register event handlers
    received_events = []

    async def handle_order_created(event: OrderCreated) -> None:
        print(f"[Consumer] Handling OrderCreated: {event.order_number} for aggregate {event.aggregate_id}")
        received_events.append(event)

    async def handle_order_shipped(event: OrderShipped) -> None:
        print(f"[Consumer] Handling OrderShipped: tracking {event.tracking_number}")
        received_events.append(event)

    bus.subscribe(OrderCreated, handle_order_created)
    bus.subscribe(OrderShipped, handle_order_shipped)

    # 4. Connect producer and consumer
    await bus.connect()

    # Start consumer loop in background
    consumer_task = asyncio.create_task(bus.start_consuming())

    try:
        # 5. Publish events
        order_id = uuid4()
        events = [
            OrderCreated(
                aggregate_id=order_id,
                order_number="ORD-2026-001",
                total_amount=149.99,
            ),
            OrderShipped(
                aggregate_id=order_id,
                tracking_number="TRK-987654321",
            ),
        ]

        print("[Producer] Publishing 2 events to Kafka...")
        await bus.publish(events)

        # Allow consumer loop to process messages
        await asyncio.sleep(2.0)
        print(f"[Done] Total events processed: {len(received_events)}")

    finally:
        # 6. Graceful shutdown
        await bus.stop_consuming()
        await bus.disconnect()
        consumer_task.cancel()


if __name__ == "__main__":
    # Note: Requires a running Kafka broker at localhost:9092
    # Run with: uv run python 17_kafka_demo.py
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"Skipping live run (broker not running): {exc}")
```

---

## 2. Comparing Message Broker Topologies

While `KafkaEventBus` provides high throughput and partition-level ordering, other workloads
favor different broker architectures. `eventsource-py` supports RabbitMQ and Redis Streams as well.

### RabbitMQ (`RabbitMQEventBus`)

RabbitMQ is an AMQP broker centered on **exchanges**, **queues**, and **bindings**. Rather than
storing messages in long-term partitioned logs, RabbitMQ holds messages in queues until acknowledged.

```mermaid
flowchart LR
    Producer -->|Publish: events| Exchange["Topic Exchange: events"]
    Exchange -->|Routing Key: Order.OrderCreated| Q1["Queue: projections.order"]
    Exchange -->|Routing Key: Order.*| Q2["Queue: audit.all_orders"]
    Q1 --> Consumer1["Projection Worker"]
    Q2 --> Consumer2["Audit Worker"]
```

#### Key Characteristics:
- **Exchange Topology**: `RabbitMQEventBus` automatically declares a `topic` exchange (default: `events`).
- **Routing Keys**: Events are routed using dot-notated routing keys: `{aggregate_type}.{event_type}` (e.g., `Order.OrderCreated`).
- **Consumer Queues**: Each `consumer_group` creates a dedicated durable queue bound to the exchange. Multiple workers in the same group read concurrently from the single queue with competing consumers.
- **Prefetch Control (`prefetch_count`)**: Limits the number of unacknowledged messages pushed to a worker, preventing memory exhaustion during spikes.
- **Dead Letter Exchanges**: Failed messages with exhausted retries are published with `x-death` headers to a DLQ exchange (e.g., `events.dlq`).

```python
from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

rabbit_config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",
    consumer_group="order_projections",
    prefetch_count=50,
    max_retries=3,
    enable_dlq=True,
)
rabbit_bus = RabbitMQEventBus(config=rabbit_config, event_registry=registry)
```

### Redis Streams (`RedisEventBus`)

Redis 5.0+ introduced **Redis Streams**, an append-only log data structure (`XADD`, `XREADGROUP`, `XACK`).
It provides Kafka-like log semantics with minimal operational overhead if Redis is already in your stack.

```mermaid
flowchart LR
    Producer -->|XADD| Stream["Redis Stream: myapp:events"]
    Stream -->|XREADGROUP group=projections| Group["Consumer Group: projections"]
    Group -->|Claim pending| Worker1["Worker A"]
    Group -->|Claim pending| Worker2["Worker B"]
```

#### Key Characteristics:
- **Lightweight Infrastructure**: Reuses existing Redis clusters without deploying Kafka or ZooKeeper/KRaft.
- **Consumer Groups (`XREADGROUP`)**: Supports distributed consumers tracking consumer offsets via Redis IDs (`<millisecondsTime>-<sequenceNumber>`).
- **Pending Message Recovery**: If a worker crashes while processing an event, another worker can inspect the Pending Entries List (PEL) using `XPENDING` and claim it with `XCLAIM` after `pending_idle_ms`.
- **Stream Trimming**: Redis streams reside in RAM. You can bound the stream length using `MAXLEN` to prevent unbounded memory growth.

```python
from eventsource.adapters.redis import RedisEventBus, RedisEventBusConfig

redis_config = RedisEventBusConfig(
    redis_url="redis://localhost:6379/0",
    stream_prefix="ordering_service",
    consumer_group="projections",
    pending_idle_ms=60000,               # Reclaim messages idle for > 60s
    enable_dlq=True,
)
redis_bus = RedisEventBus(config=redis_config, event_registry=registry)
```

---

## 3. Comparison Matrix: Kafka vs RabbitMQ vs Redis Streams

| Feature | Apache Kafka (`KafkaEventBus`) | RabbitMQ (`RabbitMQEventBus`) | Redis Streams (`RedisEventBus`) |
| :--- | :--- | :--- | :--- |
| **Model** | Distributed Partitioned Log | AMQP Broker (Exchanges & Queues) | In-Memory Durable Stream |
| **Ordering** | Strict per-partition (by `aggregate_id`) | Strict FIFO per-queue (single consumer) | Strict FIFO per-stream |
| **Scalability** | Massive (millions of msgs/sec) | Medium-High (tens of thousands msgs/sec) | High (memory-bound) |
| **Consumer Scaling** | Limited by partition count | Unlimited competing consumers per queue | Controlled by consumer group |
| **Event Replay** | Yes (rewind offsets arbitrarily) | No (messages deleted after ACK) | Yes (read from stream ID 0) |
| **Routing Flexibility** | Topic-level partitioning | Fine-grained topic/routing keys (`*`, `#`) | Prefix-based streams |
| **Operational Overhead** | High (JVM, broker clusters, storage) | Moderate (Erlang, clustering) | Low (Single binary or managed Redis) |
| **Best Used For** | High-volume streaming, event-driven architectures, long retention | Complex routing, low-latency task distribution, flexible worker topologies | Small-to-medium systems, existing Redis stacks, simple replay needs |

---

## 4. Operational Best Practice: The Outbox Pattern

A critical danger in distributed event-driven systems is the **Dual-Write Hazard**:

```python
# ANTI-PATTERN: DO NOT DO THIS
await event_store.append(stream_id, events)
await kafka_bus.publish(events)  # If network drops or broker is down, publish fails!
```

If the event store write succeeds but the Kafka publish fails:
- The event store has committed the new version.
- Projections and downstream services never receive the event.
- Your read models and microservices fall permanently out of sync.

### The Solution: Transactional Outbox

As covered in [Tutorial 15: Outbox Pattern](15-outbox.md), you must write events to your relational
database (the event store) and an `event_outbox` table in the **same atomic database transaction**.

A dedicated outbox relay worker then polls the outbox table and reliably publishes those events to
`KafkaEventBus` (or `RabbitMQEventBus`/`RedisEventBus`), marking each row as published only after
receiving broker acknowledgment.

```mermaid
flowchart TD
    subgraph Transaction["Single DB Transaction"]
        WriteEvents["1. Append to event_store"]
        WriteOutbox["2. Insert into event_outbox"]
    end

    subgraph OutboxProcessor["Outbox Relay Worker"]
        ReadOutbox["3. Read unpublished outbox rows"]
        PublishKafka["4. KafkaEventBus.publish()"]
        AckOutbox["5. Mark outbox rows published"]
    end

    WriteEvents --> WriteOutbox
    WriteOutbox -.-> ReadOutbox
    ReadOutbox --> PublishKafka
    PublishKafka -->|Broker Ack| AckOutbox
```

---

## Summary

In this tutorial, you learned how to scale event delivery across distributed microservices:

1. **`KafkaEventBus`** partitions event delivery by `aggregate_id`, ensuring strict causal ordering
   for each aggregate while scaling horizontally across consumer groups.
2. **`RabbitMQEventBus`** provides flexible topic exchange routing and queue bindings with AMQP
   prefetch controls.
3. **`RedisEventBus`** delivers lightweight, in-memory stream processing with consumer groups and
   idle message reclamation.
4. **At-least-once delivery** requires all downstream event handlers and projections to be idempotent.
5. In production, always pair distributed message buses with the **Transactional Outbox pattern**
   to avoid inconsistent dual writes.

Next, continue to [Tutorial 18: Observability](18-observability.md) to learn how to trace distributed
events across message buses and monitor system health with OpenTelemetry.
