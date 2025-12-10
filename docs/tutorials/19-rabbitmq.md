# Tutorial 19: Using RabbitMQ Event Bus

**Estimated Time:** 60-75 minutes
**Difficulty:** Advanced
**Progress:** Tutorial 19 of 21 | Phase 4: Advanced Patterns

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 7: Distributing Events with Event Bus](07-event-bus.md)
- Completed [Tutorial 18: Using Kafka Event Bus](18-kafka.md)
- Python 3.11+ installed
- RabbitMQ running locally (Docker recommended)
- Basic understanding of AMQP concepts (exchanges, queues, bindings)

This tutorial covers distributed event distribution using RabbitMQ with AMQP. If you are building applications that need flexible routing patterns and reliable message delivery, the RabbitMQ event bus is an excellent choice.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Configure `RabbitMQEventBus` for flexible event distribution
2. Understand exchange types and their routing behaviors
3. Set up queue bindings for consumer groups
4. Handle message acknowledgment and implement retry patterns
5. Configure and monitor dead letter queues (DLQ)
6. Set up SSL/TLS for production deployments

---

## When to Use RabbitMQ Event Bus

RabbitMQ excels at flexible message routing and traditional enterprise messaging patterns. Consider RabbitMQ when:

- **Flexible Routing**: Need pattern-based or content-based routing
- **Enterprise Integration**: Working with existing AMQP-based systems
- **Request-Reply Patterns**: Need RPC-style communication
- **Message Priority**: Need to prioritize certain messages over others
- **Reliable Delivery**: Need at-least-once delivery with DLQ support

### RabbitMQ vs Kafka vs Redis

| Feature | RabbitMQ | Kafka | Redis |
|---------|----------|-------|-------|
| **Routing** | Flexible (topic, direct, fanout) | Partition-based | Stream-based |
| **Protocol** | AMQP | Kafka protocol | RESP |
| **Throughput** | Moderate (thousands/sec) | Very High (10,000+/sec) | High (1,000+/sec) |
| **Ordering** | Per-queue | Per-partition | Per-stream |
| **Retention** | Until consumed | Configurable (days/weeks) | Limited |
| **Complexity** | Medium | High | Low |
| **Best For** | Complex routing, enterprise | Event streaming, high volume | Moderate scale, caching |

### Decision Criteria

Use **RabbitMQ** when you need:

- Flexible routing patterns (wildcards, header-based routing)
- Traditional message queue semantics
- Integration with existing AMQP infrastructure
- Message priority queues

Use **Kafka** when you need:

- The highest throughput and lowest latency
- Long-term event retention and replay
- Per-key ordering at massive scale

Use **Redis** when you need:

- Simpler setup and operations
- Good performance with existing Redis infrastructure
- Combined caching and messaging

---

## Installation

Install eventsource-py with RabbitMQ support:

```bash
pip install eventsource-py[rabbitmq]
```

This installs `aio-pika`, an async AMQP client for Python.

### Verifying Installation

Check that RabbitMQ support is available:

```python
from eventsource.bus.rabbitmq import RABBITMQ_AVAILABLE, RabbitMQNotAvailableError

if RABBITMQ_AVAILABLE:
    print("RabbitMQ support is available!")
else:
    print("RabbitMQ not available. Install with: pip install eventsource-py[rabbitmq]")
```

---

## RabbitMQ Setup with Docker

The easiest way to run RabbitMQ locally is with Docker.

### Docker Compose File

Create a file named `docker-compose.rabbitmq.yml`:

```yaml
version: '3.8'

services:
  rabbitmq:
    image: rabbitmq:3-management-alpine
    ports:
      - "5672:5672"    # AMQP protocol
      - "15672:15672"  # Management UI
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "check_running"]
      interval: 30s
      timeout: 10s
      retries: 5

volumes:
  rabbitmq_data:
```

### Starting RabbitMQ

```bash
docker compose -f docker-compose.rabbitmq.yml up -d
```

Wait for RabbitMQ to be ready:

```bash
docker compose -f docker-compose.rabbitmq.yml logs rabbitmq | grep "started"
```

You can access the RabbitMQ Management UI at http://localhost:15672 (login: guest/guest) to monitor exchanges, queues, and messages.

### Quick Start with Docker Run

For quick testing, you can also use:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management-alpine
```

---

## Configuring RabbitMQ Event Bus

The `RabbitMQEventBusConfig` class provides comprehensive configuration options.

### Basic Configuration

```python
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="myapp.events",
    exchange_type="topic",
    consumer_group="projections",
)

bus = RabbitMQEventBus(config=config)
```

The configuration creates:

- **Exchange**: `myapp.events` (for publishing events)
- **Queue**: `myapp.events.projections` (for consuming)
- **DLQ Exchange**: `myapp.events_dlq` (for failed messages)
- **DLQ Queue**: `myapp.events.projections.dlq` (dead letter storage)

### Configuration Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `rabbitmq_url` | `str` | `"amqp://guest:guest@localhost:5672/"` | AMQP connection URL |
| `exchange_name` | `str` | `"events"` | Exchange name for events |
| `exchange_type` | `str` | `"topic"` | Exchange type (topic, direct, fanout, headers) |
| `consumer_group` | `str` | `"default"` | Queue name prefix for this consumer |
| `consumer_name` | `str` | Auto-generated | Unique consumer identifier |
| `prefetch_count` | `int` | `10` | Max unacknowledged messages |
| `max_retries` | `int` | `3` | Retries before DLQ |
| `enable_dlq` | `bool` | `True` | Enable dead letter queue |
| `durable` | `bool` | `True` | Exchanges/queues survive restarts |
| `auto_delete` | `bool` | `False` | Delete queue when consumers disconnect |
| `heartbeat` | `int` | `60` | Heartbeat interval (seconds) |
| `enable_tracing` | `bool` | `True` | Enable OpenTelemetry tracing |

### Exchange Type Selection

Choose the exchange type based on your routing needs:

```python
# Topic exchange (default) - Pattern-based routing
config = RabbitMQEventBusConfig(
    exchange_name="events",
    exchange_type="topic",  # Supports wildcards: * and #
)

# Direct exchange - Exact routing key match
config = RabbitMQEventBusConfig(
    exchange_name="events",
    exchange_type="direct",  # Messages go to queues with exact key match
)

# Fanout exchange - Broadcast to all queues
config = RabbitMQEventBusConfig(
    exchange_name="events",
    exchange_type="fanout",  # All bound queues receive all messages
)
```

---

## Understanding Exchange Types

RabbitMQ supports four exchange types, each with different routing behaviors.

### Topic Exchange (Default)

Topic exchanges route messages based on routing key patterns with wildcards:

- `*` matches exactly one word
- `#` matches zero or more words

```
Routing Key Pattern Examples:

Event: Order.OrderCreated
  Matches: "Order.*"        (all Order events)
  Matches: "*.OrderCreated" (OrderCreated from any aggregate)
  Matches: "#"              (all events)

Event: Payment.PaymentReceived
  Matches: "Payment.*"      (all Payment events)
  Matches: "#.Received"     (events ending with Received)
```

```python
# Topic exchange with custom routing pattern
config = RabbitMQEventBusConfig(
    exchange_name="domain-events",
    exchange_type="topic",
    consumer_group="order-service",
    routing_key_pattern="Order.*",  # Only receive Order events
)
```

### Direct Exchange

Direct exchanges route messages to queues with exact routing key matches:

```python
# Direct exchange - each consumer type gets specific events
config = RabbitMQEventBusConfig(
    exchange_name="commands",
    exchange_type="direct",
    consumer_group="order-processor",
    routing_key_pattern="Order.OrderCreated",  # Exact match required
)
```

### Fanout Exchange

Fanout exchanges broadcast all messages to all bound queues, ignoring routing keys:

```python
# Fanout exchange - broadcast to all consumers
config = RabbitMQEventBusConfig(
    exchange_name="notifications",
    exchange_type="fanout",
    consumer_group="email-service",
    # routing_key_pattern is ignored for fanout
)

# All consumer groups bound to this exchange receive every message
```

Use fanout for:
- Notification broadcasting
- Audit logging (capture all events)
- Cache invalidation
- Development monitoring

See the [Fanout Exchange Guide](../guides/fanout-exchange.md) for detailed patterns.

---

## Creating and Connecting RabbitMQEventBus

### Basic Setup

```python
import asyncio
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str
    total: float

async def main():
    # Create configuration
    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="orders",
        consumer_group="order-handlers",
    )

    # Create bus with event registry
    bus = RabbitMQEventBus(config=config, event_registry=default_registry)

    # Connect to RabbitMQ
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
    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="orders",
        consumer_group="handlers",
    )

    async with RabbitMQEventBus(config=config, event_registry=default_registry) as bus:
        # Bus is connected here
        await bus.publish([event])
        # Bus is automatically disconnected on exit
```

### Connection Properties

Check the connection state:

```python
bus = RabbitMQEventBus(config=config, event_registry=default_registry)

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
event = OrderCreated(
    aggregate_id=uuid4(),
    customer_id="CUST-001",
    total=99.99,
    aggregate_version=1,
)

# Publish to RabbitMQ
await bus.publish([event])
```

### Routing Keys

Events are automatically assigned routing keys based on aggregate type and event type:

```python
# Event: OrderCreated with aggregate_type="Order"
# Routing Key: "Order.OrderCreated"

# Event: PaymentReceived with aggregate_type="Payment"
# Routing Key: "Payment.PaymentReceived"
```

This allows topic exchanges to route events to specific consumers based on patterns.

### Batch Publishing

For higher throughput, publish events in batches:

```python
# Create multiple events
events = [
    OrderCreated(
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

### Advanced Batch Publishing

For fine-grained control over batch operations:

```python
# Concurrent batch publishing (default) - maximum throughput
result = await bus.publish_batch(events, preserve_order=False)
print(f"Published {result['published']}/{result['total']} events")

# Sequential batch publishing - maintains strict ordering
result = await bus.publish_batch(events, preserve_order=True)
print(f"Published in order: {result['published']} events")
```

### Background Publishing

For fire-and-forget scenarios:

```python
# Returns immediately, doesn't wait for broker confirmation
await bus.publish([event], background=True)
```

**Note**: Background publishing is faster but provides weaker delivery guarantees. Use with caution.

---

## Subscribing Handlers

Subscribe handlers to receive events when consuming.

### Function Handlers

```python
async def handle_order_created(event: DomainEvent) -> None:
    if isinstance(event, OrderCreated):
        print(f"Processing order {event.aggregate_id}: ${event.total}")

# Subscribe to specific event type
bus.subscribe(OrderCreated, handle_order_created)
```

### Class-Based Handlers

```python
class OrderNotificationHandler:
    def __init__(self):
        self.notifications_sent = 0

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            await self.send_confirmation_email(event)
            self.notifications_sent += 1

    async def send_confirmation_email(self, event: OrderCreated):
        print(f"Sending email for order {event.aggregate_id}")

handler = OrderNotificationHandler()
bus.subscribe(OrderCreated, handler)
```

### Projection Handlers

Use `subscribe_all()` for projections that handle multiple event types:

```python
from eventsource import DeclarativeProjection, handles

class OrderProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.orders: dict = {}

    @handles(OrderCreated)
    async def _on_created(self, event: OrderCreated) -> None:
        self.orders[str(event.aggregate_id)] = {
            "customer": event.customer_id,
            "total": event.total,
            "status": "created",
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

For cross-cutting concerns like audit logging or metrics:

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
bus.subscribe(OrderCreated, handle_order)

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
async def consume_for_duration(bus: RabbitMQEventBus, seconds: float):
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

RabbitMQ event bus automatically reconnects on connection loss using aio-pika's robust connection:

```python
# Auto-reconnect is enabled by default via RobustConnection
# The bus will automatically:
# - Reconnect on connection loss
# - Re-declare exchanges and queues
# - Resume consuming (if was consuming before disconnect)
```

---

## Consumer Groups and Queue Bindings

Consumer groups enable multiple services to receive and process events independently.

### How Consumer Groups Work

```
Exchange: orders (topic)

Consumer Group: "order-projections"
  Queue: orders.order-projections
  - Instance 1: handles messages (competing consumers)
  - Instance 2: handles messages (load balanced)

Consumer Group: "analytics"
  Queue: orders.analytics
  - Instance 1: handles ALL messages independently

Each message is delivered to ONE consumer per group.
Different groups receive ALL messages independently.
```

### Setting Up Multiple Consumer Groups

```python
# Order processing service
order_config = RabbitMQEventBusConfig(
    exchange_name="domain-events",
    exchange_type="topic",
    consumer_group="order-service",
    routing_key_pattern="Order.*",  # Only Order events
)

# Analytics service
analytics_config = RabbitMQEventBusConfig(
    exchange_name="domain-events",
    exchange_type="topic",
    consumer_group="analytics",
    routing_key_pattern="#",  # All events
)

# Notification service
notification_config = RabbitMQEventBusConfig(
    exchange_name="domain-events",
    exchange_type="topic",
    consumer_group="notifications",
    routing_key_pattern="*.Created",  # All *Created events
)
```

### Competing Consumers (Load Balancing)

Multiple instances with the same consumer group share the workload:

```python
# Worker 1
config_1 = RabbitMQEventBusConfig(
    exchange_name="orders",
    consumer_group="order-processors",  # Same group
    consumer_name="worker-1",           # Unique name
)

# Worker 2
config_2 = RabbitMQEventBusConfig(
    exchange_name="orders",
    consumer_group="order-processors",  # Same group
    consumer_name="worker-2",           # Unique name
)

# Messages are distributed between workers
```

### Custom Queue Bindings

Bind to specific routing keys programmatically:

```python
await bus.connect()

# Bind to additional routing patterns
await bus.bind_routing_key("Order.*")
await bus.bind_routing_key("Payment.PaymentReceived")

# Or bind for specific event types
await bus.bind_event_type(OrderCreated)
await bus.bind_event_type(OrderShipped)
```

---

## Dead Letter Queue (DLQ)

Failed messages are automatically routed to a dead letter queue for investigation.

### How DLQ Works

```
Message Flow:

1. Message arrives at main queue
2. Handler processes message
3. On success: Message acknowledged (removed from queue)
4. On failure: Message requeued with incremented retry count
5. After max_retries: Message sent to DLQ with error metadata

DLQ Naming:
  Main exchange: "events"
  DLQ exchange:  "events_dlq"
  DLQ queue:     "events.consumer-group.dlq"
```

### DLQ Configuration

```python
config = RabbitMQEventBusConfig(
    exchange_name="orders",
    consumer_group="handlers",

    # DLQ settings
    enable_dlq=True,
    max_retries=3,                    # Retries before DLQ
    dlq_exchange_suffix="_dlq",       # DLQ exchange suffix
    dlq_message_ttl=86400000,         # Message TTL in DLQ (24 hours in ms)
    dlq_max_length=10000,             # Max messages in DLQ

    # Retry settings
    retry_base_delay=1.0,             # Initial retry delay (seconds)
    retry_max_delay=60.0,             # Max retry delay
    retry_jitter=0.1,                 # Jitter to prevent thundering herd
)
```

### Monitoring DLQ

```python
# Get number of messages in DLQ
count = await bus.get_dlq_message_count()
print(f"DLQ has {count} messages")

# Get DLQ messages for inspection
messages = await bus.get_dlq_messages(limit=10)
for msg in messages:
    print(f"Message {msg.message_id}: {msg.event_type}")
    print(f"  Failed due to: {msg.dlq_reason}")
    print(f"  Error type: {msg.dlq_error_type}")
    print(f"  Retries: {msg.dlq_retry_count}")
```

### Replaying DLQ Messages

After fixing the underlying issue, replay messages from DLQ:

```python
# Replay a specific message
success = await bus.replay_dlq_message("message-id-here")
if success:
    print("Message replayed successfully")

# Purge all DLQ messages (use with caution!)
purged_count = await bus.purge_dlq()
print(f"Purged {purged_count} messages from DLQ")
```

---

## SSL/TLS Configuration

For production deployments, configure SSL/TLS for secure connections.

### Basic TLS

```python
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://user:pass@rabbitmq.example.com:5671/",
    exchange_name="secure-events",
    consumer_group="secure-handlers",
    # TLS is automatically enabled for amqps:// URLs
)
```

### Custom CA Certificate

```python
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://rabbitmq.internal:5671/",
    exchange_name="events",
    consumer_group="handlers",
    ca_file="/path/to/ca.crt",  # Custom CA certificate
)
```

### Mutual TLS (mTLS)

For client certificate authentication:

```python
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://rabbitmq.internal:5671/",
    exchange_name="events",
    consumer_group="handlers",
    ca_file="/path/to/ca.crt",
    cert_file="/path/to/client.crt",
    key_file="/path/to/client.key",
)
```

### Custom SSL Context

For full control over SSL configuration:

```python
import ssl

# Create custom SSL context
ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ssl_context.load_verify_locations("/path/to/ca.crt")
ssl_context.load_cert_chain(
    certfile="/path/to/client.crt",
    keyfile="/path/to/client.key",
)

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://rabbitmq.internal:5671/",
    exchange_name="events",
    consumer_group="handlers",
    ssl_context=ssl_context,
)
```

### Disable Certificate Verification (Development Only)

```python
# WARNING: Only for development - NOT recommended for production
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://localhost:5671/",
    exchange_name="events",
    consumer_group="handlers",
    verify_ssl=False,  # Disables certificate verification
)
```

---

## Statistics and Monitoring

Monitor event bus operations using built-in statistics.

### Getting Statistics

```python
# Property access
stats = bus.stats
print(f"Published: {stats.events_published}")
print(f"Consumed: {stats.events_consumed}")
print(f"Failed: {stats.events_processed_failed}")
print(f"Sent to DLQ: {stats.messages_sent_to_dlq}")

# Dictionary format (for JSON serialization)
stats_dict = bus.get_stats_dict()
import json
print(json.dumps(stats_dict, indent=2))
```

### Available Statistics

| Statistic | Description |
|-----------|-------------|
| `events_published` | Total events published |
| `events_consumed` | Total events consumed |
| `events_processed_success` | Successfully processed events |
| `events_processed_failed` | Failed event processing |
| `messages_sent_to_dlq` | Messages routed to DLQ |
| `handler_errors` | Handler execution errors |
| `reconnections` | Number of reconnections |
| `batch_publishes` | Batch publish operations |
| `last_publish_at` | Timestamp of last publish |
| `last_consume_at` | Timestamp of last consume |
| `connected_at` | Connection timestamp |

### Queue Information

```python
# Get queue info
info = await bus.get_queue_info()
print(f"Queue: {info.name}")
print(f"Messages: {info.message_count}")
print(f"Consumers: {info.consumer_count}")
print(f"State: {info.state}")
```

### Health Checks

```python
result = await bus.health_check()
if result.healthy:
    print("RabbitMQ event bus is healthy")
else:
    print(f"Unhealthy: {result.error}")
    print(f"Connection: {result.connection_status}")
    print(f"Channel: {result.channel_status}")
    print(f"Queue: {result.queue_status}")
```

---

## Complete Example

Here is a comprehensive example demonstrating RabbitMQ event bus usage:

```python
"""
Tutorial 19: Using RabbitMQ Event Bus

This example demonstrates AMQP-based event distribution.
Run with: python tutorial_19_rabbitmq.py

Prerequisites:
- RabbitMQ running on localhost:5672
- pip install eventsource-py[rabbitmq]
"""
import asyncio
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry

# Check RabbitMQ availability
from eventsource.bus.rabbitmq import (
    RABBITMQ_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)

if not RABBITMQ_AVAILABLE:
    print("RabbitMQ not available. Install with: pip install eventsource-py[rabbitmq]")
    exit(1)


# =============================================================================
# Events
# =============================================================================

@register_event
class InventoryUpdated(DomainEvent):
    event_type: str = "InventoryUpdated"
    aggregate_type: str = "Inventory"
    product_id: str
    quantity_change: int


@register_event
class OrderFulfilled(DomainEvent):
    event_type: str = "OrderFulfilled"
    aggregate_type: str = "Order"
    customer_id: str


@register_event
class ShipmentCreated(DomainEvent):
    event_type: str = "ShipmentCreated"
    aggregate_type: str = "Shipment"
    order_id: str
    carrier: str


# =============================================================================
# Event Handlers
# =============================================================================

class InventoryHandler:
    """Handles inventory events."""

    def __init__(self):
        self.updates: list[dict] = []

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, InventoryUpdated):
            self.updates.append({
                "product": event.product_id,
                "change": event.quantity_change,
            })
            print(f"  [Inventory] {event.product_id}: {event.quantity_change:+d}")


class FulfillmentHandler:
    """Handles order fulfillment events."""

    def __init__(self):
        self.orders_fulfilled = 0

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderFulfilled):
            self.orders_fulfilled += 1
            print(f"  [Fulfillment] Order fulfilled for {event.customer_id}")


class ShippingHandler:
    """Handles shipping events."""

    def __init__(self):
        self.shipments: list[str] = []

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, ShipmentCreated):
            self.shipments.append(event.order_id)
            print(f"  [Shipping] Shipment via {event.carrier} for order {event.order_id}")


# =============================================================================
# RabbitMQ Event Bus Examples
# =============================================================================

async def basic_rabbitmq_example():
    """Demonstrate basic RabbitMQ event bus usage."""
    print("=== Basic RabbitMQ Event Bus ===\n")

    # Create configuration
    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="tutorial_events",
        exchange_type="topic",
        consumer_group="demo-handlers",
        prefetch_count=10,
        enable_dlq=True,
        enable_tracing=True,
    )

    print(f"Exchange: {config.exchange_name} (type: {config.exchange_type})")
    print(f"Queue: {config.queue_name}")
    print(f"Consumer group: {config.consumer_group}")

    # Create bus
    bus = RabbitMQEventBus(config=config, event_registry=default_registry)

    # Create handlers
    inventory_handler = InventoryHandler()
    fulfillment_handler = FulfillmentHandler()
    shipping_handler = ShippingHandler()

    # Subscribe handlers
    bus.subscribe(InventoryUpdated, inventory_handler)
    bus.subscribe(OrderFulfilled, fulfillment_handler)
    bus.subscribe(ShipmentCreated, shipping_handler)

    try:
        # Connect to RabbitMQ
        print("\nConnecting to RabbitMQ...")
        await bus.connect()
        print("Connected!")

        # Publish events
        print("\nPublishing events...")

        await bus.publish([
            InventoryUpdated(
                aggregate_id=uuid4(),
                product_id="SKU-001",
                quantity_change=-5,
                aggregate_version=1,
            ),
        ])

        await bus.publish([
            OrderFulfilled(
                aggregate_id=uuid4(),
                customer_id="CUST-123",
                aggregate_version=1,
            ),
        ])

        await bus.publish([
            ShipmentCreated(
                aggregate_id=uuid4(),
                order_id="ORD-456",
                carrier="FedEx",
                aggregate_version=1,
            ),
        ])

        print("Events published!")

        # Start consuming
        print("\nConsuming events...")
        consume_task = asyncio.create_task(bus.start_consuming())

        # Wait for processing
        await asyncio.sleep(3)

        # Stop consuming
        await bus.stop_consuming()
        consume_task.cancel()
        try:
            await consume_task
        except asyncio.CancelledError:
            pass

        print(f"\nResults:")
        print(f"  Inventory updates: {len(inventory_handler.updates)}")
        print(f"  Orders fulfilled: {fulfillment_handler.orders_fulfilled}")
        print(f"  Shipments created: {len(shipping_handler.shipments)}")

        # Show statistics
        stats = bus.get_stats_dict()
        print(f"\nStatistics:")
        print(f"  Published: {stats['events_published']}")
        print(f"  Consumed: {stats['events_consumed']}")
        print(f"  Success: {stats['events_processed_success']}")

    finally:
        await bus.disconnect()
        print("\nDisconnected from RabbitMQ")


async def exchange_types_example():
    """Explain exchange types."""
    print("\n=== Exchange Types ===\n")

    print("RabbitMQ supports several exchange types:\n")

    print("1. TOPIC (default in eventsource)")
    print("   - Routes based on routing key patterns")
    print("   - Supports wildcards: * (one word), # (zero or more)")
    print("   - Example: 'Order.*' matches 'Order.OrderCreated', 'Order.OrderShipped'")
    print()

    print("2. FANOUT")
    print("   - Broadcasts to all bound queues")
    print("   - Ignores routing key")
    print("   - Use for: notifications, audit logging, cache invalidation")
    print()

    print("3. DIRECT")
    print("   - Routes based on exact routing key match")
    print("   - Use for: specific service targeting, work queues")
    print()

    print("4. HEADERS")
    print("   - Routes based on message headers")
    print("   - Most flexible but complex")


async def dead_letter_example():
    """Demonstrate DLQ concepts."""
    print("\n=== Dead Letter Queue ===\n")

    print("When enable_dlq=True, failed messages are routed to DLQ:\n")
    print("Flow:")
    print("  1. Message arrives at main queue")
    print("  2. Handler throws exception")
    print("  3. Message requeued with retry count incremented")
    print("  4. After max_retries (default: 3), message goes to DLQ")
    print()
    print("DLQ naming:")
    print("  Main exchange: 'events'")
    print("  DLQ exchange:  'events_dlq'")
    print("  Main queue:    'events.my-service'")
    print("  DLQ queue:     'events.my-service.dlq'")
    print()
    print("DLQ message headers include:")
    print("  - x-dlq-reason: Error message")
    print("  - x-dlq-error-type: Exception class name")
    print("  - x-dlq-retry-count: Retries before DLQ")
    print("  - x-dlq-timestamp: When sent to DLQ")
    print()
    print("Monitor DLQ for investigation and manual replay.")


async def main():
    print("Tutorial 19: RabbitMQ Event Bus\n")
    print("=" * 60)

    try:
        await basic_rabbitmq_example()
        await exchange_types_example()
        await dead_letter_example()
    except Exception as e:
        print(f"\nError: {e}")
        print("Make sure RabbitMQ is running:")
        print("  docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management")

    print("\n" + "=" * 60)
    print("Tutorial complete!")


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_19_rabbitmq.py` and run it:

```bash
python tutorial_19_rabbitmq.py
```

---

## Exercises

Now it is time to practice what you have learned!

### Exercise 1: Event-Driven Microservices

**Objective:** Build a microservices communication layer with RabbitMQ using topic-based routing.

**Time:** 25-30 minutes

**Requirements:**

1. Configure `RabbitMQEventBus` with topic exchange
2. Create events for Customer, Order, and Payment domains
3. Create handlers for each service that only receive relevant events
4. Use routing patterns to filter events by aggregate type
5. Publish events and verify routing works correctly

**Starter Code:**

```python
"""
Tutorial 19 - Exercise 1: Event-Driven Microservices

Your task: Build a microservices layer with topic-based routing.
"""
import asyncio
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry
from eventsource.bus.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RABBITMQ_AVAILABLE,
)

if not RABBITMQ_AVAILABLE:
    print("Install rabbitmq: pip install eventsource-py[rabbitmq]")
    exit(1)


# Step 1: Define events for different domains
# TODO: Create CustomerCreated, OrderCreated, PaymentReceived events


# Step 2: Create service handlers
# TODO: CustomerService, OrderService, PaymentService handlers


async def main():
    print("=== Exercise 19-1: Event-Driven Microservices ===\n")

    # Step 3: Configure RabbitMQ with topic exchange
    # TODO: Create RabbitMQEventBusConfig

    # Step 4: Create bus and subscribe handlers
    # TODO: Subscribe each service to its relevant events

    # Step 5: Publish events from different domains
    # TODO: Publish CustomerCreated, OrderCreated, PaymentReceived

    # Step 6: Verify routing
    # TODO: Check that each service received only its events


if __name__ == "__main__":
    asyncio.run(main())
```

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 19 - Exercise 1 Solution: Event-Driven Microservices
"""
import asyncio
from uuid import uuid4

from eventsource import DomainEvent, register_event, default_registry
from eventsource.bus.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RABBITMQ_AVAILABLE,
)

if not RABBITMQ_AVAILABLE:
    print("Install rabbitmq: pip install eventsource-py[rabbitmq]")
    exit(1)


# Step 1: Define events for different domains
@register_event
class CustomerCreated(DomainEvent):
    event_type: str = "CustomerCreated"
    aggregate_type: str = "Customer"
    name: str
    email: str


@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str
    total: float


@register_event
class PaymentReceived(DomainEvent):
    event_type: str = "PaymentReceived"
    aggregate_type: str = "Payment"
    order_id: str
    amount: float


# Step 2: Create service handlers
class CustomerService:
    def __init__(self):
        self.customers = {}

    async def handle(self, event: DomainEvent):
        if isinstance(event, CustomerCreated):
            self.customers[str(event.aggregate_id)] = event.name
            print(f"  [CustomerService] Registered: {event.name}")


class OrderService:
    def __init__(self):
        self.orders = []

    async def handle(self, event: DomainEvent):
        if isinstance(event, OrderCreated):
            self.orders.append(str(event.aggregate_id))
            print(f"  [OrderService] Order ${event.total:.2f} for {event.customer_id}")


class PaymentService:
    def __init__(self):
        self.payments = []

    async def handle(self, event: DomainEvent):
        if isinstance(event, PaymentReceived):
            self.payments.append(str(event.aggregate_id))
            print(f"  [PaymentService] Payment ${event.amount:.2f} for order {event.order_id}")


async def main():
    print("=== Exercise 19-1: Event-Driven Microservices ===\n")

    # Step 3: Configure RabbitMQ with topic exchange
    print("Step 1: Configuring RabbitMQ bus with topic exchange...")
    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="microservices",
        exchange_type="topic",
        consumer_group="exercise19",
        prefetch_count=10,
    )
    bus = RabbitMQEventBus(config=config, event_registry=default_registry)
    print(f"   Exchange: {config.exchange_name}")
    print(f"   Queue: {config.queue_name}")

    # Step 4: Create bus and subscribe handlers
    print("\nStep 2: Setting up service handlers...")
    customer_svc = CustomerService()
    order_svc = OrderService()
    payment_svc = PaymentService()

    # Each service subscribes to relevant events
    bus.subscribe(CustomerCreated, customer_svc)
    bus.subscribe(OrderCreated, order_svc)
    bus.subscribe(PaymentReceived, payment_svc)
    print("   Services subscribed!")

    await bus.connect()

    # Step 5: Publish events from different domains
    print("\nStep 3: Publishing events...")

    customer_id = uuid4()
    await bus.publish([CustomerCreated(
        aggregate_id=customer_id,
        name="Alice Smith",
        email="alice@example.com",
        aggregate_version=1,
    )])
    print("   Published: CustomerCreated")

    order_id = uuid4()
    await bus.publish([OrderCreated(
        aggregate_id=order_id,
        customer_id=str(customer_id),
        total=150.0,
        aggregate_version=1,
    )])
    print("   Published: OrderCreated")

    payment_id = uuid4()
    await bus.publish([PaymentReceived(
        aggregate_id=payment_id,
        order_id=str(order_id),
        amount=150.0,
        aggregate_version=1,
    )])
    print("   Published: PaymentReceived")

    # Consume events
    print("\nStep 4: Consuming events...")
    consume_task = asyncio.create_task(bus.start_consuming())
    await asyncio.sleep(3)
    await bus.stop_consuming()
    consume_task.cancel()
    try:
        await consume_task
    except asyncio.CancelledError:
        pass

    # Step 6: Verify routing
    print("\nStep 5: Verifying service isolation:")
    print(f"   CustomerService customers: {len(customer_svc.customers)}")
    print(f"   OrderService orders: {len(order_svc.orders)}")
    print(f"   PaymentService payments: {len(payment_svc.payments)}")

    await bus.disconnect()
    print("\n=== Exercise Complete! ===")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

The solution file is available at: `docs/tutorials/exercises/solutions/19-1.py`

---

## Summary

In this tutorial, you learned:

- **RabbitMQ provides flexible message routing** via topic, direct, fanout, and header exchanges
- **Topic exchanges** use routing key patterns with wildcards (* and #) for flexible routing
- **Consumer groups** enable independent processing with dedicated queues per service
- **Dead letter queues** capture failed messages for investigation and replay
- **SSL/TLS configuration** secures production deployments with certificates
- **At-least-once delivery** means handlers should be designed to handle duplicates

---

## Event Bus Comparison Summary

| Aspect | In-Memory | Redis | Kafka | RabbitMQ |
|--------|-----------|-------|-------|----------|
| **Scale** | Single process | Moderate | High | Moderate |
| **Routing** | Direct | Stream | Partition | Flexible |
| **Durability** | None | Optional | High | High |
| **Complexity** | Low | Low | High | Medium |
| **Best For** | Testing | Caching + events | High volume | Complex routing |

---

## Key Takeaways

!!! note "Remember"
    - Always `connect()` before publishing or consuming
    - Choose exchange type based on your routing needs
    - Use topic exchanges for pattern-based routing
    - Acknowledge messages only after successful processing
    - Design handlers to be idempotent for at-least-once delivery

!!! tip "Best Practice"
    For production deployments:
    - Enable SSL/TLS for secure connections
    - Set appropriate prefetch_count for your workload
    - Monitor DLQ for failed messages
    - Use durable exchanges and queues
    - Configure reasonable retry delays to prevent overwhelming

!!! warning "Common Mistakes"
    - Not enabling DLQ (failed messages are lost)
    - Setting prefetch_count too high (memory issues)
    - Ignoring routing key patterns (receiving unwanted messages)
    - Not handling duplicate messages (at-least-once delivery)

---

## Next Steps

You now understand how to use RabbitMQ for flexible event distribution. In the next tutorial, you will learn about observability with OpenTelemetry.

Continue to [Tutorial 20: Observability with OpenTelemetry](20-observability.md) to learn about monitoring and tracing your event-sourced system.

---

## Related Documentation

- [Fanout Exchange Guide](../guides/fanout-exchange.md) - Broadcast messaging patterns
- [Event Bus API Reference](../api/bus.md) - Complete API documentation
- [Tutorial 7: Event Bus](07-event-bus.md) - Event bus basics
- [Tutorial 17: Redis Event Bus](17-redis.md) - Alternative distributed bus
- [Tutorial 18: Kafka Event Bus](18-kafka.md) - High-throughput event streaming
