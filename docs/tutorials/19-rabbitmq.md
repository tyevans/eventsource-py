# Tutorial 19: RabbitMQ Event Bus - Flexible Message Routing

**Difficulty:** Advanced

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 7: Event Bus - Distributing Events Across Your System](07-event-bus.md)
- [Tutorial 17: Redis Event Bus - Distributed Event Distribution](17-redis.md)
- [Tutorial 18: Kafka Event Bus - High-Throughput Event Streaming](18-kafka.md)
- [Tutorial 11: PostgreSQL Event Store](11-postgresql.md)
- [Tutorial 13: Subscription Management](13-subscriptions.md)
- Python 3.10 or higher
- Understanding of async/await
- Basic AMQP/RabbitMQ knowledge helpful but not required
- Docker for running RabbitMQ locally

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain why RabbitMQ is ideal for flexible message routing patterns
2. Install and configure RabbitMQ for event distribution
3. Set up RabbitMQEventBus with proper configuration
4. Understand exchange types (topic, direct, fanout) and routing keys
5. Implement consumer groups with queue bindings
6. Use dead letter queues for failed messages
7. Configure retry policies with exponential backoff
8. Monitor RabbitMQ event bus health and statistics
9. Deploy with SSL/TLS for production security
10. Choose between Redis, RabbitMQ, and Kafka event buses

---

## What is RabbitMQ Event Bus?

The **RabbitMQEventBus** is a distributed event bus implementation that uses **RabbitMQ** (AMQP protocol) for flexible routing and reliable message delivery across multiple processes and servers. While `RedisEventBus` (from Tutorial 17) provides simple stream-based distribution and `KafkaEventBus` (from Tutorial 18) handles ultra-high throughput, RabbitMQEventBus excels at:

- **Flexible routing**: Topic patterns, direct routing, fanout broadcasting, header-based routing
- **Enterprise messaging**: Battle-tested broker used by thousands of companies
- **At-least-once delivery**: Messages acknowledged only after successful processing
- **Durable persistence**: Events replicated and survive broker restarts
- **Consumer groups**: Automatic load balancing across workers via queue bindings
- **Dead letter queues**: Configurable retry policies with automatic DLQ routing
- **Management UI**: Built-in web interface for monitoring and debugging

---

## Why RabbitMQ?

RabbitMQ is a mature message broker that implements the AMQP protocol. It's designed for reliability and flexible routing patterns:

| Feature | Benefit |
|---------|---------|
| **Exchange Types** | Route messages with topic patterns, direct routing, fanout, or headers |
| **Consumer Groups** | Multiple consumers share workload via queue bindings |
| **Durability** | Messages and queues persist across broker restarts |
| **Publisher Confirms** | Guaranteed delivery with broker acknowledgments |
| **Prefetch Control** | Prevent overwhelming slow consumers with QoS settings |
| **Dead Letter Exchanges** | Automatic routing of failed messages for later analysis |
| **Management Plugin** | Web UI for monitoring queues, consumers, and message rates |
| **SSL/TLS Support** | Mutual TLS for secure production deployments |

### Comparing Event Bus Implementations

| Implementation | Throughput | Routing | Durability | Complexity | Best For |
|----------------|------------|---------|------------|------------|----------|
| **InMemoryEventBus** | Low | None | None | Simple | Single instance, testing |
| **RedisEventBus** | Medium (10k/s) | Stream-based | Good | Medium | Multi-instance, moderate scale |
| **RabbitMQEventBus** | Medium (10k/s) | Very Flexible | Excellent | Medium | Complex routing, enterprise messaging |
| **KafkaEventBus** | Very High (100k+/s) | Partition-based | Excellent | High | Event streaming, high throughput |

### When to Use RabbitMQ Event Bus

**Good for:**
- Enterprise applications requiring flexible routing
- Systems needing complex message routing patterns
- Applications requiring strong delivery guarantees
- Multi-tenant systems with per-tenant routing
- RPC-style request/reply patterns
- When operational maturity matters (RabbitMQ is very stable)
- Teams familiar with traditional message queuing

**Less suitable for:**
- Single-instance applications (use InMemoryEventBus)
- Ultra-high throughput (>50k events/sec - consider Kafka)
- Simple pub/sub without routing (Redis may be simpler)
- When operational complexity is a major concern

---

## Installation

Install RabbitMQ support for eventsource-py:

```bash
pip install eventsource-py[rabbitmq]
```

This installs the `aio-pika` package with async support for RabbitMQ.

### Verify Installation

```python
from eventsource.bus import RABBITMQ_AVAILABLE

if RABBITMQ_AVAILABLE:
    print("RabbitMQ support is available!")
else:
    print("RabbitMQ not available. Install with: pip install eventsource-py[rabbitmq]")
```

---

## Setting Up RabbitMQ

### Using Docker

Start RabbitMQ with management plugin in a Docker container:

```bash
# Start RabbitMQ with management UI
docker run -d \
  --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management-alpine

# Verify RabbitMQ is running
docker exec rabbitmq rabbitmq-diagnostics check_running
# Output: Checking if RabbitMQ is running... yes
```

Access the management UI at http://localhost:15672 (login: guest/guest)

### Using Docker Compose

Create `docker-compose.yml` for local development:

```yaml
version: '3.8'

services:
  rabbitmq:
    image: rabbitmq:3-management-alpine
    container_name: rabbitmq
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

Start RabbitMQ:

```bash
docker compose up -d

# Check logs
docker compose logs -f rabbitmq
```

---

## Basic RabbitMQEventBus Setup

### Minimal Configuration

```python
from eventsource import EventRegistry, DomainEvent, register_event
from eventsource.bus import RabbitMQEventBus, RabbitMQEventBusConfig

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: str
    total_amount: float

# Create event registry
registry = EventRegistry()
registry.register(OrderPlaced)

# Configure RabbitMQ event bus
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",
    consumer_group="projections",
)

# Create bus
bus = RabbitMQEventBus(config=config, event_registry=registry)

# Connect to RabbitMQ
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

## RabbitMQEventBusConfig Options

### Connection Settings

```python
config = RabbitMQEventBusConfig(
    # RabbitMQ connection
    rabbitmq_url="amqp://guest:guest@localhost:5672/",  # AMQP connection URL

    # Exchange and queue naming
    exchange_name="events",                              # Exchange name
    consumer_group="projections",                        # Queue group name
    consumer_name=None,                                  # Auto-generated: hostname-uuid
)
```

**URL format:** `amqp://[username]:[password]@[host]:[port]/[vhost]`

- Standard AMQP: `amqp://guest:guest@localhost:5672/`
- Secure AMQPS: `amqps://user:pass@rabbitmq.example.com:5671/`
- Custom vhost: `amqp://guest:guest@localhost:5672/myapp`

### Exchange Types

```python
config = RabbitMQEventBusConfig(
    # Exchange type determines routing behavior
    exchange_type="topic",                               # "topic" | "direct" | "fanout" | "headers"

    # Topic exchange (default) - pattern matching
    routing_key_pattern="#",                             # "#" = all, "Order.*" = Order events only

    # Direct exchange - exact match required
    # routing_key_pattern="OrderPlaced",

    # Fanout exchange - broadcast to all (ignores routing keys)
    # routing_key_pattern="#",
)
```

**Exchange types explained:**

- **topic** (default): Routes based on routing key patterns with wildcards
  - `*` matches exactly one word: `Order.*` matches `Order.OrderPlaced`
  - `#` matches zero or more words: `Order.#` matches `Order.OrderPlaced.v1`
- **direct**: Routes to queues with exact routing key match
- **fanout**: Broadcasts to all bound queues (ignores routing key)
- **headers**: Routes based on message header attributes (advanced)

### Consumer Settings

```python
config = RabbitMQEventBusConfig(
    # Flow control
    prefetch_count=10,                                   # Max unacknowledged messages

    # Connection stability
    heartbeat=60,                                        # Heartbeat interval (seconds)
    reconnect_delay=1.0,                                 # Initial reconnection delay
    max_reconnect_delay=30.0,                            # Max reconnection delay
)
```

**prefetch_count** controls how many messages RabbitMQ delivers before waiting for acknowledgments. Lower values provide better load balancing, higher values improve throughput.

### Durability Settings

```python
config = RabbitMQEventBusConfig(
    # Persistence
    durable=True,                                        # Survive broker restarts
    auto_delete=False,                                   # Don't delete when consumers disconnect
)
```

**Production:** Always set `durable=True` and `auto_delete=False` to prevent message loss.

**Testing:** Use `durable=False` and `auto_delete=True` for easier cleanup.

### Retry and DLQ Settings

```python
config = RabbitMQEventBusConfig(
    # Retry policy
    max_retries=3,                                       # Retries before DLQ
    retry_base_delay=1.0,                                # Base delay (seconds)
    retry_max_delay=60.0,                                # Max delay (seconds)
    retry_jitter=0.1,                                    # 10% jitter to prevent thundering herd

    # Dead letter queue
    enable_dlq=True,                                     # Enable DLQ
    dlq_exchange_suffix="_dlq",                          # DLQ exchange suffix
    dlq_message_ttl=86400000,                            # DLQ message TTL (24h in milliseconds)
    dlq_max_length=None,                                 # No limit on DLQ size
)
```

**Exponential Backoff Formula:**
```python
delay = min(retry_base_delay * (2 ** attempt), retry_max_delay)
delay *= (1 + random.uniform(-retry_jitter, retry_jitter))
```

### Batch Publishing

```python
config = RabbitMQEventBusConfig(
    # Batch optimization
    batch_size=100,                                      # Events per batch chunk
    max_concurrent_publishes=10,                         # Concurrent publishes per chunk
)
```

### Observability Settings

```python
config = RabbitMQEventBusConfig(
    enable_tracing=True,                                 # OpenTelemetry tracing
    shutdown_timeout=30.0,                               # Graceful shutdown timeout
)
```

### Queue Naming

RabbitMQEventBus creates queues and exchanges based on your configuration:

```python
config = RabbitMQEventBusConfig(
    exchange_name="myapp.events",
    consumer_group="order-projections",
)

# Main exchange: myapp.events
# Main queue:    myapp.events.order-projections
# DLQ exchange:  myapp.events_dlq
# DLQ queue:     myapp.events.order-projections.dlq
```

Access names programmatically:

```python
print(config.queue_name)         # myapp.events.order-projections
print(config.dlq_exchange_name)  # myapp.events_dlq
print(config.dlq_queue_name)     # myapp.events.order-projections.dlq
```

---

## Exchange Types and Routing

RabbitMQ's flexibility comes from its exchange types. Each provides different routing semantics.

### Topic Exchange (Default)

Routes messages based on routing key patterns with wildcards:

```python
config = RabbitMQEventBusConfig(
    exchange_type="topic",
    routing_key_pattern="#",                             # Receive all events
)

# RabbitMQEventBus automatically generates routing keys:
# Format: {aggregate_type}.{event_type}
# Examples:
#   Order.OrderPlaced
#   Order.OrderShipped
#   Payment.PaymentReceived
```

**Pattern matching:**

```python
# Receive all Order events
config = RabbitMQEventBusConfig(
    exchange_type="topic",
    routing_key_pattern="Order.*",
)

# Receive all events
config = RabbitMQEventBusConfig(
    exchange_type="topic",
    routing_key_pattern="#",
)

# Receive specific event type
config = RabbitMQEventBusConfig(
    exchange_type="topic",
    routing_key_pattern="*.OrderPlaced",
)
```

### Direct Exchange

Routes to queues with exact routing key match:

```python
config = RabbitMQEventBusConfig(
    exchange_type="direct",
    routing_key_pattern="Order.OrderPlaced",             # Only this event type
)
```

**Use cases:**
- Send events to specific services
- Work queue patterns
- Point-to-point messaging

### Fanout Exchange

Broadcasts to all bound queues (ignores routing key):

```python
config = RabbitMQEventBusConfig(
    exchange_type="fanout",
    # routing_key_pattern is ignored for fanout
)
```

**Use cases:**
- Broadcast notifications to all services
- Cache invalidation across all instances
- Audit logging that needs every event

### Dynamic Routing Key Bindings

Add bindings at runtime to receive additional event types:

```python
await bus.connect()

# Add binding for specific event pattern
await bus.bind_routing_key("Payment.*")

# Add binding for specific event type
await bus.bind_event_type(PaymentReceived)
```

---

## Consumer Groups and Load Balancing

Consumer groups enable automatic load balancing: RabbitMQ distributes messages from a queue across all connected consumers.

### Single Consumer Group (Load Balancing)

```python
# Worker 1
config1 = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",
    consumer_group="order-processors",
    consumer_name="worker-1",
)
bus1 = RabbitMQEventBus(config=config1, event_registry=registry)
await bus1.connect()

# Worker 2 (same group = same queue)
config2 = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",
    consumer_group="order-processors",
    consumer_name="worker-2",
)
bus2 = RabbitMQEventBus(config=config2, event_registry=registry)
await bus2.connect()

# Subscribe both workers
bus1.subscribe(OrderPlaced, worker1_handler)
bus2.subscribe(OrderPlaced, worker2_handler)

# Start consuming
task1 = asyncio.create_task(bus1.start_consuming())
task2 = asyncio.create_task(bus2.start_consuming())

# RabbitMQ automatically distributes messages:
# - Worker 1 receives some messages
# - Worker 2 receives other messages
# Each message delivered to exactly one consumer
```

**Load balancing behavior:**
- RabbitMQ uses round-robin distribution by default
- Respects `prefetch_count` for flow control
- If one worker is slow, fast workers get more messages
- Worker failures don't lose messages (redelivered to other workers)

### Multiple Consumer Groups (Fan-Out)

Different consumer groups all receive the same events:

```python
# Projection consumer group
projection_config = RabbitMQEventBusConfig(
    exchange_name="events",
    consumer_group="projections",
)
projection_bus = RabbitMQEventBus(config=projection_config, event_registry=registry)

# Notification consumer group
notification_config = RabbitMQEventBusConfig(
    exchange_name="events",
    consumer_group="notifications",
)
notification_bus = RabbitMQEventBus(config=notification_config, event_registry=registry)

# Analytics consumer group
analytics_config = RabbitMQEventBusConfig(
    exchange_name="events",
    consumer_group="analytics",
)
analytics_bus = RabbitMQEventBus(config=analytics_config, event_registry=registry)

# Each group processes ALL events independently
projection_bus.subscribe(OrderPlaced, update_projection)
notification_bus.subscribe(OrderPlaced, send_notification)
analytics_bus.subscribe(OrderPlaced, track_metrics)
```

**Result:** Each consumer group has its own queue and processes every event from the exchange.

---

## Dead Letter Queue (DLQ)

Messages that fail after `max_retries` are automatically sent to the DLQ for later analysis.

### DLQ Configuration

```python
config = RabbitMQEventBusConfig(
    enable_dlq=True,                                     # Enable DLQ
    max_retries=3,                                       # Retries before DLQ
    dlq_exchange_suffix="_dlq",                          # DLQ exchange name suffix
    dlq_message_ttl=86400000,                            # 24 hours in milliseconds
    dlq_max_length=10000,                                # Max 10k messages in DLQ
)
```

### How DLQ Works

1. **Message delivered**: Handler receives message
2. **Handler fails**: Exception raised during processing
3. **Retry with backoff**: Message requeued with delay
4. **Max retries exceeded**: After 3 failures (default)
5. **Sent to DLQ**: Message routed to DLQ exchange
6. **DLQ headers added**: Error reason, retry count, timestamps

### Inspecting DLQ Messages

```python
# Get DLQ message count
count = await bus.get_dlq_message_count()
print(f"DLQ messages: {count}")

# Get DLQ messages
dlq_messages = await bus.get_dlq_messages(limit=100)

for msg in dlq_messages:
    print(f"Message ID: {msg['message_id']}")
    print(f"Event Type: {msg['event_type']}")
    print(f"Error: {msg.get('dlq_reason', 'Unknown')}")
    print(f"Retry Count: {msg.get('retry_count', 0)}")
    print(f"DLQ Timestamp: {msg.get('dlq_timestamp')}")
    print("---")
```

### Replaying DLQ Messages

After fixing the underlying issue, replay messages from DLQ:

```python
# Replay specific message by ID
success = await bus.replay_dlq_message(message_id="msg-123")

if success:
    print("Message replayed successfully")
else:
    print("Message not found in DLQ")

# Replay all messages (careful!)
dlq_messages = await bus.get_dlq_messages(limit=1000)
for msg in dlq_messages:
    await bus.replay_dlq_message(msg['message_id'])
```

Replayed messages are:
1. Removed from DLQ
2. Published to main exchange with retry count reset
3. Include `x-replayed-from-dlq` header with replay timestamp
4. Processed by consumers normally

### Purging DLQ

```python
# Remove all messages from DLQ (cannot be undone!)
await bus.purge_dlq()
```

### Monitoring DLQ

```python
# Set up alerts for DLQ
count = await bus.get_dlq_message_count()

if count > 100:
    logger.warning(f"WARNING: {count} messages in DLQ!")
    # Alert operations team
    # Investigate root cause
```

---

## SSL/TLS Security

### Basic TLS (Server Verification)

```python
config = RabbitMQEventBusConfig(
    # Use amqps:// for TLS
    rabbitmq_url="amqps://user:pass@rabbitmq.example.com:5671/",
    verify_ssl=True,                                     # Verify server certificate
)
```

### Custom CA Certificate

```python
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://rabbitmq.internal:5671/",
    verify_ssl=True,
    ca_file="/path/to/ca.crt",                          # Custom CA for verification
)
```

### Mutual TLS (mTLS)

```python
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://rabbitmq.internal:5671/",
    verify_ssl=True,
    ca_file="/path/to/ca.crt",                          # Verify server
    cert_file="/path/to/client.crt",                    # Client certificate
    key_file="/path/to/client.key",                     # Client private key
)
```

### Advanced SSL Configuration

```python
import ssl

# Create custom SSL context
ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ssl_context.load_verify_locations("/path/to/ca.crt")
ssl_context.load_cert_chain(
    certfile="/path/to/client.crt",
    keyfile="/path/to/client.key"
)
ssl_context.check_hostname = True
ssl_context.verify_mode = ssl.CERT_REQUIRED

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://rabbitmq.internal:5671/",
    ssl_context=ssl_context,                            # Use custom context
)
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
print(f"Messages in DLQ: {stats['messages_sent_to_dlq']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Reconnections: {stats['reconnections']}")
print(f"Last publish: {stats['last_publish_at']}")
print(f"Last consume: {stats['last_consume_at']}")
```

### Queue Information

```python
# Get queue details from RabbitMQ
queue_info = await bus.get_queue_info()

print(f"Queue name: {queue_info['name']}")
print(f"Messages: {queue_info['message_count']}")
print(f"Consumers: {queue_info['consumer_count']}")
```

### Health Check

```python
# Health check for monitoring
health = await bus.health_check()

if health.healthy:
    print("RabbitMQ event bus is healthy")
else:
    print(f"Unhealthy: {health.error}")
```

### Subscriber Counts

```python
# Count subscribers
total = bus.get_subscriber_count()
order_count = bus.get_subscriber_count(OrderPlaced)
wildcard_count = bus.get_wildcard_subscriber_count()

print(f"Total subscribers: {total}")
print(f"OrderPlaced subscribers: {order_count}")
print(f"Wildcard subscribers: {wildcard_count}")
```

### RabbitMQ Management UI

Access http://localhost:15672 to view:

- **Queues**: Message counts, consumer counts, message rates
- **Exchanges**: Bindings, publish rates
- **Connections**: Active connections and channels
- **Consumers**: Per-consumer statistics
- **Messages**: Browse and purge messages

---

## Integration with Repository

Connect RabbitMQEventBus to your repository for automatic event publishing:

```python
from eventsource import AggregateRepository, PostgreSQLEventStore
from sqlalchemy.ext.asyncio import async_sessionmaker

# Create infrastructure
session_factory = async_sessionmaker(...)
event_store = PostgreSQLEventStore(session_factory)

# Create RabbitMQ event bus
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="orders",
    consumer_group="projections",
)
bus = RabbitMQEventBus(config=config, event_registry=registry)
await bus.connect()

# Create repository with event publishing
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=bus,                                # Events auto-published to RabbitMQ
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
await repo.save(order)                                  # Events automatically flow through RabbitMQ

# Handler receives event automatically
```

**Publishing flow:**
1. Repository saves events to PostgreSQL event store
2. Repository publishes events to RabbitMQEventBus
3. RabbitMQ routes events to bound queues based on exchange type
4. Consumers in each group receive events
5. Handlers process events and update projections

---

## Complete Working Example

```python
"""
Tutorial 19: RabbitMQ Event Bus - Complete Example
Demonstrates publishing, consuming, routing, and monitoring.

Requirements:
- pip install eventsource-py[rabbitmq]
- docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
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
    RABBITMQ_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)

if not RABBITMQ_AVAILABLE:
    print("ERROR: RabbitMQ support not installed.")
    print("Install with: pip install eventsource-py[rabbitmq]")
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

    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="demo1",
        exchange_type="topic",
        consumer_group="demo-consumers",
    )
    bus = RabbitMQEventBus(config=config, event_registry=registry)

    try:
        await bus.connect()
        print("Connected to RabbitMQ")

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
        await asyncio.sleep(2.0)

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


async def demo_exchange_types():
    """Demo 2: Different exchange types."""
    print("\n" + "=" * 60)
    print("Demo 2: Exchange Types and Routing")
    print("=" * 60)

    print("RabbitMQ supports several exchange types:\n")

    print("1. TOPIC (default in eventsource)")
    print("   - Routes based on routing key patterns")
    print("   - Supports wildcards: * (one word), # (zero or more)")
    print("   - Example: 'Order.*' matches 'Order.OrderPlaced', 'Order.OrderShipped'")
    print()

    print("2. DIRECT")
    print("   - Routes based on exact routing key match")
    print("   - Use for: specific service targeting, work queues")
    print()

    print("3. FANOUT")
    print("   - Broadcasts to all bound queues")
    print("   - Ignores routing key")
    print("   - Use for: notifications, audit logging, cache invalidation")
    print()

    print("4. HEADERS")
    print("   - Routes based on message headers")
    print("   - Most flexible but complex")


async def demo_consumer_groups():
    """Demo 3: Consumer groups for load balancing."""
    print("\n" + "=" * 60)
    print("Demo 3: Consumer Groups and Load Balancing")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderPlaced)

    base_config = {
        "rabbitmq_url": "amqp://guest:guest@localhost:5672/",
        "exchange_name": "demo3",
        "consumer_group": "load-balanced-consumers",
    }

    config1 = RabbitMQEventBusConfig(**base_config, consumer_name="worker-1")
    config2 = RabbitMQEventBusConfig(**base_config, consumer_name="worker-2")

    bus1 = RabbitMQEventBus(config=config1, event_registry=registry)
    bus2 = RabbitMQEventBus(config=config2, event_registry=registry)

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
        await asyncio.sleep(3.0)

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


async def demo_with_projections():
    """Demo 4: Integration with projections."""
    print("\n" + "=" * 60)
    print("Demo 4: Integration with Projections")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderPlaced)
    registry.register(OrderShipped)

    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="demo4",
        consumer_group="projection-consumers",
    )
    bus = RabbitMQEventBus(config=config, event_registry=registry)

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
        await asyncio.sleep(2.0)
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
    print("Tutorial 19: RabbitMQ Event Bus")
    print("=" * 60)

    try:
        await demo_basic_publish_subscribe()
        await demo_exchange_types()
        await demo_consumer_groups()
        await demo_with_projections()

        print("\n" + "=" * 60)
        print("Tutorial 19 Complete!")
        print("=" * 60)

    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure RabbitMQ is running:")
        print("  docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management")
        raise


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
============================================================
Tutorial 19: RabbitMQ Event Bus
============================================================

============================================================
Demo 1: Basic Publish/Subscribe
============================================================
Connected to RabbitMQ
Subscribed handler to OrderPlaced
Published OrderPlaced for <uuid>
  Received: OrderPlaced
Received 1 event(s)

============================================================
Demo 2: Exchange Types and Routing
============================================================
RabbitMQ supports several exchange types:

1. TOPIC (default in eventsource)
   - Routes based on routing key patterns
   - Supports wildcards: * (one word), # (zero or more)
   - Example: 'Order.*' matches 'Order.OrderPlaced', 'Order.OrderShipped'

2. DIRECT
   - Routes based on exact routing key match
   - Use for: specific service targeting, work queues

3. FANOUT
   - Broadcasts to all bound queues
   - Ignores routing key
   - Use for: notifications, audit logging, cache invalidation

4. HEADERS
   - Routes based on message headers
   - Most flexible but complex

============================================================
Demo 3: Consumer Groups and Load Balancing
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
Tutorial 19 Complete!
============================================================
```

---

## Production Deployment Patterns

### Graceful Shutdown

```python
import signal

class RabbitMQEventBusService:
    def __init__(self, bus: RabbitMQEventBus):
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
    service = RabbitMQEventBusService(bus)

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
async def health_check(bus: RabbitMQEventBus) -> dict:
    """
    Health check for Kubernetes liveness/readiness probes.

    Returns:
        Health status dictionary
    """
    try:
        health = await bus.health_check()
        stats = bus.get_stats_dict()

        return {
            "status": "healthy" if health.healthy else "unhealthy",
            "error": health.error if not health.healthy else None,
            "connected": bus._connected,
            "consuming": bus._consuming,
            "events_published": stats['events_published'],
            "events_consumed": stats['events_consumed'],
            "handler_errors": stats['handler_errors'],
            "dlq_messages": stats.get('messages_sent_to_dlq', 0),
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

config = RabbitMQEventBusConfig(
    # Connection
    rabbitmq_url=os.getenv(
        "RABBITMQ_URL",
        "amqps://user:pass@rabbitmq.example.com:5671/"
    ),

    # Exchange and queue
    exchange_name=os.getenv("RABBITMQ_EXCHANGE", "myapp.events"),
    exchange_type=os.getenv("RABBITMQ_EXCHANGE_TYPE", "topic"),
    consumer_group=os.getenv("RABBITMQ_CONSUMER_GROUP", "default"),
    consumer_name=os.getenv("HOSTNAME"),                # Kubernetes pod name

    # Durability (production settings)
    durable=True,                                       # Survive restarts
    auto_delete=False,                                  # Don't auto-delete

    # Flow control
    prefetch_count=int(os.getenv("RABBITMQ_PREFETCH", "10")),

    # Security
    verify_ssl=True,
    ca_file=os.getenv("RABBITMQ_CA_FILE"),
    cert_file=os.getenv("RABBITMQ_CERT_FILE"),
    key_file=os.getenv("RABBITMQ_KEY_FILE"),

    # Error handling
    max_retries=3,
    enable_dlq=True,

    # Observability
    enable_tracing=True,
)
```

---

## Key Takeaways

1. **RabbitMQEventBus enables flexible message routing** with topic patterns, direct routing, and fanout
2. **Exchange types provide routing flexibility** - choose based on your distribution needs
3. **Consumer groups enable load balancing** - multiple workers share queue automatically
4. **Dead letter queues capture poison messages** - preventing processing blocks
5. **Exponential backoff with jitter prevents thundering herd** during retries
6. **SSL/TLS support for production security** - mutual TLS for client authentication
7. **Management UI provides visibility** - monitor queues, consumers, and message rates
8. **RabbitMQ excels at enterprise messaging** - battle-tested reliability and flexibility
9. **Same EventBus interface as InMemory/Redis/Kafka** - easy migration between implementations
10. **Production deployments require careful configuration** - durability, SSL, monitoring

---

## Troubleshooting

### Consumer Not Receiving Events

**Check connection:**
```python
assert bus._connected, "Not connected to RabbitMQ"
```

**Verify consumer is running:**
```python
assert bus._consuming, "Consumer not running"
```

**Check queue bindings:**
Visit http://localhost:15672 → Queues → Click your queue → Bindings

### High Unacknowledged Message Count

**Check prefetch_count:**
```python
# Lower prefetch for better load balancing
config = RabbitMQEventBusConfig(
    prefetch_count=5,                                   # Was 10
)
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
    print(f"Error: {msg.get('dlq_reason')}")
```

**Fix and replay:**
```python
# Fix the underlying bug
# Then replay all DLQ messages
dlq_messages = await bus.get_dlq_messages(limit=1000)
for msg in dlq_messages:
    await bus.replay_dlq_message(msg['message_id'])
```

### Connection Issues

**Check RabbitMQ is running:**
```bash
docker ps | grep rabbitmq
```

**Check logs:**
```bash
docker logs rabbitmq
```

**Test connection:**
```bash
# Using rabbitmq-diagnostics
docker exec rabbitmq rabbitmq-diagnostics check_running
```

---

## Next Steps

Now that you understand RabbitMQ event distribution, continue exploring observability and advanced patterns:

- [Tutorial 20: Observability with OpenTelemetry](20-observability.md) - Tracing and metrics for event-sourced systems

For comparing distributed event buses:
- [Tutorial 17: Redis Event Bus](17-redis.md) - Stream-based event distribution
- [Tutorial 18: Kafka Event Bus](18-kafka.md) - Ultra-high throughput event streaming

---

## Additional Resources

- [RabbitMQ Documentation](https://www.rabbitmq.com/documentation.html)
- [AMQP Protocol](https://www.rabbitmq.com/tutorials/amqp-concepts.html)
- [RabbitMQ Management Plugin](https://www.rabbitmq.com/management.html)
- [aio-pika Documentation](https://aio-pika.readthedocs.io/)
- `tests/integration/bus/test_rabbitmq.py` - Comprehensive integration tests
- `src/eventsource/bus/rabbitmq.py` - RabbitMQEventBus implementation
