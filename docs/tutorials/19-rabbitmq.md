# Tutorial 19: Using RabbitMQ Event Bus

**Difficulty:** Advanced | **Progress:** Tutorial 19 of 21 | Phase 4: Advanced Patterns

## Prerequisites

- [Tutorial 7: Distributing Events with Event Bus](07-event-bus.md) and [Tutorial 18: Using Kafka Event Bus](18-kafka.md)
- Python 3.11+, RabbitMQ running locally (Docker recommended)
- Basic AMQP understanding (exchanges, queues, bindings)

This tutorial covers RabbitMQ for flexible routing patterns and reliable message delivery.

## When to Use RabbitMQ

Choose based on your needs:

| Feature | RabbitMQ | Kafka | Redis |
|---------|----------|-------|-------|
| **Routing** | Flexible (topic, direct, fanout) | Partition-based | Stream-based |
| **Protocol** | AMQP | Kafka protocol | RESP |
| **Throughput** | Moderate (thousands/sec) | Very High (10,000+/sec) | High (1,000+/sec) |
| **Ordering** | Per-queue | Per-partition | Per-stream |
| **Retention** | Until consumed | Configurable (days/weeks) | Limited |
| **Complexity** | Medium | High | Low |
| **Best For** | Complex routing, enterprise | Event streaming, high volume | Moderate scale, caching |

## Installation

```bash
pip install eventsource-py[rabbitmq]
```

Verify installation:

```python
from eventsource.bus.rabbitmq import RABBITMQ_AVAILABLE, RabbitMQNotAvailableError

if RABBITMQ_AVAILABLE:
    print("RabbitMQ support is available!")
else:
    print("RabbitMQ not available. Install with: pip install eventsource-py[rabbitmq]")
```

## RabbitMQ Setup with Docker

Start RabbitMQ:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management-alpine
```

Or with Docker Compose (`docker-compose.rabbitmq.yml`):

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

Access Management UI at http://localhost:15672 (guest/guest).

## Configuration

Basic setup:

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

Creates: Exchange `myapp.events`, Queue `myapp.events.projections`, DLQ exchange `myapp.events_dlq`, DLQ queue `myapp.events.projections.dlq`.

Key parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rabbitmq_url` | `"amqp://guest:guest@localhost:5672/"` | AMQP connection URL |
| `exchange_name` | `"events"` | Exchange name |
| `exchange_type` | `"topic"` | topic, direct, fanout, or headers |
| `consumer_group` | `"default"` | Queue name prefix |
| `prefetch_count` | `10` | Max unacknowledged messages |
| `max_retries` | `3` | Retries before DLQ |
| `enable_dlq` | `True` | Enable dead letter queue |
| `durable` | `True` | Survive restarts |
| `enable_tracing` | `True` | OpenTelemetry tracing |

Exchange types:

```python
# Topic (default) - Pattern matching with * (one word) and # (zero or more words)
config = RabbitMQEventBusConfig(exchange_type="topic", routing_key_pattern="Order.*")

# Direct - Exact match
config = RabbitMQEventBusConfig(exchange_type="direct", routing_key_pattern="Order.OrderCreated")

# Fanout - Broadcast to all (ignores routing key)
config = RabbitMQEventBusConfig(exchange_type="fanout")
```

## Connecting and Publishing

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
    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="orders",
        consumer_group="handlers",
    )

    # Context manager handles connect/disconnect
    async with RabbitMQEventBus(config=config, event_registry=default_registry) as bus:
        # Publish events
        await bus.publish([
            OrderCreated(
                aggregate_id=uuid4(),
                customer_id="CUST-001",
                total=99.99,
                aggregate_version=1,
            )
        ])

        # Batch publishing
        events = [OrderCreated(aggregate_id=uuid4(), customer_id=f"CUST-{i:04d}",
                               total=float(i * 10), aggregate_version=1)
                  for i in range(100)]
        await bus.publish(events)

        # Advanced batch control
        result = await bus.publish_batch(events, preserve_order=False)
        print(f"Published {result['published']}/{result['total']}")

asyncio.run(main())
```

Events get routing keys like `Order.OrderCreated` (format: `{aggregate_type}.{event_type}`).

## Subscribing Handlers

```python
# Function handler
async def handle_order_created(event: DomainEvent) -> None:
    if isinstance(event, OrderCreated):
        print(f"Processing order {event.aggregate_id}: ${event.total}")

bus.subscribe(OrderCreated, handle_order_created)

# Class-based handler
class OrderNotificationHandler:
    def __init__(self):
        self.notifications_sent = 0

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self.notifications_sent += 1

handler = OrderNotificationHandler()
bus.subscribe(OrderCreated, handler)

# Projection handler
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

projection = OrderProjection()
bus.subscribe_all(projection)

# Wildcard handler (all events)
async def audit_all_events(event: DomainEvent) -> None:
    print(f"[AUDIT] {event.event_type} at {event.occurred_at}")

bus.subscribe_to_all_events(audit_all_events)
```

## Consuming Events

```python
# Connect and start consuming (blocks)
await bus.connect()
bus.subscribe(OrderCreated, handle_order)
await bus.start_consuming()

# Background consumption
task = bus.start_consuming_in_background()
# ... do other work ...
await bus.stop_consuming()
await task

# Time-limited consumption
task = bus.start_consuming_in_background()
await asyncio.sleep(seconds)
await bus.stop_consuming()
task.cancel()
try:
    await task
except asyncio.CancelledError:
    pass
```

Auto-reconnect enabled by default via RobustConnection.

## Consumer Groups

Multiple services with independent queues:

```python
# Service 1: Only Order events
order_config = RabbitMQEventBusConfig(
    exchange_name="domain-events",
    consumer_group="order-service",
    routing_key_pattern="Order.*",
)

# Service 2: All events
analytics_config = RabbitMQEventBusConfig(
    exchange_name="domain-events",
    consumer_group="analytics",
    routing_key_pattern="#",
)

# Load balancing: Same group, different workers
config_1 = RabbitMQEventBusConfig(consumer_group="processors", consumer_name="worker-1")
config_2 = RabbitMQEventBusConfig(consumer_group="processors", consumer_name="worker-2")
```

Dynamic bindings:

```python
await bus.connect()
await bus.bind_routing_key("Order.*")
await bus.bind_event_type(OrderCreated)
```

## Dead Letter Queue (DLQ)

Failed messages go to DLQ after `max_retries`. DLQ naming: exchange `events` → DLQ exchange `events_dlq`, queue `events.group` → DLQ queue `events.group.dlq`.

```python
config = RabbitMQEventBusConfig(
    enable_dlq=True,
    max_retries=3,
    dlq_message_ttl=86400000,  # 24 hours in ms
    retry_base_delay=1.0,
    retry_max_delay=60.0,
)

# Monitor DLQ
count = await bus.get_dlq_message_count()
messages = await bus.get_dlq_messages(limit=10)
for msg in messages:
    print(f"{msg.event_type}: {msg.dlq_reason}")

# Replay or purge
await bus.replay_dlq_message("message-id")
await bus.purge_dlq()
```

## SSL/TLS

```python
# Basic TLS (amqps:// auto-enables)
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://user:pass@rabbitmq.example.com:5671/",
)

# Custom CA
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://rabbitmq.internal:5671/",
    ca_file="/path/to/ca.crt",
)

# Mutual TLS
config = RabbitMQEventBusConfig(
    rabbitmq_url="amqps://rabbitmq.internal:5671/",
    ca_file="/path/to/ca.crt",
    cert_file="/path/to/client.crt",
    key_file="/path/to/client.key",
)

# Custom SSL context
import ssl
ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ssl_context.load_verify_locations("/path/to/ca.crt")
ssl_context.load_cert_chain(certfile="/path/to/client.crt", keyfile="/path/to/client.key")
config = RabbitMQEventBusConfig(rabbitmq_url="amqps://...", ssl_context=ssl_context)
```

## Monitoring

```python
# Statistics
stats = bus.stats
print(f"Published: {stats.events_published}, Consumed: {stats.events_consumed}")
stats_dict = bus.get_stats_dict()

# Queue info
info = await bus.get_queue_info()
print(f"Queue: {info.name}, Messages: {info.message_count}")

# Health check
result = await bus.health_check()
if not result.healthy:
    print(f"Unhealthy: {result.error}")
```

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

Run with: `python tutorial_19_rabbitmq.py`

## Exercises

### Exercise 1: Event-Driven Microservices

Build microservices with topic-based routing. Create events for Customer, Order, and Payment domains. Configure handlers to receive only relevant events using routing patterns.

Starter code:

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

Solution: `docs/tutorials/exercises/solutions/19-1.py`

## Next Steps

Continue to [Tutorial 20: Observability with OpenTelemetry](20-observability.md).
