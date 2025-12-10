# Event Bus Guide

The event bus distributes domain events to subscribers, decoupling event producers from consumers. This enables projections, notifications, and other handlers to react to events independently.

## Quick Start

```python
from eventsource import InMemoryEventBus, DomainEvent, register_event
from uuid import uuid4

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str

# Create bus and subscribe handler
bus = InMemoryEventBus()

async def handle_order(event: DomainEvent) -> None:
    print(f"Order {event.aggregate_id} created for {event.customer_id}")

bus.subscribe(OrderCreated, handle_order)

# Publish event
await bus.publish([
    OrderCreated(
        aggregate_id=uuid4(),
        customer_id="customer-123",
        aggregate_version=1,
    )
])
```

## Choosing an Event Bus

| Implementation | Use Case | Scaling | Persistence |
|----------------|----------|---------|-------------|
| `InMemoryEventBus` | Development, testing, single-process | None | No |
| `RedisEventBus` | Multi-process, real-time | Consumer groups | Redis Streams |
| `RabbitMQEventBus` | Enterprise messaging, routing | Consumer groups | Durable queues |
| `KafkaEventBus` | High-throughput, log retention | Partitions | Topic logs |

## Core Concepts

### Publishing Events

All bus implementations share the same publish API:

```python
# Publish and wait for handlers to complete
await bus.publish([event1, event2])

# Fire-and-forget (InMemoryEventBus only)
await bus.publish([event], background=True)
```

### Subscribing to Events

Three subscription patterns are supported:

```python
# 1. Subscribe to specific event type
bus.subscribe(OrderCreated, my_handler)

# 2. Subscribe to all declared types (for projections)
class OrderProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        # Handle events
        pass

bus.subscribe_all(OrderProjection())

# 3. Wildcard subscription (receive ALL events)
bus.subscribe_to_all_events(audit_logger)
```

### Handler Types

Handlers can be functions or objects with a `handle` method:

```python
# Function handler
async def log_event(event: DomainEvent) -> None:
    print(f"Event: {event.event_type}")

bus.subscribe(OrderCreated, log_event)

# Object handler
class NotificationService:
    async def handle(self, event: DomainEvent) -> None:
        await self.send_email(event)

bus.subscribe(OrderCreated, NotificationService())

# Lambda handler
bus.subscribe(OrderCreated, lambda e: print(e.event_type))
```

## In-Memory Event Bus

For development, testing, and single-process applications:

```python
from eventsource import InMemoryEventBus

bus = InMemoryEventBus()

# Optional: disable tracing for tests
bus = InMemoryEventBus(enable_tracing=False)

# Subscribe handlers
bus.subscribe(OrderCreated, order_handler)
bus.subscribe_to_all_events(audit_logger)

# Publish events
await bus.publish([event])

# Background publishing (non-blocking)
await bus.publish([event], background=True)

# Shutdown (waits for background tasks)
await bus.shutdown(timeout=30.0)
```

### Statistics

```python
stats = bus.get_stats()
# {
#     "events_published": 100,
#     "handlers_invoked": 250,
#     "handler_errors": 2,
#     "background_tasks_created": 10,
#     "background_tasks_completed": 10,
# }
```

## Redis Event Bus

For distributed systems using Redis Streams:

```python
from eventsource.bus import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="myapp",
    consumer_group="order-service",
    batch_size=100,
    max_retries=3,
    enable_dlq=True,
)

bus = RedisEventBus(config=config)
await bus.connect()

# Subscribe handlers (before or after connect)
bus.subscribe(OrderCreated, order_handler)

# Start consuming in the event loop
await bus.start_consuming()

# Publish from another process
await bus.publish([event])

# Graceful shutdown
await bus.shutdown()
```

### Consumer Groups

Multiple consumers in the same group share the workload:

```python
# Worker 1
config1 = RedisEventBusConfig(
    stream_prefix="orders",
    consumer_group="processors",
    consumer_name="worker-1",
)
worker1 = RedisEventBus(config=config1)

# Worker 2 (same group = shared workload)
config2 = RedisEventBusConfig(
    stream_prefix="orders",
    consumer_group="processors",
    consumer_name="worker-2",
)
worker2 = RedisEventBus(config=config2)
```

### Dead Letter Queue

Failed messages are automatically sent to DLQ after max retries:

```python
# Get DLQ messages
dlq_messages = await bus.get_dlq_messages(count=100)

# Replay a message
await bus.replay_dlq_message(message_id)

# Get stream info
info = await bus.get_stream_info()
print(f"Pending: {info['pending_messages']}")
print(f"DLQ: {info['dlq_messages']}")
```

## RabbitMQ Event Bus

For enterprise messaging with AMQP:

```python
from eventsource.bus import RabbitMQEventBus, RabbitMQEventBusConfig

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",
    exchange_type="topic",  # or "direct", "fanout"
    consumer_group="projections",
    prefetch_count=10,
    enable_dlq=True,
)

bus = RabbitMQEventBus(config=config)
await bus.connect()

bus.subscribe(OrderCreated, order_handler)
await bus.start_consuming()
```

### Exchange Types

```python
# Topic exchange (default) - routing by pattern
config = RabbitMQEventBusConfig(exchange_type="topic")
# Events routed as: {aggregate_type}.{event_type}
# e.g., "Order.OrderCreated", "User.UserRegistered"

# Direct exchange - exact routing key match
config = RabbitMQEventBusConfig(exchange_type="direct")

# Fanout exchange - broadcast to all queues
config = RabbitMQEventBusConfig(exchange_type="fanout")
```

## Kafka Event Bus

For high-throughput event streaming:

```python
from eventsource.bus import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="myapp.events",
    consumer_group="projections",
    acks="all",
    compression_type="gzip",
    max_retries=3,
    enable_dlq=True,
)

bus = KafkaEventBus(config=config)
await bus.connect()

bus.subscribe(OrderCreated, order_handler)
task = bus.start_consuming_in_background()

# Context manager for automatic cleanup
async with KafkaEventBus(config=config) as bus:
    bus.subscribe(OrderCreated, handler)
    await bus.start_consuming()
```

### Partition Ordering

Events are partitioned by `aggregate_id`, ensuring ordering per aggregate:

```python
# All events for the same aggregate go to the same partition
await bus.publish([
    OrderCreated(aggregate_id=order_id, ...),
    OrderShipped(aggregate_id=order_id, ...),  # Same partition
])
```

For detailed Kafka configuration, security, and metrics, see the [Kafka Event Bus Guide](kafka-event-bus.md).

## Integration with Repository

Publish events automatically when saving aggregates:

```python
from eventsource import AggregateRepository, InMemoryEventStore, InMemoryEventBus

event_store = InMemoryEventStore()
event_bus = InMemoryEventBus()

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=event_bus,  # Events auto-published on save
)

# Events published after save
order = repo.create_new(uuid4())
order.create(customer_id=uuid4())
await repo.save(order)  # Publishes OrderCreated to bus
```

## Error Handling

Handler errors are logged but do not stop other handlers:

```python
class ResilientHandler:
    async def handle(self, event: DomainEvent) -> None:
        try:
            await self.process(event)
        except TransientError:
            # Re-raise to trigger retry (distributed buses)
            raise
        except PermanentError as e:
            # Log and swallow - don't block other handlers
            logger.error(f"Permanent failure: {e}")
```

For distributed buses, unhandled exceptions trigger the retry mechanism. After `max_retries`, messages go to the DLQ.

## Observability

All event buses support OpenTelemetry tracing:

```python
# Tracing enabled by default
bus = InMemoryEventBus()  # enable_tracing=True by default

# Disable for testing
bus = InMemoryEventBus(enable_tracing=False)

# Distributed buses: tracing via config
config = RedisEventBusConfig(enable_tracing=True)
config = RabbitMQEventBusConfig(enable_tracing=True)
config = KafkaEventBusConfig(enable_tracing=True, enable_metrics=True)
```

Span names follow the pattern:
- `eventsource.event_bus.publish` - Publishing events
- `eventsource.event_bus.dispatch` - Dispatching to handlers
- `eventsource.event_bus.handle` - Individual handler execution

## Graceful Shutdown

Always shutdown the bus during application teardown:

```python
try:
    await bus.start_consuming()
except asyncio.CancelledError:
    pass
finally:
    await bus.shutdown(timeout=30.0)
```

## Best Practices

1. **Keep handlers idempotent**: Distributed buses provide at-least-once delivery

2. **Use wildcard subscriptions sparingly**: Only for cross-cutting concerns (audit, metrics)

3. **Set appropriate timeouts**: Match `max_poll_interval_ms` (Kafka) or `block_ms` (Redis) to your processing time

4. **Monitor DLQ**: Set up alerts for DLQ growth

5. **Use consumer groups**: Enable horizontal scaling without message duplication

6. **Prefer background=False**: Only use background publishing when you understand the consistency tradeoffs

## See Also

- [Event Bus API Reference](../api/bus.md)
- [Kafka Event Bus Guide](kafka-event-bus.md)
- [Kafka Metrics Guide](kafka-metrics.md)
- [Subscriptions Guide](subscriptions.md) - For production projections with catch-up
- [Architecture Overview](../architecture.md)
