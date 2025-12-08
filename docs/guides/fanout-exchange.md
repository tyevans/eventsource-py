# Fanout Exchange Guide for RabbitMQ Event Bus

This guide explains how to use fanout exchanges with the RabbitMQ event bus for broadcast messaging patterns.

## Overview

Fanout exchanges broadcast all messages to all bound queues, regardless of routing keys. This makes them ideal for scenarios where multiple services or components need to receive every event.

### Key Characteristics

- **Broadcast Delivery**: Every message published to a fanout exchange is delivered to ALL bound queues
- **Routing Key Ignored**: Routing keys are ignored during message routing (though still logged for observability)
- **Multiple Consumer Groups**: Each consumer group gets its own queue, so each group receives all messages
- **Simple Configuration**: No routing key patterns to configure

## When to Use Fanout Exchanges

Fanout exchanges are ideal for:

### 1. Notification Broadcasting

When you need to notify multiple services about every event:

```python
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

# Email notification service
email_config = RabbitMQEventBusConfig(
    exchange_name="notifications",
    exchange_type="fanout",
    consumer_group="email-handler",
    durable=True,
)

# SMS notification service
sms_config = RabbitMQEventBusConfig(
    exchange_name="notifications",
    exchange_type="fanout",
    consumer_group="sms-handler",
    durable=True,
)

# Both services receive ALL notifications
```

### 2. Audit Logging

When you need to capture all events for audit purposes:

```python
audit_config = RabbitMQEventBusConfig(
    exchange_name="domain-events",
    exchange_type="fanout",
    consumer_group="audit-logger",
    durable=True,
    prefetch_count=50,  # Higher throughput for logging
)
```

### 3. Cache Invalidation

When you need to broadcast cache invalidation commands:

```python
cache_config = RabbitMQEventBusConfig(
    exchange_name="cache-invalidation",
    exchange_type="fanout",
    consumer_group="frontend-cache",
    durable=False,  # Cache invalidation can be transient
    auto_delete=True,  # OK to lose messages if consumer disconnects
)
```

### 4. Development/Debug Monitoring

When you want to monitor all events during development:

```python
dev_config = RabbitMQEventBusConfig(
    exchange_name="all-events",
    exchange_type="fanout",
    consumer_group="dev-monitor",
    durable=False,
    auto_delete=True,
    enable_dlq=False,
)
```

## Configuration

### Basic Configuration

```python
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="broadcast-events",
    exchange_type="fanout",  # Key setting for fanout
    consumer_group="my-service",
)

bus = RabbitMQEventBus(config=config)
await bus.connect()
```

### Queue Naming

With fanout exchanges, each consumer group gets its own queue. The queue name is derived from:

```
{exchange_name}.{consumer_group}
```

For example:
- Exchange: `notifications`
- Consumer Group: `email-handler`
- Queue Name: `notifications.email-handler`

### Multiple Consumer Groups Example

```python
import asyncio
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
from eventsource.events.base import DomainEvent
from uuid import uuid4

# Define event
class UserCreated(DomainEvent):
    event_type: str = "UserCreated"
    aggregate_type: str = "User"
    email: str

# Service 1: Email notifications
async def run_email_service():
    config = RabbitMQEventBusConfig(
        exchange_name="user-events",
        exchange_type="fanout",
        consumer_group="email-service",
    )

    async with RabbitMQEventBus(config=config) as bus:
        @bus.subscribe(UserCreated)
        async def handle_user_created(event: UserCreated):
            print(f"Email service: Sending welcome email to {event.email}")

        await bus.start_consuming()

# Service 2: Analytics
async def run_analytics_service():
    config = RabbitMQEventBusConfig(
        exchange_name="user-events",
        exchange_type="fanout",
        consumer_group="analytics-service",
    )

    async with RabbitMQEventBus(config=config) as bus:
        @bus.subscribe(UserCreated)
        async def handle_user_created(event: UserCreated):
            print(f"Analytics: Recording new user registration")

        await bus.start_consuming()

# Publisher: Any service publishing user events
async def publish_event():
    config = RabbitMQEventBusConfig(
        exchange_name="user-events",
        exchange_type="fanout",
        consumer_group="publisher",  # Publisher also needs a consumer group
    )

    async with RabbitMQEventBus(config=config) as bus:
        event = UserCreated(
            aggregate_id=uuid4(),
            email="user@example.com",
        )
        await bus.publish([event])
        # Both email-service and analytics-service receive this event
```

## Routing Key Behavior

### Why Routing Keys Still Exist

Although fanout exchanges ignore routing keys for message routing, the RabbitMQ event bus still generates them for:

1. **Logging**: Routing keys appear in logs for debugging
2. **Tracing**: OpenTelemetry spans include routing key information
3. **Consistency**: Same message format regardless of exchange type

### Binding with Empty Routing Key

Fanout bindings use an empty routing key by default:

```python
# Internally, the binding looks like:
await queue.bind(exchange=exchange, routing_key="")
```

You can verify this in the logs:
```
INFO: Bound queue notifications.email-handler to fanout exchange notifications
      (broadcast mode - all messages will be delivered to this queue regardless
      of routing key)
```

## Best Practices

### 1. Use Meaningful Consumer Group Names

Consumer group names should clearly identify the service or purpose:

```python
# Good
consumer_group="email-notification-handler"
consumer_group="audit-log-writer"
consumer_group="cache-invalidation-service"

# Avoid
consumer_group="service1"
consumer_group="handler"
```

### 2. Consider Durability Requirements

- **Durable exchanges/queues** (`durable=True`): For production, mission-critical events
- **Non-durable** (`durable=False`): For transient data, development, cache invalidation

### 3. Set Appropriate Prefetch Count

For high-throughput consumers like audit loggers:

```python
config = RabbitMQEventBusConfig(
    exchange_type="fanout",
    prefetch_count=50,  # Higher for better throughput
)
```

For services that need careful processing:

```python
config = RabbitMQEventBusConfig(
    exchange_type="fanout",
    prefetch_count=1,  # Process one at a time
)
```

### 4. Use DLQ for Production

Enable dead letter queues for production fanout exchanges:

```python
config = RabbitMQEventBusConfig(
    exchange_type="fanout",
    enable_dlq=True,
    max_retries=3,
)
```

## Comparison with Other Exchange Types

| Feature | Fanout | Topic | Direct |
|---------|--------|-------|--------|
| Routing Key | Ignored | Pattern matching | Exact match |
| All queues receive all messages | Yes | No (based on pattern) | No (based on key) |
| Use case | Broadcast | Content-based routing | Work queues |
| Complexity | Simple | Medium | Simple |
| Performance | Highest | Medium | High |

## Troubleshooting

### All Messages Going to One Consumer

If you have multiple instances but only one receives messages, check that they have **different consumer groups**:

```python
# Instance 1
config1 = RabbitMQEventBusConfig(
    exchange_type="fanout",
    consumer_group="service-1",  # Different group
)

# Instance 2
config2 = RabbitMQEventBusConfig(
    exchange_type="fanout",
    consumer_group="service-2",  # Different group
)
```

If they share the same consumer group, messages will be load-balanced between them (competing consumers pattern).

### Messages Not Being Delivered

1. Verify exchange type is "fanout":
   ```python
   assert config.exchange_type == "fanout"
   ```

2. Check the logs for binding confirmation:
   ```
   INFO: Bound queue X to fanout exchange Y (broadcast mode...)
   ```

3. Ensure the consumer is started:
   ```python
   await bus.start_consuming()
   ```

### Performance Considerations

Fanout exchanges are efficient, but broadcasting to many queues can increase load:

- **Message Copies**: Each bound queue gets a copy of the message
- **Network Traffic**: More queues = more network traffic
- **Consumer Lag**: Slow consumers can fall behind

Consider using topic exchanges with selective subscriptions if you only need events some of the time.

## Related Documentation

- [RabbitMQ Exchange Types](https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges)
