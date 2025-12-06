# Event Bus API Reference

This document covers the event bus system for publishing and subscribing to domain events.

## Overview

The event bus decouples event producers from consumers, allowing projections and other handlers to react to events independently.

```python
from eventsource import (
    # Interface
    EventBus,
    EventHandler,
    EventSubscriber,
    EventHandlerFunc,
    AsyncEventHandler,

    # Implementations
    InMemoryEventBus,
    RedisEventBus,
    RedisEventBusConfig,
    RedisEventBusStats,
)
```

---

## EventBus Interface

Abstract base class defining the event bus contract.

### Core Methods

```python
class EventBus(ABC):
    @abstractmethod
    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to all registered subscribers."""
        pass

    @abstractmethod
    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: EventHandler | EventHandlerFunc,
    ) -> None:
        """Subscribe a handler to a specific event type."""
        pass

    @abstractmethod
    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: EventHandler | EventHandlerFunc,
    ) -> bool:
        """Unsubscribe a handler from a specific event type."""
        pass

    @abstractmethod
    def subscribe_all(self, subscriber: EventSubscriber) -> None:
        """Subscribe an EventSubscriber to all its declared event types."""
        pass

    @abstractmethod
    def subscribe_to_all_events(
        self,
        handler: EventHandler | EventHandlerFunc,
    ) -> None:
        """Subscribe a handler to all event types (wildcard)."""
        pass

    @abstractmethod
    def unsubscribe_from_all_events(
        self,
        handler: EventHandler | EventHandlerFunc,
    ) -> bool:
        """Unsubscribe from wildcard subscription."""
        pass
```

---

## Handler Protocols

### EventHandler

Protocol for objects that handle events:

```python
@runtime_checkable
class EventHandler(Protocol):
    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """Handle a domain event (sync or async)."""
        ...
```

**Usage:**

```python
class MyHandler:
    async def handle(self, event: DomainEvent) -> None:
        print(f"Handling {event.event_type}")

handler = MyHandler()
event_bus.subscribe(OrderCreated, handler)
```

### EventSubscriber

Protocol for subscribers that declare their event types:

```python
@runtime_checkable
class EventSubscriber(Protocol):
    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return list of event types this subscriber handles."""
        ...

    def handle(self, event: DomainEvent) -> Awaitable[None] | None:
        """Handle a domain event."""
        ...
```

**Usage:**

```python
class OrderProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            await self.create_order(event)
        # ...

projection = OrderProjection()
event_bus.subscribe_all(projection)  # Subscribes to all declared types
```

### EventHandlerFunc

Type alias for function-based handlers:

```python
EventHandlerFunc = Callable[[DomainEvent], Awaitable[None] | None]

# Usage
async def my_handler(event: DomainEvent) -> None:
    print(f"Got event: {event.event_type}")

event_bus.subscribe(OrderCreated, my_handler)
```

---

## InMemoryEventBus

In-memory implementation for single-process deployments.

### Features

- Thread-safe subscription management
- Support for sync and async handlers
- Wildcard subscriptions
- Error isolation (handler failures don't stop other handlers)
- Optional OpenTelemetry tracing
- Background task management

### Usage

```python
from eventsource import InMemoryEventBus

bus = InMemoryEventBus()

# Subscribe to specific event type
bus.subscribe(OrderCreated, order_handler)

# Subscribe using lambda
bus.subscribe(OrderCreated, lambda e: print(e))

# Subscribe to all events (audit logging, metrics)
bus.subscribe_to_all_events(audit_logger)

# Subscribe projection to multiple event types
bus.subscribe_all(order_projection)

# Publish events
await bus.publish([order_created_event])

# Publish in background (fire-and-forget)
await bus.publish([event], background=True)
```

### Methods

| Method | Description |
|--------|-------------|
| `subscribe(event_type, handler)` | Subscribe to specific event type |
| `unsubscribe(event_type, handler)` | Remove subscription |
| `subscribe_all(subscriber)` | Subscribe to all declared types |
| `subscribe_to_all_events(handler)` | Wildcard subscription |
| `unsubscribe_from_all_events(handler)` | Remove wildcard subscription |
| `publish(events, background)` | Publish events to subscribers |
| `clear_subscribers()` | Remove all subscriptions |
| `get_subscriber_count(event_type)` | Count subscribers |
| `get_wildcard_subscriber_count()` | Count wildcard subscribers |
| `get_stats()` | Get operation statistics |
| `get_background_task_count()` | Count active background tasks |
| `shutdown(timeout)` | Wait for background tasks to complete |

### Statistics

```python
stats = bus.get_stats()
print(f"Events published: {stats['events_published']}")
print(f"Handlers invoked: {stats['handlers_invoked']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Background tasks: {stats['background_tasks_created']}")
```

### Shutdown

Ensure all background tasks complete during application shutdown:

```python
async def shutdown():
    await bus.shutdown(timeout=30.0)
```

---

## RedisEventBus

Distributed event bus using Redis Streams.

### Features

- Distributed event publishing
- Consumer groups for load balancing
- Reliable delivery with acknowledgments
- Dead letter queue support
- Automatic reconnection
- Stream trimming

### Setup

```python
from eventsource import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_name="events",
    consumer_group="my-app",
    consumer_name="worker-1",
    max_stream_length=10000,
    block_ms=5000,
    batch_size=10,
)

bus = RedisEventBus(config)
await bus.start()  # Start consuming
```

### Configuration

```python
@dataclass
class RedisEventBusConfig:
    redis_url: str = "redis://localhost:6379"
    stream_name: str = "events"
    consumer_group: str = "default"
    consumer_name: str = "consumer-1"
    max_stream_length: int = 10000      # Stream trimming
    block_ms: int = 5000                 # Block time for reads
    batch_size: int = 10                 # Events per batch
    claim_min_idle_time: int = 60000     # Claim idle messages (ms)
    enable_dead_letter: bool = True      # Enable DLQ
    max_retries: int = 3                 # Max processing attempts
```

### Usage

```python
bus = RedisEventBus(config)

# Subscribe handlers (same as InMemoryEventBus)
bus.subscribe(OrderCreated, order_handler)
bus.subscribe_all(order_projection)

# Start consuming
await bus.start()

# Publish events
await bus.publish([order_created_event])

# Get statistics
stats = bus.get_stats()
print(f"Pending: {stats.pending_count}")
print(f"Processed: {stats.processed_count}")
print(f"Failed: {stats.failed_count}")

# Shutdown
await bus.stop()
```

### Consumer Groups

Multiple consumers can process events in parallel:

```python
# Worker 1
config1 = RedisEventBusConfig(
    consumer_group="order-processors",
    consumer_name="worker-1",
)
bus1 = RedisEventBus(config1)

# Worker 2
config2 = RedisEventBusConfig(
    consumer_group="order-processors",
    consumer_name="worker-2",
)
bus2 = RedisEventBus(config2)

# Events are distributed between workers
```

### Availability Check

```python
from eventsource import REDIS_AVAILABLE, RedisNotAvailableError

if not REDIS_AVAILABLE:
    print("Redis support not installed")
    # pip install eventsource[redis]

try:
    bus = RedisEventBus(config)
except RedisNotAvailableError:
    print("Redis package not available")
```

---

## AsyncEventHandler

Base class for structured async handlers.

```python
from eventsource import AsyncEventHandler

class OrderNotificationHandler(AsyncEventHandler):
    def event_types(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            await self.send_confirmation_email(event)
        elif isinstance(event, OrderShipped):
            await self.send_shipping_notification(event)

    def can_handle(self, event: DomainEvent) -> bool:
        # Custom filtering logic (optional override)
        return type(event) in self.event_types()
```

---

## Complete Example

```python
import asyncio
from uuid import uuid4
from eventsource import (
    DomainEvent,
    register_event,
    InMemoryEventBus,
    InMemoryEventStore,
    AggregateRepository,
)

# Events
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str

# Handler classes
class NotificationService:
    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            print(f"Sending order confirmation for {event.aggregate_id}")
        elif isinstance(event, OrderShipped):
            print(f"Sending shipping notification for {event.aggregate_id}")

class AnalyticsService:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        print(f"Analytics: Recording {event.event_type}")

class AuditLogger:
    async def handle(self, event: DomainEvent) -> None:
        print(f"Audit: {event.event_type} at {event.occurred_at}")

async def main():
    # Set up event bus
    bus = InMemoryEventBus()

    # Subscribe handlers
    notification_service = NotificationService()
    bus.subscribe(OrderCreated, notification_service)
    bus.subscribe(OrderShipped, notification_service)

    # Subscribe projection (subscribes to all declared types)
    analytics = AnalyticsService()
    bus.subscribe_all(analytics)

    # Subscribe audit logger to ALL events
    audit = AuditLogger()
    bus.subscribe_to_all_events(audit)

    # Set up repository with event publishing
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=bus,  # Events published after save
    )

    # Create and save order (events auto-published)
    order = repo.create_new(uuid4())
    order.create(customer_id=uuid4())
    await repo.save(order)

    # Or publish directly
    await bus.publish([
        OrderShipped(
            aggregate_id=order.aggregate_id,
            tracking_number="TRACK-123",
            aggregate_version=2,
        )
    ])

    # Check stats
    stats = bus.get_stats()
    print(f"\nStats: {stats}")

    # Cleanup
    await bus.shutdown()

asyncio.run(main())
```

---

## Best Practices

### Handler Isolation

Handlers should be independent and not assume order:

```python
class MyHandler:
    async def handle(self, event: DomainEvent) -> None:
        # Don't assume other handlers have run
        # Don't modify shared state without synchronization
        await self._process_independently(event)
```

### Error Handling

Handler errors are logged but don't stop other handlers:

```python
class ResilientHandler:
    async def handle(self, event: DomainEvent) -> None:
        try:
            await self._do_work(event)
        except TemporaryError:
            # Let it propagate - will be logged
            raise
        except PermanentError as e:
            # Log and continue - don't block other handlers
            logger.error(f"Permanent failure: {e}")
```

### Background Publishing

Use background publishing for better response times:

```python
# Synchronous (wait for handlers)
await bus.publish([event], background=False)

# Asynchronous (fire-and-forget)
await bus.publish([event], background=True)
# Response returns immediately, handlers run in background
```

### Graceful Shutdown

Always shutdown properly:

```python
try:
    # Application runs
    pass
finally:
    await bus.shutdown(timeout=30.0)
```

### Wildcard Subscriptions

Use sparingly for cross-cutting concerns:

```python
# Good uses for wildcard
bus.subscribe_to_all_events(audit_logger)     # Audit trail
bus.subscribe_to_all_events(metrics_handler)  # Metrics collection
bus.subscribe_to_all_events(debug_handler)    # Development debugging

# Bad: Using wildcard when specific subscription is better
# bus.subscribe_to_all_events(order_handler)  # Only handles OrderCreated anyway
bus.subscribe(OrderCreated, order_handler)     # Better
```
