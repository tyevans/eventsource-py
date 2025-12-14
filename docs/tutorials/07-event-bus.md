# Tutorial 7: Event Bus - Distributing Events Across Your System

**Difficulty:** Intermediate

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 2: Your First Domain Event](02-first-event.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- [Tutorial 6: Projections - Building Read Models](06-projections.md)
- Python 3.10 or higher
- Understanding of async/await
- Basic understanding of pub/sub patterns

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain what an event bus is and why it's essential for event-driven systems
2. Use InMemoryEventBus for single-process event distribution
3. Subscribe handlers to specific event types
4. Subscribe projections using subscribe_all()
5. Integrate event buses with repositories for automatic publishing
6. Use wildcard subscriptions for cross-cutting concerns
7. Understand distributed event bus implementations (Redis, RabbitMQ, Kafka)
8. Choose the right event bus for your deployment scenario
9. Handle background publishing for improved performance

---

## What is an Event Bus?

An **event bus** is a pub/sub (publish-subscribe) mechanism that distributes domain events to registered subscribers. It decouples event producers (aggregates/repositories) from event consumers (projections, handlers, integrations).

### Key Concepts

- **Publisher**: Emits events to the bus (usually your repository)
- **Subscriber**: Receives events from the bus (projections, handlers)
- **Decoupling**: Publishers don't know about subscribers, subscribers don't know about publishers
- **Fan-out**: One event can trigger multiple subscribers

### Real-World Analogy

Think of an event bus like a company newsletter:
- **Publishers** (departments) send announcements to the newsletter
- **Subscribers** (employees) receive announcements they care about
- No department needs to know who reads the newsletter
- New readers can subscribe without changing departments
- One announcement reaches all interested parties simultaneously

---

## The EventBus Interface

All event bus implementations share a common interface:

```python
from eventsource.bus import EventBus, DomainEvent

class EventBus(ABC):
    """Abstract event bus for publishing and subscribing to domain events."""

    async def publish(self, events: list[DomainEvent], background: bool = False) -> None:
        """Publish events to all registered subscribers."""
        pass

    def subscribe(self, event_type: type[DomainEvent], handler) -> None:
        """Subscribe a handler to a specific event type."""
        pass

    def unsubscribe(self, event_type: type[DomainEvent], handler) -> bool:
        """Unsubscribe a handler from a specific event type."""
        pass

    def subscribe_all(self, subscriber) -> None:
        """Subscribe a projection to all its declared event types."""
        pass

    def subscribe_to_all_events(self, handler) -> None:
        """Subscribe a handler to all event types (wildcard)."""
        pass

    def unsubscribe_from_all_events(self, handler) -> bool:
        """Unsubscribe a handler from wildcard subscription."""
        pass
```

**Key methods:**

- `publish()`: Send events to subscribers
- `subscribe()`: Register handler for specific event type
- `subscribe_all()`: Register projection for its declared events
- `subscribe_to_all_events()`: Receive all events (logging, metrics)

---

## InMemoryEventBus - Single Process Distribution

The `InMemoryEventBus` distributes events within a single process. It's perfect for:

- Development and testing
- Single-instance deployments
- Applications that don't need distributed messaging

### Basic Usage

```python
from eventsource.bus import InMemoryEventBus
from eventsource import DomainEvent, register_event
from uuid import uuid4

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_name: str
    total_amount: float

# Create event bus
bus = InMemoryEventBus()

# Create a simple handler
async def on_order_placed(event: OrderPlaced):
    print(f"Order placed by {event.customer_name} for ${event.total_amount}")

# Subscribe handler to event type
bus.subscribe(OrderPlaced, on_order_placed)

# Publish events
event = OrderPlaced(
    aggregate_id=uuid4(),
    customer_name="Alice",
    total_amount=99.99,
    aggregate_version=1,
)
await bus.publish([event])  # Prints: Order placed by Alice for $99.99
```

**Features:**

- Thread-safe subscription management
- Supports both sync and async handlers
- Error isolation (one handler's failure doesn't stop others)
- Concurrent handler execution
- Background task management

---

## Subscribing Handlers

### Function-Based Handlers

```python
# Async handler
async def send_email(event: OrderPlaced):
    print(f"Sending email to {event.customer_name}")

# Sync handler (also supported)
def log_event(event: OrderPlaced):
    print(f"Log: OrderPlaced {event.aggregate_id}")

bus.subscribe(OrderPlaced, send_email)
bus.subscribe(OrderPlaced, log_event)

# Both handlers are called when event is published
await bus.publish([event])
```

### Class-Based Handlers

```python
class NotificationHandler:
    """Handler with handle() method."""

    def __init__(self, notification_service):
        self.notification_service = notification_service

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            await self.notification_service.send(
                f"Order {event.aggregate_id} placed"
            )

handler = NotificationHandler(notification_service)
bus.subscribe(OrderPlaced, handler)
```

**Handler requirements:**

- Function: `async def handler(event: DomainEvent) -> None`
- Class: Must have `async def handle(event: DomainEvent) -> None` method
- Both sync and async handlers are supported

---

## Subscribing Projections

Projections from Tutorial 6 can be subscribed directly using `subscribe_all()`:

```python
from eventsource import DomainEvent
from collections import defaultdict

class OrderSummaryProjection:
    """Maintains order counts by status."""

    def __init__(self):
        self.order_counts: dict[str, int] = defaultdict(int)
        self.total_revenue: float = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which events this projection handles."""
        return [OrderPlaced, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Process events and update read model."""
        if isinstance(event, OrderPlaced):
            self.order_counts["placed"] += 1
            self.total_revenue += event.total_amount
        elif isinstance(event, OrderShipped):
            if self.order_counts["placed"] > 0:
                self.order_counts["placed"] -= 1
            self.order_counts["shipped"] += 1
        elif isinstance(event, OrderCancelled):
            # Handle cancellation
            pass

# Create projection
projection = OrderSummaryProjection()

# Subscribe to all its declared event types at once
bus.subscribe_all(projection)

# Now projection receives OrderPlaced, OrderShipped, and OrderCancelled
await bus.publish([
    OrderPlaced(aggregate_id=uuid4(), customer_name="Bob", total_amount=50.0, aggregate_version=1)
])

print(f"Total revenue: ${projection.total_revenue}")  # 50.0
```

**Benefits of subscribe_all():**

- Single call subscribes to all event types
- Projection declares its own dependencies
- Consistent with SubscriptionManager pattern from Tutorial 6
- Cleaner code: `bus.subscribe_all(projection)` vs multiple `subscribe()` calls

---

## Repository Integration

Connect your repository to an event bus to automatically publish events after saves:

```python
from eventsource import AggregateRepository, InMemoryEventStore, InMemoryEventBus

# Create infrastructure
event_store = InMemoryEventStore()
event_bus = InMemoryEventBus()

# Create repository with event publishing
repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=event_bus,  # Events auto-published after save
)

# Subscribe handlers
async def on_order_placed(event: OrderPlaced):
    print(f"Notification: New order from {event.customer_name}")

bus.subscribe(OrderPlaced, on_order_placed)

# Create and save aggregate
order = repo.create_new(uuid4())
order.place(customer_name="Charlie", total_amount=150.0)
await repo.save(order)  # Automatically publishes OrderPlaced event

# Handler is called automatically after save completes
```

**Publishing sequence:**

1. Repository persists events to event store
2. Events are marked as committed on aggregate
3. Events are published to event bus (if configured)
4. All subscribed handlers receive events

This ensures events are only published after they're safely persisted.

---

## Multiple Subscribers

Multiple handlers can subscribe to the same event type:

```python
# Define handlers
async def send_email(event: OrderPlaced):
    print(f"Email: Order {event.aggregate_id}")

async def update_inventory(event: OrderPlaced):
    print(f"Inventory: Reserved for {event.aggregate_id}")

async def send_slack_notification(event: OrderPlaced):
    print(f"Slack: New order from {event.customer_name}")

class OrderAnalytics:
    async def handle(self, event: DomainEvent):
        if isinstance(event, OrderPlaced):
            print(f"Analytics: Tracked order {event.aggregate_id}")

# Subscribe all handlers
bus.subscribe(OrderPlaced, send_email)
bus.subscribe(OrderPlaced, update_inventory)
bus.subscribe(OrderPlaced, send_slack_notification)
bus.subscribe(OrderPlaced, OrderAnalytics())

# Publish event - all 4 handlers are called concurrently
await bus.publish([OrderPlaced(...)])
```

**Handler execution:**

- All handlers execute concurrently (via asyncio.gather)
- One handler's failure doesn't prevent others from running
- Errors are logged but don't raise exceptions
- Handlers should be independent and idempotent

---

## Wildcard Subscriptions

Subscribe to all events regardless of type (useful for cross-cutting concerns):

```python
class AuditLogger:
    """Logs all events for audit trail."""

    async def handle(self, event: DomainEvent) -> None:
        print(f"[AUDIT] {event.event_type} on {event.aggregate_type} {event.aggregate_id}")

class MetricsCollector:
    """Collects metrics on all events."""

    def __init__(self):
        self.event_count = 0

    async def handle(self, event: DomainEvent) -> None:
        self.event_count += 1

audit_logger = AuditLogger()
metrics = MetricsCollector()

# Subscribe to ALL event types
bus.subscribe_to_all_events(audit_logger)
bus.subscribe_to_all_events(metrics)

# These handlers receive every event published
await bus.publish([OrderPlaced(...)])
await bus.publish([OrderShipped(...)])
await bus.publish([OrderCancelled(...)])

print(f"Total events: {metrics.event_count}")  # 3
```

**Wildcard subscription use cases:**

- Audit logging
- Metrics collection
- Debugging/tracing
- Event replay to external systems
- Cross-aggregate analytics

---

## Background Publishing

Improve API response times by publishing events in the background:

```python
# Synchronous publishing (default)
await bus.publish([event])  # Waits for all handlers to complete

# Background publishing (fire-and-forget)
await bus.publish([event], background=True)  # Returns immediately
```

**Trade-offs:**

| Mode | Response Time | Consistency | Use Case |
|------|--------------|-------------|----------|
| Synchronous | Slower | Immediate | Critical handlers, testing |
| Background | Faster | Eventual | Non-critical handlers, high throughput |

**Background publishing example:**

```python
# Fast API endpoint - don't wait for handlers
async def create_order_endpoint(data):
    order = repo.create_new(uuid4())
    order.place(data['customer_name'], data['total_amount'])

    # Save and publish in background
    await repo.save(order)  # With background=True on bus

    return {"order_id": str(order.aggregate_id)}  # Returns immediately
```

**Note:** Configure background publishing on the repository:

```python
# This requires custom repository configuration
# Default InMemoryEventBus publishes synchronously
```

---

## Distributed Event Bus Implementations

For multi-process or multi-server deployments, use distributed event buses:

### RedisEventBus

Uses Redis Streams for durable, distributed event messaging:

```python
from eventsource.bus import RedisEventBus, RedisEventBusConfig
from eventsource.events import get_event_registry

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="myapp",           # Prefix for Redis streams
    consumer_group="projections",    # Consumer group name
    batch_size=100,                  # Events per batch
    enable_dlq=True,                 # Dead letter queue for failures
)

# Create event bus
bus = RedisEventBus(
    config=config,
    event_registry=get_event_registry(),  # For event deserialization
)

# Connect to Redis
await bus.connect()

# Subscribe handlers (same API as InMemoryEventBus)
bus.subscribe(OrderPlaced, on_order_placed)

# Start consuming events from Redis
await bus.start_consuming()

# Publish events to Redis
await bus.publish([OrderPlaced(...)])
```

**Features:**

- Durable event storage (survives restarts)
- Consumer groups for load balancing
- At-least-once delivery guarantees
- Pending message recovery
- Dead letter queue for poison messages

### RabbitMQEventBus

Uses RabbitMQ for enterprise-grade messaging:

```python
from eventsource.bus import RabbitMQEventBus, RabbitMQEventBusConfig

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",          # Exchange for publishing
    consumer_group="projections",    # Queue group name
    max_retries=3,                   # Retry failed messages
    enable_dlq=True,                 # Dead letter queue
)

bus = RabbitMQEventBus(
    config=config,
    event_registry=get_event_registry(),
)

await bus.connect()
await bus.start_consuming()
```

**Features:**

- Topic-based routing
- Multiple exchange types (fanout, topic, direct)
- Configurable retry policies
- Dead letter exchanges
- High availability with clustering

### KafkaEventBus

Uses Apache Kafka for high-throughput event streaming:

```python
from eventsource.bus import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="events",           # Topic prefix
    consumer_group="projections",    # Consumer group
    enable_metrics=True,             # OpenTelemetry metrics
)

bus = KafkaEventBus(
    config=config,
    event_registry=get_event_registry(),
)

await bus.connect()
await bus.start_consuming()
```

**Features:**

- High-throughput publishing (100k+ events/sec)
- Partition-based ordering by aggregate_id
- Consumer rebalancing for scaling
- Event replay from any offset
- Built-in metrics via OpenTelemetry

---

## Choosing the Right Event Bus

| Implementation | Use Case | Throughput | Complexity | Infrastructure |
|----------------|----------|------------|------------|----------------|
| **InMemoryEventBus** | Single instance, development, testing | Low | Simple | None |
| **RedisEventBus** | Multi-instance, moderate scale | Medium | Medium | Redis |
| **RabbitMQEventBus** | Enterprise messaging, complex routing | Medium | High | RabbitMQ |
| **KafkaEventBus** | High-throughput, event streaming | Very High | High | Kafka cluster |

**Decision matrix:**

- **Single server?** → InMemoryEventBus
- **Multiple servers, moderate scale?** → RedisEventBus
- **Enterprise with complex routing?** → RabbitMQEventBus
- **High throughput, event streaming?** → KafkaEventBus

---

## Event Bus Statistics

Monitor event bus health with statistics:

```python
# InMemoryEventBus statistics
stats = bus.get_stats()
print(f"Events published: {stats['events_published']}")
print(f"Handlers invoked: {stats['handlers_invoked']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Background tasks: {stats['background_tasks_created']}")

# Check subscriber counts
count = bus.get_subscriber_count(OrderPlaced)
print(f"Subscribers for OrderPlaced: {count}")

wildcard_count = bus.get_wildcard_subscriber_count()
print(f"Wildcard subscribers: {wildcard_count}")
```

---

## Complete Working Example

```python
"""
Tutorial 7: Event Bus
Run with: python tutorial_07_event_bus.py
"""

import asyncio
from collections import defaultdict
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventBus,
    InMemoryEventStore,
    register_event,
)


# =============================================================================
# Events
# =============================================================================


@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_name: str
    total_amount: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str


@register_event
class OrderCancelled(DomainEvent):
    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"

    reason: str


# =============================================================================
# Aggregate
# =============================================================================


class OrderState(BaseModel):
    order_id: UUID
    customer_name: str = ""
    total_amount: float = 0.0
    status: str = "draft"


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_name=event.customer_name,
                total_amount=event.total_amount,
                status="placed",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={"status": "shipped"})
        elif isinstance(event, OrderCancelled):
            if self._state:
                self._state = self._state.model_copy(update={"status": "cancelled"})

    def place(self, customer_name: str, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        event = OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_name=customer_name,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "placed":
            raise ValueError("Order must be placed to ship")
        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def cancel(self, reason: str) -> None:
        if not self.state or self.state.status in ("shipped", "cancelled"):
            raise ValueError("Cannot cancel this order")
        event = OrderCancelled(
            aggregate_id=self.aggregate_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Projections
# =============================================================================


class OrderSummaryProjection:
    """Maintains order counts and revenue."""

    def __init__(self):
        self.order_counts: dict[str, int] = defaultdict(int)
        self.total_revenue: float = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self.order_counts["placed"] += 1
            self.total_revenue += event.total_amount

        elif isinstance(event, OrderShipped):
            if self.order_counts["placed"] > 0:
                self.order_counts["placed"] -= 1
            self.order_counts["shipped"] += 1

        elif isinstance(event, OrderCancelled):
            for status in ["placed", "shipped"]:
                if self.order_counts[status] > 0:
                    self.order_counts[status] -= 1
                    break
            self.order_counts["cancelled"] += 1


# =============================================================================
# Handlers
# =============================================================================


class NotificationHandler:
    """Sends notifications when orders are placed."""

    def __init__(self):
        self.notifications_sent = 0

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            print(f"  [Notification] Order placed by {event.customer_name}")
            self.notifications_sent += 1


class AuditLogger:
    """Logs all events for audit trail."""

    def __init__(self):
        self.audit_log: list[dict] = []

    async def handle(self, event: DomainEvent) -> None:
        entry = {
            "event_type": event.event_type,
            "aggregate_id": str(event.aggregate_id),
            "aggregate_type": event.aggregate_type,
            "timestamp": event.occurred_at,
        }
        self.audit_log.append(entry)


# =============================================================================
# Demo
# =============================================================================


async def main():
    print("=" * 60)
    print("Tutorial 7: Event Bus")
    print("=" * 60)

    # Setup infrastructure
    print("\n1. Setting up infrastructure...")
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()

    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,  # Auto-publish on save
    )
    print("   - Event store, bus, and repository initialized")

    # Create handlers and projections
    print("\n2. Creating handlers and projections...")
    projection = OrderSummaryProjection()
    notifications = NotificationHandler()
    audit_logger = AuditLogger()

    # Subscribe to event bus
    print("\n3. Subscribing to event bus...")
    bus.subscribe_all(projection)  # Subscribe projection to OrderPlaced, OrderShipped, OrderCancelled
    bus.subscribe(OrderPlaced, notifications)  # Specific event type
    bus.subscribe_to_all_events(audit_logger)  # Wildcard - all events
    print(f"   - Projection subscribed to {len(projection.subscribed_to())} event types")
    print("   - Notification handler subscribed to OrderPlaced")
    print("   - Audit logger subscribed to all events")

    # Create and publish orders
    print("\n4. Creating orders...")
    order_ids = []

    for i in range(3):
        order_id = uuid4()
        order_ids.append(order_id)

        order = repo.create_new(order_id)
        order.place(f"Customer {i+1}", 100.0 + i * 50)
        await repo.save(order)  # Triggers event bus automatically

    print(f"   Created {len(order_ids)} orders")

    # Ship one order
    print("\n5. Shipping an order...")
    order = await repo.load(order_ids[0])
    order.ship("TRACK-123")
    await repo.save(order)
    print("   Order shipped")

    # Cancel one order
    print("\n6. Cancelling an order...")
    order = await repo.load(order_ids[1])
    order.cancel("Customer request")
    await repo.save(order)
    print("   Order cancelled")

    # Query projection
    print("\n7. Querying projection:")
    print(f"   Total revenue: ${projection.total_revenue:.2f}")
    print(f"   Order counts: {dict(projection.order_counts)}")

    # Check notifications
    print("\n8. Notification handler:")
    print(f"   Notifications sent: {notifications.notifications_sent}")

    # Check audit log
    print("\n9. Audit log:")
    print(f"   Total events logged: {len(audit_logger.audit_log)}")
    for entry in audit_logger.audit_log[:3]:  # Show first 3
        print(f"   - {entry['event_type']} on {entry['aggregate_type']}")

    # Event bus statistics
    print("\n10. Event bus statistics:")
    stats = event_bus.get_stats()
    print(f"    Events published: {stats['events_published']}")
    print(f"    Handlers invoked: {stats['handlers_invoked']}")
    print(f"    Handler errors: {stats['handler_errors']}")

    print(f"\n    Subscribers for OrderPlaced: {event_bus.get_subscriber_count(OrderPlaced)}")
    print(f"    Wildcard subscribers: {event_bus.get_wildcard_subscriber_count()}")

    # Shutdown
    print("\n11. Shutting down...")
    await event_bus.shutdown()
    print("    Event bus shutdown complete")

    print("\n" + "=" * 60)
    print("Tutorial complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
============================================================
Tutorial 7: Event Bus
============================================================

1. Setting up infrastructure...
   - Event store, bus, and repository initialized

2. Creating handlers and projections...

3. Subscribing to event bus...
   - Projection subscribed to 3 event types
   - Notification handler subscribed to OrderPlaced
   - Audit logger subscribed to all events

4. Creating orders...
  [Notification] Order placed by Customer 1
  [Notification] Order placed by Customer 2
  [Notification] Order placed by Customer 3
   Created 3 orders

5. Shipping an order...
   Order shipped

6. Cancelling an order...
   Order cancelled

7. Querying projection:
   Total revenue: $450.00
   Order counts: {'placed': 1, 'shipped': 1, 'cancelled': 1}

8. Notification handler:
   Notifications sent: 3

9. Audit log:
   Total events logged: 5
   - OrderPlaced on Order
   - OrderPlaced on Order
   - OrderPlaced on Order

10. Event bus statistics:
    Events published: 5
    Handlers invoked: 13
    Handler errors: 0
    Subscribers for OrderPlaced: 2
    Wildcard subscribers: 1

11. Shutting down...
    Event bus shutdown complete

============================================================
Tutorial complete!
============================================================
```

---

## Event Bus Patterns

### Pattern 1: Multiple Projections

Build different views from the same events:

```python
# Subscribe multiple projections
bus.subscribe_all(order_summary_projection)
bus.subscribe_all(customer_stats_projection)
bus.subscribe_all(daily_revenue_projection)

# All three update automatically when events are published
```

### Pattern 2: Integration Handlers

Trigger external systems on events:

```python
class StripePaymentHandler:
    async def handle(self, event: DomainEvent):
        if isinstance(event, OrderPlaced):
            await self.stripe_api.create_payment_intent(
                amount=event.total_amount
            )

bus.subscribe(OrderPlaced, stripe_handler)
```

### Pattern 3: Saga Coordination

Coordinate multi-aggregate workflows:

```python
class OrderFulfillmentSaga:
    async def handle(self, event: DomainEvent):
        if isinstance(event, OrderPlaced):
            # Reserve inventory
            # Authorize payment
            # Schedule shipping
            pass

bus.subscribe(OrderPlaced, saga_handler)
```

### Pattern 4: Metrics Collection

```python
class MetricsHandler:
    def __init__(self, metrics_client):
        self.client = metrics_client

    async def handle(self, event: DomainEvent):
        self.client.increment(f"{event.event_type}.count")

bus.subscribe_to_all_events(metrics_handler)
```

---

## Key Takeaways

1. **Event buses decouple producers from consumers**: Publishers don't know about subscribers
2. **InMemoryEventBus for single-process**: Perfect for development and single-instance deployments
3. **Distributed buses for production**: Redis, RabbitMQ, or Kafka for multi-server deployments
4. **subscribe_all() for projections**: Subscribe projections to their declared event types
5. **Wildcard subscriptions for cross-cutting**: Audit, metrics, logging across all events
6. **Repository integration**: Auto-publish events after successful saves
7. **Multiple subscribers per event**: Fan-out pattern enables many consumers
8. **Background publishing improves performance**: Trade immediate consistency for speed
9. **Error isolation**: One handler's failure doesn't affect others
10. **Statistics for monitoring**: Track events, handlers, and errors

---

## Common Patterns

### Unsubscribing Handlers

```python
# Subscribe
bus.subscribe(OrderPlaced, my_handler)

# Unsubscribe
removed = bus.unsubscribe(OrderPlaced, my_handler)
print(f"Handler removed: {removed}")  # True if found
```

### Clearing All Subscribers

```python
# Useful for testing
bus.clear_subscribers()
```

### Graceful Shutdown

```python
# Wait for background tasks to complete
await bus.shutdown(timeout=30.0)
```

---

## Distributed Event Bus Setup

### RedisEventBus Example

```python
from eventsource.bus import RedisEventBus, RedisEventBusConfig
from eventsource.events import get_event_registry

# Configuration
config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="myapp",
    consumer_group="projections",
    batch_size=100,
    max_retries=3,
)

# Create bus
bus = RedisEventBus(
    config=config,
    event_registry=get_event_registry(),
)

# Connect
await bus.connect()

# Subscribe (same API as InMemoryEventBus)
bus.subscribe(OrderPlaced, on_order_placed)

# Start consuming from Redis
await bus.start_consuming()

# Publish to Redis
await bus.publish([OrderPlaced(...)])

# Shutdown
await bus.stop_consuming()
await bus.disconnect()
```

### Key Differences from InMemoryEventBus

1. **Requires event registry**: For event serialization/deserialization
2. **Connection management**: `connect()` and `disconnect()` methods
3. **Consumer lifecycle**: `start_consuming()` and `stop_consuming()`
4. **Durability**: Events survive process restarts
5. **Consumer groups**: Multiple consumers share load

---

## Next Steps

Now that you understand event buses, you're ready to learn about testing event-sourced systems.

Continue to [Tutorial 8: Testing Event-Sourced Systems](08-testing.md) to learn about:
- Writing unit tests for aggregates
- Testing projections with event fixtures
- Integration testing with event stores and buses
- Test patterns for event-driven systems

For more examples, see:
- `tests/unit/test_event_bus.py` - Comprehensive event bus tests
- `tests/integration/bus/` - Redis, RabbitMQ, and Kafka integration tests
- `src/eventsource/bus/` - Event bus implementations
