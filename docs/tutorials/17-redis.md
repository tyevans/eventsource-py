# Tutorial 17: Using Redis Event Bus

**Difficulty:** Advanced
**Progress:** Tutorial 17 of 21 | Phase 4: Advanced Patterns

---

## Prerequisites

- Completed [Tutorial 15: Production Deployment Guide](15-production.md) and [Tutorial 7: Distributing Events with Event Bus](07-event-bus.md)
- Python 3.11+, Docker installed
- Redis support: `pip install eventsource-py[redis]`

Redis Streams enable distributed event distribution across multiple processes and servers, providing durability, load balancing, and fault tolerance.

---

## Why Redis for Event Distribution?

`InMemoryEventBus` works only within a single process. Redis Streams provide:

| Feature | Benefit |
|---------|---------|
| **Durability** | Events persist in Redis, surviving restarts |
| **Consumer Groups** | Multiple consumers can share the workload |
| **At-Least-Once Delivery** | Messages acknowledged after successful processing |
| **Message Recovery** | Pending messages can be recovered after crashes |
| **Dead Letter Queue** | Failed messages captured for later analysis |
| **Horizontal Scaling** | Add more consumers to handle increased load |

Use `RedisEventBus` for multi-process/multi-server architectures. Use `InMemoryEventBus` for single-process apps or tests.

---

## Setting Up Redis

Start Redis with Docker:

```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
docker exec redis redis-cli ping  # Should output: PONG
```

Test connection:

```python
import asyncio
import redis.asyncio as aioredis

async def test_redis():
    client = await aioredis.from_url("redis://localhost:6379")
    try:
        result = await client.ping()
        print(f"Redis connection: {'OK' if result else 'FAILED'}")
    finally:
        await client.aclose()

asyncio.run(test_redis())
```

---

## RedisEventBus Configuration

```python
from eventsource.bus import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",  # default
    stream_prefix="myapp",  # default: "events"
    consumer_group="projections",  # default: "default"
    consumer_name="worker-1",  # auto-generated if omitted
    batch_size=100,  # max events per read
    max_retries=3,  # retries before DLQ
)
bus = RedisEventBus(config=config)
```

---

## Basic Usage

```python
import asyncio
from uuid import uuid4
from eventsource import DomainEvent, EventRegistry, register_event
from eventsource.bus import RedisEventBus, RedisEventBusConfig

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str
    total: float

# Setup
registry = EventRegistry()
registry.register(OrderCreated)

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="orders",
    consumer_group="order-processors",
)
bus = RedisEventBus(config=config, event_registry=registry)
await bus.connect()

# Subscribe
async def log_order_created(event: DomainEvent) -> None:
    print(f"Order created: {event.aggregate_id}")

bus.subscribe(OrderCreated, log_order_created)

# Publish
order_id = uuid4()
await bus.publish([OrderCreated(
    aggregate_id=order_id,
    customer_id="cust-123",
    total=99.99,
    aggregate_version=1,
)])

# Consume
consumer_task = asyncio.create_task(bus.start_consuming())
await bus.stop_consuming()
await consumer_task

# Shutdown
await bus.shutdown(timeout=30.0)
```

---

## Consumer Groups and Load Balancing

Consumer groups enable load balancing - each event goes to exactly one consumer within the group:

```python
# Multiple workers in same group (events distributed)
config_worker1 = RedisEventBusConfig(
    stream_prefix="orders", consumer_group="order-processors", consumer_name="worker-1")
config_worker2 = RedisEventBusConfig(
    stream_prefix="orders", consumer_group="order-processors", consumer_name="worker-2")

# Different groups (both receive all events)
config_projections = RedisEventBusConfig(consumer_group="projections")
config_notifications = RedisEventBusConfig(consumer_group="notifications")

# Check status
info = await bus.get_stream_info()
for group in info['consumer_groups']:
    print(f"{group['name']}: {group['consumers']} consumers, {group['pending']} pending")
```

---

## Dead Letter Queue (DLQ)

Messages that fail after `max_retries` are sent to the DLQ:

```python
# Configure
config = RedisEventBusConfig(enable_dlq=True, max_retries=3, dlq_stream_suffix="_dlq")

# Inspect
dlq_messages = await bus.get_dlq_messages(count=100)
for msg in dlq_messages:
    print(f"{msg['message_id']}: {msg['data'].get('event_type')} (retries: {msg['data'].get('retry_count')})")

# Replay after fixing issue
await bus.replay_dlq_message(message_id="1234567890-0")

# Monitor
info = await bus.get_stream_info()
if info.get("dlq_messages", 0) > 0:
    print(f"WARNING: {info['dlq_messages']} messages in DLQ!")
```

---

## Pending Message Recovery

Messages read but not acknowledged (due to crashes) can be recovered:

```python
# Manual recovery
recovery_stats = await bus.recover_pending_messages(
    min_idle_time_ms=60000, max_retries=3, batch_size=100)
print(f"Claimed: {recovery_stats['claimed']}, Reprocessed: {recovery_stats['reprocessed']}")

# Automatic recovery for production
async def recovery_worker(bus: RedisEventBus):
    while True:
        stats = await bus.recover_pending_messages()
        if stats['reprocessed'] > 0 or stats['dlq'] > 0:
            print(f"Recovery: {stats['reprocessed']} reprocessed, {stats['dlq']} to DLQ")
        await asyncio.sleep(60)

recovery_task = asyncio.create_task(recovery_worker(bus))
```

---

## Statistics and Monitoring

```python
# Statistics
stats = bus.get_stats_dict()
print(f"Published: {stats['events_published']}, Consumed: {stats['events_consumed']}")
print(f"Success: {stats['events_processed_success']}, Failed: {stats['events_processed_failed']}")

# Stream info
info = await bus.get_stream_info()
print(f"Stream: {info['stream']['name']}, Length: {info['stream']['length']}, Pending: {info['pending_messages']}")
```

---

## Integration with Repository

```python
from eventsource import AggregateRepository, PostgreSQLEventStore

bus = RedisEventBus(config=RedisEventBusConfig(stream_prefix="orders"), event_registry=registry)
await bus.connect()

repo = AggregateRepository(
    event_store=PostgreSQLEventStore(session_factory),
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=bus,  # Events auto-published to Redis
)

bus.subscribe_all(order_projection)
order = repo.create_new(uuid4())
order.create(customer_id="cust-123", total=99.99)
await repo.save(order)  # Events flow through Redis
```

---

## Complete Example

```python
"""Tutorial 17: Using Redis Event Bus - Complete working example."""
import asyncio
import signal
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    EventRegistry,
    InMemoryEventStore,
    register_event,
)
from eventsource.bus import (
    REDIS_AVAILABLE,
    RedisEventBus,
    RedisEventBusConfig,
)

if not REDIS_AVAILABLE:
    print("ERROR: Redis support not installed.")
    print("Install with: pip install eventsource-py[redis]")
    exit(1)

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str
    total: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str

class OrderState(BaseModel):
    order_id: UUID
    customer_id: str | None = None
    total: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                total=event.total,
                status="created",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                    }
                )

    def create(self, customer_id: str, total: float) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(
            OrderCreated(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                total=total,
                aggregate_version=self.get_next_version(),
            )
        )

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                tracking_number=tracking_number,
                aggregate_version=self.get_next_version(),
            )
        )

class OrderCountProjection:
    """Simple projection that counts orders."""

    def __init__(self):
        self.created_count = 0
        self.shipped_count = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self.created_count += 1
            print(f"  [Projection] Order created: {event.aggregate_id}")
        elif isinstance(event, OrderShipped):
            self.shipped_count += 1
            print(f"  [Projection] Order shipped: {event.aggregate_id}")


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
        print(f"  [Audit] {event.event_type} at {event.occurred_at}")

async def demo_basic_publish_subscribe():
    """Demonstrate basic publishing and subscribing."""
    print("\n" + "=" * 60)
    print("Demo 1: Basic Publish/Subscribe")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderCreated)
    registry.register(OrderShipped)

    config = RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="demo1",
        consumer_group="demo-consumers",
        block_ms=1000,
        single_connection_client=True,
    )
    bus = RedisEventBus(config=config, event_registry=registry)

    try:
        await bus.connect()
        print("Connected to Redis")

        received_events: list[DomainEvent] = []

        async def event_handler(event: DomainEvent) -> None:
            received_events.append(event)
            print(f"  Received: {event.event_type}")

        bus.subscribe(OrderCreated, event_handler)
        print("Subscribed handler to OrderCreated")

        order_id = uuid4()
        event = OrderCreated(
            aggregate_id=order_id,
            customer_id="cust-123",
            total=99.99,
            aggregate_version=1,
        )
        await bus.publish([event])
        print(f"Published OrderCreated for {order_id}")

        consume_task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(2.0)

        await bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        stats = bus.get_stats_dict()
        print(f"Received {len(received_events)} event(s). Published: {stats['events_published']}, Consumed: {stats['events_consumed']}")

    finally:
        await bus.disconnect()


async def demo_consumer_groups():
    """Demonstrate consumer groups for load balancing."""
    print("\n" + "=" * 60)
    print("Demo 2: Consumer Groups")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderCreated)

    base_config = {
        "redis_url": "redis://localhost:6379",
        "stream_prefix": "demo2",
        "consumer_group": "load-balanced-consumers",
        "block_ms": 500,
        "single_connection_client": True,
    }

    config1 = RedisEventBusConfig(**base_config, consumer_name="worker-1")
    config2 = RedisEventBusConfig(**base_config, consumer_name="worker-2")

    bus1 = RedisEventBus(config=config1, event_registry=registry)
    bus2 = RedisEventBus(config=config2, event_registry=registry)

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

        bus1.subscribe(OrderCreated, worker1_handler)
        bus2.subscribe(OrderCreated, worker2_handler)

        print("\nPublishing 6 events...")
        for i in range(6):
            event = OrderCreated(
                aggregate_id=uuid4(),
                customer_id=f"cust-{i}",
                total=10.0 * i,
                aggregate_version=1,
            )
            await bus1.publish([event])

        task1 = asyncio.create_task(bus1.start_consuming())
        task2 = asyncio.create_task(bus2.start_consuming())
        await asyncio.sleep(3.0)

        await bus1.stop_consuming()
        await bus2.stop_consuming()

        for task in [task1, task2]:
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except TimeoutError:
                task.cancel()

        print(f"Results: Worker 1 processed {len(worker1_events)}, Worker 2 processed {len(worker2_events)} (total: {len(worker1_events) + len(worker2_events)})")

    finally:
        await bus1.disconnect()
        await bus2.disconnect()


async def demo_statistics_and_monitoring():
    """Demonstrate statistics and monitoring capabilities."""
    print("\n" + "=" * 60)
    print("Demo 3: Statistics and Monitoring")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderCreated)
    registry.register(OrderShipped)

    config = RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="demo3",
        consumer_group="monitoring-demo",
        single_connection_client=True,
    )
    bus = RedisEventBus(config=config, event_registry=registry)

    try:
        await bus.connect()

        for i in range(10):
            event = OrderCreated(
                aggregate_id=uuid4(),
                customer_id=f"cust-{i}",
                total=100.0 + i,
                aggregate_version=1,
            )
            await bus.publish([event])

        info = await bus.get_stream_info()
        print(f"Published 10 events. Stream length: {info['stream']['length']}")
        print(f"Consumer groups: {len(info['consumer_groups'])}, Pending: {info['pending_messages']}, DLQ: {info['dlq_messages']}")

        stats = bus.get_stats_dict()
        print(f"Stats - Published: {stats['events_published']}, Consumed: {stats['events_consumed']}")
        print(f"Subscribers: {bus.get_subscriber_count()} total, {bus.get_wildcard_subscriber_count()} wildcard")

    finally:
        await bus.disconnect()


async def demo_with_projection():
    """Demonstrate integration with projections."""
    print("\n" + "=" * 60)
    print("Demo 4: Integration with Projections")
    print("=" * 60)

    registry = EventRegistry()
    registry.register(OrderCreated)
    registry.register(OrderShipped)

    config = RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="demo4",
        consumer_group="projection-consumers",
        block_ms=500,
        single_connection_client=True,
    )
    bus = RedisEventBus(config=config, event_registry=registry)

    projection = OrderCountProjection()
    auditor = AuditLogger()

    try:
        await bus.connect()

        bus.subscribe_all(projection)
        print("Subscribed projection to OrderCreated and OrderShipped")

        bus.subscribe_to_all_events(auditor)
        print("Subscribed auditor to all events")

        order_id = uuid4()
        print(f"\nProcessing order: {order_id}")

        await bus.publish([
            OrderCreated(
                aggregate_id=order_id,
                customer_id="premium-customer",
                total=499.99,
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

        consume_task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(2.0)
        await bus.stop_consuming()

        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        print(f"Projection: {projection.created_count} created, {projection.shipped_count} shipped")
        print(f"Audit log: {len(auditor.log)} entries")

    finally:
        await bus.disconnect()


async def main():
    """Run all demos."""
    print("=" * 60)
    print("Tutorial 17: Using Redis Event Bus")
    print("=" * 60)

    try:
        await demo_basic_publish_subscribe()
        await demo_consumer_groups()
        await demo_statistics_and_monitoring()
        await demo_with_projection()

        print("\nTutorial 17 Complete!")

    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure Redis is running:")
        print("  docker run -d --name redis -p 6379:6379 redis:7-alpine")
        raise


if __name__ == "__main__":
    asyncio.run(main())
```

Run with: `python tutorial_17_redis.py`

---

## Next Steps

Continue to [Tutorial 18: Using Kafka Event Bus](18-kafka.md) to learn about Kafka integration for even higher throughput scenarios.
