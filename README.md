# eventsource-py

[![PyPI version](https://img.shields.io/pypi/v/eventsource-py.svg)](https://pypi.org/project/eventsource-py/)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/tyevans/eventsource-py/actions/workflows/ci.yml/badge.svg)](https://github.com/tyevans/eventsource-py/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

**Stop losing data. Start capturing history.**

Traditional databases overwrite state on every update. Event sourcing captures *what happened* as a sequence of immutable events, giving you:

- **Complete audit trail** - Know exactly what changed, when, and why
- **Time travel** - Reconstruct state at any point in history
- **Multiple views** - Build different read models from the same events
- **Reliable debugging** - Replay events to reproduce any bug

eventsource-py makes this practical for Python applications with a clean, async-first API.

```bash
pip install eventsource-py
```

## Quick Start

```python
import asyncio
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    DomainEvent, register_event, AggregateRoot, AggregateRepository,
    InMemoryEventStore, InMemoryEventBus, InMemoryCheckpointRepository,
)
from eventsource.application.subscriptions import SubscriptionManager, SubscriptionConfig

# 1. Define events - immutable facts that capture what happened
@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"
    customer_id: UUID
    total: float

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str

# 2. Define aggregate state and business logic
class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    total: float = 0.0
    status: str = "draft"

class Order(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        match event:
            case OrderPlaced():
                self._state = OrderState(
                    order_id=self.aggregate_id,
                    customer_id=event.customer_id,
                    total=event.total,
                    status="placed",
                )
            case OrderShipped():
                self._state = self._state.model_copy(update={"status": "shipped"})

    def place(self, customer_id: UUID, total: float) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        self.apply_event(OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            total=total,
            aggregate_version=self.get_next_version(),
        ))

    def ship(self, tracking_number: str) -> None:
        if self.state.status != "placed":
            raise ValueError("Order must be placed before shipping")
        self.apply_event(OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        ))

# 3. Define a projection - build read models from the event stream
class SalesReport:
    """Read model that tracks sales metrics from events."""
    def __init__(self):
        self.total_revenue = 0.0
        self.orders_placed = 0
        self.orders_shipped = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        match event:
            case OrderPlaced():
                self.total_revenue += event.total
                self.orders_placed += 1
            case OrderShipped():
                self.orders_shipped += 1

# 4. Wire it together
async def main():
    # Infrastructure
    store = InMemoryEventStore()
    bus = InMemoryEventBus()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=Order,
        event_publisher=bus,  # Publishes events to the bus after saving
    )

    # Set up subscription manager with our projection
    manager = SubscriptionManager(store, bus, InMemoryCheckpointRepository())
    report = SalesReport()
    await manager.subscribe(report, SubscriptionConfig(start_from="beginning"), name="SalesReport")
    await manager.start()

    # Create some orders
    for i in range(3):
        order = repo.create_new(uuid4())
        order.place(customer_id=uuid4(), total=100.0 * (i + 1))
        await repo.save(order)

        if i == 0:  # Ship the first order
            shipped_order_id = order.aggregate_id
            order.ship(tracking_number="TRACK-001")
            await repo.save(order)

    await asyncio.sleep(0.1)  # Let events propagate

    # The projection built a read model from the event stream
    print(f"Revenue: ${report.total_revenue}")      # Revenue: $600.0
    print(f"Orders placed: {report.orders_placed}")  # Orders placed: 3
    print(f"Orders shipped: {report.orders_shipped}")  # Orders shipped: 1

    # Events are the source of truth - reload aggregate from its event history
    order = await repo.load(shipped_order_id)
    print(f"Order status: {order.state.status}")  # Order status: shipped
    print(f"Order version: {order.version}")      # Order version: 2 (placed + shipped)

    await manager.stop()

asyncio.run(main())
```

### Alternative: the decider style

The `Order` aggregate above is imperative — commands are methods, and business rules
live inside them. If you prefer your domain as **pure functions**, the same aggregate
can be written in the decider style: commands become values, and the whole domain
becomes two functions you can unit-test with plain asserts — no aggregate instance, no
event loop, no fixtures.

```python
from eventsource import DeciderAggregate, DomainCommand, CommandRejectedError

# Commands - intents as values, may be rejected
class PlaceOrder(DomainCommand):
    customer_id: UUID
    total: float

class ShipOrder(DomainCommand):
    tracking_number: str

OrderCommand = PlaceOrder | ShipOrder

# The aggregate as three pure static methods - no I/O, no self, no versions
class Order(DeciderAggregate[OrderState]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> OrderState:
        return OrderState(order_id=aggregate_id)

    @staticmethod
    def decide(command: OrderCommand, state: OrderState) -> list[DomainEvent]:
        """Command + current state -> new events (or a rejection)."""
        match command, state:
            case PlaceOrder(customer_id=cid, total=total), OrderState(status="draft"):
                return [OrderPlaced(aggregate_id=state.order_id, customer_id=cid, total=total)]
            case PlaceOrder(), _:
                raise CommandRejectedError("Order already placed", command)
            case ShipOrder(tracking_number=tn), OrderState(status="placed"):
                return [OrderShipped(aggregate_id=state.order_id, tracking_number=tn)]
            case ShipOrder(), _:
                raise CommandRejectedError("Order must be placed before shipping", command)

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        """State + event -> next state."""
        match event:
            case OrderPlaced(customer_id=cid, total=total):
                return state.model_copy(update={"customer_id": cid, "total": total, "status": "placed"})
            case OrderShipped():
                return state.model_copy(update={"status": "shipped"})
            case _:
                return state
```

Callers issue commands as values instead of calling methods — `execute()` is inherited
from `DeciderAggregate`, and everything else (events, state, projection, wiring) is
identical to the example above, and so is the output:

```python
order = repo.create_new(uuid4())
order.execute(PlaceOrder(customer_id=uuid4(), total=100.0))
await repo.save(order)

order.execute(ShipOrder(tracking_number="TRACK-001"))
await repo.save(order)
```

See [The Decider Pattern](https://tyevans.github.io/eventsource-py/explanation/decider-pattern/)
for the trade-offs and benchmarks (spoiler: identical on replay, a few microseconds
per command — maintainability is the deciding factor, not speed).

## Production Ready

Swap in production backends when you're ready to deploy:

| Component | Development | Production |
|-----------|-------------|------------|
| Event Store | `InMemoryEventStore` | `PostgreSQLEventStore`, `SQLiteEventStore` |
| Event Bus | `InMemoryEventBus` | `RedisEventBus`, `RabbitMQEventBus`, `KafkaEventBus` |
| Checkpoints | `InMemoryCheckpointRepository` | `PostgreSQLCheckpointRepository` |

```bash
# Add PostgreSQL + Redis for production
pip install eventsource-py[postgresql,redis]
```

<details>
<summary>All installation options</summary>

```bash
pip install eventsource-py[postgresql]  # PostgreSQL event store
pip install eventsource-py[sqlite]      # SQLite event store
pip install eventsource-py[redis]       # Redis event bus
pip install eventsource-py[rabbitmq]    # RabbitMQ event bus
pip install eventsource-py[kafka]       # Kafka event bus
pip install eventsource-py[telemetry]   # OpenTelemetry tracing
pip install eventsource-py[all]         # Everything
```

</details>

## Features

- **Event Stores** - PostgreSQL, SQLite, In-Memory with optimistic concurrency
- **Event Bus** - Redis Streams, RabbitMQ, Kafka, In-Memory with consumer groups
- **Subscriptions** - Catch-up from history, live events, checkpointing, graceful shutdown
- **Projections** - Declarative handlers, retry logic, dead letter queues
- **Snapshots** - Optimize aggregate loading for long event streams
- **Multi-tenancy** - Built-in tenant isolation
- **Observability** - OpenTelemetry integration

## Documentation

**[Full Documentation](https://tyevans.github.io/eventsource-py)** - Guides, examples, and API reference

## License

MIT
