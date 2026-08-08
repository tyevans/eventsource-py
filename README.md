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

The smallest thing that works — one event, one command, one aggregate, saved and
reloaded. Copy it into `order.py` and run it:

```python
import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    DeciderAggregate,
    DomainCommand,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)


@register_event
class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"
    total: Decimal


class PlaceOrder(DomainCommand):
    order_id: UUID
    total: Decimal


class OrderState(BaseModel):
    total: Decimal = Decimal("0")
    status: str = "draft"


class Order(DeciderAggregate[OrderState, PlaceOrder]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state() -> OrderState:
        return OrderState()

    @staticmethod
    def decide(command: PlaceOrder, state: OrderState) -> list[DomainEvent]:
        return [OrderPlaced(aggregate_id=command.order_id, total=command.total)]

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        match event:
            case OrderPlaced(total=total):
                return state.model_copy(update={"total": total, "status": "placed"})
            case _:
                return state


async def main() -> None:
    repo = AggregateRepository(event_store=InMemoryEventStore(), aggregate_factory=Order)

    order_id = uuid4()
    order = repo.create_new(order_id)
    order.execute(PlaceOrder(order_id=order_id, total=Decimal("42.00")))
    await repo.save(order)

    reloaded = await repo.load(order_id)
    print(f"status: {reloaded.state.status}")
    print(f"total: {reloaded.state.total}")
    print(f"version: {reloaded.version}")


asyncio.run(main())
```

```
status: placed
total: 42.00
version: 1
```

Nothing was stored but the event. `reloaded` was rebuilt by replaying it — that is the
whole idea, and everything below is elaboration on it.

## A Fuller Example

The same `Order`, with a second event, a business rule, and a command the domain
refuses. This is the **decider** style — the aggregate is three pure static functions
(`initial_state`, `decide`, `evolve`) with no `self`, no I/O, and nothing to mock.

```python
import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    CommandRejectedError,
    DeciderAggregate,
    DomainCommand,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)


# 1. Events - immutable facts. `event_type` is derived from the class name.
@register_event
class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"

    customer_id: UUID
    total: Decimal


@register_event
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"

    tracking_number: str


# 2. State - derived from the events, never the source of truth.
class OrderState(BaseModel):
    model_config = {"frozen": True}

    customer_id: UUID | None = None
    total: Decimal = Decimal("0")
    status: str = "draft"


# 3. Commands - intents as values, which the domain may refuse.
class PlaceOrder(DomainCommand):
    order_id: UUID
    customer_id: UUID
    total: Decimal


class ShipOrder(DomainCommand):
    order_id: UUID
    tracking_number: str


OrderCommand = PlaceOrder | ShipOrder


# 4. The decider - the whole domain as two pure functions.
class Order(DeciderAggregate[OrderState, OrderCommand]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state() -> OrderState:
        return OrderState()

    @staticmethod
    def decide(command: OrderCommand, state: OrderState) -> list[DomainEvent]:
        match command, state:
            case PlaceOrder(), OrderState(status="draft"):
                return [
                    OrderPlaced(
                        aggregate_id=command.order_id,
                        customer_id=command.customer_id,
                        total=command.total,
                    )
                ]
            case PlaceOrder(), _:
                raise CommandRejectedError("order already placed", command=command)
            case ShipOrder(), OrderState(status="placed"):
                return [
                    OrderShipped(
                        aggregate_id=command.order_id,
                        tracking_number=command.tracking_number,
                    )
                ]
            case ShipOrder(), _:
                raise CommandRejectedError("order must be placed before shipping", command=command)

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        match event:
            case OrderPlaced(customer_id=customer_id, total=total):
                return state.model_copy(
                    update={"customer_id": customer_id, "total": total, "status": "placed"}
                )
            case OrderShipped():
                return state.model_copy(update={"status": "shipped"})
            case _:
                return state


# 5. Wire it together.
async def main() -> None:
    repo: AggregateRepository[Order] = AggregateRepository(
        event_store=InMemoryEventStore(),
        aggregate_factory=Order,
    )

    order_id = uuid4()
    order = repo.create_new(order_id)
    order.execute(PlaceOrder(order_id=order_id, customer_id=uuid4(), total=Decimal("99.95")))
    order.execute(ShipOrder(order_id=order_id, tracking_number="TRACK-001"))
    await repo.save(order)

    # Events are the source of truth - reload from history alone.
    reloaded = await repo.load(order_id)
    print(f"status: {reloaded.state.status}")
    print(f"total: {reloaded.state.total}")
    print(f"version: {reloaded.version}")

    # A rejected command leaves the aggregate untouched.
    try:
        reloaded.execute(ShipOrder(order_id=order_id, tracking_number="TRACK-002"))
    except CommandRejectedError as exc:
        print(f"rejected: {exc}")
    print(f"version after rejection: {reloaded.version}")


asyncio.run(main())
```

```
status: shipped
total: 99.95
version: 2
rejected: order must be placed before shipping
version after rejection: 2
```

Three conventions in there are worth calling out, because they are what the library
expects everywhere:

- **Never declare `event_type` by hand.** `DomainEvent.__init_subclass__` derives it
  from the class name, so there is nothing that can drift out of sync with the class.
  Declare it only to pin a wire name that must differ from the class name.
- **Money is `Decimal`, never `float`.** See
  [Money and Precision](https://tyevans.github.io/eventsource-py/guides/money-and-precision/).
- **`aggregate_id` and `aggregate_version` are stamped for you.** `execute()` stamps
  every event `decide()` returns; in the imperative style below, `create_event()` does
  the same. Writing `aggregate_version=self.get_next_version()` by hand is a bug
  waiting to happen.

See [The Decider Pattern](https://tyevans.github.io/eventsource-py/explanation/decider-pattern/)
for the trade-offs and benchmarks (spoiler: identical on replay, a few microseconds
per command — maintainability is the deciding factor, not speed).

<details>
<summary>The same aggregate in the imperative style</summary>

If you prefer commands as methods, subclass `AggregateRoot` instead. Events, state,
repository, and output are identical; `create_event()` stamps `aggregate_id`,
`aggregate_type`, and `aggregate_version` so you never write them out.

```python
from eventsource import AggregateRoot


class Order(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState()

    def _apply(self, event: DomainEvent) -> None:
        match event:
            case OrderPlaced(customer_id=customer_id, total=total):
                self._state = OrderState(
                    customer_id=customer_id, total=total, status="placed"
                )
            case OrderShipped():
                self._state = self._state.model_copy(update={"status": "shipped"})

    def place(self, customer_id: UUID, total: Decimal) -> None:
        if self.version > 0:
            raise ValueError("order already placed")
        self.create_event(OrderPlaced, customer_id=customer_id, total=total)

    def ship(self, tracking_number: str) -> None:
        if self.state.status != "placed":
            raise ValueError("order must be placed before shipping")
        self.create_event(OrderShipped, tracking_number=tracking_number)
```

```python
order = repo.create_new(order_id)
order.place(customer_id=uuid4(), total=Decimal("99.95"))
order.ship(tracking_number="TRACK-001")
await repo.save(order)
```

```
status: shipped
total: 99.95
version: 2
```

</details>

## Reading the Same Events a Second Way

One stream, many views: a **projection** folds the events into a read model, and a
**subscription** feeds it — catching up from history, then following live events, with
checkpoints so a restart resumes where it left off. Building on the `Order` decider
above:

```python
from decimal import Decimal

from eventsource import DomainEvent, InMemoryCheckpointRepository, InMemoryEventBus
from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager


class SalesReport:
    """Read model built by folding the same events a second way."""

    def __init__(self) -> None:
        self.revenue = Decimal("0")
        self.placed = 0
        self.shipped = 0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        match event:
            case OrderPlaced(total=total):
                self.revenue += total
                self.placed += 1
            case OrderShipped():
                self.shipped += 1


async def main() -> None:
    store = InMemoryEventStore()
    bus = InMemoryEventBus()
    repo: AggregateRepository[Order] = AggregateRepository(
        event_store=store,
        aggregate_factory=Order,
        event_publisher=bus,  # publishes to the bus after a successful save
    )

    manager = SubscriptionManager(store, bus, InMemoryCheckpointRepository())
    report = SalesReport()
    await manager.subscribe(
        report, SubscriptionConfig(start_from="beginning"), name="SalesReport"
    )
    await manager.start()

    for i in range(3):
        order_id = uuid4()
        order = repo.create_new(order_id)
        order.execute(
            PlaceOrder(order_id=order_id, customer_id=uuid4(), total=Decimal("100.00") * (i + 1))
        )
        if i == 0:
            order.execute(ShipOrder(order_id=order_id, tracking_number="TRACK-001"))
        await repo.save(order)

    # Delivery is asynchronous: give the bus a moment to drain before reading.
    await asyncio.sleep(0.1)

    print(f"revenue: {report.revenue}")
    print(f"placed: {report.placed}")
    print(f"shipped: {report.shipped}")

    await manager.stop()


asyncio.run(main())
```

```
revenue: 600.00
placed: 3
shipped: 1
```

The `asyncio.sleep(0.1)` is a script-only shortcut: publication is asynchronous, so a
process that exits immediately after `save()` may exit before the projection has seen
anything. Real services stay up; if you need to *know* the projection has caught up,
read its checkpoint rather than sleeping. See
[Projections](https://tyevans.github.io/eventsource-py/tutorials/06-projections/).

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
- **Testing toolkit** - `eventsource.testing` ships an in-memory harness, an event
  builder, BDD helpers, a recording bus, and port conformance suites

## Testing Your Domain

`eventsource.testing` is a first-class part of the package, not an internal detail:

- `DeciderScenario` — given/when/then over `decide()` and `evolve()` directly, with no
  store, no bus, and no event loop.
- `InMemoryTestHarness` — a pre-wired in-memory store, bus, and repository, plus the
  `given_events` / `when_command` / `then_event_published` BDD helpers.
- `EventBuilder` and `EventAssertions` — build test events without restating the
  aggregate plumbing, and assert on them with messages that say what went wrong.
- `RecordingEventBus` — a bus that remembers what was published.
- **Port conformance suites** (`AppenderConformance`, `StreamReaderConformance`,
  `CheckpointRepositoryConformance`, and a dozen more) — subclass one, supply a
  `store` fixture, and your own backend is held to the same contract the built-in
  adapters are. See
  [Validate a Custom Backend](https://tyevans.github.io/eventsource-py/guides/validate-custom-backend/).

```python
from eventsource.testing import DeciderScenario, InMemoryTestHarness, AppenderConformance
```

## Documentation

**[Full Documentation](https://tyevans.github.io/eventsource-py)** - Guides, examples, and API reference

## License

MIT
