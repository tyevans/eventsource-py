# 8. Testing Event-Sourced Code

In this tutorial you will test an event-sourced `Order` end to end without touching a
database: you will seed history, run a command, assert on what was published, replay the
stored events through a projection, and finally point the library's conformance suites at
a backend.

Event sourcing makes tests unusually pleasant, because the input and the output of a
command are both just events. `eventsource.testing` leans into that with four pieces:
`InMemoryTestHarness` (wired in-memory infrastructure), `EventBuilder` (fluent event
construction), the Given-When-Then helpers (`given_events`, `when_command`, `then_*`),
and `EventAssertions` for when the helpers run out of road. There are also two abstract
conformance suites you can subclass to prove a custom `EventStore` or `EventBus` obeys
the contract.

Everything in Steps 1-6 runs in plain Python with pytest -- no Docker, no PostgreSQL.

## What you'll build

A pytest module containing:

- a `harness` fixture wrapping `InMemoryTestHarness`
- a Given-When-Then test that ships a paid order and asserts `OrderShipped` was published
- tighter assertions for sequence, count, and no-op commands
- a projection test that folds stored events into a read model
- conformance test classes that run the built-in suites against a backend

## Prerequisites

Before you start:

- Work through [Your First Aggregate](03-first-aggregate.md) -- you should be comfortable
  with `DeciderAggregate`, `decide()`/`evolve()`, `execute()`, and `uncommitted_events`.
- [Building Projections](06-projections.md) helps for Step 6, but is not required.
- Install `pytest` and `pytest-asyncio`. Nothing in Steps 1-6 needs an optional extra:
  `eventsource.testing` and the in-memory backends ride on the core dependencies
  (pydantic and sqlalchemy). No Docker, no database.

  ```bash
  uv add --dev pytest pytest-asyncio
  ```

- Enable pytest's async mode. This repository sets it in `pyproject.toml`:

  ```toml
  [tool.pytest.ini_options]
  asyncio_mode = "auto"
  ```

  With `auto`, plain `async def test_...` functions run without a decorator -- that is
  what the examples below assume. If your project leaves the default `strict` mode, add
  `@pytest.mark.asyncio` to every async test in this tutorial.

You will write the code as a test module (for example `tests/unit/test_orders.py`) and
run it with `uv run pytest tests/unit/test_orders.py -v`.

## The sample domain (Order aggregate and its events)

Put this at the top of your test module (or in a shared `conftest.py`):

```python
from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    DeclarativeAggregate,
    DomainEvent,
    handles,
)


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: UUID
    total: Decimal


class OrderPaid(DomainEvent):
    aggregate_type: str = "Order"
    amount: Decimal


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str
    carrier: str = "UPS"


class OrderState(BaseModel):
    customer_id: UUID | None = None
    total: Decimal = Decimal("0")
    status: str = "new"
    tracking_number: str | None = None


class OrderAggregate(DeclarativeAggregate[OrderState]):
    aggregate_type: str = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState()

    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            customer_id=event.customer_id, total=event.total, status="created"
        )

    @handles(OrderPaid)
    def _on_paid(self, event: OrderPaid) -> None:
        self._state = self.state.model_copy(update={"status": "paid"})

    @handles(OrderShipped)
    def _on_shipped(self, event: OrderShipped) -> None:
        self._state = self.state.model_copy(
            update={"status": "shipped", "tracking_number": event.tracking_number}
        )

    def create(self, customer_id: UUID, total: Decimal) -> None:
        self.create_event(OrderCreated, customer_id=customer_id, total=total)

    def pay(self, amount: Decimal) -> None:
        self.create_event(OrderPaid, amount=amount)

    def ship(self, tracking_number: str) -> None:
        if self.state.status != "paid":
            raise ValueError("Cannot ship an unpaid order")
        self.create_event(OrderShipped, tracking_number=tracking_number)

    def cancel_if_unpaid(self) -> None:
        # A deliberate no-op when the order is already paid.
        if self.state.status == "paid":
            return
```

Note that `event_type` is not set on any of the events: `DomainEvent` derives it from the
class name automatically.

## Step 1: Spin up an InMemoryTestHarness

`InMemoryTestHarness` takes no arguments and builds all four components immediately, with
tracing disabled so tests stay fast and traces stay clean.

```python
from eventsource.testing import InMemoryTestHarness


@pytest.fixture
def harness():
    h = InMemoryTestHarness()
    yield h
    h.reset()
```

### What the harness gives you: event_store, event_bus, checkpoint_repo, dlq_repo

Four read-only properties, plus a convenience view of what has been published:

| Property | Type | Use it for |
| --- | --- | --- |
| `harness.event_store` | `InMemoryEventStore` | appending and reading streams |
| `harness.event_bus` | `InMemoryEventBus` | publishing, subscribing |
| `harness.checkpoint_repo` | `InMemoryCheckpointRepository` | projection checkpoints |
| `harness.dlq_repo` | `InMemoryDLQRepository` | dead-lettered events |
| `harness.published_events` | `list[DomainEvent]` | everything the bus published, in order |

There is also `harness.get_events_of_type(SomeEvent)`, which filters
`published_events` by `isinstance`.

The harness does *not* create repositories for you -- wire an `AggregateRepository`
yourself, passing the bus as the publisher so that saved events show up in
`published_events`:

```python
repo = AggregateRepository(
    event_store=harness.event_store,
    aggregate_factory=OrderAggregate,
    event_publisher=harness.event_bus,
)
```

`AggregateRepository.save()` publishes only after the append succeeds, so
`published_events` reflects committed events, not attempted ones.

### Isolating tests with reset() and clear_published_events()

- `harness.reset()` throws away all four components and builds fresh ones. Use it in
  fixture teardown (or just build a new harness per test).
- `harness.clear_published_events()` clears only the bus's record, keeping the event
  store, checkpoints, and DLQ intact. This is the one you want after an arrange phase
  that itself publishes events:

```python
await seed_orders(harness)
harness.clear_published_events()   # ignore setup noise
await run_the_thing_under_test(harness)
then_event_count(harness, 1)
```

Two details worth knowing:

- `reset()` rebinds all four attributes to new objects, so any component reference you
  captured earlier (`store = harness.event_store`) goes stale -- read through the harness
  rather than holding references.
- `published_events` returns a fresh copy of the bus's internal list each time you touch
  it, so a list you grabbed before `clear_published_events()` still holds the old events.
  Assert on `harness.published_events` (or on the copy `EventAssertions` takes) rather
  than caching it.

One harness per test. The harness object is not thread-safe, though `InMemoryEventBus`
operations are guarded by an internal `threading.RLock`.

## Step 2: Build test events with EventBuilder

Writing `OrderCreated(aggregate_id=..., aggregate_type=..., aggregate_version=...,
customer_id=..., total=...)` in twenty tests is how test files rot. `EventBuilder` fills
in the plumbing fields and lets each test name only what it cares about.

### Starting from an event class: EventBuilder(OrderCreated)

```python
from eventsource.testing import EventBuilder

builder = EventBuilder(OrderCreated)
```

The constructor raises `TypeError` immediately if you pass something that is not a
`DomainEvent` subclass. It seeds exactly two defaults: a random `aggregate_id` and
`aggregate_version=1`. Everything else (`event_id`, `occurred_at`, `event_type`,
`correlation_id`) is left to `DomainEvent`'s own field defaults, which cover them.

One field the builder deliberately does *not* touch is `aggregate_type`, which
`DomainEvent` declares as required. The sample events above give it a class-level default
(`aggregate_type: str = "Order"`), so `build()` works. If your events do not, pass it with
`.with_field("aggregate_type", "Order")` or `build()` will raise `ValidationError`.

### Identity and stream fields: with_aggregate_id, with_event_id, with_version

```python
order_id = uuid4()

builder = (
    EventBuilder(OrderCreated)
    .with_aggregate_id(order_id)
    .with_version(1)
)
```

Use `with_event_id(...)` only when a test needs to recognise a specific event later --
otherwise let it stay random. `with_tenant_id(...)` and `with_occurred_at(...)` cover the
multi-tenant and time-sensitive cases.

`aggregate_version` is constrained to `>= 1`, so `with_version(0)` is not a way to express
"no version yet" -- it fails at `build()`. Stream versions start at 1.

### Domain payload: with_field and with_fields

`with_field(name, value)` sets one field; `with_fields(**kwargs)` sets several. Both
accept any name -- the builder does no validation of its own, it just accumulates a dict
and passes it to the event constructor.

```python
event = (
    EventBuilder(OrderCreated)
    .with_aggregate_id(order_id)
    .with_fields(customer_id=uuid4(), total=Decimal("99.99"))
    .build()
)
```

### Correlation metadata: with_correlation_id, with_causation_id, with_actor_id, with_metadata

```python
event = (
    EventBuilder(OrderShipped)
    .with_aggregate_id(order_id)
    .with_correlation_id(request_id)
    .with_causation_id(payment_event.event_id)
    .with_actor_id("user-42")
    .with_metadata({"source": "api"})
    .with_field("tracking_number", "TRACK123")
    .build()
)
```

`with_actor_id` takes a `str`; the two ID methods take `UUID`s; `with_metadata` replaces
the whole metadata dict rather than merging into it. Note that `correlation_id` is always
populated -- `DomainEvent` defaults it to a fresh UUID -- so set it explicitly whenever a
test asserts that several events belong to the same request.

### Calling build() and reusing a builder across fixtures

`build()` calls the event class constructor with the accumulated fields, so Pydantic
validation happens there -- a missing required field surfaces as
`pydantic.ValidationError` from `build()`, not earlier.

Every `with_*` method returns the same builder instance (it mutates and returns `self`),
so a "shared" builder is shared mutable state. Calling `build()` twice gives you two
events with the *same* explicitly-set fields but freshly defaulted `event_id` and
`occurred_at`. If you want a reusable starting point, expose a factory function rather
than a module-level builder:

```python
def order_created(order_id: UUID, **overrides) -> OrderCreated:
    return (
        EventBuilder(OrderCreated)
        .with_aggregate_id(order_id)
        .with_fields(customer_id=uuid4(), total=Decimal("99.99"))
        .with_fields(**overrides)
        .build()
    )
```

## Step 3: Write your first Given-When-Then test

### Given: seeding history with given_events(harness, [...])

`given_events` groups the events you hand it by `(aggregate_id, aggregate_type)` and
appends each group to `harness.event_store` with `expected_version=0` -- i.e. it assumes
every aggregate is fresh. It is `async`, and it returns immediately if the list is empty.
It writes to the *store*, not the bus, so seeded history never pollutes
`published_events`.

```python
history = [
    EventBuilder(OrderCreated)
    .with_aggregate_id(order_id)
    .with_version(1)
    .with_fields(customer_id=customer_id, total=Decimal("99.99"))
    .build(),
    EventBuilder(OrderPaid)
    .with_aggregate_id(order_id)
    .with_version(2)
    .with_fields(amount=Decimal("99.99"))
    .build(),
]
await given_events(harness, history)
```

Set `with_version` explicitly on seeded events: they must be consecutive from 1, because
the aggregate validates event versions as it replays them.

### When: executing a command with when_command(aggregate, lambda a: ...)

`when_command` is synchronous and takes the aggregate plus a callable. It records how
many uncommitted events exist, runs the callable, and returns only the events added
during that call:

```python
order = await repo.load(order_id)
new_events = when_command(order, lambda o: o.ship("TRACK123"))

assert len(new_events) == 1
assert isinstance(new_events[0], OrderShipped)
```

That "only the new ones" behaviour is what lets you probe one command at a time on an
aggregate that already has pending events. `when_command` does not persist anything --
call `await repo.save(order)` when you want the events appended and published.

### Then: asserting with then_event_published(...)

`then_event_published(harness, EventType, **expected_fields)` scans
`harness.published_events`, and returns the matching event so you can keep asserting on
it. With no field constraints it returns the first event of that type; with constraints
it returns the first event matching all of them, and otherwise raises `AssertionError`
listing what it did find.

```python
event = then_event_published(harness, OrderShipped, tracking_number="TRACK123")
assert event.aggregate_id == order_id
```

### Running the test under pytest asyncio

Putting it together:

```python
from eventsource.testing import (
    given_events,
    then_event_published,
    when_command,
)


async def test_paid_order_can_be_shipped(harness):
    order_id, customer_id = uuid4(), uuid4()

    # Given
    await given_events(harness, [
        EventBuilder(OrderCreated).with_aggregate_id(order_id).with_version(1)
        .with_fields(customer_id=customer_id, total=Decimal("99.99")).build(),
        EventBuilder(OrderPaid).with_aggregate_id(order_id).with_version(2)
        .with_fields(amount=Decimal("99.99")).build(),
    ])

    repo = AggregateRepository(
        event_store=harness.event_store,
        aggregate_factory=OrderAggregate,
        event_publisher=harness.event_bus,
    )
    order = await repo.load(order_id)

    # When
    new_events = when_command(order, lambda o: o.ship("TRACK123"))
    await repo.save(order)

    # Then
    assert len(new_events) == 1
    event = then_event_published(harness, OrderShipped, tracking_number="TRACK123")
    assert event.aggregate_id == order_id
```

Run it with `uv run pytest tests/unit/test_orders.py -v`. With `asyncio_mode = "auto"`
there is no decorator to remember.

## Step 4: Assert more precisely

### then_event_sequence for ordered expectations

Checks types *and* order across all published events, and returns the published list:

```python
events = then_event_sequence(harness, [OrderPaid, OrderShipped])
assert events[1].tracking_number == "TRACK123"
```

It compares the full published list, so it fails if there are extra events. A count
mismatch is reported before any type mismatch, and both messages print the expected and
actual type names side by side.

### then_event_count for exact totals

```python
then_event_count(harness, 1)
```

Useful as a guard right after `clear_published_events()` -- it catches the "my command
accidentally emitted two events" class of bug that a type assertion misses.

### then_no_events_published for no-op commands

With no second argument it asserts nothing at all was published; with an event type it
asserts only that no event of that type appeared.

```python
async def test_cancel_is_a_noop_when_paid(harness):
    # ... arrange a paid order, load it, clear published events ...
    when_command(order, lambda o: o.cancel_if_unpaid())
    await repo.save(order)          # no uncommitted events, so save() is a no-op
    then_no_events_published(harness)
```

### Matching payload fields in a then_* assertion

`then_event_published` compares with `==` against `getattr`, and a field that does not
exist on the event simply fails to match rather than raising `AttributeError`. All named
fields must match on the *same* event:

```python
then_event_published(harness, OrderShipped, tracking_number="TRACK123", carrier="UPS")
```

If several events of the type exist and none matches, the error message prints each
candidate with just those attributes -- so a typo'd field name shows up as
`{'trackingnumber': None}` in the failure output.

## Step 5: Drop down to EventAssertions when the BDD helpers aren't enough

The `then_*` helpers only ever look at `harness.published_events`. When you want to
assert against a different list -- uncommitted events on an aggregate, events read back
out of the store, a filtered subset -- use `EventAssertions`, which wraps any sequence of
events.

### Wrapping harness.published_events in EventAssertions

```python
from eventsource.testing import EventAssertions

assertions = EventAssertions(harness.published_events)
# or: EventAssertions(order.uncommitted_events)
```

It copies the sequence on construction, and `assertions.events` hands back another copy,
so later publishes do not change what you are asserting against. It offers
`assert_event_published`, `assert_no_event_published`, `assert_event_count`,
`assert_event_sequence`, `assert_no_events_published`, plus the three below.

### assert_event_with_fields and assert_event_for_aggregate

```python
shipped = assertions.assert_event_with_fields(OrderShipped, tracking_number="TRACK123")
shipped = assertions.assert_event_for_aggregate(OrderShipped, order_id)
```

`assert_event_for_aggregate` is the one worth reaching for in multi-aggregate tests: when
events of the right type exist but for other aggregates, its failure message lists the
aggregate IDs it *did* find, which turns a puzzling failure into an obvious one.

### get_events_of_type for custom assertions

When no built-in assertion fits, filter and assert yourself:

```python
item_events = assertions.get_events_of_type(ItemAdded)
assert [e.sku for e in item_events] == ["A", "B", "C"]
```

## Step 6: Test a projection against the harness

A projection is just an event consumer, so testing one means feeding it events and
looking at the read model. `DeclarativeProjection` accepts the harness repositories
directly, and its handlers take `(self, conn, event)` -- for a non-database projection
the connection argument is `None`.

```python
from eventsource import DeclarativeProjection


class OrderSummaryProjection(DeclarativeProjection):
    def __init__(self, checkpoint_repo=None, dlq_repo=None):
        self.summaries: dict[UUID, dict] = {}
        super().__init__(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)

    @handles(OrderCreated)
    async def _on_created(self, _conn, event: OrderCreated) -> None:
        self.summaries[event.aggregate_id] = {"status": "created", "total": event.total}

    @handles(OrderShipped)
    async def _on_shipped(self, _conn, event: OrderShipped) -> None:
        self.summaries[event.aggregate_id]["status"] = "shipped"
```

Set instance attributes *before* calling `super().__init__()`: the base constructor
inspects the class for `@handles` methods and validates them straight away.

### Feeding stored events through the projection

Seed with `given_events`, read the stream back, and call `handle()` per event -- the same
entry point a real subscription runner uses, so retries, DLQ, and checkpointing all
behave as they will in production.

```python
async def test_order_summary_projection(harness):
    order_id = uuid4()
    await given_events(harness, [
        EventBuilder(OrderCreated).with_aggregate_id(order_id).with_version(1)
        .with_fields(customer_id=uuid4(), total=Decimal("10")).build(),
        EventBuilder(OrderShipped).with_aggregate_id(order_id).with_version(2)
        .with_fields(tracking_number="T9").build(),
    ])

    projection = OrderSummaryProjection(
        checkpoint_repo=harness.checkpoint_repo,
        dlq_repo=harness.dlq_repo,
    )

    stream = await harness.event_store.get_events(order_id, "Order")
    for event in stream.events:
        await projection.handle(event)
```

### Asserting on the resulting read model

```python
    assert projection.summaries[order_id]["status"] == "shipped"
    assert await projection.get_checkpoint() is not None
```

Because you passed the harness repositories in, you can also assert on failure paths: an
event whose handler keeps raising ends up in `harness.dlq_repo` after the retries are
exhausted.

## Step 7: Verify a custom backend with the conformance suites

If you write your own event store adapter or `EventBus`, do not hand-write contract
tests -- subclass the suites. The event store side is five narrow, per-port suites in
`eventsource.testing.conformance_ports`, one per capability protocol in
`eventsource.ports.store`; the bus side is the single `EventBusConformanceSuite` from
`eventsource.testing.conformance`. Every suite supplies a set of `async def test_*`
methods; pytest collects those inherited tests when your subclass is named `Test*`.

### The store side: one `store` fixture per port suite

Each port suite is abstract on exactly one thing -- an async `store` pytest fixture
that yields a fresh adapter instance. There is no `create_store()` /
`create_test_event()` factory pair to implement: the suites already share a
registered, minimal event type internally, so you only supply the adapter.

```python
from collections.abc import AsyncIterator

import pytest

from eventsource.testing.conformance_ports import (
    AppenderConformance,
    CategoryQueryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    StreamReaderConformance,
)


class TestMyStoreAppenderConformance(AppenderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator["MyEventStore"]:
        yield MyEventStore()


class TestMyStoreStreamReaderConformance(StreamReaderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator["MyEventStore"]:
        yield MyEventStore()
```

Subclass only the suites your adapter's capabilities match --
`AppenderConformance` and `StreamReaderConformance` are the minimum for any
append/read backend; add `EventLookupConformance`, `GlobalFeedConformance`,
and `CategoryQueryConformance` only if your adapter implements `event_exists`,
`read_all`, and `read_category` respectively. The fixture must yield a
*fresh, empty* instance -- each test method requests it independently, which
is what keeps the suite's tests isolated from one another. Put any teardown
(closing a connection, disposing an engine) after the `yield`.

### The bus side: create_bus() and create_test_event()

The bus suite keeps its original factory-method shape:

```python
from uuid import UUID

from eventsource.events.base import DomainEvent
from eventsource.testing.conformance import EventBusConformanceSuite


class TestMyBusConformance(EventBusConformanceSuite):
    def create_bus(self):
        return MyEventBus()

    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        return OrderPaid(aggregate_id=aggregate_id, amount=Decimal("1"))
```

You can add your own backend-specific tests to any of these subclasses, and you can
override an inherited test and call `await super().test_...()` inside it if a backend
needs extra setup around the standard check.

### What the suites cover (roundtrip, stream isolation, optimistic locking, metadata, global position)

The `conformance_ports` store suites, one row per suite:

| Suite | Contract it pins down |
| --- | --- |
| `AppenderConformance` | `append()` honors every `ExpectedVersion` kind (`exact`, `any_`, `no_stream`, `stream_exists`), raises `OptimisticLockError` on a mismatch, rejects duplicate `event_id`s atomically |
| `StreamReaderConformance` | `read_stream()` returns exactly the appended events in order, honors `StreamReadOptions` (direction, version range, limit), `get_stream_version()` matches the appended count, streams stay isolated from one another |
| `EventLookupConformance` | `event_exists()` answers correctly before and after append, and for unknown ids |
| `GlobalFeedConformance` | `read_all()` returns events in position order, resumption from a `from_position` is exclusive, `current_position()` is `None` on an empty store and matches the last envelope otherwise |
| `CategoryQueryConformance` | `read_category()` only returns events for streams in the named category, ordered by `stored_at`, honoring timestamp/tenant filters and limits |

`EventBusConformanceSuite`: `test_publish_and_subscribe_roundtrip`,
`test_multiple_subscribers`, `test_unsubscribe_stops_delivery`,
`test_subscribe_to_all_events`, `test_unsubscribe_from_all_events`, and
`test_handler_error_isolation` (one handler raising must not stop the others, and the bus
must keep working afterwards).

The library runs the port suites against its own `InMemoryEventStore` and
`SQLiteEventStore` in `tests/unit/adapters/test_memory_conformance.py` and
`test_sqlite_conformance.py`, and the bus suite against `InMemoryEventBus` in
`tests/unit/test_conformance.py` -- working references if you get stuck.

## Step 8: Test a decider-style aggregate with DeciderScenario

If your domain is built with `DeciderAggregate` and the decider pattern — pure
`decide` and `evolve` functions — you can test it synchronously and without any
infrastructure using `DeciderScenario`. No event store, no event bus, no async.

### The decider pattern: pure functions

The decider pattern models the aggregate as three pure functions. This state carries its
own `order_id` (the shared `OrderState` from earlier steps does not, since the
`DeclarativeAggregate` example above never needed it), and the command comes from
`eventsource.DomainCommand`, exactly as introduced in
[Your First Aggregate](03-first-aggregate.md):

```python
from eventsource import CommandRejectedError, DomainCommand


class ShipOrder(DomainCommand):
    tracking_number: str


class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    total: Decimal = Decimal("0")
    status: str = "new"
    tracking_number: str | None = None


def initial_state(order_id: UUID) -> OrderState:
    """Return the initial state for a new aggregate."""
    return OrderState(order_id=order_id, status="new")

def decide(command: object, state: OrderState) -> list[DomainEvent]:
    """Given a command and the current state, decide what events to produce."""
    match command:
        case ShipOrder(tracking_number=tn):
            if state.status != "paid":
                raise CommandRejectedError("Cannot ship unpaid order")
            return [OrderShipped(aggregate_id=state.order_id, tracking_number=tn)]
        case _:
            raise CommandRejectedError(f"Unknown command: {command}")

def evolve(state: OrderState, event: DomainEvent) -> OrderState:
    """Given the current state and an event, return the next state."""
    match event:
        case OrderCreated(customer_id=cid, total=total):
            return state.model_copy(update={"customer_id": cid, "total": total, "status": "created"})
        case OrderPaid():
            return state.model_copy(update={"status": "paid"})
        case OrderShipped(tracking_number=tn):
            return state.model_copy(update={"status": "shipped", "tracking_number": tn})
        case _:
            return state
```

`evolve` must handle every event the aggregate emits, not just the one `decide` cares
about here -- `given()` replays the whole history through it, so a missing case (as
`OrderCreated`/`OrderPaid` would be if only `OrderShipped` were matched) silently leaves
`state.status` at its default and the "paid" precondition can never be satisfied.

These functions are wrapped in a `DeciderAggregate` subclass:

```python
from eventsource import DeciderAggregate

class Order(DeciderAggregate[OrderState]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> OrderState:
        return initial_state(aggregate_id)

    @staticmethod
    def decide(command: object, state: OrderState) -> list[DomainEvent]:
        return decide(command, state)

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        return evolve(state, event)
```

### Testing with DeciderScenario: synchronous and infrastructure-free

`DeciderScenario` is a synchronous Given-When-Then harness that tests the three
pure functions directly, with no event store, event bus, or async machinery:

```python
from eventsource.testing import DeciderScenario

def test_paid_order_ships():
    order_id = uuid4()

    (DeciderScenario(Order)
     .given(
        OrderCreated(aggregate_id=order_id, aggregate_version=1,
                    customer_id=uuid4(), total=Decimal("99.99")),
        OrderPaid(aggregate_id=order_id, aggregate_version=2,
                 amount=Decimal("99.99")),
     )
     .when(ShipOrder(tracking_number="TRACK123"))
     .then_events(OrderShipped))
```

The three methods chain:

- **`given(*events)`**: Folds prior events into state via `evolve`, building up
  scenario state before the command is issued.
- **`when(command)`**: Runs `decide(command, state)`, capturing the returned
  events or any raised exception.
- **`then_events(*types)`** or **`then_rejected(exc_type=...)`**: Asserts the
  outcome — either the event types produced or the exception raised.

### Asserting on rejection

When `decide` raises an exception, use `then_rejected`. `decide()` above raises
`CommandRejectedError`, which is also `then_rejected`'s default `exc_type`, so the
type argument can be omitted:

```python
def test_unpaid_order_cannot_ship():
    order_id = uuid4()

    (DeciderScenario(Order)
     .given(
        OrderCreated(aggregate_id=order_id, aggregate_version=1,
                    customer_id=uuid4(), total=Decimal("99.99")),
     )
     .when(ShipOrder(tracking_number="TRACK123"))
     .then_rejected(match="Cannot ship unpaid"))
```

The `match` parameter is optional; if provided, the exception message must match
the regex. `then_rejected` is not limited to `CommandRejectedError` -- if your
`decide()` raises a different exception type (say, a plain `ValueError`), pass it
explicitly and it is checked the same way:

```python
     .when(ShipOrder(tracking_number="TRACK123"))
     .then_rejected(ValueError, match="Cannot ship unpaid"))
```

### Accessing the produced events

After `when()`, the `events` property returns the list of events that `decide`
produced:

```python
scenario = (DeciderScenario(Order)
    .given(...)
    .when(ShipOrder(tracking_number="TRACK123")))

for event in scenario.events:
    print(f"Produced: {event}")
```

### When to use DeciderScenario vs. the async helpers

**Use `DeciderScenario`** when:
- Your aggregate is a `DeciderAggregate`.
- You want to test pure domain logic with no infrastructure.
- You prefer synchronous tests.

**Use the async BDD helpers** (`given_events`, `when_command`, `then_*`) when:
- Your aggregate is `DeclarativeAggregate` or hand-written `_apply`.
- You need to test the full aggregate lifecycle: load, save, publish.
- You are testing behavior that involves the repository and bus.

The two approaches test different layers. `DeciderScenario` isolates the domain
(pure functions), while the async helpers validate the aggregate's contract with
the store and bus.

## Choosing between the harness and real backends

Use the harness for aggregate behaviour, command validation, projection logic, and event
flow -- anything where the database is incidental. It is fast enough to run on every
save, and it needs no Docker.

Use a real backend when the thing under test *is* the backend interaction: SQL in a
`DatabaseProjection`, PostgreSQL advisory locks, actual concurrent appends racing for the
same version, or JSON round-tripping through a real column type. `InMemoryEventStore`
implements the same contract (the conformance suite proves it), but it cannot tell you
that your `text()` query has a typo. Those tests belong in `tests/integration/`, behind
the `postgres` / `sqlite` / `redis` markers.

## Common pitfalls

### Forgetting expected_version semantics in given_events

`given_events` always appends with `expected_version=0`. Two consequences:

- Calling it twice for the same aggregate in one test raises `OptimisticLockError` --
  build the whole history in a single call.
- The versions you set with `with_version` must run 1, 2, 3... in order. Skip one and the
  aggregate's version validation rejects the history when you load it.

### Leaking state between tests

A module-level `harness = InMemoryTestHarness()` will carry published events from one
test into the next and make failures order-dependent. Create it in a fixture, and prefer
`clear_published_events()` over manual list mutation when you only want to forget the
arrange phase. Remember that `EventBuilder` mutates itself, so a shared builder is the
same trap in miniature.

### Importing eventsource.testing from production code

`eventsource.testing` is documented as test-only. Importing it from an application module
drags the in-memory infrastructure into your production import graph and blurs the line
between a fake and the real thing. Keep the imports inside `tests/`.

## Recap

You now have the whole testing toolkit:

- `InMemoryTestHarness` gives you `event_store`, `event_bus`, `checkpoint_repo`,
  `dlq_repo`, and `published_events`, with `reset()` and `clear_published_events()` for
  isolation. It does not build repositories -- wire `AggregateRepository` yourself with
  `event_publisher=harness.event_bus`.
- `EventBuilder` fills in `aggregate_id` and `aggregate_version=1`, and validation happens
  at `build()`.
- `given_events` / `when_command` / `then_*` express one scenario per test;
  `when_command` returns only the events that command produced.
- `EventAssertions` covers the cases the `then_*` helpers do not, over any event list.
- Projections are tested by calling `handle()` with harness-backed checkpoint and DLQ
  repositories.
- The `conformance_ports` store suites and `EventBusConformanceSuite` verify a backend
  against the contract -- a `store` fixture per port suite, and two factory methods for
  the bus suite.

## Next steps

- Tutorial 11 (PostgreSQL) -- move the same tests onto a real store and see which ones
  genuinely need it.
- Tutorial 16 (Multi-Tenancy) -- `EventBuilder.with_tenant_id` and the projection's
  `tenant_filter` are the testing entry points there.
- Read `src/eventsource/testing/conformance_ports/` and
  `src/eventsource/testing/conformance.py` before writing a custom backend; together
  they are the most precise statement of the store-port and `EventBus` contracts.
