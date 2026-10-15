# Testing Aggregates, Deciders, and Event Buses

`eventsource.testing` provides fast, deterministic testing utilities that verify
domain behavior through public frontdoors without database or broker setup:

- **`Scenario` / `DeciderScenario`** for synchronous, zero-I/O Given-When-Then
  testing of `DeciderAggregate`s and pure `(decide, evolve, initial_state)`
  triples, including `.then_event_types(*EventClasses)`, `.then_events(*EventClasses)`,
  `.then_event(EventCls, **expected_fields)`, and `.then_no_events()`.
- **`EventBuilder`** with `EventBuilder(EventCls, **fields)` and
  `EventBuilder.build_many(count, *, start_version=None)` for constructing
  individual test events and auto-versioned event sequences with minimal boilerplate.
- **`InMemoryTestHarness`** with `harness.create_repository(OrderAggregate, snapshot_threshold=...)`,
  `await harness.append_events(*events, expected_version=None)`,
  `harness.snapshot_store`, and `harness.assertions` (`EventAssertions` wrapper
  over `harness.published_events`) for pre-wired repository and bus testing.
- **`RecordingEventBus.assertions`** (`bus.assertions.assert_event_published(...)`,
  `bus.assertions.assert_event_with_fields(...)`, `bus.assertions.assert_event_count(...)`)
  and **`published_of_type(EventType)`** on `InMemoryTestHarness`, `InMemoryEventBus`,
  and `RecordingEventBus` for expressive assertions and type-safe inspection of
  published domain events.

For an introductory walkthrough of testing in `eventsource-py`, see
[Tutorial 8: Testing](../tutorials/08-testing.md) and the
[Testing API Reference](../api/testing.md).

## Synchronous decider testing with `Scenario`

`Scenario` (an alias for `DeciderScenario` in `eventsource.testing.scenario`)
runs entirely in memory without an event store or `asyncio` event loop.

### Given prior commands with `given_commands`

Instead of hand-constructing historical `DomainEvent` instances with version
numbers and aggregate IDs for every test setup, use `.given_commands(...)` to
fold prior commands through `decide` and `evolve` into the aggregate's state:

```python
from uuid import uuid4
from eventsource.testing.scenario import Scenario


def test_deposit_after_opening_account() -> None:
    account_id = uuid4()

    (
        Scenario(Account)
        .given_commands(
            OpenAccount(account_id=account_id, owner="alice"),
            DepositMoney(account_id=account_id, amount=20.0),
        )
        .when(DepositMoney(account_id=account_id, amount=15.0))
        .then_events(MoneyDeposited)
        .then_state(AccountState(owner="alice", balance=35.0, is_open=True))
    )
```

You can also mix `.given(*events)` and `.given_commands(*commands)` in any order
when setting up a scenario:

```python
def test_mixed_given_events_and_commands() -> None:
    account_id = uuid4()

    (
        Scenario(Account)
        .given(AccountOpened(aggregate_id=account_id, aggregate_version=1, owner="alice"))
        .given_commands(DepositMoney(account_id=account_id, amount=10.0))
        .when(DepositMoney(account_id=account_id, amount=25.0))
        .then_events(MoneyDeposited)
        .then_state(lambda s: s.balance == 35.0 and s.is_open)
    )
```

### Multi-command execution with `when_all`

Use `.when_all(*commands)` when the action under test is a sequence of commands.
Each command runs against the state evolved by the previous command, and all
emitted events are collected in order for `.then_events(...)`,
`.then_event_types(...)`, `.then_event(...)`, and `.then_state(...)`:

```python
def test_multiple_commands_in_when_all() -> None:
    account_id = uuid4()

    (
        Scenario(Account)
        .given_commands(OpenAccount(account_id=account_id, owner="alice"))
        .when_all(
            DepositMoney(account_id=account_id, amount=10.0),
            DepositMoney(account_id=account_id, amount=40.0),
        )
        .then_event_types(MoneyDeposited, MoneyDeposited)
        .then_state(AccountState(owner="alice", balance=50.0, is_open=True))
    )
```

If any command in `when_all` raises an exception (such as `CommandRejectedError`),
execution stops at that command and the rejection can be asserted with
`.then_rejected(...)`:

```python
from eventsource import CommandRejectedError


def test_when_all_rejection() -> None:
    account_id = uuid4()

    (
        Scenario(Account)
        .when_all(
            OpenAccount(account_id=account_id, owner="alice"),
            OpenAccount(account_id=account_id, owner="alice"),
        )
        .then_rejected(CommandRejectedError, match="already open")
    )
```

### Asserting emitted event types with `then_event_types`

Both `Scenario` and `DeciderScenario` provide `.then_event_types(*EventClasses)`
(as well as `.then_events(*EventClasses)`) to verify that `when(...)` or
`when_all(...)` succeeded and emitted the exact sequence of domain event classes
in order:

```python
from eventsource.testing.scenario import DeciderScenario, Scenario


def test_then_event_types() -> None:
    account_id = uuid4()

    (
        Scenario(Account)
        .when_all(
            OpenAccount(account_id=account_id, owner="alice"),
            DepositMoney(account_id=account_id, amount=25.0),
        )
        .then_event_types(AccountOpened, MoneyDeposited)
    )

    (
        DeciderScenario(Account)
        .given_commands(OpenAccount(account_id=account_id, owner="alice"))
        .when(DepositMoney(account_id=account_id, amount=10.0))
        .then_event_types(MoneyDeposited)
    )
```

### Asserting event type and field values with `then_event`

Use `.then_event(EventCls, **expected_fields)` to assert that an event of type
`EventCls` was emitted by `when(...)` or `when_all(...)` and that its attributes
match every `field=expected_value` pair. Like `.then_events(...)` and
`.then_event_types(...)`, `.then_event(...)` returns the `Scenario` instance so
you can chain multiple `.then_event(...)` assertions or follow up with
`.then_state(...)`:

```python
def test_then_event_with_fields_and_state() -> None:
    account_id = uuid4()

    (
        Scenario(Account)
        .given_commands(OpenAccount(account_id=account_id, owner="alice"))
        .when(DepositMoney(account_id=account_id, amount=25.0))
        .then_event(MoneyDeposited, aggregate_id=account_id, amount=25.0)
        .then_state(AccountState(owner="alice", balance=25.0, is_open=True))
    )
```

When `when_all(...)` emits multiple events, chain `.then_event(...)` calls to
inspect specific events and their payloads without breaking the fluent pipeline:

```python
def test_chained_then_event_assertions() -> None:
    account_id = uuid4()

    (
        Scenario(Account)
        .when_all(
            OpenAccount(account_id=account_id, owner="alice"),
            DepositMoney(account_id=account_id, amount=40.0),
        )
        .then_event(AccountOpened, owner="alice")
        .then_event(MoneyDeposited, amount=40.0)
        .then_state(lambda state: state.balance == 40.0 and state.is_open)
    )
```

### Asserting no-op commands with `then_no_events`

When an idempotent or no-op command succeeds without raising an error and
intentionally emits zero domain events (`decide(...)` returns `[]`), use
`.then_no_events()` to verify both that no exception was raised and that no
events were produced:

```python
def test_idempotent_command_emits_no_events() -> None:
    account_id = uuid4()

    (
        Scenario(Account)
        .given_commands(OpenAccount(account_id=account_id, owner="alice"))
        .when(DepositMoney(account_id=account_id, amount=0.0))
        .then_no_events()
        .then_state(AccountState(owner="alice", balance=0.0, is_open=True))
    )
```

### Asserting evolved state with `then_state`

`.then_state(expected_or_predicate, *, aggregate_id=None)` first verifies that
the command succeeded without raising an error (`.then_no_error()`), then checks
the resulting state in one of two ways:

1. **Exact state equality**: pass an expected state value (such as a Pydantic
   `BaseModel` or dataclass instance).
2. **Predicate function**: pass a callable `Callable[[TState], bool]` returning
   `True` when the evolved state satisfies your invariant.

```python
# Exact value comparison
Scenario(Account).when(
    OpenAccount(account_id=account_id, owner="alice")
).then_state(AccountState(owner="alice", balance=0.0, is_open=True))

# Predicate assertion
Scenario(Account).when(
    OpenAccount(account_id=account_id, owner="alice")
).then_state(lambda state: state.is_open and state.owner == "alice")
```

In multi-aggregate scenarios where commands target distinct aggregate IDs, pass
`aggregate_id=...` to `.then_state(..., aggregate_id=first_id)` to assert the
state of a specific aggregate instance.

## Building test events with `EventBuilder`

`EventBuilder` constructs `DomainEvent` instances with auto-generated defaults
for `aggregate_id`, `event_id`, `aggregate_version` (default `1`), and
`occurred_at`.

### Passing initial keyword fields to `EventBuilder(EventCls, **fields)`

Pass event payload and metadata fields directly to `EventBuilder(EventCls, **fields)`
instead of calling `.with_field(...)` or `.with_fields(...)` separately:

```python
from uuid import uuid4
from eventsource.testing import EventBuilder


def test_event_builder_initial_fields() -> None:
    cid = uuid4()

    event = EventBuilder(OrderCreated, customer_id=cid, total=99.99).build()

    assert event.customer_id == cid
    assert event.total == 99.99
    assert event.aggregate_version == 1
```

You can still chain `.with_aggregate_id(...)`, `.with_version(...)`, or
`.with_fields(...)` on top of initial keyword fields when needed.

### Building auto-versioned sequences with `EventBuilder.build_many`

Use `EventBuilder.build_many(count, *, start_version=None)` to generate a list
of `count` events for the same aggregate with sequentially incrementing
`aggregate_version` values (starting from `start_version` when provided, or the
builder's configured `aggregate_version`, which defaults to `1`):

```python
from uuid import uuid4
from eventsource.testing import EventBuilder


def test_event_builder_build_many() -> None:
    order_id = uuid4()

    # Versions 1, 2, 3
    events = EventBuilder(ItemAdded, item="Widget").build_many(3)
    assert [e.aggregate_version for e in events] == [1, 2, 3]

    # Versions 2, 3, 4 on a specific aggregate_id
    follow_up = (
        EventBuilder(ItemAdded, aggregate_id=order_id, item="Gadget")
        .build_many(3, start_version=2)
    )
    assert [e.aggregate_version for e in follow_up] == [2, 3, 4]
    assert all(e.aggregate_id == order_id for e in follow_up)
```

## Pre-wired repositories, snapshots, and assertions with `InMemoryTestHarness`

`InMemoryTestHarness` bundles in-memory infrastructure (`event_store`,
`event_bus`, `checkpoint_repo`, `dlq_repo`, and `snapshot_store`) with helper
methods for seeding events, wiring aggregate repositories, and asserting
published domain events:

- **`harness.create_repository(OrderAggregate, snapshot_threshold=..., with_snapshots=...)`**:
  creates an `AggregateRepository` pre-wired with `harness.event_store`,
  `harness.event_bus`, and `harness.snapshot_store` (when `with_snapshots=True`
  or `snapshot_threshold` is specified) with tracing disabled for fast tests.
- **`harness.create_read_model_repository(OrderSummary)`**:
  creates an `InMemoryReadModelRepository` for the given `ReadModel` subclass
  with tracing disabled for fast unit and projection tests.
- **`await harness.append_events(*events, expected_version=None)`**:
  seeds one or more pre-existing events (or sequences of events) into
  `harness.event_store`, automatically grouped by `(aggregate_id, aggregate_type)`
  stream and using `ExpectedVersion.any_()` when `expected_version` is omitted.
- **`await harness.publish_events(*events, persist=False, expected_version=None)`**:
  publishes one or more events through `harness.event_bus` (and optionally
  appends them to `harness.event_store` when `persist=True`).
- **`await harness.replay(projections, **kwargs)`**:
  replays all events stored in `harness.event_store` into a single projection or
  a sequence of projections and returns a `ReplayReport` (accepting all optional
  keyword arguments supported by `replay()`, such as `strict=True` or
  `tenant_id=...`).
- **`harness.snapshot_store` & `harness.outbox_repo`**: provide pre-configured
  `InMemorySnapshotStore` and `InMemoryOutboxRepository` instances that are reset
  alongside `checkpoint_repo` and `dlq_repo` when `harness.reset()` is called.
  All in-memory repositories (`snapshot_store`, `checkpoint_repo`, `dlq_repo`,
  `outbox_repo`) and `harness` itself support `len(...)`.
- **`harness.assertions`**: returns an `EventAssertions` wrapper over
  `harness.published_events` supporting `len(harness.assertions)` and expressive
  helpers (`assert_event_published`, `assert_no_event_published`,
  `assert_event_count`, `assert_event_type_count`, `assert_event_sequence`,
  `assert_event_with_fields`, `assert_event_for_aggregate`, and
  `assert_no_events_published`).

```python
from uuid import uuid4
from eventsource.testing import InMemoryTestHarness


async def test_harness_repository_snapshots_and_assertions() -> None:
    harness = InMemoryTestHarness()
    repo = harness.create_repository(OrderAggregate, snapshot_threshold=2)
    read_repo = harness.create_read_model_repository(OrderSummary)

    order_id = uuid4()
    customer_id = uuid4()

    await repo.execute(
        order_id,
        lambda order: order.create(customer_id=customer_id),
    )
    await repo.execute(
        order_id,
        lambda order: order.ship("TRACK-123"),
        create_if_missing=False,
    )

    # EventAssertions wrapper and len(harness)
    assert len(harness) == 2
    assert len(harness.assertions) == 2
    harness.assertions.assert_event_count(2)
    harness.assertions.assert_event_type_count(OrderShipped, 1)
    harness.assertions.assert_event_sequence([OrderCreated, OrderShipped])
    shipped = harness.assertions.assert_event_with_fields(
        OrderShipped,
        tracking_number="TRACK-123",
    )
    assert shipped.aggregate_id == order_id

    # Verify automatic snapshot creation in harness.snapshot_store at version 2
    assert len(harness.snapshot_store) == 1
    snapshot = await harness.snapshot_store.get_snapshot(order_id, "Order")
    assert snapshot is not None
    assert snapshot.version == 2
```

### Seeding pre-existing events and replaying projections with `harness.append_events` and `harness.replay`

When testing repositories, projections, or read models against historical state,
use `await harness.append_events(*events, expected_version=None)` to seed
pre-existing events directly into `harness.event_store` and
`await harness.replay(projection)` to rebuild projections from those stored
events:

```python
from uuid import uuid4
from eventsource.testing import EventBuilder, InMemoryTestHarness


async def test_seed_events_and_replay_projection() -> None:
    harness = InMemoryTestHarness()
    repo = harness.create_repository(OrderAggregate)
    projection = OrderSummaryProjection()

    order_id = uuid4()
    customer_id = uuid4()

    created = EventBuilder(
        OrderCreated,
        aggregate_id=order_id,
        customer_id=customer_id,
    ).build()
    items = EventBuilder(
        ItemAdded,
        aggregate_id=order_id,
        item="Widget",
    ).build_many(2, start_version=2)

    await harness.append_events(created, *items)

    order = await repo.load(order_id)
    assert order.version == 3

    report = await harness.replay(projection)
    report.raise_for_failures()
    assert report.applied == 3
```

## Asserting recorded events with `RecordingEventBus.assertions`

When wrapping any `EventBus` in a `RecordingEventBus` outside `InMemoryTestHarness`,
use `bus.assertions` to access an `EventAssertions` helper over `bus.published_events`
directly—including `bus.assertions.assert_event_published(...)`,
`bus.assertions.assert_event_with_fields(...)`, and
`bus.assertions.assert_event_count(...)`—without manually constructing
`EventAssertions(bus.published_events)`:

```python
from uuid import uuid4
from eventsource import AggregateRepository, InMemoryEventStore
from eventsource.testing import RecordingEventBus


async def test_recording_event_bus_assertions() -> None:
    # Omit `wrapped` to use a default InMemoryEventBus(enable_tracing=False):
    bus = RecordingEventBus()

    async with InMemoryEventStore() as store:
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=Account,
            event_publisher=bus,
        )
        account_id = uuid4()

        await repo.execute(
            account_id,
            OpenAccount(account_id=account_id, owner="alice"),
        )
        await repo.execute(
            account_id,
            DepositMoney(account_id=account_id, amount=50.0),
        )

    assert len(bus) == 2
    bus.assertions.assert_event_count(2)
    opened = bus.assertions.assert_event_published(AccountOpened)
    assert opened.owner == "alice"

    deposited = bus.assertions.assert_event_with_fields(
        MoneyDeposited,
        aggregate_id=account_id,
        amount=50.0,
    )
    assert deposited.amount == 50.0
```

## Inspecting published events with `published_of_type`

`InMemoryTestHarness`, `InMemoryEventBus`, and `RecordingEventBus` all provide
`published_of_type(event_type: type[TEvent]) -> list[TEvent]`, returning all
published events that are instances of `event_type` in publication order with
their static type narrowed to `list[TEvent]`:

```python
from uuid import uuid4
from eventsource import AggregateRepository, InMemoryEventBus, InMemoryEventStore
from eventsource.testing import InMemoryTestHarness, RecordingEventBus


async def test_repository_publishes_typed_events() -> None:
    harness = InMemoryTestHarness()
    repo = harness.create_repository(Account)
    account_id = uuid4()

    await repo.execute(
        account_id,
        OpenAccount(account_id=account_id, owner="alice"),
    )
    await repo.execute(
        account_id,
        DepositMoney(account_id=account_id, amount=50.0),
    )

    # Narrowed to list[MoneyDeposited] directly on InMemoryTestHarness
    harness_deposits = harness.published_of_type(MoneyDeposited)
    assert len(harness_deposits) == 1
    assert harness_deposits[0].amount == 50.0


async def test_event_bus_published_of_type() -> None:
    bus = InMemoryEventBus()
    recording_bus = RecordingEventBus(bus)

    async with InMemoryEventStore() as store:
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=Account,
            event_publisher=recording_bus,
        )
        account_id = uuid4()

        await repo.execute(
            account_id,
            OpenAccount(account_id=account_id, owner="alice"),
        )
        await repo.execute(
            account_id,
            DepositMoney(account_id=account_id, amount=50.0),
        )

    # Narrowed to list[MoneyDeposited] on both InMemoryEventBus and RecordingEventBus
    deposits = bus.published_of_type(MoneyDeposited)
    assert len(deposits) == 1
    assert deposits[0].amount == 50.0

    recorded_opened = recording_bus.published_of_type(AccountOpened)
    assert len(recorded_opened) == 1
    assert recorded_opened[0].owner == "alice"
```

Use `harness.clear_published_events()`, `bus.clear_published_events()`, or
`recording_bus.clear_published_events()` between phases of a test when you want
to reset the recorded publication history. When using the functional BDD helpers
from `eventsource.testing.bdd`, pass `event_type=...` to
`then_event_count(harness, count, event_type=MoneyDeposited)` to assert the
count of a specific event type rather than the total event count.
