# Testing Aggregates, Deciders, and Event Buses

`eventsource.testing` provides fast, deterministic testing utilities that verify
domain behavior through public frontdoors without database or broker setup:

- **`Scenario` / `DeciderScenario`** for synchronous, zero-I/O Given-When-Then
  testing of `DeciderAggregate`s and pure `(decide, evolve, initial_state)`
  triples, including `.then_event_types(*EventClasses)` and `.then_events(*EventClasses)`.
- **`published_of_type(EventType)`** on `InMemoryTestHarness`, `InMemoryEventBus`,
  and `RecordingEventBus` for type-safe inspection of published domain events.

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
`.then_event_types(...)`, and `.then_state(...)`:

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
    repo = AggregateRepository(
        event_store=harness.event_store,
        aggregate_factory=Account,
        event_publisher=harness.event_bus,
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
to reset the recorded publication history.
