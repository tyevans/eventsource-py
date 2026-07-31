# Testing

Technical reference for the `eventsource.testing` package: the fluent
`EventBuilder`, the `InMemoryTestHarness` that bundles in-memory infrastructure,
the `EventAssertions` helper, the Given/When/Then BDD functions, and the two
abstract conformance suites used to validate backend implementations.

These names are **not** re-exported from the top-level `eventsource` package.
Import them from `eventsource.testing` (or the individual submodules). The
package is intended for test code only and should not be imported from
production code paths.

The package is organized into six source modules:

| Module | Contains |
| --- | --- |
| `eventsource.testing.builder` | `EventBuilder` |
| `eventsource.testing.harness` | `InMemoryTestHarness` |
| `eventsource.testing.assertions` | `EventAssertions` |
| `eventsource.testing.recording` | `RecordingEventBus` |
| `eventsource.testing.bdd` | `given_events`, `when_command`, `then_event_published`, `then_no_events_published`, `then_event_sequence`, `then_event_count`, `DeciderScenario` |
| `eventsource.testing.conformance` | `EventStoreConformanceSuite`, `EventBusConformanceSuite` |

Everything listed above is re-exported from `eventsource.testing` itself, and
those thirteen names are the whole of its `__all__`.

The pieces are designed to compose but are independent: `EventBuilder` produces
`DomainEvent` instances with no harness involved, `InMemoryTestHarness` wires
`InMemoryEventStore`, `InMemoryEventBus`, `InMemoryCheckpointRepository`, and
`InMemoryDLQRepository` together with tracing disabled, `EventAssertions` wraps
any list of events, and the BDD helpers are thin functions over a harness. The
conformance suites are separate: they are abstract `unittest`-style test base
classes you subclass in your own pytest suite to check that a custom
`EventStore` or `EventBus` backend honors the interface contract.

> **Note on the harness docstring.** The module docstring in `harness.py` shows
> `harness.create_repository(OrderAggregate)`. No such method exists on
> `InMemoryTestHarness`; construct an `AggregateRepository` yourself from
> `harness.event_store` and `harness.event_bus`. See
> [InMemoryTestHarness](#inmemorytestharness) below for the real surface.

## Overview

`eventsource.testing` addresses four separate testing concerns. They can be used
individually or together; nothing in the package requires anything else in it.

**Constructing events.** `EventBuilder` is a generic fluent builder
(`EventBuilder(OrderCreated).with_aggregate_id(...).with_fields(...).build()`)
that supplies the base `DomainEvent` fields you would otherwise repeat in every
test. Each `with_*` method mutates and returns the same builder, so calls chain;
`build()` constructs the event by passing the accumulated field dict to the
event class, meaning pydantic validation still applies and missing required
fields still raise.

**Providing infrastructure.** `InMemoryTestHarness()` constructs
`InMemoryEventStore`, `InMemoryEventBus`, `InMemoryCheckpointRepository`, and
`InMemoryDLQRepository` in `__init__`, all with `enable_tracing=False`, and
exposes them as read-only properties (`event_store`, `event_bus`,
`checkpoint_repo`, `dlq_repo`). It also exposes the bus's published events via
`published_events` and offers `reset()`, `clear_published_events()`, and
`get_events_of_type()`. The harness is not thread-safe; use one per test.

**Asserting on events.** `EventAssertions(events)` wraps any
`Sequence[DomainEvent]` and provides assertions with failure messages that name
the expected and actual event types — `assert_event_published`,
`assert_no_event_published`, `assert_event_count`, `assert_event_sequence`,
`assert_event_with_fields`, `assert_no_events_published`,
`assert_event_for_aggregate`, plus a non-asserting `get_events_of_type`. The
positive assertions return the matched event so you can inspect it further.

**Given/When/Then.** The BDD helpers read as a scenario over a harness:
`await given_events(harness, events)` seeds prior state, `when_command(aggregate,
command)` runs a callable against an aggregate and returns *only* the events
that command added to `uncommitted_events`, and the `then_*` functions assert
against `harness.published_events`. Note the asymmetry: `given_events` and the
`then_*` helpers take the harness, but `when_command` takes an aggregate and a
callable — it never touches the harness.

**Backend conformance.** `EventStoreConformanceSuite` and
`EventBusConformanceSuite` are ABCs holding `async def test_*` methods written
against the `EventStore` / `EventBus` interfaces. You subclass one, implement
its two abstract factory methods, and pytest collects the inherited tests
against your backend. They are contract checks for implementors, not helpers for
application tests.

A typical application test uses `EventBuilder` to make history,
`InMemoryTestHarness` to hold it, and either `EventAssertions` or the `then_*`
helpers to check the outcome. Everything is in-memory, so no Docker services or
`postgres`/`redis` pytest markers are involved.

## Installation and Imports

`eventsource.testing` ships inside the `eventsource-py` distribution. There is
no `testing` extra to install, and the package needs nothing beyond the core
dependencies (`pydantic>=2.0,<3.0` and `sqlalchemy>=2.0,<3.0`) declared in
`[project.dependencies]`. A plain install is enough:

```bash
pip install eventsource-py
# or, in this repo
uv sync --all-extras
```

Everything the package imports is either stdlib or first-party: `EventBuilder`
imports only `DomainEvent`; `EventAssertions` only `DomainEvent`; `bdd` imports
`AggregateRoot`, `DomainEvent`, and `InMemoryTestHarness`; the conformance
suites import the `EventStore`/`EventBus` interfaces plus `OptimisticLockError`
and `ExpectedVersion`; and `InMemoryTestHarness` imports the four in-memory
implementations. None of the backend extras (`postgresql`, `sqlite`, `redis`,
`rabbitmq`, `kafka`, `telemetry`) are required to use anything in this package
— you only need them if the backend you are conformance-testing needs them.

### Importing

The eleven public names are re-exported from the package root, which is the
import path to prefer:

```python
from eventsource.testing import (
    EventBuilder,
    InMemoryTestHarness,
    EventAssertions,
    given_events,
    when_command,
    then_event_published,
    then_no_events_published,
    then_event_sequence,
    then_event_count,
    DeciderScenario,
    EventStoreConformanceSuite,
    EventBusConformanceSuite,
)
```

Submodule imports work identically and are useful when you want only one piece:

```python
from eventsource.testing.builder import EventBuilder
from eventsource.testing.conformance import EventBusConformanceSuite
```

Two import facts worth internalizing:

- **Nothing here is exported from the top-level `eventsource` package.**
  `from eventsource import EventBuilder` raises `ImportError`. The top-level
  `__init__.py` deliberately covers only production API; test utilities stay
  behind the `eventsource.testing` path.
- **Import it from test code only.** The package docstring states this
  explicitly. Importing `eventsource.testing` from an application module drags
  in-memory infrastructure into your production import graph for no benefit.

### Test-runner requirements

Every conformance test method and several helpers (`given_events`, the
store/bus calls you make around them) are `async def`, so your runner must be
able to execute coroutine tests. This project uses `pytest-asyncio` in auto
mode, configured in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
```

With `asyncio_mode = "auto"` you write `async def test_...` with no
`@pytest.mark.asyncio` decorator. If your project uses `strict` mode instead,
decorate your own tests — and, for conformance subclasses, apply
`pytestmark = pytest.mark.asyncio` at module level so the inherited async test
methods are collected as coroutine tests too. `anyio` works as well, provided
the inherited methods end up under an anyio-aware collection rule.

`pytest-asyncio` is a development dependency of this repo (`[project.optional-dependencies].dev`),
not a runtime dependency of `eventsource-py`; add it to your own test
requirements.

None of the in-memory pieces require Docker, so tests built on this package
carry no `integration`, `postgres`, `sqlite`, `redis`, `kafka`, or `rabbitmq`
marker. Conformance suites are the exception: mark *your subclass* according to
the backend it instantiates, since that subclass is what needs a live service.

## EventBuilder

`eventsource.testing.builder.EventBuilder` is a generic fluent builder for
`DomainEvent` subclasses. It exists to remove the base-field boilerplate
(`aggregate_id`, `aggregate_version`, and friends) that every test event
otherwise repeats, so a test only spells out the fields the scenario cares
about.

```python
from eventsource.testing import EventBuilder

event = (
    EventBuilder(OrderCreated)
    .with_aggregate_id(order_id)
    .with_fields(customer_id=customer_id, amount=99.99)
    .build()
)
```

Three properties govern all of the below:

- **Mutable and self-returning.** Every `with_*` method mutates the builder's
  internal field dict and returns `self`. Chaining is a convenience, not a
  copy-on-write; two references to one builder are the same builder.
- **Last write wins.** Setting the same field twice — by any combination of
  methods — keeps the later value.
- **Validation happens in `build()`, not before.** The builder never inspects
  field names against the event model. Typos, missing required fields, and bad
  types all surface as a `pydantic.ValidationError` when you call `build()`.

### `EventBuilder(event_class)`

```python
EventBuilder(event_class: type[TEvent]) -> EventBuilder[TEvent]
```

`TEvent` is a `TypeVar` bound to `DomainEvent`, so the builder is generic in the
event type and `build()` returns that exact type rather than `DomainEvent`.

The constructor validates its argument eagerly: if `event_class` is not a class
or is not a `DomainEvent` subclass, it raises
`TypeError("event_class must be a DomainEvent subclass, got ...")`. This fires
for `dict`, for a string, and for `None`. `DomainEvent` itself is accepted (it
is a subclass of itself), though building it fails on required fields.

After construction the field dict already contains two entries:

| Field | Default |
| --- | --- |
| `aggregate_id` | a fresh `uuid4()` |
| `aggregate_version` | `1` |

Nothing else is pre-seeded. `event_id`, `occurred_at`, `correlation_id`, and
`event_version` are *not* set by the builder — they come from `DomainEvent`'s
own pydantic defaults (`uuid4()`, `datetime.now(UTC)`, `uuid4()`, and `1`
respectively) at construction time. The practical consequence: calling `build()`
twice on one builder yields events with the *same* `aggregate_id` (it lives in
the builder) but *different* `event_id`s (regenerated per instance).

`aggregate_type` is a required field on `DomainEvent` with no default and the
builder does not supply one. Event classes intended for use with the builder
should declare it as a class-level default (`aggregate_type: str = "Order"`),
which is the normal convention for this library; otherwise pass it explicitly
via `with_field("aggregate_type", "Order")`.

(The class docstring lists `event_id` and `occurred_at` under "auto-generated
defaults." They are auto-generated, but by `DomainEvent`, not by the builder —
the distinction matters only for the repeated-`build()` behavior described
above.)

`__repr__` renders as `EventBuilder(<ClassName>, fields=[...])`, listing the
currently-set field names — useful when a `build()` failure leaves you unsure
what the builder was holding.

`eventsource.testing.builder.__all__` is `["EventBuilder", "TEvent"]`, so the
type variable is importable from the submodule if you need to annotate helpers
that pass builders around; only `EventBuilder` is re-exported from
`eventsource.testing`.

### Field methods: `with_aggregate_id`, `with_event_id`, `with_tenant_id`, `with_version`, `with_occurred_at`

These five methods each write exactly one base `DomainEvent` field into the
builder's field dict and `return self`. None of them validate; every one is a
one-line assignment, so a bad value is only rejected when `build()` hands the
dict to pydantic.

```python
with_aggregate_id(aggregate_id: UUID) -> EventBuilder[TEvent]
with_event_id(event_id: UUID) -> EventBuilder[TEvent]
with_tenant_id(tenant_id: UUID) -> EventBuilder[TEvent]
with_version(version: int) -> EventBuilder[TEvent]
with_occurred_at(occurred_at: datetime) -> EventBuilder[TEvent]
```

| Method | Field written | Model default | Notes |
| --- | --- | --- | --- |
| `with_aggregate_id` | `aggregate_id` | required (`Field(...)`) | Builder pre-seeds a `uuid4()`; this overwrites it. |
| `with_event_id` | `event_id` | `default_factory=uuid4` | Not pre-seeded by the builder. Set it only when an assertion needs a known id. |
| `with_tenant_id` | `tenant_id` | `None` (`UUID \| None`) | Multi-tenant scenarios only. |
| `with_version` | **`aggregate_version`** | `1`, constrained `ge=1` | Builder pre-seeds `1`; this overwrites it. |
| `with_occurred_at` | `occurred_at` | `datetime.now(UTC)` | Not pre-seeded by the builder. |

Three specifics are worth holding onto.

**`with_version` writes `aggregate_version`.** The method name and the field
name differ, and there is no `with_aggregate_version` alias. The field carries
`ge=1`, so `with_version(0)` returns the builder happily and raises
`pydantic.ValidationError` at `build()` — the traceback points at the `build()`
call, not at `with_version`.

**Only `aggregate_id` and `aggregate_version` are pre-seeded.** `with_event_id`
and `with_occurred_at` set fields the builder otherwise leaves entirely to
`DomainEvent`'s own defaults, which are evaluated per constructed instance. That
is why `event_id` differs between two `build()` calls on one builder unless you
pin it with `with_event_id`, while `aggregate_id` stays the same.

**`occurred_at` is stored verbatim.** Neither the builder nor the model coerces
the timezone, so a naive `datetime` stays naive on the built event. Pass
tz-aware values (`datetime(2023, 1, 1, tzinfo=UTC)`) to match the UTC
`default_factory` and keep ordering comparisons safe.

Pinning `occurred_at` is the usual reason to reach for this group — replay
ordering, retention windows, and any assertion about elapsed time:

```python
from datetime import UTC, datetime
from uuid import uuid4

event = (
    EventBuilder(SampleEvent)
    .with_aggregate_id(order_id)
    .with_occurred_at(datetime(2023, 1, 1, tzinfo=UTC))
    .with_version(1)
    .with_fields(customer_id=uuid4(), amount=100.0)
    .build()
)

assert event.occurred_at == datetime(2023, 1, 1, tzinfo=UTC)
assert event.aggregate_version == 1
```

To build an ordered history for one aggregate, hold the id in a variable and
walk `with_version` upward — the builder will not increment it for you:

```python
order_id = uuid4()
history = [
    EventBuilder(SampleEvent)
    .with_aggregate_id(order_id)
    .with_version(v)
    .with_fields(customer_id=uuid4(), amount=float(v))
    .build()
    for v in (1, 2, 3)
]
```

Each iteration constructs a fresh builder, so the events share `aggregate_id`
and differ in everything the loop varies. Reusing one builder across the loop
would work too, since `with_version` overwrites, but see
[`build()`](#build) for the shared-state caveat.

Every one of these is equivalent to the corresponding `with_field` call —
`with_version(3)` and `with_field("aggregate_version", 3)` produce identical
state. Use the named methods for readability and the generic ones for fields
that have no named method (including clearing `tenant_id` back to `None`, which
`with_tenant_id` cannot express because it is typed `UUID`).

### Correlation methods: `with_correlation_id`, `with_causation_id`, `with_actor_id`

These three write the `DomainEvent` provenance fields — the ones that say which
logical operation an event belongs to, which event triggered it, and who was
responsible. Like every other `with_*` method they are single assignments into
the field dict followed by `return self`; nothing is validated until `build()`.

```python
with_correlation_id(correlation_id: UUID) -> EventBuilder[TEvent]
with_causation_id(causation_id: UUID) -> EventBuilder[TEvent]
with_actor_id(actor_id: str) -> EventBuilder[TEvent]
```

| Method | Field written | Model type and default | Meaning |
| --- | --- | --- | --- |
| `with_correlation_id` | `correlation_id` | `UUID`, `default_factory=uuid4` | Links related events across aggregates — one saga or request. |
| `with_causation_id` | `causation_id` | `UUID \| None`, default `None` | The `event_id` of the event that caused this one. |
| `with_actor_id` | `actor_id` | `str \| None`, default `None` | The user or system that triggered the event. |

None of the three are pre-seeded by the builder, so an event built without them
gets `correlation_id=uuid4()`, `causation_id=None`, and `actor_id=None` from the
model's own defaults.

**`actor_id` is a `str`, not a `UUID`.** The field is `str | None`, and the
method is typed accordingly. Passing a `UUID` is a type error under mypy and
pydantic will reject it at `build()`; stringify it yourself. Values in this
codebase's tests look like `"user-123"` and `"system:cron"` — any opaque
identifier scheme works, including a `"system:"`-prefixed convention for
non-human actors.

**`correlation_id` defaults per event, not per scenario.** Because the model
generates a fresh `uuid4()` for each instance, two events built independently
are *not* correlated. Any test that asserts on
`DomainEvent.is_correlated_with()` must set the id explicitly on both events.

**`causation_id` is not chained for you.** The builder never looks at previously
built events; point it at the predecessor's `event_id` by hand. That is exactly
the relationship `DomainEvent.is_caused_by()` checks.

To model a two-step chain, share one correlation id and link the second event to
the first:

```python
from uuid import uuid4

agg_id = uuid4()
correlation_id = uuid4()

first = (
    EventBuilder(SampleEvent)
    .with_aggregate_id(agg_id)
    .with_correlation_id(correlation_id)
    .with_actor_id("user-123")
    .with_version(1)
    .with_fields(customer_id=uuid4(), amount=100.0)
    .build()
)

second = (
    EventBuilder(SampleEvent)
    .with_aggregate_id(agg_id)
    .with_correlation_id(correlation_id)
    .with_causation_id(first.event_id)
    .with_actor_id("user-123")
    .with_version(2)
    .with_fields(customer_id=uuid4(), amount=50.0)
    .build()
)

assert second.is_caused_by(first)
assert second.is_correlated_with(first)
```

Note the ordering constraint this creates: `first.event_id` only exists after
`first` has been built, so chained events cannot be produced from a single
chained expression. Build the predecessor, then the successor.

An alternative for the same effect is `DomainEvent.with_causation()`, which
returns a copy with both `causation_id` and `correlation_id` taken from the
causing event:

```python
second = builder.build().with_causation(first)
```

Use the builder methods when the ids are part of the scenario you are setting
up, and `with_causation()` when you already have both events in hand and only
want to express the link. Since `DomainEvent` is frozen, `with_causation()`
returns a new instance rather than mutating.

### Generic methods: `with_metadata`, `with_field`, `with_fields`

```python
with_metadata(metadata: dict[str, Any]) -> EventBuilder[TEvent]
with_field(name: str, value: Any) -> EventBuilder[TEvent]
with_fields(**kwargs: Any) -> EventBuilder[TEvent]
```

These three are the escape hatches: one for the `metadata` dict, two for
arbitrary field names. Together they cover every field the named methods do not
— in practice, the event-specific payload. Their bodies are, respectively,
`self._fields["metadata"] = metadata`, `self._fields[name] = value`, and
`self._fields.update(kwargs)`, each followed by `return self`.

| Method | Writes | Semantics |
| --- | --- | --- |
| `with_metadata` | `metadata` | Replaces the whole dict |
| `with_field` | the key you name | One key at a time; the key may be anything |
| `with_fields` | every keyword you pass | `dict.update`; keyword syntax, so keys must be identifiers |

#### `with_metadata` replaces, it does not merge

`metadata` on `DomainEvent` is `dict[str, Any]` with `default_factory=dict`, so
an event built without it gets `{}`. `with_metadata` assigns straight over the
key: two calls leave only the second dict.

```python
builder.with_metadata({"key1": "value1"})
builder.with_metadata({"key2": "value2"})
# builder will build metadata == {"key2": "value2"}
```

Build the whole dict in one call:

```python
event = (
    EventBuilder(SampleEvent)
    .with_metadata(
        {
            "ip_address": "192.168.1.1",
            "user_agent": "TestClient/1.0",
            "request_id": "req-12345",
        }
    )
    .with_fields(customer_id=uuid4(), amount=250.75)
    .build()
)
```

Do not confuse this with `DomainEvent.with_metadata(**kwargs)`, which is a
different method with different semantics: it takes keywords rather than a dict,
**merges** them into the existing metadata, and returns a copy of the event
(`model_copy`) because `DomainEvent` is frozen. The builder method takes a dict,
overwrites, and returns the builder.

```python
enriched = event.with_metadata(trace_id="abc123")  # merges, new event
builder.with_metadata({"trace_id": "abc123"})      # replaces, same builder
```

Passing `{}` is legitimate and produces `metadata == {}` — identical to the
model default, but explicit. Pydantic validates the dict at `build()`, which
copies it, so later mutation of the dict you passed does not reach the built
event.

#### `with_field` and `with_fields` accept any key

Neither method checks the name against the event model. Any key is accepted,
including base fields, so the named methods are pure convenience:

```python
builder.with_field("aggregate_version", 3)   # same as builder.with_version(3)
builder.with_field("tenant_id", None)        # clears an optional field
```

That last line is the one case where the generic method is strictly more capable
than the named one: `with_tenant_id` is typed `UUID`, so `None` can only be set
through `with_field`.

Both write into the same dict, so ordering — not method choice — decides the
winner. A later `with_fields` overwrites an earlier `with_field` and vice versa:

```python
event = (
    EventBuilder(SampleEvent)
    .with_field("amount", 100.0)
    .with_fields(customer_id=uuid4(), amount=200.0)
    .build()
)
assert event.amount == 200.0
```

Choose between them by shape, not by meaning: `with_fields(customer_id=cid,
amount=99.99)` is the readable form for a payload known at the call site, while
`with_field(name, value)` is what you need for a key held in a variable or one
that is not a valid Python identifier.

#### Typos are silent

`DomainEvent` sets `model_config = ConfigDict(frozen=True)` and nothing else, so
pydantic's default `extra="ignore"` applies. An unknown key is **discarded at
construction without error**:

```python
EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amonut=100.0).build()
# pydantic.ValidationError: amount — Field required
```

The failure names the *missing* required field, never the misspelled one, and if
the mistyped field happened to be optional there is no failure at all — the
event just builds without it. When an assertion about a payload field fails
inexplicably, check the spelling of the `with_field`/`with_fields` key before
anything else.

### `build()`

```python
build() -> TEvent
```

Constructs the event by splatting the accumulated dict into the event class:
`self._event_class(**self._fields)`. There is no post-processing — pydantic does
all validation and coercion, and the returned instance is frozen like any other
`DomainEvent`.

`build()` raises `pydantic.ValidationError` when:

- a required field was never set (`EventBuilder(SampleEvent).build()` with
  `customer_id`/`amount` unset),
- a value has the wrong type (`customer_id="not-a-uuid"`),
- a value violates a model constraint (`with_version(0)` against `ge=1`).

`build()` is repeatable and non-consuming. The builder keeps its state, so you
can build, mutate, and build again to produce a variant:

```python
builder = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=100.0)
first = builder.build()
second = builder.with_field("amount", 200.0).build()

assert first.amount == 100.0
assert second.amount == 200.0
assert first.aggregate_id == second.aggregate_id  # held by the builder
assert first.event_id != second.event_id          # regenerated per instance
```

That reuse is the main sharp edge: a builder shared across several events in one
test silently shares `aggregate_id` and every other field you set. When you want
independent events, construct a new builder.

## DeciderScenario

`DeciderScenario` is a synchronous Given-When-Then harness for testing
decider-style aggregates. Unlike the async `given_events` / `when_command` / `then_*`
helpers which require a harness and infrastructure, `DeciderScenario` is standalone,
synchronous, and requires only the domain's three pure functions: `decide`,
`evolve`, and `initial_state`.

### For decider-style testing: no store, no bus, no async

The BDD helpers above (`given_events`, `when_command`, `then_event_published`) are
built on top of `InMemoryTestHarness` and do async operations under the hood. They
are ideal for testing imperative aggregates (`DeclarativeAggregate`, hand-written
`_apply`) because those aggregates live inside a store-and-bus ecosystem.

`DeciderScenario` addresses a different testing need: when your domain is three
pure functions, you should be able to test it with plain asserts and zero
infrastructure. A decider-style domain looks like:

```python
from eventsource import DeciderAggregate

class Order(DeciderAggregate[OrderState]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> OrderState:
        return OrderState(order_id=aggregate_id)

    @staticmethod
    def decide(command: object, state: OrderState) -> list[DomainEvent]:
        # Pure function: returns events or raises an exception
        ...

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        # Pure function: folds an event into the state
        ...
```

### `DeciderScenario(aggregate_class, ...)`

```python
DeciderScenario(
    aggregate_class: type[Any] | None = None,
    *,
    decide: Callable[[Any, Any], list[DomainEvent]] | None = None,
    evolve: Callable[[Any, DomainEvent], Any] | None = None,
    initial_state: Callable[[UUID], Any] | None = None,
    aggregate_id: UUID | None = None,
) -> DeciderScenario
```

Create a scenario by passing a `DeciderAggregate` subclass (which provides the three
functions) or by passing the three functions directly. If `aggregate_id` is not
provided, one is generated.

```python
from eventsource.testing import DeciderScenario

# From an aggregate class
scenario = DeciderScenario(Order)

# Or from individual functions (useful for testing pure functions in isolation)
scenario = DeciderScenario(
    decide=decide,
    evolve=evolve,
    initial_state=initial_state,
)
```

### Methods: `given()`, `when()`, `then_events()`, `then_rejected()`

All methods return `self`, so calls chain:

```python
scenario.given(...).when(...).then_events(...)
```

#### `given(*events: DomainEvent) -> DeciderScenario`

Folds prior events into the initial state via `evolve`, building up a scenario's
state before issuing a command. Multiple `given` calls fold in sequence:

```python
scenario = (
    DeciderScenario(Order)
    .given(OrderCreated(aggregate_id=order_id, aggregate_version=1, ...))
    .given(OrderPaid(aggregate_id=order_id, aggregate_version=2, ...))
)
```

`given()` does not check that the events it is handed belong to the scenario's
`aggregate_id` -- it folds whatever it is given through `evolve` unconditionally, so
passing an event stamped with a different `aggregate_id` is folded in silently rather
than rejected.

#### `when(command: object) -> DeciderScenario`

Runs `decide(command, state)`, capturing either the returned events or any raised
exception. The scenario records the outcome for inspection by `then_*` methods.

```python
scenario = scenario.when(ShipOrder(tracking_number="TRACK123"))
```

#### `then_events(*event_types: type[DomainEvent]) -> DeciderScenario`

Asserts that `decide` produced exactly the given event types, in order. Raises
`AssertionError` if the types or count do not match:

```python
scenario.then_events(OrderShipped)  # exactly one event, of that type
scenario.then_events(OrderPaid, OrderShipped)  # two events in sequence
```

#### `then_rejected(exc_type: type[BaseException] = CommandRejectedError, match: str | None = None) -> DeciderScenario`

Asserts that `decide` raised an exception. The default is `CommandRejectedError`,
but any exception type can be checked. If `match` is provided, the exception message
must match the regex:

```python
scenario.then_rejected()  # command raised CommandRejectedError
scenario.then_rejected(ValueError, match="Cannot ship unpaid")  # specific type and message
```

#### `events` property

Read-only. Returns the list of events produced by `when()`, or an empty list if
`when()` has not been called or if `decide` raised an exception:

```python
events = scenario.events
assert len(events) == 1
assert events[0].aggregate_id == order_id
```

### Example: testing a decider with DeciderScenario

Here is a complete test:

```python
from decimal import Decimal
from uuid import uuid4

from eventsource.testing import DeciderScenario

def test_paid_order_ships():
    order_id = uuid4()

    (DeciderScenario(Order)
     .given(
        OrderCreated(aggregate_id=order_id, aggregate_version=1, ...),
        OrderPaid(aggregate_id=order_id, aggregate_version=2, ...),
     )
     .when(ShipOrder(tracking_number="TRACK123"))
     .then_events(OrderShipped))

def test_unpaid_order_cannot_ship():
    order_id = uuid4()

    (DeciderScenario(Order)
     .given(OrderCreated(aggregate_id=order_id, aggregate_version=1, ...))
     .when(ShipOrder(tracking_number="TRACK123"))
     .then_rejected(match="Cannot ship unpaid"))  # exc_type defaults to CommandRejectedError
```

`then_rejected` is not limited to `CommandRejectedError` -- pass any exception type
explicitly and it is checked the same way, e.g. `then_rejected(ValueError, match="...")`
if your `decide()` raises a plain `ValueError` instead.

### When to use DeciderScenario vs. the async BDD helpers

Use `DeciderScenario` when:
- Your aggregate is a `DeciderAggregate` or uses the decider pattern.
- You want to test domain logic in isolation with no infrastructure.
- You prefer synchronous tests (no `async`/`await`).

Use the async BDD helpers (`given_events`, `when_command`, `then_*`) when:
- Your aggregate is `DeclarativeAggregate` or uses hand-written `_apply`.
- You need to test the full aggregate lifecycle: loading, saving, publishing.
- You are testing behavior that spans the command and the repository.

The two approaches test different layers: `DeciderScenario` isolates the domain
logic (pure functions), while the async helpers validate the aggregate's contract
with the store and bus.
