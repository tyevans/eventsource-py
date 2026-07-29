# Validate a Custom Backend with the Conformance Suites

This guide shows you how to check that your own `EventStore` or `EventBus`
implementation actually honors the contract the rest of the library depends on.

`eventsource.testing.conformance` ships two abstract base suites --
`EventStoreConformanceSuite` and `EventBusConformanceSuite`. Each one defines a
small set of factory methods you implement (`create_store()` / `create_bus()`
and `create_test_event()`) plus a body of ready-written async test methods that
exercise the contract: append/get roundtrips, stream isolation, optimistic
locking, global position tracking, publish/subscribe delivery, unsubscribe, and
handler error isolation. You supply the backend; the suite supplies the tests.

By the end you will have a pytest module that runs the full inherited suite
against your backend, and you will know how to add backend-specific tests
alongside the inherited ones.


## When to use this guide

Use this guide when you have written -- or are about to write -- your own
implementation of `EventStore` (from `eventsource.stores.interface`) or
`EventBus` (from `eventsource.bus.interface`) and you want confidence that it
behaves the way aggregates, repositories, projections, and subscriptions
already assume. Typical cases:

- You are adding a backend the library does not ship, such as a MySQL,
  DynamoDB, or EventStoreDB event store, or an NATS or SQS event bus.
- You have wrapped or decorated a built-in backend -- adding caching,
  encryption, sharding, or multi-tenant routing -- and need to prove the
  wrapper did not break contract semantics such as optimistic locking or
  stream isolation.
- You are upgrading a driver or database version and want a regression check
  that goes deeper than your own feature tests.
- You are reviewing a third-party backend before adopting it.

The suites are contract tests, not a full test plan. They check behavior that
must be true of *every* backend: that `append_events` and `get_events`
roundtrip, that streams stay isolated from one another, that a wrong
`expected_version` raises `OptimisticLockError` while
`ExpectedVersion.ANY` does not, that event metadata survives serialization,
that `event_exists` supports idempotent writes, that global positions advance,
and -- on the bus side -- that publish reaches subscribers, that multiple
subscribers each receive an event, that `unsubscribe` stops delivery for both
typed and all-event subscriptions, and that one failing handler does not
prevent others from running.

They deliberately say nothing about behavior that is yours to define:
connection pooling and retries, schema and index layout, transaction
boundaries you expose to callers, snapshot storage, delivery ordering
guarantees stronger than the contract, at-least-once versus exactly-once
semantics, or performance. Keep writing backend-specific tests for those --
this guide shows you how to put them in the same class as the inherited ones.

Skip this guide if you are only *using* a built-in backend. The shipped
PostgreSQL, SQLite, and in-memory stores and the in-memory, Redis, RabbitMQ,
and Kafka buses are already covered by the library's own test suite; you do not
need to re-validate them.

## Prerequisites

Before you start, you need:

- **Python 3.11 or newer.** The library declares `requires-python = ">=3.11"`.
- **`eventsource-py` installed**, with whatever optional extras your backend
  driver needs (`postgresql`, `sqlite`, `redis`, `rabbitmq`, `kafka`,
  `telemetry`, or `all`). The conformance suites themselves live in
  `eventsource.testing.conformance` and pull in nothing beyond the core
  dependencies, pydantic and SQLAlchemy.
- **pytest and pytest-asyncio.** The suites are plain classes of `async def`
  test methods; they do not bundle a runner or an event loop. Both ship in the
  library's `dev` extra (`pytest>=8.0`, `pytest-asyncio>=0.23`), or you can
  install them directly into your own project. Step 1 covers the
  `asyncio_mode = "auto"` setting that makes pytest actually await those
  methods.
- **A backend implementation to test**: a concrete subclass of `EventStore`
  from `eventsource.stores.interface`, or of `EventBus` from
  `eventsource.bus.interface`. It does not have to be finished -- running the
  suite against a partial implementation is a reasonable way to drive the work
  -- but it must be importable and constructible from your test module.
- **Any external resources your backend needs**, running and reachable from the
  test process: a database, broker, or container. The suite calls your
  `create_store()` / `create_bus()` factory and expects a usable instance back;
  it does not start infrastructure for you. See
  [Backends that need external resources](#backends-that-need-external-resources-fixtures-docker-markers)
  for wiring that up with fixtures and markers.

You should also be comfortable with the basics of the contract you are
implementing -- `append_events`, `get_events`, `ExpectedVersion`, and
`OptimisticLockError` for stores; `publish`, `subscribe`, and `unsubscribe` for
buses. This guide validates an implementation; it does not teach you how to
write one.

## Step 1: Install the test dependencies and enable `asyncio_mode = "auto"`

Install pytest, pytest-asyncio, and `eventsource-py` with the extras your
backend driver needs. If you are working inside a checkout of this repository,
the `dev` extra already pins both (`pytest>=8.0`, `pytest-asyncio>=0.23`):

```bash
uv sync --all-extras
```

In your own project, install them alongside the library:

```bash
uv add --dev pytest pytest-asyncio
uv add "eventsource-py[postgresql]"   # or sqlite, redis, rabbitmq, kafka, all
```

Or with pip:

```bash
pip install pytest pytest-asyncio "eventsource-py[postgresql]"
```

Now configure pytest-asyncio. Every test method on
`EventStoreConformanceSuite` and `EventBusConformanceSuite` is an
`async def` -- the interfaces they exercise are async all the way down -- and
the suites are plain ABCs, not fixtures or plugins, so they carry no
`@pytest.mark.asyncio` decorators and no event loop of their own. Without auto
mode, pytest collects each inherited test, gets a coroutine object back, never
awaits it, and reports a pass. Nothing is actually verified. Add
`asyncio_mode = "auto"` so pytest-asyncio runs every async test it collects,
inherited ones included.

In `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
```

Or, if your project uses `pytest.ini`:

```ini
[pytest]
asyncio_mode = auto
```

That single setting is all the suites require. Two related options are worth
knowing about, because this repository sets them and their default differs from
what a fresh project gets:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "session"
asyncio_default_test_loop_scope = "session"
```

With the pytest-asyncio default (function scope), each test gets a fresh event
loop. That is the safest choice and the one to start with. Session scope shares
one loop across the run, which matters if your backend holds a connection pool
or a long-lived client on a fixture that outlives a single test -- a pool bound
to a closed loop raises `RuntimeError: Event loop is closed` or
`attached to a different loop` on the next test. If you hit that, either move
your backend's setup into a session-scoped fixture and match the loop scope, or
keep function scope and construct a fresh client per test inside
`create_store()` / `create_bus()`.

Verify the setting took effect before you write any suite code. Drop a
throwaway async test into your test directory:

```python
async def test_asyncio_mode_is_on():
    assert True
```

Run it. If it passes, auto mode is active. If pytest instead skips it with
"async def functions are not natively supported", or warns
`coroutine 'test_asyncio_mode_is_on' was never awaited`, the configuration is
not being picked up -- usually because the config file lives outside pytest's
rootdir, or because pytest-asyncio is not installed in the interpreter running
pytest. Fix that now; every inherited conformance test depends on it.

## Step 2: Define a test event type

Both suites call `create_test_event()` and store or publish whatever it hands
back, so before you subclass either suite you need one concrete `DomainEvent`
subclass to feed them. Put it at module scope in your test file (or in a shared
`conftest.py` if both the store and bus suites will use it):

```python
from uuid import UUID

from eventsource.events.base import DomainEvent
from eventsource.events.registry import register_event


@register_event
class ConformanceTestEvent(DomainEvent):
    aggregate_type: str = "ConformanceTest"
    payload: str = "test"
```

Three things in that six-line class are doing real work.

**`aggregate_type` needs a default.** On `DomainEvent` it is a required field
(`Field(...)`), and the suites never pass it: every store test reads it back
off the event you return -- `aggregate_type = event1.aggregate_type` -- and
then uses that value for `append_events` and `get_events`. Give it a default
and the suite's factory calls stay to the two arguments it actually passes.

**`event_type` should be left alone.** `DomainEvent.__init_subclass__`
auto-derives it from the class name, so `ConformanceTestEvent` gets
`event_type == "ConformanceTestEvent"` for free. If you do set it explicitly to
something other than the class name, the library logs a warning unless you also
set `suppress_event_type_warning = True` on the class. There is no reason to
bother here.

**Registration is not automatic.** Defining a `DomainEvent` subclass does not
put it in the `EventRegistry` -- you register it with the `@register_event`
decorator (or `default_registry.register(ConformanceTestEvent)`). Stores that
persist events as JSON look the class back up by `event_type` on read; the
shipped PostgreSQL store raises `EventTypeNotFoundError` from
`_deserialize_event` when the type is missing. So without the decorator,
`test_append_and_get_roundtrip` fails on the `get_events` call rather than on
anything your backend got wrong. If your store takes an `event_registry`
argument -- as the built-in ones do, defaulting to the module-level
`default_registry` -- you can instead build an isolated `EventRegistry()`,
register the event on it, and pass it in from `create_store()`. That keeps the
global registry clean across a large test suite.

Beyond that, keep the event small. The suites do not inspect your payload; they
only check the envelope fields (`event_id`, `event_type`, `aggregate_id`,
`aggregate_version`, `occurred_at`). One trivial extra field like `payload`
above is enough to prove that event-specific data survives a serialization
roundtrip in your own added tests. Do add fields with non-trivial types --
`UUID`, `datetime`, `Decimal`, nested models -- if your backend does its own
encoding and you want the roundtrip exercised, but add them as *extra* tests;
the inherited ones will not look.

Two details are worth knowing before you write the factory in Step 3:

- **`aggregate_version` must be at least 1** (`Field(default=1, ge=1)`). The
  store suite asks for versions 1 and 2 and never 0, so passing the suite's
  `version` argument straight through is safe -- but do not "helpfully" clamp
  or offset it, or `test_append_and_get_roundtrip`'s
  `assert result.new_version == 2` will not line up.
- **`occurred_at` must survive a roundtrip exactly.**
  `test_event_metadata_preserved` asserts
  `retrieved_event.occurred_at == event.occurred_at`, and the default is
  `datetime.now(UTC)` with microsecond precision. A backend that stores
  timestamps in a column without sub-second precision, or that drops the
  timezone, fails that assertion. That is a genuine contract failure, not a
  quirk of the test event -- fix the storage, do not round the event.

`DomainEvent` is frozen (`model_config = ConfigDict(frozen=True)`), so each
call to `create_test_event()` must construct a new instance rather than mutate
a cached one. Returning a fresh object per call also keeps `event_id` unique,
which `test_event_exists_idempotency` and the stream-isolation tests rely on.

## Step 3: Validate an EventStore implementation

With the event type in place, wiring the store suite is three small pieces of
code: a subclass, two factory methods, and a `Test*`-named class that pytest
will actually collect.

### Subclass `EventStoreConformanceSuite`

Import the suite and subclass it. Note that it is an `ABC` whose name does not
start with `Test`, which is deliberate -- pytest will not collect the base
class itself, so the inherited tests only run against your concrete subclass:

```python
from uuid import UUID

from eventsource.stores.interface import EventStore
from eventsource.testing.conformance import EventStoreConformanceSuite


class MyStoreConformance(EventStoreConformanceSuite):
    ...
```

Give the intermediate class a non-`Test` name too. It carries your factory
implementations and any shared setup, and the last sub-step below adds the
thin `Test*` class on top. If you name this class `TestMyStore` directly,
everything still runs -- the split just makes it easier to reuse the same
factories under several parameterizations later.

The suite is abstract on exactly two methods, `create_store()` and
`create_test_event()`. Everything else is inherited and needs no code from
you. Because they are `@abstractmethod`s, forgetting one is a construction-time
`TypeError` at collection rather than a silent skip.

### Implement `create_store()`

`create_store()` is a **synchronous** method returning an `EventStore`. All
eight inherited tests call it as their first statement and never call it again,
so it runs exactly once per test:

```python
    def create_store(self) -> EventStore:
        return MyEventStore(dsn=self.dsn)
```

Two properties matter, and the suite depends on both:

- **It must return a fresh, empty store.** `test_global_position_tracking`
  opens with `assert await store.get_global_position() == 0`, which only holds
  on a store with no events in it. If your factory hands back a store backed by
  a database that already holds rows from an earlier test, that assertion fails
  first -- and the failure tells you nothing about your position tracking. The
  same test then appends one event and asserts
  `result.global_position > 0` and
  `await store.get_global_position() == result.global_position`, so a store
  whose global counter carries over from a previous test will not match either.
- **It cannot await.** The signature is `def`, not `async def`, so any
  connection, pool, or schema creation your backend needs must happen outside
  it. Do the async setup in a pytest fixture and have `create_store()` return a
  cheap object built from what the fixture stashed on `self`:

```python
import pytest


class MyStoreConformance(EventStoreConformanceSuite):
    @pytest.fixture(autouse=True)
    async def _setup(self, tmp_path):
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
        await create_schema(self.engine)
        yield
        await self.engine.dispose()

    def create_store(self) -> EventStore:
        return MyEventStore(engine=self.engine)
```

An `autouse=True` fixture runs before each test method, including the inherited
ones, which is what makes per-test isolation work without touching the suite's
code. Point it at a fresh database, schema, or key prefix each time --
`tmp_path` above, a truncate in setup, or a uniquely named schema per test all
work. See
[Backends that need external resources](#backends-that-need-external-resources-fixtures-docker-markers)
for the Docker-backed variants.

### Implement `create_test_event(aggregate_id, version=1)`

The store suite's factory takes both an `aggregate_id` and a `version`, and it
must pass both straight through to the event you defined in Step 2:

```python
    def create_test_event(
        self, aggregate_id: UUID, version: int = 1
    ) -> DomainEvent:
        return ConformanceTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=version,
        )
```

Keep the `version` default at `1` so the signature matches the abstract method.
The suite calls it both ways -- `create_test_event(aggregate_id)` in
`test_empty_stream` and `create_test_event(aggregate_id, version=2)` in the
roundtrip and locking tests -- and it always passes `version` as a keyword.

Do not cache the returned event. Several tests compare `event_id` values
between two events created for the same aggregate
(`test_append_and_get_roundtrip` asserts
`stream.events[0].event_id == event1.event_id` and
`stream.events[1].event_id == event2.event_id`), so returning the same instance
twice makes those assertions pass vacuously while hiding real ordering bugs.

Also note that the suite never passes `aggregate_type`. It reads it back off
the first event it creates -- `aggregate_type = event1.aggregate_type` -- and
uses that value for every `append_events` and `get_events` call in the test.
That is why Step 2 gave the field a default. If your store needs a specific
aggregate type name to route to a table or collection, set it as the default on
your test event class rather than trying to inject it through the factory.

### Expose the suite to pytest with a `Test*` subclass

Finally, add the collectable class. It needs no body:

```python
class TestMyEventStoreConformance(MyStoreConformance):
    """Runs the full EventStore conformance suite against MyEventStore."""
```

Pytest collects classes matching `Test*` (and, by default, skips any class with
an `__init__`, which these do not have). The eight inherited test methods now
show up under it:

```
tests/test_my_store_conformance.py::TestMyEventStoreConformance::test_append_and_get_roundtrip
tests/test_my_store_conformance.py::TestMyEventStoreConformance::test_stream_isolation
tests/test_my_store_conformance.py::TestMyEventStoreConformance::test_optimistic_locking
tests/test_my_store_conformance.py::TestMyEventStoreConformance::test_empty_stream
tests/test_my_store_conformance.py::TestMyEventStoreConformance::test_event_metadata_preserved
tests/test_my_store_conformance.py::TestMyEventStoreConformance::test_event_exists_idempotency
tests/test_my_store_conformance.py::TestMyEventStoreConformance::test_expected_version_any
tests/test_my_store_conformance.py::TestMyEventStoreConformance::test_global_position_tracking
```

Confirm that list before you trust a green run -- `pytest --collect-only -q`
against your module is the fastest check. Eight ids means the suite is wired
up; zero means the class name or file name does not match pytest's discovery
patterns, and a passing run with zero tests is the most common way this setup
silently does nothing. See
[No tests are collected](#no-tests-are-collected) if the count is wrong.
