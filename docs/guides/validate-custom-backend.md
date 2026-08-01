# Validate a Custom Backend with the Conformance Suites

This guide shows you how to check that your own event store adapter or `EventBus`
implementation actually honors the contract the rest of the library depends on.

The event store side is validated through `eventsource.testing.conformance_ports`
-- five narrow, per-port ABC test mixins (`AppenderConformance`,
`StreamReaderConformance`, `EventLookupConformance`, `GlobalFeedConformance`,
`CategoryQueryConformance`) that mirror the five store capability protocols in
`eventsource.ports.store` (`EventAppender`, `StreamReader`, `EventLookup`,
`GlobalEventFeed`, `CategoryQuery`), plus `SnapshotConformance` for snapshot
stores. Each suite is abstract on exactly one thing -- a `store` pytest fixture
that yields a fresh adapter instance -- and supplies a body of ready-written
async test methods that exercise its port's contract. A backend author
subclasses whichever suites the adapter honors; an adapter that only appends
and reads streams (no global feed, no category queries) runs
`AppenderConformance` and `StreamReaderConformance` and stops there. The bus
side keeps its existing suite, `EventBusConformanceSuite` from
`eventsource.testing.conformance`.

By the end you will have pytest classes that run the relevant inherited suites
against your backend, grounded in the same pattern the library uses to certify
its own `InMemoryEventStore` adapter
(`tests/unit/adapters/test_memory_conformance.py`).

## When to use this guide

Use this guide when you have written -- or are about to write -- your own
event store adapter (structurally conforming to one or more of the protocols
in `eventsource.ports.store`) or your own `EventBus` (from
`eventsource.ports.bus`) and you want confidence that it behaves the way
aggregates, repositories, projections, and subscriptions already assume.
Typical cases:

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
must be true of *every* backend that honors a given port: that `append` and
`read_stream` roundtrip, that streams stay isolated from one another, that a
wrong `ExpectedVersion` raises `OptimisticLockError` while
`ExpectedVersion.any_()` does not, that `event_exists` supports idempotent
writes, that the global feed's positions advance monotonically, that
`read_category` only returns events for streams in that category, and -- on
the bus side -- that publish reaches subscribers, that multiple subscribers
each receive an event, that `unsubscribe` stops delivery for both typed and
all-event subscriptions, and that one failing handler does not prevent others
from running.

They deliberately say nothing about behavior that is yours to define:
connection pooling and retries, schema and index layout, transaction
boundaries you expose to callers, delivery ordering guarantees stronger than
the contract, at-least-once versus exactly-once semantics, or performance.
Keep writing backend-specific tests for those -- this guide shows you how to
put them in the same class as the inherited ones.

Skip this guide if you are only *using* a built-in backend. The shipped
PostgreSQL, SQLite, and in-memory stores and the in-memory, Redis, RabbitMQ,
and Kafka buses are already covered by the library's own test suite; you do not
need to re-validate them.

## Prerequisites

Before you start, you need:

- **Python 3.11 or newer.** The library declares `requires-python = ">=3.11"`.
- **`eventsource-py` installed**, with whatever optional extras your backend
  driver needs (`postgresql`, `sqlite`, `redis`, `rabbitmq`, `kafka`,
  `telemetry`, or `all`). `eventsource.testing.conformance_ports` is
  sqlalchemy-free -- it only imports from `eventsource.ports`,
  `eventsource.domain`, `eventsource.events`, `eventsource.domain.exceptions`, and
  pytest/stdlib -- so it pulls in nothing beyond the core dependencies.
- **pytest and pytest-asyncio.** The suites are plain classes of `async def`
  test methods; they do not bundle a runner or an event loop. Both ship in the
  library's `dev` extra (`pytest>=8.0`, `pytest-asyncio>=0.23`), or you can
  install them directly into your own project. Step 1 covers the
  `asyncio_mode = "auto"` setting that makes pytest actually await those
  methods.
- **A backend implementation to test**: a class that structurally implements
  one or more of the protocols in `eventsource.ports.store`
  (`EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`,
  `CategoryQuery`), or an `EventBus` subclass from `eventsource.ports.bus`.
  It does not have to be finished -- running the suite against a partial
  implementation is a reasonable way to drive the work -- but it must be
  importable and constructible from your test module.
- **Any external resources your backend needs**, running and reachable from the
  test process: a database, broker, or container. The suite's `store` (or
  `create_bus()`) fixture is responsible for producing a usable instance; the
  suite does not start infrastructure for you. See
  [Backends that need external resources](#backends-that-need-external-resources-fixtures-docker-markers)
  for wiring that up with fixtures and markers.

You should also be comfortable with the basics of the contract you are
implementing -- `append`, `read_stream`, `ExpectedVersion`, and
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

Now configure pytest-asyncio. Every test method on the `conformance_ports`
suites and on `EventBusConformanceSuite` is an
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
keep function scope and construct a fresh client per test inside your
`store` fixture (or `create_bus()` for the bus suite).

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

## Step 2: Pick the port suites your adapter honors

Unlike the bus suite, the store suites need no test-event class from you --
`eventsource.testing.conformance_ports._fixtures` already ships a minimal,
registered `ConformanceEvent` (`aggregate_type = "Conformance"`) plus two
factories, `make_stream()` and `make_event(aggregate_id)`, that every suite's
test methods call internally. You do not import or use `_fixtures` directly;
it is wired in by the suites themselves. All you supply is the adapter.

Each of the five store suites corresponds to exactly one protocol in
`eventsource.ports.store`:

| Suite | Protocol | Exercises |
| --- | --- | --- |
| `AppenderConformance` | `EventAppender` | `append()`, every `ExpectedVersion` kind, duplicate-`event_id` handling, atomic batches |
| `StreamReaderConformance` | `StreamReader` | `read_stream()` ordering, `StreamReadOptions` (direction, version range, limit), `get_stream_version()`, stream isolation |
| `EventLookupConformance` | `EventLookup` | `event_exists()` before/after append, unknown ids |
| `GlobalFeedConformance` | `GlobalEventFeed` | `read_all()` ordering, position resumption, `current_position()`, tenant filtering |
| `CategoryQueryConformance` | `CategoryQuery` | `read_category()` scoping, timestamp/tenant filters, limits |

Subclass whichever suites your adapter's capabilities match -- a backend that
only appends and reads single streams (no global feed, no category index)
subclasses `AppenderConformance` and `StreamReaderConformance` and stops
there; `eventsource.adapters.memory.InMemoryEventStore`, which implements all
five ports, runs all five (see
`tests/unit/adapters/test_memory_conformance.py` for the reference wiring).
Snapshot stores use the separate `SnapshotConformance` suite the same way.

## Step 3: Wire the `store` fixture for each suite

Every suite is abstract on exactly one thing: an async pytest `store`
fixture that yields a fresh adapter instance. There are no `create_store()` /
`create_test_event()` factory methods to implement -- the fixture *is* the
extension point.

### The reference pattern

This is the whole of `tests/unit/adapters/test_memory_conformance.py`:

```python
from collections.abc import AsyncIterator

import pytest

from eventsource.adapters.memory import InMemoryEventStore, InMemorySnapshotStore
from eventsource.testing.conformance_ports import (
    AppenderConformance,
    CategoryQueryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    SnapshotConformance,
    StreamReaderConformance,
)


class TestMemoryAppender(AppenderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemoryStreamReader(StreamReaderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemoryEventLookup(EventLookupConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemoryGlobalFeed(GlobalFeedConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemoryCategoryQuery(CategoryQueryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemorySnapshotStore(SnapshotConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemorySnapshotStore]:
        yield InMemorySnapshotStore()
```

Unlike the retired store suite, **the collectable class *is* the suite
subclass** -- there is no separate non-`Test`-named intermediate class,
because there are no factory methods to keep out of pytest's collection.
Each `Test*` class subclasses exactly one port suite and supplies exactly one
`store` fixture. Pytest collects each independently, so a fixture problem
specific to (say) the feed suite fails only `TestMemoryGlobalFeed`, not the
whole module.

Adapt this for your own backend by swapping `InMemoryEventStore()` for
your adapter:

```python
class MyStoreAppenderConformance(AppenderConformance):
    @pytest.fixture
    async def store(self, tmp_path):
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
        await create_schema(engine)
        yield MyEventStore(engine=engine)
        await engine.dispose()
```

Two properties matter, and every suite depends on both:

- **It must yield a fresh, empty store.** `GlobalFeedConformance` opens with
  `assert await store.current_position() is None`, which only holds on a
  store with no events in it. If your fixture hands back a store backed by a
  database that already holds rows from an earlier test, that assertion fails
  first -- and the failure tells you nothing about your position tracking.
- **It is an async fixture that can tear down.** Unlike the retired
  `create_store()` (a synchronous method), the `store` fixture is `async def`
  and a plain pytest fixture -- `yield` the instance and put teardown (closing
  a connection, disposing an engine) after the `yield`, exactly like any other
  pytest async fixture. A fresh instance per test method is what gives you
  per-test isolation; point it at a fresh database, schema, or key prefix each
  time -- `tmp_path` above, a truncate in setup, or a uniquely named schema
  per test all work. See
  [Backends that need external resources](#backends-that-need-external-resources-fixtures-docker-markers)
  for the Docker-backed variants.

### Combining suites without duplicating the fixture

If several port suites share the same fixture body, factor it into a mixin
rather than repeating it, or -- if your adapter implements all five ports the
way the shipped backends do -- inherit from all five suites on one class and
provide the `store` fixture once:

```python
class TestMyEventStoreConformance(
    AppenderConformance,
    StreamReaderConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    CategoryQueryConformance,
):
    @pytest.fixture
    async def store(self, tmp_path):
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
        await create_schema(engine)
        yield MyEventStore(engine=engine)
        await engine.dispose()
```

MRO makes this safe because none of the five suites overrides a method
another one defines -- each contributes only its own `test_*` methods plus
the abstract `store` fixture, so combining them is pure addition. Splitting
into one class per suite (as the memory adapter's own tests do) is still
worth it if your fixtures differ per capability -- for instance, a
feed-less partitioned adapter that skips `GlobalFeedConformance` entirely.

### Confirm what got collected

```bash
pytest --collect-only -q tests/test_my_store_conformance.py
```

```
tests/test_my_store_conformance.py::TestMyStoreAppenderConformance::test_no_stream_append_to_fresh_stream_succeeds
tests/test_my_store_conformance.py::TestMyStoreAppenderConformance::test_no_stream_append_to_existing_stream_conflicts
tests/test_my_store_conformance.py::TestMyStoreAppenderConformance::test_exact_append_matching_version_succeeds
tests/test_my_store_conformance.py::TestMyStoreAppenderConformance::test_exact_append_mismatched_version_conflicts
tests/test_my_store_conformance.py::TestMyStoreAppenderConformance::test_duplicate_event_id_raises_and_batch_is_atomic
...
```

A non-empty, suite-specific list means the fixture is wired up; a class with
zero collected tests means the class name does not match pytest's `Test*`
discovery pattern, and a passing run with zero tests is the most common way
this setup silently does nothing. See
[No tests are collected](#no-tests-are-collected) if the count is wrong.

For an adapter that also has a snapshot store, add a `TestMySnapshotStore`
subclassing `SnapshotConformance` the same way -- it exercises `save_snapshot`,
`get_snapshot`, and deletion the same way the memory adapter's
`TestMemorySnapshotStore` does above.
