# Tutorial Series

This is a hands-on, in-progress series that takes you from "I have heard of event
sourcing" to a running, tested, production-shaped service built with `eventsource-py`.
The roadmap below sketches a full 21-part arc across four phases; **8 tutorials are
written so far** (2, 3, 6, 7, 8, 11, 14, and 16). Start with
[Tutorial 2: First Event](02-first-event.md) -- it is the earliest one that exists and
the real entry point today, even though it is not numbered 1.

The series is a *learning* path, not a lookup table. Every tutorial is written to be
typed out and run: you write code, run it, see output, and only then read the
explanation of what happened. Nothing here asks you to trust a snippet you have not
executed. If you want task-focused recipes instead, read
[Choose an Event Store Backend](../how-to/choose-an-event-store-backend.md); if you want
the shape of an API, read the
[Event Store Protocol reference](../reference/event-store-protocol.md); if you want the
reasoning behind a design choice, read [Aggregate Styles](../explanation/aggregate-styles.md),
[The Decider Pattern](../explanation/decider-pattern.md), or the [ADRs](../adrs/index.md).

The library itself is async-first and built on pydantic v2, SQLAlchemy 2, and orjson.
Those three packages are the only required dependencies -- every backend (PostgreSQL, SQLite, Redis,
Kafka, RabbitMQ) and OpenTelemetry tracing arrives as an optional extra. The series is
sequenced to respect that: the first ten tutorials need nothing but the core install and
run entirely in-process, and Docker only appears once you reach the backend tutorials in
Phase 3.

You do not need to read all of it, written or planned. Phase 1 alone is enough to model
a domain and persist it in memory; Phase 2 adds the patterns most services actually ship
with. Phases 3 and 4 are there when you need a real database, real subscriptions, and
real operational concerns. Jump straight to a numbered tutorial in the nav sidebar if you
already know which piece you need -- the table below shows which numbers exist today.

## What You'll Build

The series has one running example: a small **ordering service**. It starts as a single
Python file you can run with `python`, and by the end it is meant to grow into a
multi-tenant, observable service backed by PostgreSQL with subscriptions, snapshots, and
an outbox. You are not building 21 disconnected toys -- each tutorial adds the next piece
to code you already have.

The domain stays deliberately small so the machinery stays visible. Orders get placed,
shipped, and cancelled. That is it. Three events -- `OrderCreated`, `OrderShipped`,
`OrderCancelled` -- are meant to carry you through the whole arc, so every new concept
lands against a model you already understand.

## Status: What's Written, What's Planned

| # | Tutorial | Status |
| --- | --- | --- |
| 1 | Introduction / setup | planned |
| 2 | [First Event](02-first-event.md) | written |
| 3 | [First Aggregate](03-first-aggregate.md) | written |
| 4-5 | Repository, optimistic concurrency | planned |
| 6 | [Projections](06-projections.md) | written |
| 7 | [Event Bus](07-event-bus.md) | written |
| 8 | [Testing](08-testing.md) | written |
| 9-10 | DLQ, checkpoints | planned |
| 11 | [PostgreSQL](11-postgresql.md) | written |
| 12-13 | SQLite, advisory locks | planned |
| 14 | [Snapshotting](14-snapshotting.md) | written |
| 15 | Outbox pattern | planned |
| 16 | [Multi-Tenancy](16-multi-tenancy.md) | written |
| 17-21 | Distributed buses, observability, sagas, live migration | planned |

Below is the roadmap for all four phases, written up front as a design target. The
sections and tutorial numbers it references describe the intended shape of the finished
series -- treat anything not linked in the table above as not yet written, not as
something broken.

**Phase 1 (Tutorials 1-5) takes you to a working domain model in memory.** You write
`DomainEvent` subclasses for the three order events and watch them auto-register into
the global `EventRegistry`; build an `OrderAggregate` on top of
`DeciderAggregate[OrderState]` with a `decide()`/`evolve()` pair driven by
`CreateOrder`, `ShipOrder`, and `CancelOrder` commands that fold events into a frozen
pydantic state model; append and read those events through the `EventStore` interface
using `InMemoryEventStore`; and wire an `AggregateRepository` in front of the store so
saving an order is one `await` and concurrent writers collide with a real
`OptimisticLockError` instead of silently overwriting each other. Nothing is installed
beyond the core package, and nothing is persisted past the end of the process.

**Phase 2 (Tutorials 6-10) adds the read side and the safety net.** You add an
order-summary projection -- first as a hand-written `Projection`, then rebuilt as a
`ReadModelProjection` over a typed read model -- so you can answer queries without
replaying an aggregate. You publish events through `InMemoryEventBus` to several
independent subscribers (a notifier, an inventory view, an audit log) and see how handler
ordering actually works. You write real tests using `eventsource.testing` -- the
harness, the assertion helpers, the BDD given/when/then style, the event builder, and the
conformance suites that any backend implementation must pass. Then you make it
survivable: route poison events into a `DLQRepository`, and record progress in a
`CheckpointRepository` so a restarted consumer resumes where it stopped instead of
replaying from zero. Still no Docker; the in-memory backends carry all of it.

**Phase 3 (Tutorials 11-15) makes the service durable and lets it run unattended.** You
swap `InMemoryEventStore` for `PostgreSQLEventStore`, apply the SQL schema, and use
PostgreSQL advisory locks to serialize work across processes. You do the same with
`SQLiteEventStore` and `SQLiteSnapshotStore` for local development and tests. You stand
up a `SubscriptionManager` that owns the lifecycle of your projections -- runners, retry
policy, health reporting, checkpointing -- so consumers keep up without you babysitting
them. You add snapshots so an order with thousands of events still loads in constant
time. And you close the gap between "committed to the database" and "published to the
bus" with the outbox pattern, using `PostgreSQLOutboxRepository`, plus the operational
checklist that goes with deploying any of this. This is the phase where Docker appears:
`docker-compose.test.yml` brings up the PostgreSQL 15 and Redis 7 services the backend
tutorials use.

**Phase 4 (Tutorials 16-21) covers the pieces real deployments need.** Tenant isolation
via `TenantDomainEvent` and the contextvar-based tenant scopes, so one store serves many
customers without leaking rows between them. A distributed event bus -- Redis, Kafka, or
RabbitMQ, whichever fits, each behind the same `EventBus` interface you already know.
OpenTelemetry traces that follow a command from HTTP request through append, publish,
and projection. And the advanced aggregate patterns: process managers, sagas, and the
live migration tooling in `eventsource.application.migration` for moving a running
system from one event store to another with dual-write and cutover.

Two things worth setting expectations on. First, these later phases are a menu, not a
queue -- you almost certainly do not need all three message buses, and picking one is
the point. Second, each phase is designed to leave you with something that runs, once
its tutorials exist: Phase 1 alone should give you a tested domain model, Phase 1 and 2
together a complete in-process event-sourced application. Nothing is meant to be left
half-wired waiting for a tutorial you never reach -- but check the status table above
before assuming a given number is there.

## Who This Series Is For

You will get the most out of this series if you are a Python developer who has written
async code before and wants to build an event-sourced service rather than read about
one. Concretely, the series assumes you:

- Are comfortable with `async`/`await`, `asyncio.run()`, and async context managers.
  Nearly every store, bus, repository, and projection method in `eventsource-py` is a
  coroutine, and the tutorials await them without stopping to explain the syntax.
- Can read and write type-annotated Python. The library ships a `py.typed` marker and is
  type-checked with mypy; the tutorials use annotations throughout because they are how
  the API communicates intent.
- Have used pydantic, or are willing to pick it up as you go. `DomainEvent` is a pydantic
  v2 `BaseModel` with `model_config = ConfigDict(frozen=True)`, so field declaration,
  validation, and immutability all follow pydantic rules.
- Run Python 3.13 or newer. That is the floor declared in `pyproject.toml`
  (`requires-python = ">=3.13"`).

You do **not** need prior event sourcing experience. [Tutorial 2](02-first-event.md)
starts from the concepts -- events as the source of truth, aggregates as consistency
boundaries, projections as derived read models -- and no later tutorial assumes you
arrived knowing CQRS or DDD vocabulary. Terms are introduced where they are first used.

You also do not need deep expertise in any particular backend. SQLAlchemy is a required
dependency, but Phases 1 and 2 never touch a database: they run against
`InMemoryEventStore` and the in-memory event bus. When PostgreSQL, SQLite, Redis, Kafka,
and RabbitMQ do appear in Phases 3 and 4, each tutorial covers the setup it needs,
including the Docker services from `docker-compose.test.yml`.

This series is a poor fit in two cases. If you already run event sourcing in production
and just need to know how a specific API behaves, skip to the
[Event Store Protocol reference](../reference/event-store-protocol.md) and the
[Choose an Event Store Backend](../how-to/choose-an-event-store-backend.md) how-to guide
-- the tutorials deliberately move slowly and repeat themselves. And if you are
evaluating whether event sourcing is the right architecture at all, read
[Aggregate Styles](../explanation/aggregate-styles.md) and the [ADRs](../adrs/index.md)
first; this series shows you *how*, and largely takes the *why* as settled.

## Prerequisites

Before [Tutorial 2](02-first-event.md), get a Python environment, install the package,
and -- if you plan to reach Phase 3 -- confirm Docker works. That is the whole list.
Everything else the series needs, it installs when it needs it.

### Python and Tooling

You need **Python 3.13 or newer**. `pyproject.toml` declares
`requires-python = ">=3.13"`. Check what you have:

```bash
python --version
```

The tutorials use [uv](https://docs.astral.sh/uv/) for environment and dependency
management, because that is what the repository itself uses. If you would rather use
`pip` and `venv`, every command translates directly -- `uv add X` becomes
`pip install X` inside an activated virtualenv -- and nothing in the library depends on
uv.

To start a fresh project for the series:

```bash
uv init ordering-service
cd ordering-service
```

If instead you cloned this repository to follow along against its source and tests, set
up the development environment with:

```bash
uv sync --all-extras
```

That installs the library plus every optional backend and the `dev` extra's tooling
(pytest 8, pytest-asyncio, pytest-cov, mypy, ruff, testcontainers, pre-commit). Note that
pytest runs with `asyncio_mode = "auto"`, so async tests in this repository need no
`@pytest.mark.asyncio` decorator.

You will also want an editor with mypy or Pyright running. The library ships a `py.typed`
marker and is type-checked under mypy `strict`, so your editor can tell you that an
aggregate's state type does not match its events *before* you run anything. The tutorials
never require a type checker, but they are much easier to follow with one.

### Installing eventsource-py

#### Core install

```bash
uv add eventsource-py
```

This pulls in exactly three runtime dependencies, all declared in `pyproject.toml`:
**pydantic** (`>=2.8.0,<3.0`), **SQLAlchemy** (`>=2.0.43,<3.0`), and **orjson**
(`>=3.10.7`). No database driver, no message broker client, no telemetry SDK. pydantic
gives `DomainEvent` its validation and frozen-model behaviour; SQLAlchemy provides the
engine and session types the database backends are built on, which is why it is required
even before you connect to a database; orjson is the fast JSON path the serialization
layer wraps directly, with no stdlib fallback.

Confirm the install worked by importing the pieces Tutorial 2 opens with:

```bash
python -c "from eventsource import DomainEvent, InMemoryEventStore, InMemoryEventBus; print('ok')"
```

Every public name comes from the top-level `eventsource` package. You will not import
from submodules such as `eventsource.adapters.memory` in this series -- the tutorials
always use the re-exported top-level path.

The core install is all you need for **Tutorials 1 through 10** -- the whole of Phase 1
and Phase 2. Those tutorials run against `InMemoryEventStore`, `InMemoryEventBus`,
`InMemorySnapshotStore`, and the in-memory checkpoint, DLQ, and outbox repositories
(`InMemoryCheckpointRepository`, `InMemoryDLQRepository`, `InMemoryOutboxRepository`), so
they execute in-process with nothing listening on a port and nothing to clean up
afterwards. The trade-off is that those backends hold everything in Python objects:
restart the process and the event store is empty again. That is fine -- and deliberate --
until Phase 3, where durability becomes the point.

If you are working from a clone of the repository rather than a fresh project, `uv sync`
with no extras gives you the same core-only environment.

#### Optional extras

Every backend and the tracing integration ship as an extra, declared under
`[project.optional-dependencies]` in `pyproject.toml`. Install only what the tutorial in
front of you actually asks for -- there is no penalty for adding one later.

| Extra | Installs | First needed in |
| --- | --- | --- |
| `postgresql` | `asyncpg>=0.30` | Tutorial 11 -- PostgreSQL |
| `sqlite` | `aiosqlite>=0.19` | Tutorial 12 -- SQLite |
| `redis` | `redis>=8.0,<9.0` | Tutorial 17 -- Redis |
| `kafka` | `aiokafka>=0.12,<1.0` | Tutorial 18 -- Kafka |
| `rabbitmq` | `aio-pika>=9.0` | Tutorial 19 -- RabbitMQ |
| `telemetry` | `opentelemetry-api`, `opentelemetry-sdk` (both `>=1.16.0,<2.0`) | Tutorial 20 -- Observability |

Combine them in one command:

```bash
uv add "eventsource-py[postgresql,sqlite]"
```

Three bundles save you some typing. `all-backends` is `postgresql` + `sqlite` -- a
reasonable target if you intend to work through all of Phase 3. `all` pulls in every
extra at once, which is what `uv sync --all-extras` gives you in a clone of the
repository. And `kafka-schema-registry` adds `confluent-kafka` on top of `aiokafka`;
Tutorial 18 mentions it but does not require it.

Two names catch people out. The extra is spelled `postgresql`, not `postgres` -- but the
pytest marker for those tests *is* `postgres` (`uv run pytest -m postgres`). And the
OpenTelemetry extra is `telemetry`, even though the code lives in
`eventsource.observability`.

What happens if you skip an extra depends on which one:

- **Message buses fail loudly at construction.** Each bus module sets an availability
  flag at import time -- `REDIS_AVAILABLE`, `RABBITMQ_AVAILABLE`, `KAFKA_AVAILABLE`, all
  exported from `eventsource` -- and its constructor checks the flag first. Building a
  `KafkaEventBus` without `aiokafka` raises `KafkaNotAvailableError`; Redis and RabbitMQ
  raise `RedisNotAvailableError` and `RabbitMQNotAvailableError` the same way. You get a
  named error, not a traceback from somewhere inside the library.
- **SQLite fails at construction, not import.** `SQLiteEventStore` is always
  re-exported from `eventsource`, so `from eventsource import SQLiteEventStore`
  succeeds either way; `AIOSQLITE_AVAILABLE` records
  whether `aiosqlite` actually imported. Without the extra, *constructing* the
  store raises a plain `ImportError` at `__init__` time -- so if a Tutorial 12
  example fails when you instantiate the store, that is why. (`SQLiteOutboxRepository`
  is the one name that really does disappear from the namespace without the extra.)
- **PostgreSQL fails at connect time.** The PostgreSQL store is built on SQLAlchemy's
  async engine, so the missing piece is the driver in your URL:
  `create_async_engine("postgresql+asyncpg://...")` cannot resolve `asyncpg` and
  SQLAlchemy raises before any query runs.
- **Tracing degrades silently, by design.** Without the `telemetry` extra `OTEL_AVAILABLE`
  is `False` and the library hands out a `NullTracer`, so instrumented code paths keep
  working and simply emit no spans. Nothing breaks -- you just see no traces, which is
  the one failure mode here you have to notice yourself.

Nothing in this table is needed before Tutorial 11. If you are starting the series today,
skip ahead and install extras when a tutorial tells you to.

### Docker services for the backend tutorials

Phase 3 onward needs real services. The repository's `docker-compose.test.yml` defines
the two the tutorials use:

- **PostgreSQL 15** -- database `eventsource_test`, user `test`, password `test`, published
  on host port **5433** (override with `POSTGRES_PORT`).
- **Redis 7** -- published on host port **6380** (override with `REDIS_PORT`).

Both ports are deliberately non-default so the test services do not collide with a
PostgreSQL or Redis you already run locally.

```bash
docker compose -f docker-compose.test.yml up -d
```

Both services declare health checks, so give them a few seconds before connecting; when
you are done:

```bash
docker compose -f docker-compose.test.yml down -v
```

The `-v` drops the `postgres_test_data` and `redis_test_data` volumes, which is what you
want between tutorials -- each backend tutorial assumes an empty event store.

Kafka and RabbitMQ are **not** in this compose file. Tutorials 18 and 19 cover standing
those brokers up themselves, and both tutorials are optional: you only need whichever
message bus you actually plan to deploy.

You need nothing from this section until Tutorial 11. If Docker is unavailable to you,
Phases 1 and 2 still work end to end, and Tutorial 12 (SQLite) gives you durable
persistence with no service to run at all.
