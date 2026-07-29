# FAQ

This page collects the questions that come up most often about eventsource-py:
why the library is shaped the way it is, which of several plausible options to
reach for, and what the library deliberately does *not* do.

It is an explanation document, not a task list. Each answer is short and aims to
give you the reasoning behind a design decision or a recommendation, with a
pointer to the tutorial, how-to guide, or reference page that shows the actual
mechanics. If you are looking for step-by-step instructions, start from
[the documentation index](index.md) instead — the FAQ is what you read when you
already know *how* to do something and want to know *why* it works that way, or
when you are choosing between two things that both look reasonable.

The questions are grouped roughly in the order they tend to arise: first whether
event sourcing suits your problem at all, then installation and the optional
dependency story, then the three core building blocks (events, aggregates, and
the store/bus/projection triad), then the async model and testing, and finally
operational concerns — multi-tenancy, migration, GDPR, observability — and the
project's compatibility commitments.

A few conventions used throughout:

- Everything user-facing is imported from the top-level `eventsource` package,
  so examples say `from eventsource import ...` rather than reaching into
  submodules, except where a subpackage such as `eventsource.subscriptions` is
  the documented import path.
- "Core" means the parts that work with only `pydantic` and `sqlalchemy`
  installed. Anything requiring `asyncpg`, `aiosqlite`, `redis`, `aio-pika`,
  `aiokafka`, or OpenTelemetry is called out as needing an extra.
- Answers describe eventsource-py 0.5.0 on Python 3.11+.

## Concepts

### What is event sourcing, and what does it buy me?

Event sourcing stores the *facts* that happened rather than the current state
they add up to. Instead of a row that says an order is `shipped`, you keep an
append-only stream — `OrderCreated`, `ItemAdded`, `OrderShipped` — and derive
the order's state by replaying that stream through the aggregate's `_apply`
method. In this library the stream lives behind
[`EventStore`](reference/event-store-protocol.md), whose write path is a single
`append_events` call and whose read paths are `get_events`, `read_stream`, and
`read_all`.

What you get in return:

- **A perfect history for free.** Nothing overwrites anything, so "how did this
  order end up in that state?" is answerable by reading the stream, not by
  reverse-engineering it from a mutable row.
- **Derived views you can rebuild.** Read models are projections over the
  stream. If a query view is wrong or you need a new one, you delete it and
  replay — the source of truth is untouched.
- **Temporal queries.** `get_events` takes a `from_version` and optional
  `from_timestamp`/`to_timestamp` bounds, so "what did this aggregate look like
  as of last Tuesday?" is a normal read followed by a replay, not an archaeology
  project.
- **Explicit business vocabulary.** Events are named after things the domain
  actually does, which tends to surface modelling disagreements early.

The costs are real too: every write is a new event class, every query needs a
projection, and eventual consistency between the write side and the read side
becomes something you design for rather than something you can ignore. The next
question is about when those costs are not worth paying.

### When should I *not* use event sourcing?

Event sourcing is a good trade when history and auditability are part of the
product. It is a bad trade when they are not. Reach for a plain table if:

- **The data is inherently current-state.** A user's theme preference or a cache
  of upstream data has no meaningful history; storing `ThemeChanged` events is
  ceremony with no payoff.
- **You mostly need ad-hoc queries over mutable data.** Every query shape you
  care about needs a projection to maintain. If your access patterns are
  unstable and exploratory, SQL against normalized tables is far cheaper.
- **The domain has no real invariants.** Aggregates earn their keep by
  protecting consistency boundaries. If nothing needs protecting, the aggregate
  is an empty wrapper.
- **The team cannot absorb eventual consistency.** Projections are updated after
  the write commits. If your UI must read its own write synchronously from the
  read model, you will spend your time fighting that gap.
- **Unbounded streams with no natural end.** High-frequency telemetry appended
  to one aggregate will grow without limit; snapshots help, but a time-series
  store is the better tool.

It is entirely reasonable to event-source one bounded context — orders,
payments, entitlements — and leave the rest of the system on ordinary CRUD.
Nothing in this library assumes it owns your whole database.

### How is this different from a plain audit log?

An audit log is written *alongside* the state; an event stream *is* the state.
That difference sounds academic until something goes wrong with it.

With an audit log, the state table is authoritative and the log is a
side-effect. Nothing structurally prevents a state change from being written
without its log entry — a bug, a migration script, or a manual `UPDATE` and the
two have silently diverged. You cannot detect the divergence, because there is
nothing to compare against.

With event sourcing, there is only one write path. `AggregateRoot` records
events through `_raise_event`, and the repository appends exactly those
uncommitted events; the aggregate's state is reconstructed by
`load_from_history` replaying them. There is no way to change state without
producing an event, because producing the event *is* how state changes.

Two practical consequences follow. First, the events carry the causal metadata
you would otherwise bolt onto a log — `actor_id`, `correlation_id`,
`causation_id`, `occurred_at`, plus a free-form `metadata` dict — as first-class
fields on `DomainEvent`, and `frozen=True` means nothing rewrites them after the
fact. Second, `aggregate_version` and the `expected_version` argument to
`append_events` (which raises `OptimisticLockError` on a mismatch) give you a
gap-free, strictly ordered stream per aggregate; an audit log with a missing row
looks exactly like an audit log with no activity.

The flip side: an audit log is additive and cheap to bolt onto an existing
system, whereas event sourcing is a modelling commitment. If all you need is
"who touched this record," write an audit log.

### Do I need CQRS to use this library?

No, though you will end up with a mild form of it whether or not you call it
that.

The library ships no command side of its own. There is no `Command` class, no
command bus, no handler dispatch for commands — you call methods on your
aggregate (`order.ship(tracking_number)`), and the aggregate raises events. What
the library does provide is the query side: `Projection` and
`DeclarativeProjection` consume events, and `ReadModelProjection` plus the
`eventsource.readmodels` package persist denormalized views you query with
`Query` and `Filter` — `repo.find(Query(filters=[Filter.eq("status",
"shipped")]))` — instead of replaying streams.

So the separation you get by default is "writes go through aggregates, reads go
through projections." That is the useful half of CQRS, and it falls out of event
sourcing naturally: rebuilding an aggregate to answer a list query would be
absurd, so you build a read model instead.

Full CQRS — a formal command object, a dispatcher, separately deployed read and
write services — is a further step you can take on top, and the library will not
get in your way. But adopt it because you have a reason (different scaling
profiles, different teams, a transport boundary), not because event sourcing
implies it. For small services, aggregate methods called directly from your HTTP
handlers are the right amount of structure.

You *can* also skip projections entirely for simple cases and load the aggregate
to answer a point lookup by id. That works fine, and it is what
[the aggregate tutorial](tutorials/03-first-aggregate.md) does before
[the projections tutorial](tutorials/06-projections.md) introduces the read side.

## Installation and dependencies

### Why are pydantic and sqlalchemy the only required dependencies?

Because those two are the only things the *core* abstractions cannot be written
without. `DomainEvent` is a pydantic `BaseModel` — validation, JSON round-trips,
and `frozen=True` immutability all come from pydantic, so it is not an optional
detail but the definition of what an event is. SQLAlchemy is required because
the SQL-backed stores and repositories are written against
`AsyncSession`/`async_sessionmaker` rather than against a specific driver:
`PostgreSQLEventStore` imports only `sqlalchemy` and issues `text()` statements,
and you hand it a sessionmaker built from an engine you created.

That split is the whole trick. SQLAlchemy is the *interface* to a database;
`asyncpg` and `aiosqlite` are the *drivers*, and they live in extras. A `pip
install eventsource-py` with nothing else gives you the event model, aggregates,
projections, subscriptions, and the in-memory `EventStore`, `EventBus`, and
checkpoint/DLQ/outbox repositories — a complete event-sourced application you
can build and unit-test with no external service and no container runtime.

The practical benefit is a small dependency surface for consumers who only need
part of the library. A service that uses PostgreSQL and nothing else does not
drag in a Kafka client, a Redis client, and an OpenTelemetry SDK just to import
`DomainEvent`.

### Why is redis an optional extra and not a core dependency?

Redis is one of five event bus backends, not a privileged one. `EventBus` is an
interface with in-memory, Redis Streams, RabbitMQ, and Kafka implementations
behind it, and the in-memory implementation is the one most applications start
with and every test suite uses. Making the Redis client mandatory would tax
every user for a transport most of them do not deploy.

It was a core dependency earlier in the project's life and was demoted during
the 0.5.0 cycle, which is why you may still see older material that assumes it
is present. `redis` is now installed by `eventsource-py[redis]` (or by `[all]`),
and `REDIS_AVAILABLE` tells you at runtime whether the import succeeded.

Nothing outside the Redis bus depends on it. Distributed locking uses PostgreSQL
advisory locks; checkpoints, dead letters, and the outbox have postgres, sqlite,
and in-memory backends. There is no hidden Redis requirement lurking in the
subscription machinery.

### Which extra do I install for my backend?

Extras are named after the thing they enable, and each one pulls in exactly the
client library that backend needs:

| Extra | Installs | Enables |
| --- | --- | --- |
| `postgresql` | `asyncpg` | `PostgreSQLEventStore`, PostgreSQL snapshots, outbox, advisory locks |
| `sqlite` | `aiosqlite` | `SQLiteEventStore`, `SQLiteSnapshotStore`, SQLite checkpoint/DLQ/outbox repos |
| `redis` | `redis` | `RedisEventBus` (Redis Streams) |
| `rabbitmq` | `aio-pika` | `RabbitMQEventBus` |
| `kafka` | `aiokafka` | `KafkaEventBus` |
| `kafka-schema-registry` | `aiokafka`, `confluent-kafka` | `KafkaEventBus` plus Confluent Schema Registry integration |
| `telemetry` | `opentelemetry-api`, `opentelemetry-sdk` | OpenTelemetry tracing and subscription metrics |
| `all-backends` | `postgresql` + `sqlite` | Both durable event stores, no message brokers |
| `all` | every extra above except `kafka-schema-registry` | Everything, for a kitchen-sink environment |

Two of those names cause confusion often enough to be worth stating plainly.
`all-backends` means *storage* backends only — PostgreSQL and SQLite — and is
what you want when you persist events durably but publish over the in-memory
bus or none at all. `all` is the superset and includes the brokers and
telemetry, but it resolves to the plain `kafka` extra, so a Schema Registry
setup needs `kafka-schema-registry` requested explicitly.

Extras compose the usual way, so the common shape for a production service is
one store plus one bus:

```bash
pip install "eventsource-py[postgresql,redis]"
```

The `dev`, `docs`, and `benchmark` extras exist for working on the library
itself, not for consuming it.

### What happens if I use a backend without installing its extra?

Importing `eventsource` always succeeds. Every optional client is imported
inside a `try`/`except ImportError` that sets a module-level flag —
`SQLITE_AVAILABLE`, `REDIS_AVAILABLE`, `RABBITMQ_AVAILABLE`, `KAFKA_AVAILABLE` —
rather than letting the failure escape. You can therefore write code that
branches on availability, and the library's own `__init__` uses those flags to
decide what to export.

Where the failure surfaces depends on the backend, and the two behaviors are
worth knowing apart:

- **The bus backends stay importable and fail at construction.** `RedisEventBus`
  is always exported; instantiating it without `redis` installed raises
  `RedisNotAvailableError`. Kafka and RabbitMQ mirror this with
  `KafkaNotAvailableError` and `RabbitMQNotAvailableError`, as does
  `SQLiteSnapshotStore` with `SQLiteNotAvailableError`. All four subclass
  `ImportError` and carry the install command in the message, so the traceback
  tells you what to do.
- **The SQLite store is not exported at all.** `SQLiteEventStore` and the
  SQLite checkpoint/DLQ/outbox repositories are imported inside the guarded
  block, so without `aiosqlite` they are absent from the namespace and
  `from eventsource import SQLiteEventStore` fails with a plain
  `ImportError: cannot import name`. That reads as a typo if you are not
  expecting it; check `SQLITE_AVAILABLE` before concluding the name is wrong.

PostgreSQL is a third case with no flag of its own. `PostgreSQLEventStore`
imports cleanly with only SQLAlchemy installed, because you supply the
sessionmaker; the missing driver shows up when SQLAlchemy tries to resolve
`postgresql+asyncpg://` and raises its own "Can't load plugin" error at engine
creation. Same for `sqlite+aiosqlite://`.

One wrinkle in the error messages themselves: they say
`pip install eventsource[redis]`, but the distribution on PyPI is
**`eventsource-py`**. Install `eventsource-py[redis]`. The import name is
`eventsource`, which is where the mismatch comes from.

For the full extra-by-extra breakdown, including service versions and
development setup, see [the installation reference](installation.md).
