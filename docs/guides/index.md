# Guides

Task-oriented instructions for getting specific jobs done with `eventsource-py`.

Each guide assumes you already have a working install (`pip install eventsource-py`,
or `uv sync --all-extras` for development) and that you are comfortable with the
basics covered in [Getting Started](../getting-started.md) and the
[tutorial series](../tutorials/index.md): defining a `DomainEvent`, building an
`AggregateRoot`, and persisting through an `EventStore`.

Guides pick up where those leave off. They answer questions of the form *"how do I
do X?"* -- swap the in-memory store for PostgreSQL, add snapshots to a long-lived
aggregate, run subscriptions with checkpoints and a dead-letter queue, scope events
to a tenant, wire up OpenTelemetry, or validate a backend you wrote yourself. They
are written to be read while you work, not read end to end.

Because the library is async-first, every guide's code is written against `async`
APIs. If you are calling from synchronous code, read the guide for the feature you
need and then apply `SyncEventStoreAdapter` (see `eventsource.adapters.sync`) at the boundary.

Use the [guide index](#guide-index) below to find the page for your task, or
[Choosing a backend](#choosing-a-backend) if you are still deciding which
store, bus, or snapshot implementation to run in production.

## When to use a guide

Reach for a guide when you have a concrete goal and a codebase to apply it to.
Guides are the right page when you can phrase your need as a task:

- **"I need to switch backends."** Move from `InMemoryEventStore` to PostgreSQL or
  SQLite, or from `InMemoryEventBus` to Redis, RabbitMQ, or Kafka, without changing
  your aggregates.
- **"I need to add a capability to an app that already works."** Snapshots for a
  long-lived aggregate, projections and read models, distributed locks, GDPR
  erasure, or OpenTelemetry spans.
- **"I need to run this in production."** Checkpointed subscriptions, retry policy,
  a dead-letter queue, flow control, schema migrations, and live event store
  migration with dual-write and cutover.
- **"I need to make it safe to change."** Test harnesses, BDD helpers, and the
  per-port conformance suites in `eventsource.testing.conformance_ports`
  for validating a backend you wrote yourself.
- **"I need to call async code from a sync context."** Apply
  `SyncEventStoreAdapter` at the boundary.

Reach for something else when:

- **You are learning the library for the first time.** Start with
  [Getting Started](../getting-started.md), then the
  [tutorials](../tutorials/index.md). Guides assume you already know what a
  `DomainEvent` and an `AggregateRoot` are and skip the narration.
- **You need an exact signature, argument, or exception.** Go to the
  [reference](../reference/event-store-protocol.md) and the
  [API docs](../api/events.md). Guides show one working path, not the full surface.
- **You are deciding whether a design is right, not how to implement it.** The
  [explanation pages](../explanation/aggregate-styles.md) cover the reasoning
  behind aggregate styles, schema design, and backend type handling; the
  [ADRs](../adrs/index.md) record decisions already made.
- **You hit an error and want to know what it means.** Try the [FAQ](../faq.md)
  and the [error handling guide](error-handling.md) before reading a topic guide
  end to end.

A guide gives you one supported path that works, not an exhaustive tour of the
options. Where a real choice exists -- which store, which bus, which snapshot
policy -- the guide names the trade-off and links to the explanation page rather
than relitigating it inline.

## How guides differ from tutorials, reference, and explanation

The documentation is split into four kinds of page. Each answers a different
question, and mixing them is what makes docs hard to use, so guides deliberately
leave three jobs to the other three sections.

| Section | Answers | Shape | Start here when |
| --- | --- | --- | --- |
| [Tutorials](../tutorials/index.md) | "Teach me the library." | A numbered lesson you follow start to finish, building one example app | You have never used `eventsource-py` |
| **Guides** (this section) | "How do I do X?" | One task, one supported path, applied to *your* code | You have a working app and a specific job |
| [Reference](../reference/event-store-protocol.md), [API](../api/events.md) | "What exactly does this do?" | Exhaustive descriptions of protocols, signatures, and exceptions | You need the precise contract |
| [Explanation](../explanation/aggregate-styles.md), [ADRs](../adrs/index.md) | "Why is it like this?" | Discussion of design, trade-offs, and decisions already taken | You are evaluating an approach |

**Guides versus tutorials.** The
[tutorial series](../tutorials/index.md) is a curriculum: it runs from
[your first event](../tutorials/02-first-event.md) through
[aggregates](../tutorials/03-first-aggregate.md),
[projections](../tutorials/06-projections.md), the
[event bus](../tutorials/07-event-bus.md),
[testing](../tutorials/08-testing.md),
[PostgreSQL](../tutorials/11-postgresql.md),
[snapshotting](../tutorials/14-snapshotting.md), and
[multi-tenancy](../tutorials/16-multi-tenancy.md), in that order, and each lesson
depends on the previous one. Guides have no order and no shared example. The
[snapshotting guide](snapshotting.md) does not build on the
[event bus guide](event-bus.md); you read whichever one matches today's task. Where
a topic has both a tutorial and a guide -- snapshotting, the event bus,
multi-tenancy, PostgreSQL -- the tutorial teaches the concept on a toy domain and
the guide shows how to apply it to a domain you already have.

**Guides versus reference.** A guide shows one call path that works and names the
arguments that matter for the task. It does not enumerate every parameter, every
overload, or every exception a method can raise. When you need the full contract --
what `append()` promises about ordering, what raises `OptimisticLockError`, what
a custom backend must implement -- read the
[EventStore protocol reference](../reference/event-store-protocol.md) and the
generated [API docs](../api/events.md). If a guide's snippet and the reference ever
disagree, the reference and the source are correct.

**Guides versus explanation.** Guides state trade-offs but do not argue them. The
[snapshotting guide](snapshotting.md) tells you how to configure a snapshot policy;
[ADR 0017](../adrs/0017-snapshot-strategy-pattern.md) explains why the strategy
pattern was chosen. The [multi-tenancy guide](multi-tenant.md) shows how to scope
events to a tenant;
[the tenant isolation ADR](../adrs/0018-tenant-isolation-model.md) explains the
isolation model behind it. Likewise,
[aggregate styles](../explanation/aggregate-styles.md),
[schema design](../explanation/schema-design.md), and
[SQL backend type handling](../explanation/sql-backend-type-handling.md) cover
reasoning that guides only reference in passing.

**Guides versus the FAQ.** The [FAQ](../faq.md) answers short, self-contained
questions -- often "is this supported?" or "why am I seeing this?" -- without
walking you through a change. If the answer needs more than a paragraph and a
snippet, it lives in a guide instead.

## Guide index

Every page below is a standalone task. The groupings are for browsing only --
nothing here has to be read in order.

### Aggregates and repositories

| Guide | Use it to |
| --- | --- |
| [Wire an AggregateRepository](repository-pattern.md) | Put `AggregateRepository` between your aggregates and the event store: declare `aggregate_type` on the aggregate class, construct the repository, and load and save through it |

`AggregateRepository` is the seam where the plumbing lives, so several other tasks
start here rather than in your aggregate:

- **Loading and creating.** `load()` reconstitutes from the event stream and raises
  `AggregateNotFoundError` when the stream is empty; `load_or_create()` returns a
  fresh instance instead. `exists()` and `get_version()` answer those questions
  without materializing the aggregate.
- **Concurrency.** `save()` derives `expected_version` from the aggregate's own
  version minus its uncommitted events, so you do not pass it yourself -- a
  concurrent writer surfaces as `OptimisticLockError` from the store. See
  [error handling](error-handling.md) for what to retry.
- **Snapshots.** Snapshotting is configured on the repository (`snapshot_store`,
  `snapshot_threshold`, and `snapshot_mode` of `"sync"`, `"background"`, or
  `"manual"`), not on the aggregate. Start with [snapshotting](snapshotting.md).
- **Publishing.** Pass an `event_publisher` and saved events are published after a
  successful append; see [event bus](event-bus.md).
- **Calling from sync code.** Wrap the store with `SyncEventStoreAdapter` at the
  boundary -- [use the event store from synchronous code](sync-usage.md).

For the design question behind these pages -- imperative `AggregateRoot` versus
`DeclarativeAggregate` with `@handles` -- see
[aggregate styles](../explanation/aggregate-styles.md).

### Event stores

| Guide | Use it to |
| --- | --- |
| [Set up the database schema](database-schema.md) | Create the tables the PostgreSQL and SQLite backends expect -- neither backend creates its own |
| [Use the event store from synchronous code](sync-usage.md) | Call an async `EventStore` from Celery tasks, Django views and management commands, RQ workers, notebooks, and scripts, via `SyncEventStoreAdapter` |
| [Live tenant store migration](live-migration.md) | Move a tenant's events between stores with dual-write, sync-lag tracking, and cutover, without downtime |

Still choosing? See
[choose an event store backend](../how-to/choose-an-event-store-backend.md) and
[Choosing a backend](#choosing-a-backend) below.

### Snapshots

| Guide | Use it to |
| --- | --- |
| [Snapshotting](snapshotting.md) | Stop replaying full streams on load: pick a snapshot store, choose a policy, and keep snapshots valid as your aggregate changes |

The strategy-pattern design is argued in
[ADR 0017: snapshot strategy pattern](../adrs/0017-snapshot-strategy-pattern.md).

### Event bus and messaging backends

| Guide | Use it to |
| --- | --- |
| [Event bus](event-bus.md) | Publish events and route them to subscribers; swap `InMemoryEventBus` for Redis, RabbitMQ, or Kafka without touching your aggregates |

Delivery guarantees differ per backend -- read
[ADR 0007: event bus delivery semantics](../adrs/0007-event-bus-delivery-semantics.md)
before you assume exactly-once.

### Projections and read models

| Guide | Use it to |
| --- | --- |
| [Work with read models](read-models.md) | Build a denormalized, directly persisted view of aggregate state and keep it current |

Projections are driven by subscriptions; once your projection works, continue with
the subscription guides below.

### Subscriptions and checkpoints

| Guide | Use it to |
| --- | --- |
| [Subscription manager](subscriptions.md) | Run projections off the store and bus: catch-up replay, gapless transition to live events, checkpoints, retries, circuit breaker, DLQ, health probes, graceful shutdown |
| [Coordinate multiple subscription instances](subscription-coordination.md) | Run the same subscription workload on several instances without double-processing |
| [Coordinate work with distributed locks](distributed-locks.md) | Serialize an operation across instances using PostgreSQL advisory locks |

Background: [ADR 0009: multi-instance subscription coordination](../adrs/0009-multi-instance-subscription-coordination.md)
and [ADR 0023: PostgreSQL advisory locks](../adrs/0023-postgresql-advisory-locks.md).

### Multi-tenancy

| Guide | Use it to |
| --- | --- |
| [Multi-tenancy](multi-tenant.md) | Bind a tenant to the current context, scope events and queries to it, and keep tenant data isolated |

The isolation model is explained in
[ADR 0018: tenant isolation model](../adrs/0018-tenant-isolation-model.md).

### Observability

| Guide | Use it to |
| --- | --- |
| [Wiring OpenTelemetry tracing](observability.md) | Add spans to stores, buses, snapshot stores, and subscriptions -- and understand what is emitted when the optional dependency is absent |

Tracing is a no-op unless you opt in; see
[ADR 0016: optional tracing, no-op by default](../adrs/0016-optional-tracing-no-op-by-default.md).

### Error handling, retries, and the DLQ

| Guide | Use it to |
| --- | --- |
| [Error handling](error-handling.md) | Decide which exceptions to retry, which mean a bug, and which should surface to the caller |
| [Repository operations](repository-operations.md) | Operate the outbox, dead letter queue, and checkpoint repositories: drain, inspect, replay, resolve |

### Testing and conformance suites

| Guide | Use it to |
| --- | --- |
| [Validate a custom backend with the conformance suites](validate-custom-backend.md) | Run the per-port suites from `eventsource.testing.conformance_ports` against a backend you wrote |

For testing your own domain code rather than a backend, the helpers in
`eventsource.testing` -- assertions, BDD helpers, the builder, and the harness --
are introduced in the [testing tutorial](../tutorials/08-testing.md).

### Production deployment

There is no single production guide. Going to production means doing several of
the tasks above, in roughly this order:

1. [Set up the database schema](database-schema.md) and apply migrations as part
   of your deploy.
2. Pick durable backends -- see [Choosing a backend](#choosing-a-backend).
3. Add [snapshots](snapshotting.md) to any aggregate whose stream grows without
   bound.
4. Run projections under the [subscription manager](subscriptions.md) with
   checkpoints, retries, and a DLQ, and
   [coordinate instances](subscription-coordination.md) if you run more than one.
5. Wire [observability](observability.md) and settle your
   [error handling](error-handling.md) and
   [DLQ operations](repository-operations.md) before you need them.

If you are moving an existing deployment between stores, do it with
[live tenant store migration](live-migration.md) rather than a stop-the-world copy.

### Event stores

| Guide | Use it to |
| --- | --- |
| [Set up the database schema](database-schema.md) | Create the tables the SQL backends read and write, from the SQL bundled in `eventsource.adapters.sql.schemas` -- by CLI, from Python, or as an Alembic revision |
| [Choose an event store backend](../how-to/choose-an-event-store-backend.md) | Pick between `InMemoryEventStore`, `SQLiteEventStore`, and `PostgreSQLEventStore`, and wire the one you picked |
| [Use the event store from synchronous code](sync-usage.md) | Call an async `EventStore` from Celery tasks, Django views and management commands, RQ workers, notebooks, and scripts, via `SyncEventStoreAdapter` |
| [Live tenant store migration](live-migration.md) | Move one tenant's events between stores with bulk copy, dual-write, sync-lag tracking, and a gated cutover, without downtime |

All three backends live under `eventsource.adapters` (`adapters.memory`,
`adapters.sqlite`, `adapters.postgresql`) and implement the same store ports
(`eventsource.ports.store`), so switching is an infrastructure change: your
aggregates, repositories, and projections do not move. What does change between
them is setup and lifecycle, and that is where these guides spend their time:

- **Schema.** `PostgreSQLEventStore` does not create tables by default -- apply
  the bundled SQL before it runs ([set up the database schema](database-schema.md)),
  or pass `create_schema=True` for tests and local dev. `SQLiteEventStore`
  self-initializes lazily on first use -- no `initialize()` call or `async with`
  needed. `InMemoryEventStore` has no schema at all.
- **Construction.** `PostgreSQLEventStore(engine, event_registry=None, *,
  store_id=None, create_schema=False, outbox_enabled=False)` takes an
  `AsyncEngine` you own; `SQLiteEventStore(database, event_registry=None, *,
  store_id=None, wal_mode=True, busy_timeout=5000)` takes a file path or
  `":memory:"`; `InMemoryEventStore(store_id="memory", *, event_registry=None)`
  takes nothing required. Only `PostgreSQLEventStore` supports the transactional
  outbox (`outbox_enabled=True`).
- **Optional extras.** `SQLiteEventStore` needs the `sqlite` extra (aiosqlite)
  installed; PostgreSQL needs the `postgresql` extra (asyncpg). See
  [installation](../installation.md).
- **Deserialization.** Every SQL backend rebuilds events through an
  `EventRegistry`, and `DomainEvent` subclasses only register when their module is
  imported -- so import your event modules at startup or events will not come back
  out.

For the reading side -- `read_stream`, `read_all`, `read_category`,
`StreamReadOptions`/`FeedReadOptions`/`CategoryReadOptions`, and
what each backend guarantees about global ordering -- use the
[EventStore protocol reference](../reference/event-store-protocol.md); note that
`read_all()` is opt-in and raises `NotImplementedError` on backends that do not
implement it. If you wrote a backend of your own, prove it against the shared
suites in
[validate a custom backend](validate-custom-backend.md).

Background reading: [schema design](../explanation/schema-design.md) and
[SQL backend type handling](../explanation/sql-backend-type-handling.md).

### Snapshots

| Guide | Use it to |
| --- | --- |
| [Snapshotting](snapshotting.md) | Stop replaying whole streams on load: implement the two aggregate hooks, pick a snapshot store, choose a mode and threshold, and keep snapshots valid as your state shape changes |

Snapshots are a read optimization and nothing else. The event stream stays the
system of record, and a snapshot that is missing, unreadable, or written under an
incompatible schema is discarded so the aggregate replays in full. That design has
a consequence worth knowing before you start: snapshot problems surface as latency,
not as exceptions -- `SnapshotDeserializationError` and `SnapshotSchemaVersionError`
are handled internally on the load path -- so the guide spends real space on
verifying that snapshots are actually being used.

The work splits three ways, and the guide follows that order:

- **On the aggregate.** Implement `_serialize_state()` and
  `_restore_from_snapshot()`, and set the `schema_version` class attribute (default
  `1`). Bumping `schema_version` is how you invalidate every snapshot written under
  an older state shape.
- **On the store.** `InMemorySnapshotStore` is re-exported from the top-level
  `eventsource` package and comes from `eventsource.adapters.memory`;
  `SQLiteSnapshotStore` and `PostgreSQLSnapshotStore` are imported from
  `eventsource.adapters.sqlite` / `eventsource.adapters.postgresql` respectively.
  All three implement the same `SnapshotStore` interface (`eventsource.ports.snapshots`).
  Neither SQL store creates its own table -- apply
  the bundled `snapshots` schema first (`get_schema("snapshots")`, or the sqlite
  variant; see [set up the database schema](database-schema.md)). `SQLiteSnapshotStore`
  needs the `sqlite` extra and raises `SQLiteNotAvailableError` without it.
- **On the repository.** Snapshotting is configured where the plumbing lives, via
  `snapshot_store`, `snapshot_threshold`, and `snapshot_mode` -- `"sync"` writes
  before `save()` returns, `"background"` hands the write to a fire-and-forget task
  (`await_pending_snapshots()` makes that deterministic in tests), and `"manual"`
  reads snapshots but only writes when you call `create_snapshot()`. `create_snapshot()`
  is the one path that raises rather than degrading quietly, which is what makes it
  the right tool for milestone snapshots.

Reach for snapshots when a small number of hot, long-lived aggregates dominate load
latency, and skip them when streams are naturally short or when the long stream is
really a sign that the aggregate boundary is too wide -- resizing the aggregate is
the fix there, and a snapshot only hides it.

The strategy-pattern design behind the three modes is argued in
[ADR 0017: snapshot strategy pattern](../adrs/0017-snapshot-strategy-pattern.md);
[tutorial 14](../tutorials/14-snapshotting.md) teaches the same feature on a toy
domain if you would rather learn it than apply it.

### Event bus and messaging backends

| Guide | Use it to |
| --- | --- |
| [Event bus](event-bus.md) | Publish events to the code that reacts to them: register handlers three ways, drive the distributed backends' connect/consume lifecycle, and swap `InMemoryEventBus` for Redis, RabbitMQ, or Kafka without touching your aggregates |

The event store records what happened; the bus is what gets that record to
projections, read models, integrations, and audit logs. All four
implementations across `eventsource.adapters.{memory,redis,rabbitmq,kafka}`
satisfy the same `eventsource.ports.bus.EventBus` ABC -- the six
methods `publish()`, `subscribe()`, `unsubscribe()`, `subscribe_all()`,
`subscribe_to_all_events()`, and `unsubscribe_from_all_events()` -- so your
publishing and subscribing code is backend-independent. What changes when you
switch is construction, the connection lifecycle, and the delivery guarantees.
The guide is organized around those three:

- **Handlers.** A handler is either an object with `handle(event)` or a plain
  callable (`EventHandlerFunc`), sync or async; the bus adapts both. Register
  one type with `subscribe()`, a self-describing subscriber with
  `subscribe_all()` (it reads `subscribed_to()`), or every event with
  `subscribe_to_all_events()` for cross-cutting concerns like audit logging.
- **Lifecycle.** `InMemoryEventBus()` takes nothing and needs no setup. The
  distributed backends take a config dataclass -- `RedisEventBusConfig`,
  `RabbitMQEventBusConfig`, `KafkaEventBusConfig` -- and run
  `connect()` -> `start_consuming()` -> `stop_consuming()` -> `disconnect()`,
  with `shutdown(timeout=...)` for a graceful stop.
- **Optional extras.** Redis, RabbitMQ, and Kafka need the `redis`,
  `rabbitmq`, and `kafka` extras. Each module exports a `*_AVAILABLE` flag and
  raises `RedisNotAvailableError` / `RabbitMQNotAvailableError` /
  `KafkaNotAvailableError` at construction when the dependency is missing, so
  you can degrade to the in-memory bus deliberately rather than crash on
  import.

Two properties hold for every distributed backend and should shape your
handlers before you pick one: **no ordering guarantee across handlers**, and
**at-least-once delivery**. Make handlers idempotent. Note also that
`await publish(...)` returning is not a success signal -- handler exceptions are
caught, logged, and counted in the bus stats rather than raised, so work that
must not be dropped belongs under `SubscriptionManager`
([subscriptions](subscriptions.md)) with checkpoints and a DLQ, not in a bare
bus subscription.

Each distributed backend brings its own retry-and-DLQ settings on the config
object (`max_retries`, `enable_dlq`, and the backend's DLQ naming), plus knobs
that reflect its broker: consumer groups and pending-message recovery on Redis
Streams, exchange type and `prefetch_count` on RabbitMQ, `acks` and
`auto_offset_reset` on Kafka. The guide covers the ones you have to set; the
rest are documented on the config dataclasses themselves.

Delivery guarantees differ per backend -- read
[ADR 0007: event bus delivery semantics](../adrs/0007-event-bus-delivery-semantics.md)
before you assume exactly-once. [Tutorial 7](../tutorials/07-event-bus.md)
teaches the same API on a toy domain, and
[observability](observability.md) covers the spans each bus emits.
