# eventsource-py

eventsource-py is an event sourcing library for Python. Instead of storing the
current state of your domain objects and overwriting it on every update, you
append immutable *events* describing what happened, and derive state by folding
those events back together.

This page is an orientation, not a task list. It explains what the library is,
what shape its pieces take, and how the documentation is organized, so that you
can decide which part to read next.

The library is built around three commitments that shape almost every API in it:

- **Async-first.** Event stores, buses, snapshot stores, and repositories all
  expose `async` interfaces. Synchronous callers are served by an explicit
  adapter (`SyncEventStoreAdapter`) rather than by a parallel blocking API.
- **Pydantic v2 as the event contract.** `DomainEvent` is a frozen Pydantic
  model, so validation, JSON serialization, and static typing come from the same
  declaration. Event classes are registered in an `EventRegistry`, which is what
  lets stored events be rehydrated into the right Python type later.
- **Backend-agnostic contracts.** Event store access is a set of narrow ports
  (`FullEventStore`, `AggregateStore`, and the five capability protocols they
  compose), and `EventBus`, `SnapshotStore`, and the checkpoint/DLQ/outbox
  repositories are interfaces first too. In-memory,
  PostgreSQL, SQLite, Redis, RabbitMQ, and Kafka implementations sit behind them,
  so the code you write against the interface in a test is the code that runs in
  production against a real database or broker.

The install footprint follows from that last point: the core package depends
only on `pydantic` and `sqlalchemy`, and every driver — `asyncpg`, `aiosqlite`,
`redis`, `aio-pika`, `aiokafka`, OpenTelemetry — is an optional extra you opt
into. A project that only ever uses the in-memory backends pulls in nothing else.

eventsource-py targets Python 3.13 and newer, is distributed on PyPI as
[`eventsource-py`](https://pypi.org/project/eventsource-py/), ships a `py.typed`
marker for downstream type checkers, and is MIT licensed. Source lives at
[github.com/tyevans/eventsource-py](https://github.com/tyevans/eventsource-py).

The rest of this page walks through why you might want event sourcing at all,
what the library gives you, how to install and run a minimal example, how to
swap development backends for production ones, and where the tutorials,
how-to guides, reference, and design explanations live.

## Why event sourcing

A conventional persistence layer keeps one row per entity and mutates it in
place. That row answers exactly one question — *what is true right now* — and it
answers it by destroying the answer to every other question. Why is this order
cancelled? Who changed the shipping address, and when? What would this report
have said last Tuesday? Those answers were overwritten, and the usual response
is to bolt on an audit table that duplicates the same writes with none of the
same guarantees.

Event sourcing inverts the relationship. The append-only sequence of events is
the source of truth, and current state is a derived value: fold the events for
an aggregate and you get its state back. Nothing is overwritten, so the history
is not a side channel that can drift from reality — it *is* reality, and current
state is the thing that can be rebuilt.

### What you get

**An audit trail that cannot drift.** Every change is a `DomainEvent` appended
to the store, and the store's write path is append-only: the `EventAppender`
port exposes a single `append(stream, events, expected)` method, and nothing in
the store ports edits or deletes an event in place. Each persisted event comes
back wrapped in an `EventEnvelope`, carrying its `stream_version` (position
within one aggregate's stream) and `position` (an opaque, store-scoped
`Position` for the global feed), plus `stored_at`. The audit log and the write
model are the same bytes, so there is no reconciliation job and no possibility
of the two disagreeing.

**Time travel.** Because state is a fold over events, you can fold over a prefix
instead. `read_stream` takes a `StreamReadOptions` — direction
(`ReadDirection.FORWARD` or `BACKWARD`) and a limit — so an aggregate can be
rehydrated as it stood at a chosen version. `read_all` takes a `FeedReadOptions`
(tenant filter, limit) starting from an optional `Position`, and
`read_category` takes a `CategoryReadOptions` (tenant filter, timestamp bound,
limit) for walking a whole aggregate-type category. Point-in-time
reconstruction is the ordinary read path with a bound on it, not a special
feature. Snapshots (`SnapshotStore`) are a performance optimization layered on
top of this, never a replacement for the events.

**Multiple read models from one write model.** The events are neutral about how
they will be queried. A `Projection` consumes the stream and writes whatever
shape a particular consumer needs — a normalized table, a denormalized search
document, a running counter — and you can add a fourth read model in six months
by projecting the events you already have. There is no migration, because the
new model is built by replaying history rather than by backfilling from a schema
that never captured the data.

**Replayable debugging.** A bug in a projection is a bug in a pure function over
a recorded input. Checkpoint repositories expose `reset_checkpoint`, so a
projection can be rewound and rebuilt from the beginning of history; you can
reproduce a production incident locally by feeding the same events through the
same handlers, and fix the projection without touching the write side. The same
property makes the library's own testing story straightforward:
`eventsource.testing` ships `InMemoryTestHarness`, `EventBuilder`,
`EventAssertions`, and given/when/then helpers (`given_events`, `when_command`,
`then_event_sequence`) that operate on event lists — because event lists are
what the system actually runs on.

### When not to reach for it

Event sourcing buys history and derivability, and it charges for them in
complexity. The cost is real and mostly paid up front:

- **Eventual consistency.** Projections trail the write model. If a use case
  demands that a write be immediately visible in a queried view, you will be
  building workarounds against the grain of the architecture.
- **Schema evolution never goes away.** Old events are immutable, so version 1
  of an event has to stay readable forever. `DomainEvent` carries an
  `event_version` field so you can tell the generations apart, but deciding what
  to do when the shapes diverge — tolerant readers, upcasting on load, parallel
  event types — is design work the library does not do for you. There is no
  `ALTER TABLE` for history.
- **Deletion is hard on purpose.** An append-only log and a "delete all data
  about this person" requirement are in genuine tension. None of the store
  ports has a delete method, and resolving the tension takes deliberate design
  (crypto-shredding, keeping personal data out of the events and referencing it
  by key) rather than a `DELETE` statement.
- **Query cost.** Ad-hoc queries over the event log are slow and awkward. Every
  question you want to ask needs a read model, which means every question has an
  operational cost.

So it is a poor fit for CRUD applications whose value is the current state,
for reference or configuration data, for anything where the domain has no
interesting history, and for teams that need to ship a first version quickly
and have no auditing or temporal requirements at all.

It is a good fit where the *sequence* of changes is itself business-critical:
finance and ledgers, order and fulfilment workflows, regulated domains with
audit obligations, systems where analytics needs keep changing, and
collaborative or distributed systems that must reconcile concurrent activity
after the fact.

A useful middle position: event sourcing is applied per bounded context, not per
application. It is normal — and usually correct — to event-source the order
lifecycle while keeping the product catalogue in an ordinary table.

## What this library provides

The library is a set of contracts plus a set of implementations behind them.
Almost everything you interact with is one of four things: an *event*, an
*aggregate*, a *store or bus*, or a *projection*. The sections below describe
the shape of each, not how to use them step by step — the tutorials do that.

### Async-first contracts

"Async-first" is not a stylistic preference here; it is a decision about what
the contracts are. Persistence and delivery are I/O, the library assumes an
event loop, and so every store, bus, and repository method is a coroutine. There
is no parallel blocking hierarchy to keep in sync, and no hidden thread pool
inside the interfaces.

The event store is not one interface but five narrow `Protocol`s
(`eventsource.ports.store`), each describing a single capability an adapter may
offer:

- **`EventAppender`** — `append(stream, events, expected)`, the only write
  path, returning an `AppendResult`. `expected` is an `ExpectedVersion`, built
  via its classmethod constructors (`ExpectedVersion.any_()`,
  `.no_stream()`, `.stream_exists()`, `.exact(version)`) rather than a bare int.
- **`StreamReader`** — `read_stream(stream, options=None)`, an async iterator
  of `EventEnvelope`, plus `get_stream_version(stream)`.
- **`EventLookup`** — `event_exists(event_id)`, for idempotency checks.
- **`GlobalEventFeed`** — `read_all(from_position=None, options=None)` for the
  store's whole ordered feed, plus `current_position()`, the highest `Position`
  in the store, used by the subscription manager to know when catch-up is
  finished.
- **`CategoryQuery`** — `read_category(category, options=None)`, for reading
  every stream of one aggregate type as a single ordered feed.

`FullEventStore` is the union of all five, and `AggregateStore` is the smaller
union of just `EventAppender` and `StreamReader` — the only two
`AggregateRepository` needs. A backend implements exactly the capabilities it
can support; nothing forces a minimal adapter to supply a global feed or
category reads it cannot honor efficiently.

There is no update method and no delete method anywhere in these ports.
Everything a caller can do to a store either appends or reads, and that is a
property of the interface rather than a convention implementations are trusted
to follow.

`EventBus` (`eventsource.ports.bus`) is the live-delivery half, and it is
where async-first meets a practical concession. `publish(events,
background=False)` is a coroutine; subscription management is not — `subscribe`
and `unsubscribe` bind a handler to one event type, `subscribe_all`,
`subscribe_to_all_events`, and `unsubscribe_from_all_events` bind a handler to
every event, and all five are ordinary synchronous calls, since registering a
callback is bookkeeping rather than I/O. Handlers themselves may be sync or
async: the bus accepts a `FlexibleEventHandler` or a plain callable, and the
canonical handler contracts live in `eventsource.ports.handlers` (`EventHandler`,
`SyncEventHandler`, `FlexibleEventHandler`, plus the ABC-based
`EventSubscriber`). The `background=True` flag makes publication
fire-and-forget, trading delivery latency for eventual consistency.

`SnapshotStore` (`eventsource.ports.snapshots`) is also an ABC, storing
`Snapshot` records via `save_snapshot`, `get_snapshot`, `delete_snapshot`,
`snapshot_exists`, and `delete_snapshots_by_type`. A snapshot is always an
optimization: it lets an aggregate skip ahead before folding the remaining
events. Deleting every snapshot in the system costs performance, not
correctness — which is exactly why the interface is allowed to expose deletion
when the event store is not.

Three supporting repositories back the projection runtime. Unlike the three
above, these are defined as `Protocol`s in `eventsource.ports` (`checkpoints.py`,
`dlq.py`, `outbox.py`), so any object with the right async methods satisfies
them — no inheritance required:

- **`CheckpointRepository`** — where a projection has got to.
  `get_checkpoint` / `update_checkpoint` record the last processed event per
  projection name, `get_position` / `save_position` track integer positions per
  subscription id, `get_lag_metrics` reports how far behind a projection is, and
  `reset_checkpoint` rewinds it so history can be replayed.
- **`DLQRepository`** — a dead-letter queue for events a projection could not
  process. `add_failed_event`, `get_failed_events`, `mark_retrying`,
  `mark_resolved`, `get_failure_stats`, `get_projection_failure_counts`, and
  `delete_resolved_events` make failures durable and queryable instead of a log
  line someone has to notice.
- **`OutboxRepository`** — the transactional outbox pattern. `add_event`,
  `get_pending_events`, `mark_published`, `mark_failed`, `increment_retry`, and
  `cleanup_published` let you commit an event and its intent-to-publish in one
  transaction, then relay to the broker separately.

Each of the three ships in-memory, PostgreSQL, and SQLite implementations, so a
projection's operational machinery behaves the same in a unit test as it does in
production.

Synchronous callers are served by exactly one explicit escape hatch:
`SyncEventStoreAdapter` (`eventsource.adapters.sync`) wraps a `FullEventStore` and
exposes the same method names as the async ports — `append`, `read_stream`,
`get_stream_version`, `event_exists`, `read_all`, `read_category`, and
`current_position` — each accepting an optional `timeout` keyword and running
the wrapped coroutine on a dedicated thread pool. The blocking nature is
visible at the call site through the adapter type itself rather than through a
`_sync` method suffix, and it is a bridge for legacy code, not a supported
second API.

### Pydantic v2 domain events and the global EventRegistry

An event is the unit the whole library is built out of, so it is worth being
precise about what one *is*: `DomainEvent` is a Pydantic v2 `BaseModel` with
`model_config = ConfigDict(frozen=True)`. That single declaration is doing four
jobs at once — runtime validation, JSON serialization, static typing, and
immutability — and choosing Pydantic rather than a dataclass or a dict is what
makes the same class usable as an in-memory value, a database row, and a
message on a broker without a translation layer in between.

Subclassing gives you a validated event with a common envelope already in place:

| | |
|---|---|
| `event_id` | unique per event instance (`uuid4` by default) |
| `event_type` | wire name, auto-derived from the class name |
| `event_version` | schema version for this event type, `>= 1` |
| `occurred_at` | UTC timestamp, defaulted at construction |
| `aggregate_id`, `aggregate_type` | required; which stream this belongs to |
| `aggregate_version` | version of the aggregate after this event |
| `tenant_id` | optional; `TenantDomainEvent` makes it required |
| `actor_id` | who or what triggered it |
| `correlation_id`, `causation_id` | causal chain across aggregates |
| `metadata` | free-form `dict[str, Any]` for cross-cutting context |

Only `aggregate_id` and `aggregate_type` are required of the caller; everything
else is defaulted, so a subclass is usually just its payload fields.

Frozen models would make "the same event but with X set" awkward, so the base
class provides copy-with helpers instead of mutation: `with_causation(event)`
returns a copy carrying that event's `event_id` as `causation_id` and inheriting
its `correlation_id`, `with_metadata(**kwargs)` merges keys into `metadata`, and
`with_aggregate_version(n)` stamps the version the aggregate assigns when the
event is recorded. `is_caused_by` and `is_correlated_with` read the same
relationships back. Serialization is `to_dict()` / `from_dict()`, thin wrappers
over `model_dump(mode="json")` and `model_validate`.

**`event_type` derives itself.** `DomainEvent.__init_subclass__` runs at class
definition time and rewrites the `event_type` field default to the class name,
so `class OrderCreated(DomainEvent)` gets `event_type == "OrderCreated"` with no
declaration. A `model_validator(mode="before")` covers the same ground for
dict-based construction, so `OrderCreated.model_validate({...})` without an
`event_type` key still ends up with one. You may set `event_type` explicitly to
decouple the wire name from the class name — the library logs a warning when the
two differ, silenceable with `suppress_event_type_warning = True` on the class,
and never fails on it.

**Deserialization is why the registry exists.** A stored event is a type *name*
plus a JSON payload; something has to turn `"OrderCreated"` back into the
`OrderCreated` class before Pydantic can validate it. Nothing in the string
`"OrderCreated"` knows where that class lives, and by the time you are reading
history the module that defined it may not have been imported. `EventRegistry`
is that lookup table — a thread-safe (`RLock`-guarded) `dict` from type name to
class, exposing `register`, `get`, `get_or_none`, `contains`, `list_types`,
`list_classes`, `unregister`, and `clear`, plus `len()`, `in`, and iteration.

There is a module-level `default_registry`, and `@register_event` writes to it:

```python
from eventsource import DomainEvent, register_event

@register_event
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
```

Registration is **explicit**. Deriving from `DomainEvent` gives you an
`event_type` but does *not* put the class in any registry; only
`@register_event` or `registry.register(cls)` does. That is deliberate — a
global side effect on every subclass would make test fixtures and throwaway
event classes collide with production types. The decorator takes both forms:
`@register_event(event_type="order.created")` registers under a different wire
name, and `@register_event(registry=my_registry)` targets an isolated registry
so a test's event types never touch the global one. The resolution order for the
name is explicit argument, then the class's `event_type` field default, then the
class name.

Registering two different classes under one name raises
`DuplicateEventTypeError` at import time rather than silently shadowing; looking
up a name that was never registered raises `EventTypeNotFoundError`, whose
message lists the names that *are* available — the usual cause is a module that
was never imported. Four convenience functions (`get_event_class`,
`get_event_class_or_none`, `is_event_registered`, `list_registered_events`)
front the default registry for the common case.

The registry is a constructor argument, not a hard-wired singleton. The
PostgreSQL and SQLite event stores and the Redis and Kafka buses all accept
`event_registry=`, falling back to `default_registry` when you do not pass one,
and each exposes the registry it is using. So the global registry is a
convenient default rather than an inescapable one: an application with two
bounded contexts, or a test suite that wants its fixtures isolated, can hand
each store its own.

### Aggregates, repositories, and optimistic locking

`DeciderAggregate[TState, TCommand]` is the **recommended** write model: two pure
static functions, `decide` and `evolve`, plus an `aggregate_type` class attribute.
`decide(command, state)` returns the events a command produces or raises
`CommandRejectedError`; `evolve(state, event)` folds one event into the next state.
The command names the aggregate it targets, which is where `decide` gets the
`aggregate_id` for the events it returns — `initial_state()` takes no arguments.
Nothing else about the domain touches `self`, so both functions are trivial to unit
test without an event store in sight:

```python
class AccountAggregate(DeciderAggregate[AccountState, AccountCommand]):
    aggregate_type = "Account"

    @staticmethod
    def decide(command: AccountCommand, state: AccountState) -> list[DomainEvent]:
        match command, state:
            case Withdraw(amount=amt), AccountState(balance=bal) if amt > bal:
                raise CommandRejectedError(f"insufficient balance: {bal}")
            case Withdraw(account_id=account_id, amount=amt), _:
                return [MoneyWithdrawn(aggregate_id=account_id, amount=amt)]
            # ... initial_state() and evolve() elided, same shape
```

`execute(command)` is the public entry point: it runs `decide`, stamps each returned
event with `aggregate_version`, `aggregate_type`, and provenance from the command, and
applies it — a rejection leaves the aggregate completely untouched. See
[Getting Started](getting-started.md) for the full walkthrough.

Two other aggregate styles remain fully supported for existing codebases:
`AggregateRoot[TState]`, the base write model with `apply_event`/`_apply`/
`_get_initial_state`, and `DeclarativeAggregate`, which layers `@handles(SomeEvent)`
routing on top of it, dispatching by event type at class initialization time and
raising `HandlerSignatureError` immediately if a handler's signature is wrong rather
than at the first event. `DeciderAggregate` is itself built on `AggregateRoot`, so all
three share `apply_event`, `load_from_history`, and `mark_events_as_committed`. See
[Aggregate Styles](explanation/aggregate-styles.md) for a side-by-side comparison and
guidance on which to reach for.

`AggregateRepository[TAggregate]` is the load/save boundary. `load` rehydrates
(via snapshot plus remaining events when a `SnapshotStore` is configured),
`save` appends the uncommitted events and optionally publishes them through an
`EventPublisher`, and `load_or_create`, `exists`, `get_version`, and
`get_or_raise` cover the usual variations. Snapshotting is configured on the
repository via `snapshot_threshold` and a `snapshot_mode` of `"sync"`,
`"background"`, or `"manual"`.

Concurrency control is optimistic and explicit. Appends carry an
`expected_version`; if the stream has moved on, the store raises
`OptimisticLockError` carrying `aggregate_id`, `expected_version`, and
`actual_version`, and the caller decides whether to retry or surface a conflict.
`ExpectedVersion` provides the three semantic constants — `ANY` (skip the check),
`NO_STREAM` (must be a new aggregate), `STREAM_EXISTS` (must already exist).
Where optimistic retry is not enough, `eventsource.adapters.postgresql.locks`
offers PostgreSQL advisory locks for genuine pessimistic serialization.

### Projections, read models, and the subscription lifecycle

A `Projection` is the read side: `handle(event)` and `reset()`. Most projections
are built on the richer stack instead of the bare interface:

- **`CheckpointTrackingProjection`** adds checkpointing, retry, DLQ routing, and
  a `reset()` that both rewinds the checkpoint and truncates the read models.
- **`DeclarativeProjection`** adds `@handles`-based routing, derives
  `subscribed_to()` from the decorated handlers, and applies tenant filtering.
- **`DatabaseProjection`** runs each event's handler inside a database
  transaction, so the read-model write and the checkpoint update commit together.
- **`ReadModelProjection[TModel]`** goes one step further and hands your handler
  a typed `ReadModelRepository` for the model class, with in-memory, PostgreSQL,
  and SQLite backends.

`SubscriptionManager` (`eventsource.application.subscriptions`) owns the runtime. You
`subscribe(subscriber, config, name)` — the manager wires up historical catch-up
from the event store *and* live delivery from the bus behind one subscription —
then `start()`, and either `stop()` or `run_until_shutdown()`. It is an async
context manager, and `register_signals()` hooks SIGINT/SIGTERM into a graceful
shutdown that stops accepting events, drains in-flight work, and saves final
checkpoints before exiting.

`SubscriptionConfig` is where the operational behavior lives: `start_from`
(`"beginning"`, `"end"`, `"checkpoint"`, or an explicit opaque `Position` token),
`batch_size` and `max_in_flight` with a `backpressure_threshold`, a
`checkpoint_strategy` and interval, event/aggregate-type and `tenant_id`
filters, `continue_on_error`, exponential-backoff retry settings, and a circuit
breaker. The manager also exposes health and error introspection —
`get_health()`, `check_all_health()`, `get_error_stats()`, `total_dlq_count`,
`is_healthy` — so a subscription's state is queryable rather than inferred from
logs.
