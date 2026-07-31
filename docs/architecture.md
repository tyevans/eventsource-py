# Architecture

`eventsource` is an async-first event sourcing library for Python built on two
runtime dependencies: pydantic for event definition and validation, and
SQLAlchemy for the relational backends. Everything else -- asyncpg, aiosqlite,
redis, aio-pika, aiokafka, OpenTelemetry -- is an optional extra, imported
behind a guard and surfaced as an `*_AVAILABLE` flag rather than a hard import
error at package load.

The shape of the library follows from one commitment: the event log is the
system of record. An aggregate holds no durable state of its own; it is a fold
over its stream, rebuilt on load and discarded after the append. A read model
holds no authority; it is a cache of that fold, rebuildable by replaying the
log. Between those two sits an append-only store whose central operation is a
version-checked append, and a bus that fans the same events out to consumers who
were not part of the write transaction.

Most of the structure you will meet -- why `DomainEvent` is frozen while read
models are explicitly mutable, why `EventStore` and `EventBus` are separate
interfaces with separate backends, why projections carry checkpoints and dead
letter queues, why almost every public method is a coroutine -- is that
commitment working itself out. Facts must not be lost or contradicted, so the
write path is narrow, ordered, and guarded by `ExpectedVersion`. Caches must be
cheap to serve and cheap to rebuild, so the read path tolerates duplicate
delivery, at-least-once semantics, and truncate-and-replay.

A second organising idea runs alongside it: contracts are separated from the
things that implement them. `events/base.py`, `stores/interface.py`,
`bus/interface.py`, `protocols.py`, `exceptions.py`, and `types.py` depend on
nothing heavier than pydantic; the PostgreSQL, SQLite, Redis, RabbitMQ, and
Kafka implementations sit behind them and are swappable.
`docs/core-surface.md` records that boundary module by module. It is also why
the in-memory backends are not toys but first-class implementations of the same
ABCs -- the ones your unit tests run against, and the ones the conformance
suites in `eventsource.testing.conformance` hold to the same contract as their
database-backed siblings.

This document explains how those pieces fit and why they are divided where they
are. It moves outward from the durable core -- events, aggregates, the store --
to the machinery that carries events onward: the bus and the outbox,
projections and read models, and the subscription runtime that keeps
projections alive in production. It ends with the concerns that cut across every
layer (async, multi-tenancy, distributed locking, observability, optional
dependencies) and with an honest account of the trade-offs, including the
problems the library deliberately leaves to you.

It is an explanation, not a walkthrough. There is no first-aggregate tutorial
here and no exhaustive parameter list; for those, see the tutorials, the how-to
guides, and the API reference. Read this when you want to know why the library
behaves the way it does, or when you are deciding how to fit it into a system
whose shape is not yet settled.

## Scope of this document

This document is a map of the library at the level of *why*, and it is written
for a particular moment: you have installed `eventsource`, or you are deciding
whether to, and you want the whole shape in one reading rather than assembled
piecemeal from module docstrings. It assumes you know roughly what event
sourcing is -- events as facts, aggregates as folds, projections as derived
views -- and that async Python is not itself the obstacle. It does not assume
you have written anything against this library yet.

What is in scope is the reasoning behind the seams. Where the package draws a
line between a contract and its implementations, why events are frozen pydantic
models that register themselves, why an aggregate keeps uncommitted events
rather than writing as it goes, why the store's append is version-checked and
the bus's publish is not, why the outbox exists at all given that both a store
and a bus are already present, why projections need checkpoints and dead letter
queues before they are production-ready, and why the subscription runtime is a
separate layer from the projections it drives. The cross-cutting sections cover
the same kind of question for async, multi-tenancy, locking, tracing, and the
optional-dependency flags: what each one buys, and what it makes harder.

Backends appear here only as a set of trade-offs. You will find the criteria for
choosing between InMemory, SQLite, and PostgreSQL event stores, and between
InMemory, Redis, RabbitMQ, and Kafka buses -- durability, ordering, fan-out,
operational weight -- but not their connection strings, tuning knobs, or SQL.
The same restraint applies to schemas: `migrations/` is append-only by design
and that design is explained, while the tables themselves belong to the
reference material.

Several neighbours are named and then left alone. Live store-to-store migration
(`eventsource.migration`), GDPR erasure (`eventsource.gdpr`), the BDD and
harness helpers in `eventsource.testing`, and the internal details of the
conformance suites are all real parts of the package and all out of scope,
mentioned only where they illustrate a boundary. Deeper single-topic arguments
live elsewhere: `docs/explanation/aggregate-styles.md` compares the imperative
and declarative aggregate styles, `docs/explanation/schema-design.md` and
`docs/explanation/sql-backend-type-handling.md` go under the SQL layer, and the
records in `docs/adrs/` capture individual decisions in their original terms --
async-first design, event bus delivery semantics, tenant isolation, advisory
locks, optional-dependency extras, and the rest. Where this document summarises
one of those decisions, the ADR remains the authority on it.

You will find no parameter tables, no code you can paste into a project, and no
step-by-step build here. The tutorials in `docs/tutorials/` teach by building;
the how-to guides answer a task you already have; the API reference gives exact
signatures; and `docs/core-surface.md` records the dependency layering module by
module. What you should be able to take away from this document is judgement:
given a system you are designing, which pieces of the library you actually need,
which backend each piece should sit on, and where the load-bearing walls are
before you start cutting through them.

## The core loop: command -> event -> state -> read model

Nearly everything in this library is an elaboration of a single cycle. A command
arrives at an aggregate; the aggregate decides whether it is allowed and turns it
into events; those events are appended to the store under a version check and
folded into the aggregate's state; and consumers downstream fold the same events
into read models. Every layer described later in this document sits somewhere on
that loop, so it is worth walking once before the pieces are taken apart.

**A command is not a type here.** There is no `Command` base class and no
dispatcher. A command is whatever method you put on your aggregate --
`order.ship(tracking_number=...)`. The library is deliberately quiet at this end
of the loop, because the interesting constraint is not how the request arrives
but what the aggregate may do with it. Such a method has exactly two legitimate
outcomes: raise, or produce events. It does not touch a database, and it does not
assign to state directly.

**Events are the only way state changes.** A command method constructs a
`DomainEvent` and hands it to `apply_event()`, which does four things in order: it
checks that the event's `aggregate_version` equals the aggregate's current version
plus one, raising `EventVersionError` if not (unless `validate_versions` is
disabled, in which case it logs a warning and continues); it advances the version
to match the event; it calls `_apply()` to fold the event into state; and it
appends the event to the uncommitted list. Recording the fact and applying it are
the same operation. There is no path by which an aggregate reaches a state its
stream does not account for -- which is exactly what makes replay-from-zero a
trustworthy way to reconstruct it.

**Nothing is durable until save.** Uncommitted events accumulate in memory on the
aggregate instance; a command that raises partway through leaves no trace,
because nothing has left the process. `AggregateRepository.save()` is where the
loop crosses into durability. It reads `aggregate.uncommitted_events`, returns
immediately if the list is empty, derives `expected_version` by subtracting the
number of new events from the aggregate's current version -- that is, the version
the stream was at when the command began -- and calls `append_events()` on the
store with it. If another writer appended in the meantime, the store's version
check fails and you get an `OptimisticLockError` rather than a silent
interleaving. This is the one place in the library that refuses work outright,
and that is deliberate: the write side is the only side that can still say no.

**After a successful append, the events travel.** Only once the store reports
success does `save()` mark the events committed on the aggregate, publish them to
the configured `EventPublisher` if one was supplied, and hand them to the snapshot
manager, which may write a snapshot depending on the configured threshold and
mode. The ordering matters more than it looks. Publishing after the append means
no consumer can observe an event that failed to persist. It also means the gap in
the other direction is real: the append can succeed and the publish can fail,
leaving events durable but unannounced. Closing that gap is precisely what the
outbox is for, and why it is a separate component rather than a flag on `save()`.

**Loading is the same fold, replayed.** `AggregateRepository.load()` does not
fetch a row. It asks the snapshot store for a valid snapshot; if one exists it
restores state from it and reads only events after that version, otherwise it
reads the stream from version 0. Each event is then applied with `is_new=False`
-- same `_apply()`, same transitions, but no version validation and no
uncommitted tracking. A snapshot is a cached prefix of that fold and nothing
more: invalidate it (by bumping `schema_version`, say, when the state model
changes shape) and the aggregate rebuilds itself from the log to the same answer.
The aggregate is transient. The stream is what persists.

**Projections close the loop on the read side, with weaker guarantees.** A
projection receives events -- pushed from the bus, or replayed from the store by
a subscription runner -- and folds them into whatever shape queries actually
want: denormalised tables, counters, search documents. Structurally this is the
same operation an aggregate performs on load, but the constraints invert. A
projection's state is mutable and shaped for reading rather than for deciding; it
carries no authority, so it can be truncated and rebuilt; and it must tolerate
being handed the same event twice, because delivery is at-least-once. Hence
`CheckpointTrackingProjection`, which records how far it has processed so it can
resume after a restart, retries a failing event before giving up on it, and
offers a `reset()` that truncates its read models so a rebuild starts clean.

The asymmetry between those last two paragraphs is the shape of the whole
library. On the write side: one aggregate, one stream, a strict version check, a
hard failure on conflict. On the read side: many consumers, no ordering guarantee
across streams, duplicate delivery assumed, poisoned events parked in a dead
letter queue rather than propagated back to the writer. The store is what the two
sides agree on. Everything else -- the bus, the outbox, checkpoints, retry, flow
control -- is machinery for carrying facts from the strict side to the tolerant
side without losing any, and without letting the tolerant side's problems become
the strict side's problems.

One consequence deserves naming early, because it surprises people: the loop is
not closed synchronously. When `save()` returns, the events are durable and the
aggregate is current, but the read models are not. A caller that writes and then
immediately queries a projection may see the old value. The library neither hides
this nor offers a "wait for projections" switch on the write path, because that
would tie the availability of writes to the health of every consumer. Designing
around the gap -- returning the aggregate's own state to the caller, polling a
checkpoint, or simply accepting the staleness -- is your decision, and the
trade-offs come up again under idempotency and delivery semantics below.

## Layer map

The loop above says nothing about which module owns which step, and
deliberately so: the same loop runs whether the store is a dictionary or
PostgreSQL. What makes that substitution possible is a second axis of
organisation running across the package -- not "write side versus read side"
but "contract versus implementation versus orchestration". `docs/core-surface.md`
records the boundary module by module and is the authority on membership; what
follows is the reasoning behind it and what each tier costs you.

The layering is enforced by nothing but discipline and that document. There is
no plugin registry, no dependency-injection container, no import hook policing
direction. A tier here is a statement about a module's import block: which
third-party packages executing it pulls into the process. That is a narrower
claim than it sounds, and the section below is partly about where the narrowness
bites.

### Tier 0: contracts and pure domain

Tier 0 is the set of modules that import nothing heavier than the standard
library and pydantic. It holds the vocabulary the rest of the library is written
in: `events/base.py` (`DomainEvent`), `events/registry.py`, `aggregates/base.py`
(`AggregateRoot`, `DeclarativeAggregate`), `stores/interface.py` (`EventStore`,
`StoredEvent`, `EventStream`), `bus/interface.py` (`EventBus`),
`snapshots/interface.py`, the `@handles` decorator and handler registry,
`protocols.py`, `exceptions.py`, `types.py`, and the multi-tenancy and read-model
contract modules. `serialization/` is the extreme case and a useful yardstick:
its entire import block is `json`, `datetime`, `typing`, and `uuid`, with no
pydantic and no reach back into the rest of the library.

Two things about Tier 0 are easy to get wrong.

The first is that **the in-memory implementations live here too** --
`stores/in_memory.py`, `bus/memory.py`, `snapshots/in_memory.py`,
`readmodels/in_memory.py`. They are not stubs kept for demos. They implement the
same ABCs the database-backed classes do, and
`eventsource.testing.conformance` holds both to the same suites, so a behaviour
your unit tests rely on from `InMemoryEventStore` is one the contract obliges
`PostgreSQLEventStore` to provide as well. This is what makes it reasonable to
run the bulk of a test suite with no Docker daemon: you are not testing a
different thing, you are testing the same contract on a backend whose storage
happens to be a dict.

The second is that **Tier 0 is not the same as standalone**. A module can be
perfectly Tier 0 and still be impossible to ship alone, because it imports other
`eventsource` modules at import time. `protocols.py` advertises itself as the
canonical contract module, but line 35 is a module-level
`from eventsource.events.base import DomainEvent` -- not under `TYPE_CHECKING`,
not deferred -- so importing it executes `events/base.py`, which imports
pydantic. `events/base.py` is an *extraction floor*: nothing above it moves
without it, and the same shape repeats for `stores/interface.py`,
`bus/interface.py`, and `aggregates/base.py`, which sit on `events/base.py`,
`exceptions.py`, and `types.py`. `snapshots/interface.py` is the odd one out,
with no in-library imports at all.

Tier 0 used to be leaky here, and it's worth knowing what changed. All three
modules under `repositories/` -- `checkpoint.py`, `dlq.py`, `outbox.py` --
used to pack four layers into one file: the `runtime_checkable` Protocol, the
data-transfer dataclasses, a stdlib-only in-memory implementation, and both
SQL backends, under a module-level `from sqlalchemy import text`. Importing
the Protocol therefore imported SQLAlchemy, and that single decision
propagated: `projections/base.py`, `projections/checkpoint_manager.py`,
`projections/dlq_manager.py`, `testing/harness.py`, `testing/bdd.py`, and
`readmodels/projection.py` were all outside Tier 0 for no reason of their own.
[ADR 0024](adrs/0024-projection-persistence-ports.md) closed the checkpoint
and DLQ half of that gap the same way ADR 0019 closed it for the event store:
the Protocols and dataclasses moved to `ports/checkpoints.py` and
`ports/dlq.py` (stdlib and typing only), the SQL implementations moved to
`adapters/sql/`, and the in-memory ones to `adapters/memory/`.
`application/projections/*` -- the module `projections/` became -- is now
Tier-0-clean as a whole ring. `repositories/` now holds the outbox alone, and
splitting it the same way is the one piece of pre-extraction cleanup that
remains between the current package and an `eventsource-core` distribution.

### Tier 1: backend implementations

Tier 1 is where the contracts meet a driver. `stores/postgresql.py` and
`stores/sqlite.py` subclass `EventStore`; `snapshots/postgresql.py` and
`snapshots/sqlite.py` subclass `SnapshotStore`; `bus/redis.py`,
`bus/rabbitmq.py`, and `bus/kafka.py` subclass `EventBus`;
`readmodels/postgresql.py` and `readmodels/sqlite.py` satisfy the
`ReadModelRepository` protocol; the PostgreSQL and SQLite classes inside
`repositories/checkpoint.py`, `repositories/dlq.py`, and
`repositories/outbox.py` back checkpoints, the dead letter queue, and the
outbox. `locks/` belongs here too, and has exactly one implementation:
`PostgreSQLLockManager`, built on advisory locks.

Note the two different ways a backend joins its contract. Stores, snapshot
stores, and buses inherit from ABCs, so the relationship is declared and the
abstract methods are enforced at instantiation. The repository backends do not
inherit from anything -- `PostgreSQLCheckpointRepository`,
`SQLiteOutboxRepository`, `PostgreSQLReadModelRepository` and their siblings are
plain classes that structurally satisfy a `runtime_checkable` Protocol defined
in the same file. Both arrangements give you substitutability; only the first
tells you at import time that you missed a method.

The defining property of the tier is that **nothing above it names a member of
it**. Application code selects a backend once, at composition time, and passes
the instance down; `AggregateRepository`, the projections, and the subscription
runtime see only the contract. That is what makes the choice of store or bus a
deployment decision rather than an architectural one, and it is why the backend
sections later in this document can be written as trade-off comparisons instead
of migration guides. Constructors are the one place the difference shows:
`PostgreSQLEventStore` wants a SQLAlchemy `async_sessionmaker`,
`SQLiteEventStore` a database path, `SQLiteReadModelRepository` an open
`aiosqlite.Connection`. Backends do not manufacture their own connections, and
none of them own the application's transaction boundary.

Optional dependencies are handled less uniformly than the `*_AVAILABLE`
convention suggests, and the difference is worth knowing before you plan an
install. SQLAlchemy is a hard dependency, so the PostgreSQL modules import
`sqlalchemy` at module level with no guard; `asyncpg` is an extra, but it is
never imported by name -- it is loaded by SQLAlchemy from the connection URL, so
a missing driver surfaces at connect time, not import time. The buses guard
their own imports and set a module-level flag: `bus/redis.py` sets
`REDIS_AVAILABLE` and its constructor raises `RedisNotAvailableError`
immediately if it is `False`, with `RABBITMQ_AVAILABLE` and `KAFKA_AVAILABLE`
following the same pattern. SQLite splits the difference by tier:
`snapshots/sqlite.py` guards its own `aiosqlite` import and exports
`SQLITE_AVAILABLE`, while `stores/sqlite.py` imports `aiosqlite` unguarded and
is instead wrapped in a `try/except ImportError` by the package barrel, which
sets the top-level `SQLITE_AVAILABLE` and simply omits `SQLiteEventStore` and
the SQLite repositories from `eventsource`'s namespace when the extra is absent.
The invariant that holds across all of it is the one that matters: `import
eventsource` succeeds on a machine with no drivers installed, and you learn
about a missing extra when you reach for the backend you did not install.

A backend is also not free to be interesting. It inherits a contract it did not
write -- the `ExpectedVersion` semantics of `append_events`, the ordering
promises of `EventStream`, the `OptimisticLockError` raised on conflict -- and
`eventsource.testing.conformance` holds it to the same suites as the in-memory
implementation. Where a backend genuinely cannot honour something, the pattern
the package follows is to say so in that backend's own documentation
(`stores/README.md`, `bus/README.md`) rather than to soften the shared
interface. What varies underneath is real: PostgreSQL and SQLite both go through
`stores/_type_converter.py` to reconcile how each database hands back JSON, and
both call `stores/_compat.py` to normalise timestamps. The contract is stable;
the accommodations are private.

### Tier 2: orchestration and lifecycle

Tier 2 is the machinery that keeps the loop running unattended. Almost all of it
sits under `subscriptions/`, the largest package in the library by a wide
margin: `SubscriptionManager` as the front door, two runners
(`CatchUpRunner` reading batches from the store, `LiveRunner` consuming the bus)
with a `TransitionCoordinator` handing one off to the other at a watermark, plus
retry and circuit breaking, flow control, event filtering, cross-instance
coordination, pause and resume, error classification, health and metrics, and a
`shutdown.py` that is longer than any store implementation. `migration/` --
live store-to-store migration through the phases `PENDING`, `BULK_COPY`,
`DUAL_WRITE`, `CUTOVER`, `COMPLETED` -- belongs here too, as do the
projection-side collaborators `CheckpointTrackingProjection` calls into:
the `record_checkpoint()` / `read_checkpoint()` / `lag_metrics_dict()` /
`reset_checkpoint()` functions and the `send_to_dlq()` / `read_failed_events()`
functions, the `RetryPolicy` protocol, and `ProjectionCoordinator`.

What separates this tier from Tier 1 is not infrastructure but *time*. Tier 1
answers "how is an event written down"; Tier 2 answers "what happens on the
hundredth restart, when one consumer is slow, another is crash-looping, and a
deploy is rolling underneath both". Its concerns -- resumption from a
checkpoint, duplicate suppression across the catch-up-to-live seam,
backpressure, poison-event quarantine, signal handling and phased drain -- only
exist because delivery is at-least-once and processes are mortal. None of them
are visible in a single pass through the loop, which is exactly why they are
separated from the projections they drive: a projection stays a fold over
events, and everything about *when* and *how often* it is called lives outside
it. `SubscriptionManager.run_until_shutdown()` is the tier in miniature -- it
adds nothing to what a projection computes, only to how long it keeps
computing it across SIGTERM.

The composition is deliberately fine-grained, and the module docstrings say why:
checkpointing, DLQ dispatch, and retry were pulled out of the projection base
class, and lifecycle, pause/resume, registry, health, and shutdown were pulled
out of the manager, each on single-responsibility grounds. The cost is a lot of
small collaborators; the benefit is that you can adopt the parts you need. A
projection can be driven by hand with no subscription runtime at all, and a
`FlowController` or `CircuitBreaker` is usable on its own.

Being Tier 2 is about needing the world -- clocks, background tasks, OS signals,
other processes -- not about importing a driver. Most of `subscriptions/` names
its repositories only under `TYPE_CHECKING` and would be dependency-light on its
own; it lands above Tier 1 because `repositories/checkpoint.py` and
`repositories/dlq.py` pull SQLAlchemy in transitively, as noted above.
`migration/` is the exception that genuinely reaches for a driver: its own
`migration/repositories/` package imports `sqlalchemy` at module level for
migration state, routing, position mapping, and the audit log.

The tiers are a dependency ordering, not a seniority ranking, and one module
makes that concrete. `aggregates/repository.py` is pure orchestration by
temperament -- it sequences snapshot lookup, stream read, append, publish, and
snapshot write -- but it is Tier 0, because it does that sequencing entirely
against ABCs and never touches a driver, and because it orchestrates a single
call, not a lifetime. Orchestration that can be written against contracts alone
stays with the contracts. Tier 2 is what is left when orchestration has to
survive time.

The tiers are a dependency ordering, not a seniority ranking, and one module
makes that concrete. `aggregates/repository.py` is pure orchestration by
temperament -- it sequences snapshot lookup, stream read, append, publish, and
snapshot write -- but it is Tier 0, because it does that sequencing entirely
against ABCs and never touches a driver. Orchestration that can be written
against contracts alone stays with the contracts. Tier 2 is what is left when
orchestration genuinely needs the world: connections, clocks, background tasks,
and other processes.

## Layered view: stores, bus, aggregates, projections, subscriptions

The package divides into five layers, and the division is not arbitrary: each
layer exists because it answers a different question about the same events.
The store answers *what happened, in what order, and is it still true*. The bus
answers *who else needs to know, now*. Aggregates answer *what may happen next*.
Projections answer *what does this look like from the outside*. Subscriptions
answer *how does a projection stay caught up without losing its place*. Reading
the source in that order is the fastest way to see why the seams fall where they
do.

### The store: an ordered, version-checked log

`EventStore` (`stores/interface.py`) is an ABC, not a Protocol, and its abstract
surface is deliberately small: `append_events`, `get_events`,
`get_events_by_type`, `event_exists`, and `get_global_position`. Everything else
on the class -- `get_stream_version`, `read_stream`, `read_all` -- is a concrete
convenience built on those primitives, so a new backend has five methods to
implement and inherits the rest.

Two details of that surface carry most of the architectural weight. The first is
that `append_events` takes an `expected_version: int` and returns an
`AppendResult`; the `ExpectedVersion` constants (`ANY = -1`, `NO_STREAM = 0`,
`STREAM_EXISTS = -2`) let a caller say "this stream must not exist yet" or "I
read version 7 and I am writing on that basis" rather than trusting a
last-writer-wins append. Concurrency control is therefore a property of the log
itself, not of a lock the caller remembers to take. The second is
`get_global_position()`, which returns the highest position across *all* streams.
Per-stream ordering is what an aggregate needs; a single monotonic ordering over
the whole store is what a subscription needs, and the fact that the store can
report its current maximum is precisely what makes the catch-up watermark
described later in this document possible.

Reads come back as `StoredEvent`, a frozen wrapper that pairs the `DomainEvent`
with `stream_id`, `stream_position`, `global_position`, and `stored_at`. The
domain event carries no position of its own -- position is a fact about
persistence, not about the domain -- and keeping the two apart is why the same
event object can be replayed, published, and buffered without any layer
mutating it.

The backends behind this interface (`in_memory.py`, `sqlite.py`,
`postgresql.py`) differ in durability and concurrency, not in contract; the
conformance suite in `eventsource.testing.conformance` holds all three to the
same behaviour, which is what makes the in-memory store usable as a real test
double rather than an approximation.

### The bus: fan-out with weaker promises

`EventBus` (`bus/interface.py`) looks superficially similar and guarantees much
less. Its abstract methods are `publish`, `subscribe`, `unsubscribe`,
`subscribe_all`, `subscribe_to_all_events`, and `unsubscribe_from_all_events`.
There is no expected version, no position, and no history: `publish` takes a
list of events and a `background` flag, and once it returns, the events are
gone. Handler errors are caught and logged so that one failing subscriber does
not starve the others, which is the right default for fan-out and exactly the
wrong default for a system of record.

That asymmetry is the point. The store is the authority and pays for it in
write-path narrowness; the bus is a delivery mechanism and pays for its speed in
guarantees. A bus subscriber learns about events that arrive *after* it
subscribed and nothing about events that came before -- there is no
`get_global_position()` equivalent, because there is no retained ordering to
report. Everything in `subscriptions/` follows from taking that limitation
seriously rather than papering over it.

Wildcard subscription (`subscribe_to_all_events`) is worth noting here because
the live half of a catch-up subscription depends on it: a projection that must
not miss anything cannot enumerate its event types at subscribe time and hope
the list stays complete.

### Aggregates: the fold that guards the write

`AggregateRoot` (`aggregates/base.py`) is generic over a state type and holds
three things: an id, a version, and a list of uncommitted events. Commands call
`_raise_event()`, which applies the event to in-memory state *and* appends it to
`uncommitted_events`; nothing is written during the command. `load_from_history`
replays stored events with `is_new=False` so replay advances version without
re-queuing anything for write.

`AggregateRepository` (`aggregates/repository.py`) is the only place where an
aggregate meets infrastructure. Its `save()` appends the uncommitted events
under optimistic locking, then -- and only after the append succeeds -- marks
them committed, publishes them to the configured `EventPublisher`, and
optionally creates a snapshot. The ordering is the design: persistence first,
notification second, and snapshot failure explicitly does not fail the save,
because a snapshot is an optimisation over a log that is already durable.
Because the whole command produces one atomic append, the batch is the unit of
consistency, and `OptimisticLockError` is the only outcome a concurrent writer
needs to handle.

`DeclarativeAggregate` layers `@handles`-style routing over the same machinery;
it changes how you write `_apply`, not what the layer is for. The two styles are
compared in `docs/explanation/aggregate-styles.md`.

### Projections: the fold that has to survive restarts

Projections invert the aggregate's relationship to the log. Where an aggregate
folds one stream to decide what may happen next, a projection folds many streams
into something queryable and holds no authority at all. The abstract `Projection`
is only `handle(event)` and `reset()` -- deliberately trivial, because that pair
is the whole contract a read model owes the system: absorb an event, or throw
everything away and be rebuilt.

Production projections need more, and `CheckpointTrackingProjection` supplies
it: checkpoint persistence (`get_checkpoint`), lag reporting
(`get_lag_metrics`), retry around `_process_event`, a dead letter path, and a
`reset()` that truncates read models before replay. `DeclarativeProjection` adds
event routing and tenant filtering via `_should_process_event`;
`DatabaseProjection` adds the transactional guarantee that matters most -- the
read-model write and the checkpoint advance happen inside the same transaction,
so a crash cannot leave a projection that has applied an event but forgotten it
did. This is the layer where `reset()` and truncate-and-replay stop being
alarming: a cache that can be rebuilt from the log can afford to be thrown away.

### Subscriptions: keeping a projection fed

Nothing so far actually moves events from the store or the bus into a
projection. That is the subscription layer, and it is separate for a reason: the
question "what does this event do to my read model" and the question "where was
I when the process died" have different failure modes and different owners.
Fusing them is how projections end up with bespoke, half-correct catch-up loops.

`SubscriptionManager` (`subscriptions/manager.py`) is the entry point --
`subscribe`/`unsubscribe`, `start`/`stop`, `run_until_shutdown`, signal
registration, health and error aggregation -- and it delegates almost
everything. Registration lives in `registry.py`, start/stop in `lifecycle.py`,
draining in `shutdown.py`, retry and circuit breaking in `retry.py`,
backpressure in `flow_control.py`, pausing in `pause_resume.py`, and the
catch-up-to-live handoff in `transition.py` and `runners/`. The manager is a
facade over collaborators, not a god object, because each of those concerns has
its own lifecycle and its own tests.

Each subscription is a `Subscription` value object with an explicit state
machine -- `STARTING`, `CATCHING_UP`, `LIVE`, `PAUSED`, `STOPPED`, `ERROR` --
with `STOPPED` terminal and `ERROR` recoverable only by restarting into
`STARTING`. Note that `LIVE -> CATCHING_UP` is a legal transition: falling
behind is a normal condition, not a failure. This state machine is the
*operator-facing* view of a subscription, and it is worth separating in your
head from the finer-grained `TransitionPhase` machine discussed later, which
describes what happens inside the single move from `CATCHING_UP` to `LIVE`.

### Why the layers stack this way

Read the five together and the dependency direction is one-way. Aggregates
depend on the store's ordering and version check. Projections depend on being
handed events in order and on being able to record how far they got.
Subscriptions depend on both the store and the bus, and are the only layer that
depends on both. Nothing depends on a projection, which is what makes read
models disposable.

That single point of contact is also the load-bearing crack. The store can
replay history but does not push; the bus pushes but cannot replay. A
subscription is asked to present those two as one ordered stream, with no gap at
the join and nothing delivered twice out of order. Neither layer beneath it can
provide that on its own, and adding it to either -- retention to the bus,
push semantics to the store -- would mean giving up the property that makes that
layer worth having. So the seam is handled in the layer above, in the open,
which is what the rest of this document is about.

## The subscription problem: two event sources, one ordered stream

A subscription is asked for something neither layer beneath it can supply. Its
consumer -- a projection, a process manager, an integration hook -- wants a
single ordered stream of every event, starting from wherever it left off and
continuing forever. What it can actually be given is a store that remembers
everything but never speaks unless asked, and a bus that speaks constantly but
remembers nothing. The subscription's whole job is to make those two look like
one, and every unusual structure in `subscriptions/` is an artefact of doing
that safely.

### Why the event store and the event bus cannot be read as one source

The two interfaces are not merely different in convenience; they are different
in kind, and the differences are all in the same direction.

The store is addressable. `get_events` takes an aggregate id and a
`from_version` (plus optional timestamp bounds), `read_all` walks the whole log
from a `from_position`, and every read comes back as `StoredEvent` with
a `global_position` attached -- a single monotonic ordering over all streams,
whose current maximum the store will report on demand via
`get_global_position()`. You can ask it "what came after 4,271?" and get a
complete, ordered, repeatable answer.

The bus is none of those things. `EventBus.publish` takes a `list[DomainEvent]`
and a `background` flag and returns nothing; `subscribe` and
`subscribe_to_all_events` register a handler for what arrives *next*. There is
no position argument anywhere on the interface, no history, and -- crucially --
no position on the delivered object either. A bus subscriber is handed a
`DomainEvent`, not a `StoredEvent`, because position is a fact about
persistence and the domain event never carried one. `LiveRunner._get_event_position()`
reflects this honestly: it looks for a `_global_position` attribute that a
backend *may* have attached and returns `None` when it finds nothing. Ordering
across publishers is likewise the bus's business, not a promise the interface
makes, and handler exceptions on the bus are caught and logged so one bad
subscriber cannot starve the others -- correct for fan-out, useless as a record
of what you have consumed.

So the naive composition fails in both directions. Read the store to the end
and *then* subscribe to the bus, and you lose every event published in the gap
between the last read and the subscription taking effect -- a window with no
upper bound, since the read itself takes time proportional to history.
Subscribe to the bus first and *then* read the store, and you have no way to
splice: the live events arriving now belong after some point in the history you
have not read yet, and nothing on the bus tells you which point.

Nor can the problem be pushed down a layer. Giving the bus retention and
replay-from-position turns it into a second event store -- a second thing that
can be inconsistent with the first, and a hard requirement that every backend
(Redis pub/sub, RabbitMQ, in-memory) support durable ordered log semantics that
several of them fundamentally do not. Giving the store push semantics means the
store owns delivery, retries, and subscriber liveness, which is exactly the
weight the write path was kept clear of. Both fixes buy gap-free delivery by
destroying the property that made the layer worth having. The seam is therefore
resolved in the layer that already depends on both, where it can be written
down and tested.

### What "seamless catch-up to live" has to guarantee: no gaps, no reordering, at-least-once with duplicate suppression

"Seamless" is not a feeling; it is three specific obligations, and it is worth
being precise about which ones are absolute and which are best-effort, because
the design trades between them deliberately.

**No gaps.** Every event that exists in the store, and every event published
while the subscription is running, must reach the handler at least once. This
is the non-negotiable one. A projection is a fold over the log; drop a single
event and the read model is silently and permanently wrong, with no error
anywhere to indicate it. Everything else in the transition is subordinate to
this, which is why the design chooses to over-deliver rather than risk
under-delivering.

**No reordering.** Events must arrive in global-position order relative to each
other. Order matters even for projections that look commutative, because
"created then deleted" and "deleted then created" are not the same read model,
and a projection has no way to detect that it saw them backwards. The
transition is where this is most at risk: for a period, history is being read
from the store while newer events are arriving on the bus. The design's answer
is to refuse to interleave the two sources at all. Live events arriving during
the transition go into a buffer instead of the handler
(`LiveRunner.start(buffer_events=True)`), catch-up runs to a fixed target
(`CatchUpRunner.run_until_position(target_position=watermark)`), and only when
that finishes does the buffer drain in arrival order. There is never a moment
when both sources are feeding the handler concurrently.

**At-least-once, with duplicate suppression as a service, not a guarantee.**
Exactly-once delivery is not on offer, and pretending otherwise would be a
lie about a distributed system. Once you have decided that gaps are
unacceptable, the overlap between the two sources must be resolved in favour
of delivering twice -- which is what the design does, on purpose: the bus
subscription opens *before* catch-up finishes, so the events in the overlap
window are both buffered from the bus and read from the store. The library
then absorbs most of that overlap for you. `LiveRunner._process_live_event()`
compares the event's position against `subscription.last_processed_position`
and returns early for anything at or below it, counting the skip in
`stats.events_skipped_duplicate`. (Contrast the neighbouring filter path: an
event dropped by an event-type filter *does* still record its position, because
it was legitimately consumed rather than already seen.) `TransitionResult`
reports the suppression count as `buffer_events_skipped`, so the overlap is
observable rather than hidden.

That filter is only as good as the position it is given, and this is the
sharpest edge in the whole design. When `_get_event_position()` returns `None`
-- which it does for any bus backend that does not attach `_global_position`
to delivered events -- the comparison cannot run, and the event is processed.
The practical consequence is that handlers must be idempotent. Not "should
be", as a matter of hygiene: the transition is *designed* to produce
duplicates, and on some backends it cannot filter them. A projection that
increments a counter without checking whether it has already seen the event
will over-count across a restart. One that upserts by key will not. The
library takes responsibility for never losing an event and for never showing
you one out of order; it hands the last mile -- tolerating a repeat -- to you,
because only your read model knows what a repeat means.

The sections that follow trace how those three obligations produce the
structure: first why the work is split across three collaborators rather than
one loop, then the watermark protocol that sequences them, then the checkpoint
discipline that makes a crash mid-handoff recoverable.
## Why the lifecycle is split into three collaborators

The obvious implementation of "read history, then follow the stream" is one
object with a mode flag. `subscriptions/` instead contains `CatchUpRunner`
(`runners/catchup.py`), `LiveRunner` (`runners/live.py`), and
`TransitionCoordinator` (`transition.py`), and the split is not a matter of
file size -- it is that the two halves of the work have incompatible control
flow, and the moment that joins them belongs to neither.

**The two halves pull events in opposite directions.** `CatchUpRunner` is a
loop: it decides when to read, computes its own batch limit from
`target_position - last_processed_position`, calls `read_all()` with
`ReadOptions(direction=FORWARD, from_position=..., limit=...)`, walks the
returned `StoredEvent`s, and stops when it reaches the target or a batch comes
back empty. It owns its clock, so it can be paused mid-batch
(`wait_if_paused()` is checked both between batches and between events within a
batch), stopped at a safe point (`_stop_requested`), and slowed by
backpressure. `LiveRunner` inverts all of that. It does not loop at all; it
registers `_LiveEventHandler` instances via `event_bus.subscribe(...)` for each
type in `subscriber.subscribed_to()` and is thereafter *called*. It cannot
decide when the next event arrives, cannot decline one, and cannot read ahead.
Merging a puller and a callback target into one class means one of the two runs
in a mode where half its machinery -- batch limits and target positions on one
side, buffers and duplicate filters on the other -- is inert, and the flag that
says which is a permanent invitation to check the wrong branch.

**They also fail differently, and the difference is visible in their return
types.** `CatchUpRunner.run_until_position()` returns a `CatchUpResult` with
`events_processed`, `final_position`, `completed`, and a captured `error`: a
catch-up run is a *bounded job* that either reached its target or did not, and
the caller is expected to inspect the answer. `LiveRunner` has no equivalent
return value anywhere -- `start()` returns `None`, and the per-event path
accumulates into a mutable `LiveRunnerStats` (`events_received`,
`events_processed`, `events_skipped_duplicate`, `events_skipped_filtered`,
`events_failed`) that you sample rather than await. Following a stream has no
completion, so it has no result. One class cannot honestly have both shapes.

**The third collaborator exists because the handoff is a decision neither
runner is positioned to make.** `TransitionCoordinator.execute()` is the only
code that reads `event_store.get_global_position()` for the watermark, and it
does so *before* constructing anything. It then constructs `LiveRunner` and
starts it with `buffer_events=True`, constructs `CatchUpRunner` and calls
`run_until_position(target_position=self._watermark)`, calls `process_buffer()`,
and finally `disable_buffer()`. Every ordering constraint in the protocol lives
in that one method body, in the order the phases have to happen. The runners
know nothing of each other: `CatchUpRunner` takes an `EventStore` and no bus,
`LiveRunner` takes an `EventBus` and no store, and neither imports the other.
Both are handed the same `Subscription` and the same `CheckpointRepository`,
and that shared `Subscription` -- specifically `last_processed_position`,
advanced by `record_event_processed()` on the catch-up side and compared
against on the live side -- is the entire channel between them.

That last detail is what the split buys. The duplicate suppression inside
`LiveRunner._process_live_event()` (`position <= last_processed_position` ->
skip, counted in `events_skipped_duplicate`) is not coordination logic; it is a
local check against a value the subscription already carries. Because the
runners communicate only through that value, the coordinator can sequence them
without either runner exposing a "now switch" method, and each can be tested
against a single source -- a store with no bus, a bus with no store.

**Ownership of the failed handoff is the clinching argument.** When any step
raises, `execute()` sets `_phase = TransitionPhase.FAILED`, calls `_cleanup()`
to stop whichever runners are still running, and returns a `TransitionResult`
with `success=False` and `phase_reached` recording how far it got. Rolling a
half-finished handoff back means stopping *both* participants and reporting
where it broke -- which requires holding references to both, exactly what
neither runner has and what the coordinator was created to hold. Had catch-up
owned the transition, a catch-up failure would have to unwind a live
subscription it had opened as a side effect; had the live runner owned it, a
buffer that never drained would have no one to report to.

The cost is real and worth naming: three objects, a seven-value
`TransitionPhase` enum layered on top of the coarser `SubscriptionState`
machine, and a `Subscription` that two collaborators mutate. What it buys is
that each piece has one job with one shape of failure, and that the one
genuinely subtle part of the design -- the ordering of watermark, subscribe,
catch up, drain, commit -- is written down as a single readable sequence
instead of being distributed across mode flags. The subsections below take each
collaborator in turn, then return to why the coordinator, rather than either
runner, holds the handoff.

### CatchUpRunner: checkpoint-driven replay of the event store

`CatchUpRunner` (`runners/catchup.py`) is the half of the subscription that
does the pulling. Its entire public surface for doing work is one method --
`run_until_position(target_position)` -- and the shape of that signature is the
design in miniature: catch-up is a *bounded job*. It is given an end, it runs a
loop until it reaches that end or cannot, and it hands back a `CatchUpResult`
(`events_processed`, `final_position`, `completed`, `error`, plus a `success`
property that requires both `completed` and no error). Nothing about it is
open-ended, which is exactly what makes it composable with a transition that
needs to know when history has been consumed.

**The loop is driven by position, not by a cursor it holds.** The `while`
condition is `self.subscription.last_processed_position < target_position`,
guarded by `self._running` and `not self._stop_requested`. The runner keeps no
private notion of "where I am"; every iteration re-reads
`subscription.last_processed_position` and derives the next read from it.
`_process_batch()` computes `remaining = target_position - current_position`
and reads `min(self.config.batch_size, remaining)` -- so the batch limit
narrows as the target approaches and the runner never reads past its target,
even by one event. If `batch_limit <= 0` it returns 0, and a batch that returns
0 breaks the loop. That is the whole termination story: reach the target, or
find nothing more to read.

Locating progress in the shared `Subscription` rather than in the runner is
what makes the runner restartable and what makes it legible to `LiveRunner`.
The same field the catch-up loop advances via `record_event_processed()` is the
field the live side compares against when it decides whether a buffered event
is a duplicate. Neither runner has to be told about the other; they meet in one
integer.

**Reads go through `read_all()`, with retry, and with the tenant scope baked
in.** `_read_batch_with_retry()` builds a `ReadOptions(direction=FORWARD,
from_position=..., limit=..., tenant_id=self.config.tenant_id)`, drains the
async iterator into a list, and submits that as an operation to
`RetryableOperation` with `TRANSIENT_EXCEPTIONS` as the retryable set. A
transient store failure therefore costs a retry, not a failed catch-up; a
non-transient one propagates and is caught by `run_until_position`, which logs
it and returns `CatchUpResult(completed=False, error=e)` rather than raising.
Catch-up reports its failures as data, because the coordinator above it needs
to decide what to do about them.

**Per event, the sequence is filter, then flow-control slot, then deliver, then
record.** The filter is checked first, before delivery, and a filtered-out
event still calls `record_event_processed()` -- position advances even for
events this subscriber does not care about, because position is progress
through the *stream*, not a count of work done. Skipping the position update
for filtered events would mean a subscription with a narrow filter never
appearing to move, and re-reading the same span forever after a restart.
Delivery itself happens inside `async with await self._flow_controller.acquire()`,
so the number of in-flight events is bounded by `max_in_flight` (default 1000);
the position update happens inside that same block, before the slot is
released. Handler failure is routed by `continue_on_error` (default `True`):
either way `record_event_failed()` is called and failure metrics are recorded,
but only with `continue_on_error=False` does the exception escape and end the
run.

**Checkpointing is a policy the runner applies, not a thing it decides.** Three
strategies are honoured at three different places in the loop.
`EVERY_EVENT` saves after each delivered event. `PERIODIC` calls
`_maybe_save_periodic_checkpoint()`, which compares `time.monotonic()` against
`_last_checkpoint_time` and writes only once
`checkpoint_interval_seconds` (default 5.0) has elapsed. `EVERY_BATCH` writes
once at the end of `_process_batch()`, using the last `StoredEvent` seen --
and note the condition is `events_in_batch > 0 or events_filtered > 0`, so a
batch consisting entirely of filtered events still checkpoints its progress,
for the same reason those events advanced the position in the first place. All
three go through `_save_checkpoint_with_retry()`, which wraps
`checkpoint_repo.save_position(subscription_id=self.subscription.name, ...)` in
the same retry machinery as the reads. The subscription's *name* is the
checkpoint key, which is why both runners writing to the same key is a
consequence of the design rather than a coincidence.

The tradeoff between the three is entirely about what a crash costs versus what
steady-state throughput costs, and it is discussed under checkpoints below.
What matters structurally is that the runner has no opinion: it reads
`config.checkpoint_strategy` and does as it is told.

**Pause is checked twice, and that is deliberate.** `wait_if_paused()` is
awaited once per batch iteration and again before each event within a batch.
Checking only between batches would mean a pause request during a 100-event
batch waits for up to 100 handler invocations to drain; checking only within
batches would leave a paused subscription spinning through the outer loop.
Both call sites are followed by a `_stop_requested` re-check, because a
subscription paused for an operator and then stopped must not resume into
another batch on the way out. `stop()` itself only sets a flag -- the runner
finishes the event in hand and stops at the next of those checks, which is what
"graceful" means here: no torn event, and a checkpoint whose position is a
position that was really processed.

**What the runner deliberately does not do** is as informative as what it does.
It never touches the event bus -- its constructor takes an `EventStore`, a
`CheckpointRepository`, and a `Subscription`, and no bus at all. It never
decides what its target should be; `target_position` is a parameter, supplied
by the coordinator from `get_global_position()`. It never announces that it is
finished to anyone; it returns. Those three absences are what allow the same
object to serve both the initial catch-up over the whole history and the short
final catch-up during the handoff -- the second call is the same code with a
nearer target -- and they are what let it be tested against a store with no bus
in sight.

## The two sides: write side and read side

The library is split down the middle by a single question: *can this operation
still refuse?* On the write side the answer is yes, and that permission shapes
everything. On the read side the answer is no, and the absence of that
permission is why `projections/` is the largest and least obvious part of the
package.

### The write side can refuse

A write is a decision. An aggregate loaded from its history is asked to do
something, it validates against its own state, and it either records events or
raises. `AggregateRepository.save` then takes the uncommitted events and hands
them to the store with a version it computed itself -- `aggregate.version -
len(uncommitted_events)`, the version the aggregate was at before it decided --
and the store appends only if reality still matches that number. If another
writer got there first, `append_events` raises `OptimisticLockError` and
nothing durable has changed. The caller reloads and decides again.

That is the whole failure model, and it is cheap because it is *pre-commit*.
Everything the write side does happens before anything becomes fact. There is
no retry loop in `repository.py`, no dead letter queue, no checkpoint, because
none of those concepts have anything to attach to: a failed decision leaves no
residue to reconcile. The write side's one persistent obligation is ordering --
the version column is the concurrency control, the append is the commit point,
and the events that come out the other side are, from that moment on,
non-negotiable.

The seam between the two sides is visible in `save` itself. After a successful
append it marks the events committed, publishes them to the configured
publisher if there is one, and only then considers a snapshot. Publication is
deliberately *after* the commit and not inside it -- which is exactly the gap
the outbox pattern exists to close, and exactly the point past which refusal
stops being available.

### The read side cannot

A projection is handed an event that has already been accepted. It has no
version to check, no conflict to lose, and no way to make the event untrue by
raising. Its handler can still fail -- a read-model table is missing, a
downstream service times out, a NOT NULL constraint fires -- but the failure
says nothing about whether the event should have happened. It says only that
*this attempt* did not work. That difference is what forces the read side to
answer questions the write side never asks:

- **Should this be tried again, and when?** A `RetryPolicy` decides, and
  `_handle_with_retry` runs the attempt loop for `max_retries + 1` tries,
  sleeping `get_backoff(attempt)` between them.
- **What happens when trying again stops helping?** `send_to_dlq()` parks the
  event, and only then -- after the policy has said `should_retry` is false --
  does the loop `raise`.
- **Where is this projection up to?** `record_checkpoint()` runs after
  `_process_event` returns cleanly, so the checkpoint records attempts that
  succeeded rather than events that arrived.
- **How does the next attempt get a clean database?** `DatabaseProjection`
  overrides the retry loop rather than the handler, so each attempt gets its
  own session -- a necessity, not a nicety, once PostgreSQL has aborted the
  transaction.

Notice what the ordering buys and what it costs. Because the checkpoint moves
only after success, a crash between the read-model write and the checkpoint
write replays the event: at-least-once, and therefore idempotent handlers.
Because the exception is re-raised after the DLQ write, a caller is never told
that a parked event succeeded. Because the DLQ write happens before the raise,
the event survives even though the pipeline is about to report failure. None of
these questions has a write-side counterpart, because on the write side the
answer to all of them is "raise and change nothing".

### Why the shapes differ

It is tempting to ask why the read side does not simply adopt the write side's
discipline -- fail fast, let the caller deal with it. The answer is that the
caller on the read side is not a person holding a request; it is a subscription
runner draining a stream. "Let the caller deal with it" means "stall the
stream" or "silently drop the event", and the pipeline exists to offer a third
option: absorb the transient failures, park the permanent ones with enough
context to replay later, keep the position honest, and still tell the truth
upward.

The rest of this document is mostly about the read side for that reason. The
write side's contract fits in a paragraph -- decide, append at an expected
version, publish -- and the interesting decisions there (aggregate style,
snapshotting, locking) are single-topic arguments with homes of their own. The
read side's contract is a composition of five small collaborators whose
*order* is the design, and that composition is what the following sections take
apart.

## The write side: aggregates rebuilt from events

The write side has exactly one durable artifact: rows in the `events` table. No
aggregate state is stored there -- only `DomainEvent` instances, each carrying
its `aggregate_id`, `aggregate_type`, and the `aggregate_version` it produced.
Everything you can query about an aggregate at runtime is computed from those
rows on demand and thrown away afterwards. That is not a performance
compromise the library is apologising for; it is the property that makes the
rest of the design possible, and the three subsections below trace what it
costs and what it buys.

### Events as the source of truth

`DomainEvent` is a pydantic model declared `model_config = ConfigDict(frozen=True)`.
Frozen is the whole argument in one line: an event is a claim about something
that already happened, and there is no coherent operation that edits the past.
If a shipment was recorded against the wrong order, the remedy is another event
-- a correction, a reversal -- not a mutation of the first one. The frozen
config makes the wrong remedy a runtime error rather than a code review
question.

The base model carries more than the payload. Alongside `event_id`,
`event_type`, and `occurred_at` it holds `event_version` (the *schema* version
of that event type, for migrating shapes over time -- not to be confused with
`aggregate_version`, the position in the stream), and the provenance fields
`actor_id`, `correlation_id`, `causation_id`, and `metadata`. Those exist
because the log is the audit trail. If the row set is the only record, it has to
answer "who caused this, and in response to what" without help from an external
system, and it can only do that if the answer was captured at append time.

What actually lands in a row is a type name and a JSON payload, so the type name
has to stay stable and resolvable -- something must turn that pair back into the
right Python class years later, possibly in a process that never imports the
aggregate at all: a projection, a migration, a replay tool. Two mechanisms serve
that. `DomainEvent.__init_subclass__` derives `event_type` from the class name
when you do not set it, which removes the most common source of drift between
code and stored data; overriding it with a name that differs from the class logs
a warning (suppressible with `suppress_event_type_warning = True`), because the
usual reason for the mismatch is an accident and the usual consequence is an
event nobody can deserialise. Resolution in the other direction is explicit
rather than automatic: subclassing does *not* put the class in a registry. You
opt in with the `@register_event` decorator or `EventRegistry.register()`, which
populate the module-level `default_registry` behind `get_event_class()`.
Registering the same class twice is a no-op, but two different classes claiming
one `event_type` raises `DuplicateEventTypeError` -- the registry would rather
fail at import than silently decide which class a stored row means.

The explicitness is a trade: the price is that a replay process must import the
modules defining its events, and a name it has never seen raises
`EventTypeNotFoundError` instead of being guessed at. The gain is that the set
of deserialisable types is a deliberate declaration rather than a side effect of
whatever happened to be imported, and that a custom `EventRegistry` can be
passed where an isolated one is wanted -- in tests, or per bounded context.

### Aggregates as transient, derived state

`AggregateRoot` is generic over a `TState` pydantic model, and every instance
starts empty: `_version = 0`, `_state = None`, `_uncommitted_events = []`. State
arrives only through `apply_event`, which updates `_version` to the event's
`aggregate_version`, calls your `_apply` to fold the event into `_state`, and --
when `is_new=True` -- appends the event to the uncommitted list.
`load_from_history` is the same call in a loop with `is_new=False`, so replay
rebuilds state without re-queueing events that are already durable.

That single method doing double duty is the point. There is no separate "restore
from database" path that could drift from the "handle a new command" path; the
fold is written once and exercised on every load. `AggregateRepository.load`
constructs a *fresh* instance through the aggregate factory, replays the stream
into it, and hands it back; if the stream is empty and no snapshot exists it
raises `AggregateNotFoundError` rather than returning a blank object, because
"no events" and "an aggregate whose state happens to be empty" are different
claims (`load_or_create` is the opt-in for treating them the same). Two
concurrent loads of the same aggregate id produce two independent objects. There
is no identity map, no session cache, no shared mutable aggregate -- and
consequently nothing to invalidate when a competing writer appends.

Commands do not write. They validate against current state and call
`apply_event` -- usually indirectly, through `_raise_event` or `create_event`,
which fills in `aggregate_id`, `aggregate_type`, and `aggregate_version` from
the instance so the version arithmetic is not retyped at every call site. The
resulting events sit in `_uncommitted_events` until `repository.save(aggregate)`
appends them; if there are none, `save` is a no-op and returns immediately.
Buffering is what makes a multi-event command atomic: `append_events` takes the
whole list with one `expected_version`, so either every event a command produced
lands or none does. Only after that append reports success does the repository
call `mark_events_as_committed`, publish to the configured publisher, and
consider a snapshot -- the in-memory aggregate is not treated as authoritative
until the log agrees. Buffering also lets the aggregate check its own arithmetic
before touching the store: `apply_event` requires
`event.aggregate_version == self._version + 1` and raises `EventVersionError`
otherwise (or logs a warning, if the class sets `validate_versions = False`), so
a gap in the stream is caught in memory, before any I/O.

Rebuilding on every load is genuinely linear in stream length, and the library
does not pretend otherwise -- it offers snapshots instead. When a snapshot store
is configured, `load` asks the snapshot manager for a valid snapshot, restores
`_state` and `_version` from it, and replays only events after that version.
"Valid" includes a `schema_version` match: bump the aggregate's `schema_version`
when `TState` changes incompatibly and old snapshots are ignored rather than
misread. If restoring one raises anyway, `load` logs the failure, discards the
partially built instance, re-fetches from version 0, and replays in full. A
snapshot is a cache of a fold, and a cache that cannot be trusted is simply
skipped -- the same reflex that governs read models on the other side of this
document.

### Optimistic concurrency on the event stream (`expected_version`)

Nothing on the write path takes a lock in the ordinary sense. The guard is a
condition on the append. `EventStore.append_events` takes an `expected_version`,
and the repository computes it rather than asking you to:
`aggregate.version - len(uncommitted_events)`, the version the stream was at
when this aggregate was loaded. If the stream has moved since, the append fails
and `OptimisticLockError` is raised carrying `aggregate_id`, `expected_version`,
and `actual_version`. Nothing is written.

That error is the write-side one, from `eventsource.exceptions`. Keep it
separate in your head from `eventsource.readmodels.exceptions.OptimisticLockError`,
which is keyed by `model_id` and belongs to a different problem; the section
below on the two locks explains why they were never merged.

`ExpectedVersion` supplies three sentinels for the cases a plain integer cannot
express: `NO_STREAM` (0) asserts the aggregate does not exist yet -- the right
guard for a creation command, since two racing creates cannot both find an empty
stream; `STREAM_EXISTS` (-2) asserts it does; and `ANY` (-1) disables the check
entirely, which is a deliberate choice to accept interleaved writes and should
be rare on an aggregate you care about. All three stores branch on these three
constants in the same order before falling through to the plain integer
comparison, so the semantics do not drift between backends. Note that `0` is
doing double duty: it is both `NO_STREAM` and the honest "the stream was empty
when I loaded it" that the repository computes for a brand-new aggregate, which
is why those two cases behave identically rather than needing to be
distinguished.

Note also what the version is scoped by. Every store reads the current version
as `COALESCE(MAX(version), 0)` filtered on *both* `aggregate_id` and
`aggregate_type`, and the unique constraint is
`uq_events_aggregate_version` on `(aggregate_id, aggregate_type, version)`. The
stream is the pair, not the id alone; two aggregate types sharing an id keep
independent version sequences.

The PostgreSQL store enforces the check twice, and the redundancy is not
accidental. It first reads that `MAX(version)` and compares, which gives a
clean, well-attributed error in the common case. But that read and the
subsequent inserts are not one atomic step, so a concurrent writer can still
slip between them -- and when it does, the unique constraint rejects the insert.
The store catches the resulting `IntegrityError`, checks whether the constraint
name appears in the message, re-reads the current version, and raises the same
`OptimisticLockError`. Correctness rests on the constraint; the pre-check exists
for the error message. Delete the constraint and you have a race; delete the
pre-check and you have worse diagnostics. SQLite does the same thing with a
looser test -- it matches on "unique" plus `aggregate_id` or `version` in the
error text, because SQLite does not name the constraint in its message. The
in-memory store needs neither belt nor braces: it holds an `asyncio.Lock` for
the whole check-and-append, which is exactly the atomicity the database backends
have to reconstruct.

One thing the append is *not* guarding is duplicate delivery of the same event.
Before inserting, each store checks whether the `event_id` already exists and
skips it if so, so re-appending an event that is already durable is a silent
no-op rather than a constraint violation. That is deliberate on a path that may
be retried after an ambiguous failure, but it means a successful append does not
prove every event in the list was newly written.

Recovery is deliberately not automated. There is no retry loop inside `save`,
because the library cannot know whether your command is still valid against the
state that won the race -- a second withdrawal against a balance that just
dropped may need to be rejected, not reissued. The correct response is to reload
the aggregate, re-run the command against the state that actually exists, and
save again, deciding at each step whether the command still makes sense. The
consistency boundary this protects is exactly one aggregate: `expected_version`
says nothing about any other stream, which is the standard event-sourcing
bargain -- serialisability inside an aggregate, eventual consistency between
them.

---

## The read side: read models persisted and mutable

Cross the seam and every rule inverts. There is no fold at query time: a read
model is a row, `repo.get(id)` reads it and returns it, and nothing is
reconstructed from anything. `ReadModel` is a pydantic `BaseModel` whose
`model_config` is `from_attributes=True, populate_by_name=True` -- and, as the
comment in `readmodels/base.py` says in as many words, *not* frozen, because
projection handlers need to mutate field values before saving. That single
difference from `DomainEvent` is the read side's whole posture in miniature.

The base class contributes five fields and nothing else of substance: `id`
(a required `UUID`, no default -- read-model identity is assigned by the
projection, usually the aggregate id), `created_at` and `updated_at`
(`datetime.now(UTC)` factories), `version` (an `int`, default 1, constrained
`ge=1`), and `deleted_at` (`datetime | None`, default `None`, with
`is_deleted()` as the predicate). `custom_field_names()` exists precisely to
subtract that set back out when generating SQL, which tells you how the library
thinks of them: infrastructure the repository manages, not domain data your
handler writes. Timestamps and `version` are in fact repository-managed --
`save()` stamps `updated_at` and bumps `version` on update; your handler sets
neither.

The table a read model lands in is likewise derived rather than declared.
`table_name()` returns `__table_name__` if you set it and otherwise runs the
class name through `_camel_to_snake` and a small `_pluralize`, so `OrderSummary`
becomes `order_summaries` and `Address` becomes `addresses`. `generate_schema()`
walks `model_fields` and maps python types to columns per dialect --
`POSTGRESQL_TYPE_MAP` and `SQLITE_TYPE_MAP`, so a `Decimal` is
`DECIMAL(18, 6)` on PostgreSQL and `REAL` on SQLite, a `dict` is `JSONB` or
`TEXT` -- with `id` emitted as `PRIMARY KEY`, optional fields left nullable, and
a `json_schema_extra={"sql_type": ...}` escape hatch when the default mapping is
wrong. `generate_indexes()` always adds an index on `deleted_at` and then
whatever `__indexes__` declares. Deriving the schema from the class is only
defensible because the table is disposable; nobody would generate the `events`
table this way.

That is the load-bearing asymmetry, and it is worth stating plainly before the
subsections work through its consequences. Events are facts, so the write path
guards every append and never overwrites. Read model rows are a cache of a fold
that the log can reproduce, so the read path can afford things the write path
cannot: mutation in place, an unconditional upsert as the default save, `save()`
being tolerant of redelivery because writing the same derived row twice yields
the same row, and `truncate()` followed by replay as a legitimate repair.
Nothing in a read model is authoritative, which is what makes it cheap.

Two places this gets confusing are worth flagging now. The `version` column is
*not* the aggregate's version -- it counts saves of this row, is incremented by
the repository on every update, and has no relationship to any position in the
event stream. And `deleted_at` exists because deletion on this side arrives as
an event about the world rather than as a command against the row; the
repositories filter `deleted_at IS NULL` by default across `get`, `get_many`,
`find`, `exists`, and `count`, with named escape hatches for the cases that need
to see through it. Both get their own sections below.

### Why read models are not rebuilt on every query

The obvious question, once you accept that an aggregate is a fold over its
stream, is why a read model is not the same thing. If replaying events is good
enough to answer "what is the state of order 42" on the write side, why does the
read side keep a table at all?

Three answers, in increasing order of how much they constrain the design.

The first is cost, and it is the one everybody reaches for. Folding a stream is
linear in the number of events, and the write side already concedes this -- it
is why snapshots exist, and why `AggregateRepository.load` will restore a
snapshot and replay only the tail. A query path cannot make that concession.
Loading one aggregate to serve one command is a bounded cost paid at a rate the
domain controls; folding a stream on every page render is a cost paid at
whatever rate your traffic happens to be.

The second is shape, and it is the one that actually decides the matter. Look at
what the store can be asked: `get_events` and `read_stream` take an
`aggregate_id`; `get_events_by_type` takes an event type; `read_all` takes a
global position. There is no `where`. The log is indexed by stream and by
position because that is what appending and replaying need, and nothing more.
Meanwhile `Query` on the read side carries a list of `Filter`s (`eq`, `ne`,
`gt`, `gte`, `lt`, `lte`, `in_`, `not_in`), an `order_by` with a direction, and
`limit`/`offset` for pagination, and `PostgreSQLReadModelRepository` turns all
of that into one `SELECT` with a `WHERE` clause and an `ORDER BY`. "Twenty
shipped orders over $100, newest first" is not a question you can ask a log
without reading all of it, in every stream, and sorting the result in memory.
The read model is not a cached fold of one aggregate; it is a *different index
over the whole history*, and an index has to be materialised to be an index.

The third is that queries frequently span aggregates. An aggregate fold is
bounded by design -- `expected_version` guards exactly one stream, and that
boundary is the whole consistency story on the write side. A read model has no
such boundary. A row can be assembled from events raised by several aggregate
types, or several instances, and no single stream replay would produce it.

So the fold still happens; it just happens once per event rather than once per
query, and it happens ahead of time. That is what a `ReadModelProjection` is
for. Each event arrives, `handle()` opens a session and a transaction, builds a
repository bound to that connection, and dispatches to your `@handles` method,
which does the incremental step: `repo.save(...)` a new row, or `repo.get(id)`,
mutate a field, `save` it back. The expensive part -- reading history -- was
already paid by whatever delivered the event. What the handler adds is one row's
worth of work.

Moving the fold to write time is what pays for everything else on this side of
the seam. Because the row already exists, `get` is a primary-key read and
`find` is one indexed query, so the read path never touches the event store at
all. Because the fold is incremental, the model must be mutable -- a handler
that cannot assign to `summary.status` cannot express "shipped" as a
modification of what was there before, and would have to reconstruct the row
from scratch, which is the thing we were avoiding. Because the derived state now
lives in a second place, it can go stale or wrong, which is why `truncate()`
exists and why replaying the log through the projection is a legitimate repair
rather than a disaster recovery procedure. And because it can be rebuilt at any
time from a log that never forgets, none of it needs the protections the write
path has: no version-checked append, no refusal to overwrite, no ceremony around
losing a row.

The price is the one every materialised view charges. The row is only as current
as the last event the projection processed, so reads are eventually consistent
with the write side, and a command that appends an event followed immediately by
a query that reads its effect is a race. That gap is real and the library does
not hide it -- it gives you checkpoints to measure it and a rebuild path to
close it -- but it is the bargain: query cost is bounded and predictable,
freshness is not guaranteed.

---

## Read-side collaborators at a glance

The read side is not one class doing five jobs; it is five small pieces, each
answerable for one question, wired together by a single method. That shape is
recent and deliberate -- the module docstrings in `retry.py`,
`checkpoints.py`, and `dlq.py` (under `application/projections/`) all say the
same thing, that the extraction exists to undo a Single Responsibility
violation in which retry, checkpointing, and DLQ handling all lived inside
`CheckpointTrackingProjection`. What is left in the projection is the ordering
of those pieces, and only that.

Before the pipeline, there are three contracts that own no policy at all.
`Projection` (async `handle` / `reset`), `SyncProjection` (the same pair,
without `await`), and `EventHandlerBase` (`can_handle` plus `handle`) are bare
ABCs in `base.py`. Nothing about retries, checkpoints, or dead letters appears
in them, which is what makes them usable directly: a projection that needs none
of the machinery can subclass `Projection`, implement two methods, and be
registered alongside the heavyweight ones. `EventHandlerBase` differs from
`Projection` in kind rather than degree -- it is per-event-type and screens with
`can_handle` before it is called, so the registry can skip it, whereas a
`Projection` is handed every event and decides internally.

`CheckpointTrackingProjection` is the shell that turns those contracts into a
pipeline. It extends `EventSubscriber` and is abstract on `_process_event`,
which is where the subclass's actual read-model work goes. Its public `handle`
does almost nothing: opens a tracing span, delegates to `_handle_with_retry`.
That inner method is the composition, and it is short enough to hold in your
head -- loop `max_retries + 1` times; call `_process_event`; on success update
the checkpoint and return; on failure ask the policy `should_retry`; if yes,
sleep `get_backoff(attempt)` and go round again; if no, write to the DLQ, log
critical, and `raise`. Every collaborator below is reached from exactly one
line of that loop.

**`RetryPolicy` (`retry.py`)** answers *should we try again, and after how
long?* It is a `runtime_checkable` Protocol with three members --
`max_retries`, `get_backoff(attempt)`, `should_retry(attempt, error)` -- and
three implementations ship with it. `ExponentialBackoffRetryPolicy` delegates
to `calculate_backoff` from `subscriptions/retry.py`, so projections and
subscriptions share one backoff implementation. `NoRetryPolicy` reports
`max_retries == 0` and returns `False` unconditionally, which collapses the
loop to a single attempt -- the right choice in tests, where the alternative is
waiting out real `asyncio.sleep` calls. `FilteredRetryPolicy` wraps another
policy and refuses anything that is not an instance of a supplied exception
tuple, which is how you say "retry `ConnectionError`, never retry
`ValueError`".

One detail worth knowing before you reason about timing: there are two
different defaults in play. Constructing `ExponentialBackoffRetryPolicy()` with
no argument gives `max_retries=3`, `initial_delay=2.0`, and `jitter=0.0` --
deterministic, as the docstring says, and that is also what the module-level
`DEFAULT_RETRY_POLICY` is. But `CheckpointTrackingProjection.__init__`, when
you pass no `retry_policy`, does *not* use that constant; it builds its own
with `max_retries=2` (three attempts total) and `jitter=0.1`. So the backoff a
default projection actually experiences is jittered, not deterministic, and it
gives you one fewer attempt than the policy class's own defaults suggest.

**Checkpoint functions (`application/projections/checkpoints.py`)** answer
*where is this projection up to?* `record_checkpoint`, `read_checkpoint`,
`lag_metrics_dict`, and `reset_checkpoint` each take a `ProjectionCheckpoints`
repository and a `Tracer` as explicit parameters -- `CheckpointTrackingProjection`
passes its own `checkpoint_repo` and `tracer` through, and either can be `None`
(checkpoint tracking disabled) or omitted respectively.
`record_checkpoint` records `(projection_name, event_id, event_type)`; it is
called from exactly one place, immediately after `_process_event` returns
without raising, which is what makes the checkpoint mean "an attempt
succeeded" rather than "an event arrived". `lag_metrics_dict` is the
operational window: last processed id, latest relevant id in the store, lag in
seconds, count processed. `reset_checkpoint` deletes the checkpoint so the
projection replays from the beginning -- the rebuild primitive. These four
functions replace `ProjectionCheckpointManager`, which held no state beyond a
repository reference and a tracer (see [ADR 0024](adrs/0024-projection-persistence-ports.md)); span names still read
`eventsource.checkpoint_manager.*` deliberately, so existing dashboards keep
working.

**DLQ functions (`application/projections/dlq.py`)** answer *where does an
event go when trying again has stopped helping?* `send_to_dlq(repo,
projection_name, event, error, retry_count, tracer)` serialises the event with
`model_dump(mode="json")` and stores it next to the error and the attempt
count, so the entry is replayable rather than merely a log line. Its return
type is the interesting part: `bool`, not `None`. A successful write logs a
warning and returns `True`; a failed write is caught, logged with
`logger.critical` and `exc_info=True`, and returns `False`. It never raises.
That is a deliberate transfer of responsibility -- the pipeline's job at that
moment is to re-raise the *original* handler error, and a secondary failure in
the parking lot must not displace it. The cost is that the return value is
currently ignored by `_handle_with_retry`, so a lost event is visible only in
the logs. `send_to_dlq` replaces `ProjectionDLQManager` for the same reason
`record_checkpoint` replaces `ProjectionCheckpointManager`; its span names
still read `eventsource.dlq_manager.*` deliberately.

**`ProjectionCoordinator`, `ProjectionRegistry`, and `SubscriberRegistry`
(`coordinator.py`)** answer *who gets this event?* The three are layered.
`ProjectionRegistry` holds projections and handlers and fans an event out to
all of them with `asyncio.gather(..., return_exceptions=True)` -- concurrent,
and explicitly non-propagating: each returned exception is logged with the
offending projection's class name and then dropped, so one broken projection
cannot stall its siblings, and equally cannot tell the caller it is broken.
`SubscriberRegistry` is the narrower variant for `EventSubscriber`
implementations, routing by `subscribed_to()` so a subscriber is only invoked
for event types it declared. `ProjectionCoordinator` sits above a registry and
adds the batch-shaped operations -- `dispatch_events`, `rebuild_all`,
`rebuild_projection`, `catchup`, `health_check` -- with `batch_size` and
`poll_interval_seconds` as its knobs. Note the ordering asymmetry that runs
through all three: fan-out *within* one event is concurrent, but
`dispatch_many` walks the list sequentially, because event order is the one
thing the read side must not reorder.

Every one of these is injectable. `CheckpointTrackingProjection.__init__` takes
`checkpoint_repo`, `dlq_repo`, `retry_policy`, and `tracer`, each defaulting to
an in-memory or no-op implementation. That default set is what makes a
projection usable in a unit test with no database and no Docker; it is also why
a projection that is never given real repositories will happily checkpoint into
a dictionary that vanishes on restart.

---

### Projection / SyncProjection / EventHandlerBase — the minimal contracts

Three of the classes in `projections/base.py` carry no policy at all.
`Projection`, `SyncProjection`, and `EventHandlerBase` are plain ABCs with two
abstract methods each and no `__init__`. Nothing in them mentions retries,
checkpoints, dead letters, tracing, or tenants. That emptiness is the point:
they are the contracts the rest of the read side is built *on*, not built
*into*, and a projection that needs none of the machinery can satisfy one of
them in a dozen lines.

`Projection` is the async contract -- `async handle(event)` and `async
reset()`. It is handed every event dispatched to it and decides internally
whether the event is interesting, typically with an `isinstance` check. `reset`
is the rebuild primitive at this level: clear the read model so it can be
refolded from the start of history. It is also the only one of the three
exported from the top-level `eventsource` package; `SyncProjection` and
`EventHandlerBase` must be imported from `eventsource.application.projections`.

`SyncProjection` is the same pair of methods without `await`. It exists for
projections that do no I/O -- in-memory counters, test doubles, anything whose
"read model" is a dictionary -- and for callers that are not running inside an
event loop. It is deliberately *not* a subclass of `Projection` and not
adaptable to it: `ProjectionRegistry.register_projection` builds its fan-out by
collecting `projection.handle(event)` coroutines and awaiting them with
`asyncio.gather`, so a `SyncProjection` handed to the registry would contribute
a `None` to that list rather than an awaitable. If you want a synchronous
projection on the dispatch path, wrap it in an async `Projection` yourself and
decide there whether the work belongs on the loop or in a thread. The library
does not make that choice for you, because the right answer depends on whether
your synchronous handler blocks.

`EventHandlerBase` differs from `Projection` in kind rather than in degree. It
adds a synchronous `can_handle(event) -> bool` in front of an async
`handle(event)`, and drops `reset` entirely. The split matters at dispatch
time: `ProjectionRegistry._dispatch_internal` calls `can_handle` first and only
then builds the coroutine, so a handler that declines an event never has a task
created for it. A `Projection`, by contrast, always gets a task and always gets
called. So the two express different intents. A projection owns a read model,
sees the whole stream, and can be rebuilt. A handler reacts to particular event
types -- send the email, call the webhook, enqueue the job -- has no state of
its own to clear, and says up front what it will accept. The missing `reset` is
the honest signal here: side effects on the outside world are not replayable,
so the contract does not pretend they are.

Two things follow from keeping these ABCs bare. First, they are the seam at
which the heavyweight pieces attach rather than being mandatory:
`CheckpointTrackingProjection` does not extend `Projection` at all -- it
extends `EventSubscriber` from `protocols.py`, which adds `subscribed_to()` for
type-based routing through `SubscriberRegistry` -- so "projection" in this
library names a shape, not a single inheritance chain, and the registry accepts
both shapes through separate registration methods. Second, the escape hatch is
real. If the retry/checkpoint/DLQ pipeline is not what you want -- because your
subscription runner already owns positions, or because your read model is
idempotent by construction and a failure should simply propagate -- subclassing
`Projection` directly is a supported choice rather than a way of working around
the library. You give up the checkpoint, the retries, and the dead letter
queue, and you take back the two methods' worth of behaviour that they were
managing on your behalf.

### CheckpointTrackingProjection (base.py) — the orchestrating shell

`CheckpointTrackingProjection` is where the read side's five collaborators
become one behaviour. It is worth being precise about how little it does
itself: it owns no read model, performs no I/O of its own, and defines no
retry, checkpoint, or dead-letter logic. What it owns is an *order*. Every
piece of policy is reached from a single line of a single method, and if you
understand that method you understand the class.

#### What it inherits and what it demands

It extends `EventSubscriber` — not `Projection`. That choice is load-bearing:
`EventSubscriber` adds an abstract `subscribed_to() -> list[type[DomainEvent]]`
alongside `handle`, which is what lets `SubscriberRegistry` route by declared
event type instead of handing every event to every projection. So a
checkpoint-tracking projection is registered through a different path than a
bare `Projection`, and it announces its interests up front.

From the subclass it demands exactly one thing: `_process_event(event)`, the
abstract method where your read-model work goes. Note the signature — a single
event argument, no connection. The class docstring's example shows
`_process_event(self, conn, event)`, which describes what `DatabaseProjection`
later does with the handshake rather than the contract declared here; the same
staleness affects `_truncate_read_models`, whose real signature on this class
takes no arguments. Overriding `_truncate_read_models()` is optional and
defaults to a no-op, which means a projection that never overrides it will
happily "reset" by clearing its checkpoint and leaving stale rows in place.

#### The constructor is where the defaults bite

Four collaborators are injected: `checkpoint_repo`, `dlq_repo`, `tracer`, and
`retry_policy`. `tracer` defaults to whatever `create_tracer(__name__,
enable_tracing)` returns (a no-op unless you opt in), and `retry_policy`
defaults to a locally constructed `ExponentialBackoffRetryPolicy`.
`checkpoint_repo` and `dlq_repo` default to `None`, and `None` means the
concern is disabled, not "construct an in-memory repository for me" ([ADR
0024](adrs/0024-projection-persistence-ports.md)): with `checkpoint_repo=None`
no checkpoint is written and `get_checkpoint()` / `get_lag_metrics()` return
`None`; with `dlq_repo=None` a permanently failed event is logged at
`critical` and re-raised, with no DLQ write attempted. Both are stored as
plain attributes -- `self._checkpoint_repo`, `self._dlq_repo` -- alongside
`self._projection_name`, which is simply `self.__class__.__name__`. That
naming choice is quiet but consequential — the checkpoint key is your class
name, so renaming a projection class orphans its checkpoint and the
projection replays from the beginning.

An in-memory default used to exist here, so a projection was constructible in
a unit test with no database and no Docker, but it was a production footgun:
a projection that never gets handed real repositories still *looks* durable
from the outside -- `get_checkpoint()` returns a value, `get_lag_metrics()`
returns real-looking numbers -- while silently checkpointing into a dictionary
that vanishes on restart and reprocessing the entire event stream every time
the process comes back up. Tests that want the old behavior pass
`InMemoryCheckpointRepository()` / `InMemoryDLQRepository()` explicitly.

The retry default deserves its own warning, because there are two of them and
they disagree. `ExponentialBackoffRetryPolicy()` constructed with no argument —
and the module-level `DEFAULT_RETRY_POLICY` — gives `max_retries=3`,
`initial_delay=2.0`, `jitter=0.0`. `CheckpointTrackingProjection.__init__` does
not use that constant. It builds its own `RetryConfig(max_retries=2,
initial_delay=2.0, exponential_base=2.0, jitter=0.1)`. So the effective default
for a projection is **three attempts with jittered backoff**, not four
deterministic ones. If you are reasoning about worst-case dispatch latency or
writing a test that asserts on sleep durations, that is the config that
applies.

#### handle() delegates; \_handle\_with\_retry() is the design

The public `handle` opens one span —
`eventsource.projection.handle`, tagged with projection name, event type, and
event id — and immediately delegates to `_handle_with_retry(event, span)`.
Nothing else. Keeping the tracing wrapper separate from the loop is what makes
the loop overridable in isolation, which is precisely what
`DatabaseProjection` goes on to do.

The loop itself runs `max_attempts = retry_policy.max_retries + 1` times, and
each pass is the whole pipeline:

```python
for attempt in range(max_attempts):
    try:
        await self._process_event(event)             # 1. do the work
        if self._checkpoint_repo is not None:
            await record_checkpoint(...)              # 2. only then advance
        return                                        # 3. success ends the loop
    except Exception as e:
        if not self._retry_policy.should_retry(attempt, e):
            if self._dlq_repo is not None:
                await send_to_dlq(...)
            logger.critical(...)
            raise                                     # 4. caller still sees the error
        await asyncio.sleep(self._retry_policy.get_backoff(attempt))
```

Read that as five commitments rather than five statements.

The checkpoint update sits *inside* the `try`, immediately after
`_process_event` returns and before the `return`. That placement is the reason
the checkpoint means "an attempt succeeded" and not "an event arrived" — and
also the reason a failure in `record_checkpoint()` itself is caught by the
same handler and retried as though the projection work had failed, which will
re-run `_process_event` on the next pass.

The policy, not the loop, decides whether to continue. `should_retry(attempt,
e)` receives both the attempt index and the exception, which is what makes
`FilteredRetryPolicy` possible: the loop asks a question and obeys the answer,
so "never retry `ValueError`" is a policy substitution rather than a subclass.

The DLQ write happens only on the branch where the policy has said no, and it
happens *before* the `raise`. The event is durably parked with its error and
attempt count before the pipeline reports failure upward. Because
`send_to_dlq` returns `bool` and never raises, a failed park cannot displace
the original exception — but the loop ignores that return value, so a lost
event is visible only as the manager's own `logger.critical`.

The bare `raise` re-raises the original handler exception with its traceback
intact. A caller is never told that a parked event succeeded; parking is a
durability measure, not a swallow. Every failed attempt is logged at `error`
with `exc_info=True` and structured `extra` fields; the terminal one adds a
`critical` line naming the DLQ.

Two smaller things fall out of the structure. `asyncio.sleep` runs in the
dispatching task, so backoff blocks whatever is feeding this projection — with
the default config, a permanently failing event costs roughly six seconds of
that task's time before it is parked. And the retry counter is per-`handle`
call, not per-event: a redelivered event arrives with a fresh budget.

#### The rest of the surface

Three read-only conveniences delegate straight through to the managers.
`get_checkpoint()` returns the last processed event id, `get_lag_metrics()`
calls the checkpoint manager with `[et.__name__ for et in self.subscribed_to()]`
so lag is measured against only the event types this projection declared, and
`projection_name` exposes the class-derived key. `reset()` logs a warning,
clears the checkpoint through the manager, then calls
`_truncate_read_models()` — in that order, so a crash between the two leaves a
projection with no checkpoint and a populated read model, which on restart
replays history into non-empty tables. Idempotent handlers make that
survivable; that is not a coincidence but the same requirement showing up in a
second place.

### RetryPolicy (retry.py) — when to try again and how long to wait

`_handle_with_retry` asks two questions on every failure and answers neither
itself: *should there be another attempt?* and *how long should we wait first?*
Both are delegated to a `RetryPolicy`. The loop's job is to obey; the policy's
job is to decide. That separation is the whole reason `retry.py` exists — its
module docstring says as much, naming the Single Responsibility violation it
was extracted to undo.

#### The contract is three members wide

`RetryPolicy` is a `runtime_checkable` `Protocol`, not an ABC, so anything with
the right shape qualifies without importing the library's base class:

- `max_retries: int` — retries *excluding* the initial attempt. The loop runs
  `max_retries + 1` times, so `max_retries=3` means four attempts.
- `get_backoff(attempt: int) -> float` — seconds to wait, with `attempt`
  0-based (after the first failure, `attempt == 0`).
- `should_retry(attempt: int, error: Exception) -> bool` — the veto.

Note that `should_retry` receives the exception, not just the counter. That is
what makes the loop indifferent to *why* something is unretryable: "we are out
of attempts" and "this exception is never worth retrying" are both expressed as
`False`, and the pipeline treats them identically — park it, log critical,
re-raise. The alternative design, a loop that inspects exception types itself,
would have baked one exception taxonomy into every projection.

Note also what is *absent*: nothing in the protocol knows about events,
checkpoints, dead letters, or databases. A policy is a pure decision function
over `(attempt, error)`, which is why the three shipped implementations are
under thirty lines each and testable without any of the rest of the read side.

#### Three implementations, three intents

**`ExponentialBackoffRetryPolicy`** is the default shape. It holds a
`RetryConfig` and forwards `get_backoff` straight to `calculate_backoff` from
`subscriptions/retry.py` — deliberately the same function the subscription
runtime uses, so there is one backoff implementation in the codebase rather
than two that drift. `should_retry` is `attempt < max_retries`: retry
everything until the budget runs out. Its own constructor default is
`max_retries=3`, `initial_delay=2.0`, `max_delay=60.0`, `exponential_base=2.0`,
`jitter=0.0`, giving 2s, 4s, 8s. The module-level `DEFAULT_RETRY_POLICY` is an
instance of exactly that.

**`NoRetryPolicy`** reports `max_retries == 0`, returns `0.0` backoff, and
returns `False` from `should_retry` unconditionally. It collapses the loop to a
single attempt that goes straight to the DLQ on failure. Its main use is
testing failure handling: with the default policy, asserting that an event
reaches the dead letter queue costs six real seconds of `asyncio.sleep`. It is
also the right choice when retries genuinely belong at a higher level — a
subscription runner or broker that will redeliver anyway.

**`FilteredRetryPolicy`** is composition rather than a strategy of its own. It
wraps a base policy and a tuple of exception types, and `should_retry` returns
`False` immediately unless `isinstance(error, retryable_exceptions)`, otherwise
deferring to the base. Pairing it with `TRANSIENT_EXCEPTIONS` from
`subscriptions/retry.py` (`ConnectionError`, `TimeoutError`,
`asyncio.TimeoutError`, `OSError`) expresses the distinction that actually
matters on the read side: a dropped connection is worth waiting out, a
`ValidationError` on the event payload will fail identically on attempt four,
and burning eight seconds to confirm that only delays the stream. `max_retries`
and `get_backoff` are delegated untouched, so the wrapper changes *whether*
without touching *how long*.

#### The two defaults disagree, and the projection's wins

This is the detail most likely to mislead you when reasoning about timing.
`ExponentialBackoffRetryPolicy()`'s own default config — the one its docstring
describes as "deterministic for projection processing", and the one behind
`DEFAULT_RETRY_POLICY` — uses `max_retries=3` and `jitter=0.0`. But
`CheckpointTrackingProjection.__init__` does not use that constant. When you
pass no `retry_policy`, it constructs a *different* config inline:

```python
ExponentialBackoffRetryPolicy(
    config=RetryConfig(
        max_retries=2,        # 3 total attempts, not 4
        initial_delay=2.0,
        exponential_base=2.0,
        jitter=0.1,           # jittered, not deterministic
    )
)
```

So a projection you construct without arguments gets **three attempts with
jittered backoff**: roughly 2s ± 0.2s, then 4s ± 0.4s, then the DLQ — about six
seconds of the dispatching task's time before a permanently failing event is
parked. The jitter is real; `calculate_backoff` adds
`random.uniform(-jitter_range, jitter_range)` whenever `jitter > 0`. If you
want the deterministic behaviour the policy class advertises, you have to ask
for it explicitly by passing `DEFAULT_RETRY_POLICY` or your own
`RetryConfig(jitter=0.0)`. Tests that assert on exact sleep durations must do
one of those, or use `NoRetryPolicy` and sidestep the question.

#### What the policy cannot decide

Three limits are worth naming, because they are properties of the loop rather
than of any policy you could write.

The backoff is a plain `await asyncio.sleep` in the dispatching task. A policy
can shorten the wait but cannot make it non-blocking; whatever is feeding this
projection is stalled for the duration. That is a deliberate simplification —
deferring the event and moving on would reorder the stream, and ordering is the
one thing the read side must preserve.

The attempt counter is per-`handle` call, not per-event. The policy sees
`attempt` reset to 0 every time an event arrives, so a redelivered event gets a
fresh budget and there is no persistent "this event has now failed nine times"
signal for a policy to act on. Cross-delivery accounting, if you need it, lives
in the DLQ contents or in your subscription runtime.

And a policy is consulted only on the failure path. It has no say in what
happens on success, no hook before the first attempt, and no way to short-
circuit the DLQ write — `send_to_dlq` runs whenever `should_retry` returns
`False`, which is precisely why `FilteredRetryPolicy` returning `False` for a
`ValidationError` parks that event rather than discarding it. "Do not retry"
and "do not record" are different statements, and the protocol can only make
the first.

### Checkpoint functions (application/projections/checkpoints.py) — where the projection is

A checkpoint is the read side's answer to "if this process dies now, where does
the next one start?" Four module-level functions own that answer — like
`RetryPolicy`, they were extracted from `CheckpointTrackingProjection` for the
reason the module docstring names outright: the projection was doing too many
jobs. Each function is small on purpose: it takes a `ProjectionCheckpoints`
repository, a projection name, and a `Tracer` as explicit parameters, wraps one
call in a tracing span and a log line, and forwards. None of the four hold
state of their own, and none contain policy — that is what replaces
`ProjectionCheckpointManager` (see [ADR 0024](adrs/0024-projection-persistence-ports.md)), which held the same
repository reference and tracer as instance state but decided nothing either.

#### Four operations, one key

The projection name passed to every call is the key for everything these
functions do, and it comes from `self.__class__.__name__` when
`CheckpointTrackingProjection` calls them. Every call below is scoped to that
one string.

`record_checkpoint(repo, projection_name, event, tracer)` is the write. It
calls `update_checkpoint(projection_name, event_id, event_type)` on the
repository and logs at `debug`. It is invoked from exactly one place in the
whole library — the line immediately after `_process_event` returns without
raising — and that single call site is what gives the checkpoint its meaning.
It records *the last event an attempt succeeded on*, not the last event that
arrived, and not the last event the store holds.

`read_checkpoint(repo, projection_name, tracer)` is the read, returning
`str(event_id)` or `None`. Note the type change: the repository deals in
`UUID`, the function hands back a string.

`lag_metrics_dict(repo, projection_name, event_types, tracer)` is the
operational window, returning a plain dict — `projection_name`,
`last_event_id`, `latest_event_id`, `lag_seconds`, `events_processed`,
`last_processed_at` — or `None` when no checkpoint exists yet. The `None` is
worth internalising: a projection that has never successfully processed
anything is indistinguishable, through this API, from one that does not
exist. There is no "zero events, infinitely behind" reading.

`reset_checkpoint(repo, projection_name, tracer)` deletes the checkpoint and
logs at `info`. That is the rebuild primitive: with no checkpoint, the
projection starts from the beginning of history. It does not touch your read
model — `CheckpointTrackingProjection.reset` calls `_truncate_read_models()`
separately, and in that order.

All four are no-ops from the projection's point of view when
`checkpoint_repo=None`: `CheckpointTrackingProjection` checks
`self._checkpoint_repo is not None` before calling `record_checkpoint`, and
`get_checkpoint()` / `get_lag_metrics()` short-circuit to `None` without
calling `read_checkpoint()` / `lag_metrics_dict()` at all.

#### An event id is a position only if something can order it

The checkpoint stores a UUID, not an offset. On its own a UUID says nothing
about *how far* along the stream you are — it is a bookmark that only the store
can dereference. That is why lag is computed in the repository rather than in
the calling function, and why the two shipped repositories give such different
answers.

`SQLCheckpointRepository.get_lag_metrics` runs a single query that finds the
most recent event whose type is one of `event_types`, joins it against the
checkpoint row, and computes the elapsed time between that event and the
checkpoint's `last_processed_at` (the PostgreSQL dialect via `EXTRACT(EPOCH
FROM (le.max_time - pc.last_processed_at))`; SQLite via the equivalent
dialect-resolved expression). So the lag reported is **wall-clock distance
between when the newest relevant event was written and when this projection
last committed a checkpoint** — a staleness measure, not a count of unprocessed
events. Two guards flatten it to `0.0`: when `last_event_id` equals
`latest_event_id` (caught up), and when the raw value is negative (the
projection checkpointed after the newest event's timestamp, which clock skew
and out-of-order writes both produce).

`InMemoryCheckpointRepository.get_lag_metrics` cannot do any of that. It has no
event store to look at, so it returns `latest_event_id=None` and
`lag_seconds=0.0` unconditionally, with a comment saying as much. The
`event_types` argument is accepted and ignored. Since `checkpoint_repo=None`
is now the constructor default (not an in-memory repository), reaching this
path requires deliberately passing `InMemoryCheckpointRepository()` — but if
you do, the same caveat applies: that zero is a placeholder rather than a
measurement.

The `event_types` list itself comes from the projection:
`get_lag_metrics()` on `CheckpointTrackingProjection` passes `[et.__name__ for
et in self.subscribed_to()]`. Lag is therefore measured only against event
types the projection declared an interest in, which is the right denominator —
a projection that handles two of your forty event types should not appear
behind because the other thirty-eight are busy. The corollary is that
`subscribed_to()` doing double duty, as both routing declaration and lag
filter, means a projection that under-declares gets flatteringly low lag
numbers.

#### Counting, concurrency, and the other position API

`events_processed` is incremented by the repository, not the calling function —
`+ 1` on every `update_checkpoint`, in the SQL `ON CONFLICT` clause and in the
in-memory dict alike. Because `record_checkpoint` is called once per
*successful* attempt, the counter follows the same rule the checkpoint does:
it counts successes, and a redelivered event that succeeds twice increments it
twice. It is a throughput signal, not a distinct-event count.

The in-memory repository guards its dict with an `asyncio.Lock`, so concurrent
updates within one process are serialised. Nothing in these functions or
either repository coordinates *across* processes: two instances of the same
projection class share a checkpoint key and will overwrite each other's
position. Running one projection in more than one place is a
subscription-runtime concern, not something the checkpoint layer solves.

Finally, the composed `CheckpointRepository` protocol carries a second,
parallel position API that these functions never touch:
`get_position(subscription_id)` and `save_position(subscription_id, position,
event_id, event_type)` from `SubscriptionPositions` — a segregated port in its
own right (ADR 0024), backed by a `global_position` column. That pair exists
for the subscription runtime, which resumes from an integer offset rather than
an event id. Be aware they share storage but not discipline —
`update_checkpoint` on the in-memory repository rebuilds `CheckpointData`
without carrying `global_position` forward, so a projection checkpointing
through `record_checkpoint` will clear a position previously saved by a
subscription under the same key. Keep the two names distinct unless you have
checked the backend you are using.
