# Core Hexagon Design

**Date:** 2026-07-29
**Status:** Approved (brainstorm complete, pending implementation plan)
**Sub-project:** 1 of 3 in the hexagonal architecture redesign

## Context

eventsource-py has no external users yet, which makes this the cheapest possible
moment for a full store-contract redesign. The trigger was a capability mismatch:
`EventStore` requires global-ordering methods (`read_all`, `get_global_position`)
that wide-column, document, and log-structured backends cannot honestly provide.
The interface already admits this inconsistently — `read_all` has a non-abstract
default that raises `NotImplementedError` while `get_global_position` is
`@abstractmethod`, and the `read_stream` default fabricates `global_position=0`.

Decisions made during brainstorming:

- **Scope:** full store contract review, executed as hexagonal architecture
  (ports & adapters) with hard DDD and SOLID discipline.
- **Design constraints:** the contract must accommodate four backend families —
  SQL (PostgreSQL, SQLite, MySQL, CockroachDB), wide-column/partitioned
  (Cassandra, ScyllaDB, DynamoDB), document (MongoDB change streams), and
  purpose-built ES/log systems (EventStoreDB, Kafka-as-store).
- **Positions:** opaque ordered tokens for the global feed; integer versions
  per stream (required for optimistic concurrency).
- **Capability model:** small composed `Protocol` classes (structural typing),
  no ABC inheritance ladder.
- **Definition of done:** new contract designed and documented; the three
  existing backends (memory, SQLite, PostgreSQL) plus conformance suites ported
  to it; full test suite green. NoSQL adapters are future work validated by
  this contract.

The redesign decomposes into three sub-projects, each with its own spec, plan,
and implementation cycle:

1. **Core hexagon** (this spec): `domain/` + `ports/` layers, store contract,
   memory/sqlite/postgresql adapters, conformance restructure.
2. **Application layer:** subscriptions, projections, migration re-architected
   as application services consuming ports; catch-up consumes `GlobalEventFeed`.
3. **Context packaging:** repositories, bus, locks, multitenancy, GDPR folded
   into the adapter structure; import-linter layer contracts locked down.

`.claude/rules/architecture.md` already encodes the target rules and governs
all new and modified code during the transition.

## Package Architecture

Target layout (sub-project 1 builds `domain/`, `ports/`, and the three store
adapters; later sub-projects fill in the rest):

```
src/eventsource/
  domain/            # DomainEvent, EventRegistry, domain VOs (StreamId, TenantId),
                     #   domain exceptions. Pure: stdlib + pydantic only.
  ports/             # Capability protocols + port-level VOs. Depends on domain only.
    positions.py     #   Position, ExpectedVersion
    envelopes.py     #   EventEnvelope, AppendResult, read-option VOs
    store.py         #   EventAppender, StreamReader, GlobalEventFeed, EventTypeQuery
    snapshots.py     #   SnapshotStore port + Snapshot VO
    repositories.py  #   Checkpoint/DLQ/Outbox ports (sub-project 3 moves impls)
    bus.py           #   Publisher/Subscriber ports, EventPublisher
  application/       # (sub-project 2) aggregates, projections, subscriptions, migration
  adapters/
    _sql/            # Shared SQL helpers: dialect machinery, int-backed Position codec
    memory/          # store.py, snapshots.py — all four store ports, dict-backed
    sqlite/          # store.py, snapshots.py
    postgresql/      # store.py, snapshots.py
    redis/ kafka/ rabbitmq/   # (sub-project 3)
  testing/           # Conformance suites, one per port
```

Properties:

- Dependencies point inward only: adapters → application → ports → domain.
- `ports/` + `domain/` have zero dependencies beyond stdlib and pydantic. This
  **is** the Tier 0 surface `docs/core-surface.md` describes; the long-standing
  blocker (protocol definitions colocated with SQLAlchemy implementations in
  `repositories/`) is resolved structurally by this layout.
- Old module paths (`stores/`, `snapshots/`) become thin re-export shims marked
  `# TRANSITION`, kept until sub-project 2 retypes the application layer, then
  deleted. Existing tests keep passing through the shims.
- Migration SQL files are untouched: same schema, same queries.

## Value Objects

All frozen (pydantic frozen models or frozen dataclasses). Domain-owned concepts
live in `domain/`; port payloads live in `ports/`.

### Position (`ports/positions.py`)

The opaque global-feed token. Guarantees:

- **Total ordering within one store**: `__lt__`/`__eq__` etc. defined; ordering
  laws (transitivity, antisymmetry, trichotomy) hold for tokens from the same
  store.
- **Serialization round-trip**: `to_str()` / `from_str()`; this string is what
  checkpoints persist. `from_str` on malformed input raises
  `PositionDecodeError` (never a bare `ValueError` from internals).
- **No arithmetic**: consumers compare and persist; they never add, subtract,
  or measure distance.
- **Store identity**: tokens embed a short `store_id` discriminator. Comparing
  positions from different stores raises `PositionForeignError` rather than
  silently misordering.

Adapter representations: SQL family backs it with an int (codec in
`adapters/_sql/`); future adapters use resume tokens (Mongo), partition-offset
vectors (Kafka), or synthetic hybrid keys (Cassandra) without contract changes.

### ExpectedVersion (`ports/positions.py`)

Per-stream concurrency stays integer-based (plain `int` in signatures) — every
candidate backend can implement a per-stream monotonic version, and optimistic
locking needs exact comparison. `ExpectedVersion` becomes a proper VO with three constructors:
`ExpectedVersion.any()`, `ExpectedVersion.no_stream()`,
`ExpectedVersion.exact(n)`. `append` checks it and raises `OptimisticLockError`
on mismatch.

### StreamId (`domain/`)

Replaces the stringly-typed `"{aggregate_id}:{AggregateType}"` convention:
`StreamId(aggregate_id: UUID, category: str)`, rendering to the wire format on
demand. `category` is validated against `[A-Za-z0-9_.-]+` in the constructor
(no `:`— the wire-format delimiter). Categories enable future by-category feeds.

### EventEnvelope (`ports/envelopes.py`)

Today's `StoredEvent`, renamed to the DDD term: the domain event plus storage
metadata — `stream_id: StreamId`, `stream_version: int`,
`position: Position | None`, `stored_at: datetime`. `position` is `None` when
the producing adapter implements no global feed; this replaces the current
fabricated `global_position=0`.

### AppendResult and read options (`ports/envelopes.py`)

`AppendResult` kept, retyped over the new VOs. The current kitchen-sink
`ReadOptions` splits per port — `StreamReadOptions`, `FeedReadOptions`,
`TypeReadOptions` — so each port only accepts options it honors. Tenant scoping
(`TenantId`, a domain VO) appears in read options, not in `StreamId`:
multitenancy is a filter, not an identity component.

## Store Ports (`ports/store.py`)

Four small protocols, independently implementable. No inheritance between
adapters and ports — adapters satisfy them structurally; conformance suites are
the compliance check, not a base class.

### EventAppender — the write side

```python
async def append(self, stream: StreamId, events: Sequence[DomainEvent],
                 expected: ExpectedVersion) -> AppendResult
```

Atomic per-stream append with optimistic concurrency. The one port every
adapter must implement — it is the definition of being an event store.

### StreamReader — rehydration

```python
def read_stream(self, stream: StreamId,
                options: StreamReadOptions | None = None) -> AsyncIterator[EventEnvelope]
async def get_stream_version(self, stream: StreamId) -> int   # -1 if absent
```

The `get_events` / `read_stream` duplication collapses into the streaming form;
an eager `collect()` helper covers the list use-case. `event_exists` is dropped
(only tests used it; a version probe answers the same question).

### GlobalEventFeed — the catch-up side

```python
def read_all(self, from_position: Position | None = None,
             options: FeedReadOptions | None = None) -> AsyncIterator[EventEnvelope]
async def current_position(self) -> Position | None   # None = empty store
```

`from_position` is a first-class argument because resumption is the point of
this port. **Resumption is exclusive: iteration starts strictly after
`from_position`.** `from_position=None` reads from the beginning. Adapters that
cannot provide global ordering do not implement this protocol; the type system
then prevents wiring them into catch-up subscriptions.

### EventTypeQuery — secondary-index reads

```python
def read_by_type(self, event_type: str,
                 options: TypeReadOptions | None = None) -> AsyncIterator[EventEnvelope]
```

A separate port because type-indexed access is orthogonal to ordering.

### Composition

`FullEventStore` — a small Protocol composing all four ports — exists as a
convenience annotation for SQL-grade adapters. Consumers still type-hint the
narrowest port they use, per `.claude/rules/architecture.md`. `EventPublisher`
moves to `ports/bus.py`.

## Adapters

Porting is re-homing plus retyping, not a rewrite; the storage logic in the
existing backends is sound. Per adapter:

- Methods regroup under the port protocols.
- `StoredEvent` construction becomes `EventEnvelope`.
- Int global positions wrap into `Position` at the boundary via the shared
  codec in `adapters/_sql/` (which also absorbs the existing `_dialect.py`
  machinery).
- `ExpectedVersion` sentinel handling switches to the VO.

memory, sqlite, and postgresql implement all four store ports plus snapshots.
`NotImplementedError` is banned: an unsupported capability is an unimplemented
port.

## Errors

Exceptions stay centralized in `domain/exceptions.py` (re-exported at top level
as today). New: `PositionDecodeError`, `PositionForeignError`. Each port
docstring enumerates exactly which exceptions its methods may raise; conformance
suites assert the absence of undeclared exception types on the happy path.

## Testing

### Conformance suites (`testing/`)

One suite per port: `AppenderConformance`, `StreamReaderConformance`,
`GlobalFeedConformance`, `TypeQueryConformance`, `SnapshotConformance`. An
adapter's test module inherits exactly the suites for the ports it implements.
A test-only partitioned in-memory store (no `GlobalEventFeed`) validates the
feed-less path, including `position=None` envelopes.

### Property-based tests (hypothesis, already a dev dependency)

Property suites live inside the conformance classes so every future adapter
inherits them.

1. **Position laws**: serialization round-trip over arbitrary payloads;
   ordering laws within a store; cross-store comparison always raises;
   `from_str` fuzzing with garbage strings raises `PositionDecodeError`.
2. **StreamId round-trip**: render/parse round-trip; category validator
   fuzzing (the `:`-in-category corruption case is why the validator exists).
3. **Stateful model-based conformance**: a hypothesis `RuleBasedStateMachine`
   drives append/read/feed operations against an adapter with a dict model as
   oracle — stream reads return exactly what was appended, in order; feed
   positions strictly increase; `get_stream_version` equals appends − 1;
   `ExpectedVersion.exact` conflicts iff the model says so. Runs against
   memory in unit tests, sqlite in integration. Known constraint: hypothesis +
   async interact badly, so the state machine drives a sync façade over the
   adapter (the `SyncEventStoreAdapter` pattern) rather than awaiting in rules.
4. **Envelope/serialization round-trip**: arbitrary event payloads through the
   JSON encoder and back.

### Mutation testing (mutmut + cosmic-ray, per ADR 0008)

The gate runs on `domain/`, `ports/`, and adapter core logic. Priority targets:

- **The resumption boundary in `read_all(from_position)`** — a `>` → `>=`
  mutant makes every resumed catch-up duplicate or skip one event. The
  exclusive-resumption conformance case must kill it.
- **`ExpectedVersion` dispatch** in adapters (`any`/`no_stream`/`exact`
  branches; `==` vs `>=` off-by-ones).
- **`Position.__lt__`/`__eq__`** strictness (`<` → `<=` survivors).
- **`_sql` dialect conversions** (UUID/timestamp/JSON) — silent-corruption
  territory.

Excluded from mutation: transition shims, re-export modules.

### Regression

The existing unit and integration suites keep passing unmodified through the
transition shims for the whole of sub-project 1.

## Out of Scope (deferred)

- Application-layer retyping (subscriptions, projections, migration) —
  sub-project 2.
- Repositories/bus/locks/multitenancy/GDPR packaging and import-linter
  contracts — sub-project 3.
- Any new backend (MySQL, Cassandra, DynamoDB, Mongo, NATS, outbox bus) —
  future work validated against this contract.
- Deleting the transition shims — end of sub-project 2.
