# Core Rings Design: Entities, Ports, and Store Adapters

**Date:** 2026-07-29
**Status:** Approved (revised after adversarial review; pending implementation plan)
**Sub-project:** 1 of 3 in the Clean Architecture redesign

## Context

eventsource-py has no external users yet, which makes this the cheapest possible
moment for a full store-contract redesign. The trigger was a capability mismatch:
`EventStore` requires global-ordering methods (`read_all`, `get_global_position`)
that wide-column, document, and log-structured backends cannot honestly provide.
The interface already admits this inconsistently — `read_all` has a non-abstract
default that raises `NotImplementedError` while `get_global_position` is
`@abstractmethod`, and the `read_stream` default fabricates `global_position=0`.

Decisions made during brainstorming:

- **Scope:** full store contract review, executed as Clean Architecture —
  concentric rings (Entities, Use Cases, Interface Adapters, Frameworks &
  Drivers), the Dependency Rule, boundary ports — with hard DDD and SOLID
  discipline. Our store/repository/bus interfaces are Clean Architecture
  **output ports** (gateways): owned by the inner rings, implemented by
  adapters.
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

1. **Core rings** (this spec): the entities ring (`domain/`) + boundary ports
   (`ports/`), store contract, memory/sqlite/postgresql adapters, conformance
   restructure.
2. **Application layer:** subscriptions, projections, migration re-architected
   as use cases consuming ports; catch-up consumes `GlobalEventFeed`.
3. **Context packaging:** repositories, bus, locks, multitenancy, GDPR folded
   into the adapter structure; import-linter ring contracts locked down.

`.claude/rules/architecture.md` already encodes the target rules and governs
all new and modified code during the transition.

## ADR Impact

Per `.claude/rules/definition-of-done.md`, dispositions for every related ADR:

| ADR | Disposition |
|-----|-------------|
| 0001 async-first design | **Stands.** All ports are async; the sync test façade is test-scaffolding, not public API. |
| 0007 event-bus delivery semantics | **Stands.** Bus contract untouched; `ports/bus.py` is packaging only. |
| 0008 mutation-testing tools | **Stands.** This spec's mutation targets use mutmut + cosmic-ray as decided. |
| 0009 postgresql advisory locks | **Stands.** Locks untouched until sub-project 3. |
| 0009 multi-instance subscription coordination | **Stands.** Sub-project 2 concern; contract here preserves what it needs (resumable feed). |
| 0010 uniform event bus contract | **Stands.** |
| 0011 handler error isolation | **Stands.** |
| 0012 event-type auto-derivation | **Stands.** |
| 0013 handler registry composition | **Stands.** |
| 0014 live-migration cutover semantics | **Amended by ADR 0019.** Cutover lag criteria are defined by int position deltas across stores; opaque positions abolish cross-store position arithmetic. Sub-project 2 re-expresses lag (see Observability below); ADR 0014's Status gains an "Amended by 0019" pointer. |
| 0015 optional-dependency extras | **Stands.** Adapters keep per-backend extras and `*_AVAILABLE` flags. |
| 0016 optional tracing no-op | **Stands.** |
| 0017 snapshot strategy pattern | **Stands.** Snapshot port semantics unchanged; it relocates to `ports/snapshots.py`. |
| 0018 tenant isolation model | **Stands.** Tenancy remains a filter (read options carry `tenant_id`), not a stream-identity component — same model. |

The store-contract redesign itself is architecturally significant and gets
**ADR 0019: clean-architecture store ports and opaque positions**
(`docs/adrs/0019-clean-architecture-store-ports.md`), written alongside this
spec.

## Package Architecture

Target layout (sub-project 1 builds `domain/`, `ports/`, and the three store
adapters; later sub-projects fill in the rest):

```
src/eventsource/
  domain/            # DomainEvent, EventRegistry, domain VOs (StreamId, TenantId),
                     #   domain exceptions. Pure: stdlib + pydantic only.
  ports/             # Boundary ports + port-level VOs. Depends on domain only.
    positions.py     #   Position, ExpectedVersion
    envelopes.py     #   EventEnvelope, AppendResult, read-option VOs
    store.py         #   EventAppender, StreamReader, EventLookup,
                     #     GlobalEventFeed, CategoryQuery
    snapshots.py     #   SnapshotStore port + Snapshot VO
    repositories.py  #   Checkpoint/DLQ/Outbox ports (sub-project 3 moves impls)
    bus.py           #   Publisher/Subscriber ports, EventPublisher
  application/       # (sub-project 2) aggregates, projections, subscriptions, migration
  adapters/
    _sql/            # Shared SQL helpers: dialect machinery, int-backed Position codec
    memory/          # store.py, snapshots.py — all store ports, dict-backed
    sqlite/          # store.py, snapshots.py
    postgresql/      # store.py, snapshots.py
    redis/ kafka/ rabbitmq/   # (sub-project 3)
  testing/           # Conformance suites, one per port
```

Properties:

- The Dependency Rule: source dependencies point only inward. Ring map:
  `domain/` = Entities; `ports/` = boundary interfaces owned by the inner
  rings; `application/` = Use Cases; `adapters/` = Interface Adapters, inside
  which framework/driver imports (sqlalchemy, asyncpg, aiosqlite) are confined
  as the outermost ring. Driver types never appear in port signatures.
- `ports/` + `domain/` have zero dependencies beyond stdlib and pydantic,
  forming the Tier 0 surface described in `docs/core-surface.md`. Sub-project 1
  delivers the domain+ports half of that goal; the Tier 0 blockers in
  `repositories/` and the `testing/__init__` sqlalchemy taint are resolved in
  sub-project 3 (conformance modules must be import-light so the taint is not
  recreated).
- Migration SQL files are untouched: same schema, same queries.

## Compatibility Layer (transition)

The old `stores/` and `snapshots/` module paths keep working until sub-project 2
retypes the application layer — but **not** as re-export shims: the old and new
surfaces differ in names and signatures, so re-exports cannot bridge them. They
become **legacy wrapper classes** with their own tests, marked `# TRANSITION`
and deleted at the end of sub-project 2:

- `stores.interface.EventStore` remains the old ABC; a `LegacyStoreAdapter`
  wraps any full new-port adapter and implements the old surface by
  translation: `append_events` → `append` (int `expected_version` sentinels
  `ANY`/`NO_STREAM`/`STREAM_EXISTS` → `ExpectedVersion` VO forms),
  `get_events`/`EventStream` → collected `read_stream`, `event_exists` →
  `EventLookup`, `get_events_by_type` → `CategoryQuery`,
  `read_all`/`get_global_position` (int) ↔ `Position` via the SQL codec,
  `StoredEvent` ↔ `EventEnvelope`, `AppendResult` old shape (success/conflict
  flags) reconstructed from the new VO / caught `OptimisticLockError`.
- Version numbers translate by **identity** — the versioning convention is
  unchanged (see below) — so the wrapper adds no ±1 arithmetic anywhere.
- Name collision rule: the canonical `ExpectedVersion` is the new VO, exported
  from `eventsource` top level. The legacy int-constants class is reachable
  only via `eventsource.stores.interface` and is never re-exported at top
  level alongside the VO.
- The existing unit/integration suites keep passing against the wrappers for
  the whole of sub-project 1; the wrapper itself gets targeted translation
  tests (sentinel mapping, envelope conversion, exception mapping).

## Versioning Convention (explicit decision)

**Stream versions remain a 1-based event count: an absent stream has version 0;
after N appended events the version is N.** This matches the current contract
(`get_stream_version` → "0 if aggregate doesn't exist"), the SQL schema's
1-based `version` column, and the aggregate repository's expectations. Nothing
rebases. `ExpectedVersion.exact(n)` means "the stream currently has exactly n
events"; `EventEnvelope.stream_version` is the version the stream reached with
that event (the first event of a stream carries `stream_version=1`).

## Value Objects

All frozen (pydantic frozen models or frozen dataclasses). Domain-owned concepts
live in `domain/`; port payloads live in `ports/`.

### Position (`ports/positions.py`)

The opaque global-feed token, **produced only by `GlobalEventFeed`
implementers**. Guarantees:

- **Total ordering within one store**: `__lt__` etc. defined; ordering laws
  (transitivity, antisymmetry, trichotomy) hold for tokens from the same store.
  A backend whose native positions are only partially ordered (e.g. a
  multi-partition Kafka topic's offset vectors) cannot implement
  `GlobalEventFeed` as-is; Kafka-as-store qualifies only via a single
  partition or a merged/compacted total order. This is intentional: exclusive
  resumption is well-defined only over a total order.
- **Serialization round-trip**: `to_str()` / `from_str()`; this string is what
  checkpoints persist. `from_str` on malformed input raises
  `PositionDecodeError` (never a bare `ValueError` from internals).
- **No arithmetic**: consumers compare and persist; they never add, subtract,
  or measure distance.
- **Store identity**: tokens embed a `store_id` — a **stable, adapter-configured
  string** defaulting to a constant derived from backend identity (postgres:
  schema+database, sqlite: database path, memory: instance name). It must not
  change across process restarts, or persisted checkpoints would become
  foreign. Ordering comparisons (`__lt__` and friends) between tokens with
  different `store_id`s raise `PositionForeignError`. **`__eq__` does not
  raise**: foreign or non-Position operands compare unequal (Python equality
  conventions — set/dict membership and `== None` guards must stay safe).
- **Legacy decodability**: the SQL codec (`adapters/_sql/`) accepts a bare
  integer string as a legacy checkpoint value, enabling sub-project 2 to
  upgrade persisted int checkpoints in place. This is a standing obligation on
  the codec, not a temporary hack.

### ExpectedVersion (`ports/positions.py`)

Per-stream concurrency stays integer-based (plain `int` in signatures). The VO
has **four** constructors matching the four supported modes today:
`ExpectedVersion.any()`, `ExpectedVersion.no_stream()`,
`ExpectedVersion.stream_exists()`, `ExpectedVersion.exact(n)`. All four are
implemented by all three adapters (the current stores implement all four; none
is dropped). `append` checks it and raises `OptimisticLockError` on mismatch.

### StreamId (`domain/`)

Replaces the stringly-typed `"{aggregate_id}:{AggregateType}"` convention:
`StreamId(aggregate_id: UUID, category: str)`, rendering to the wire format on
demand. `category` is validated against `[A-Za-z0-9_.-]+` in the constructor
(no `:` — the wire-format delimiter). `category` is today's `aggregate_type`;
`CategoryQuery` reads by it.

### EventEnvelope (`ports/envelopes.py`)

Today's `StoredEvent`, renamed: the domain event plus storage metadata —
`stream_id: StreamId`, `stream_version: int` (1-based, see Versioning),
`position: Position | None`, `stored_at: datetime`. `position` is `None` when
the producing adapter implements no global feed; this replaces the current
fabricated `global_position=0`.

### AppendResult (`ports/envelopes.py`)

Retyped and slimmed: `AppendResult(stream: StreamId, new_version: int,
position: Position | None)`. The current `success`/`conflict` flags and
`conflicted()` constructor are **dropped** — conflicts raise
`OptimisticLockError`, so a returned result always means success and the flags
are dead weight. `position` gets the same `| None` honesty as the envelope.

### Read-option VOs (`ports/envelopes.py`)

One options VO per read port; each port only accepts options it honors.
Enumerated fields (all optional):

- `StreamReadOptions`: `direction` (FORWARD/BACKWARD — every backend can walk
  one stream either way), `from_version: int`, `to_version: int`,
  `limit: int`.
- `FeedReadOptions`: `tenant_id: TenantId`, `limit: int`. The feed is
  **forward-only** — backward global iteration is a SQL luxury (log systems
  and change streams cannot honor it) and no current consumer needs it.
- `CategoryReadOptions`: `tenant_id: TenantId`, `from_timestamp: datetime`,
  `limit: int` — the fields migration's consistency tooling actually passes
  today.

Tenant scoping (`TenantId`, a domain VO) appears in read options, not in
`StreamId`: multitenancy is a filter, not an identity component (ADR 0018).

## Store Ports (`ports/store.py`)

Five small protocols, independently implementable. No inheritance between
adapters and ports — adapters satisfy them structurally; conformance suites are
the compliance check, not a base class.

### EventAppender — the write side

```python
async def append(self, stream: StreamId, events: Sequence[DomainEvent],
                 expected: ExpectedVersion) -> AppendResult
```

Atomic per-stream append with optimistic concurrency. The one port every
adapter must implement — it is the definition of being an event store.
Contract details:

- Appending an event whose `event_id` already exists in the store raises
  `DuplicateEventError` (the schema already enforces `event_id UNIQUE`; memory
  adapter tracks an id set). This gives migration tooling a race-free
  idempotency mechanism (append-and-catch) alongside `EventLookup`.
- Appending an empty `events` sequence raises `ValueError` (current behavior,
  kept).
- Batch sizes are not guaranteed unbounded: an adapter may declare
  `max_append_batch: int | None` (None for the three SQL-family adapters) and
  reject larger batches with `ValueError` — contract room for DynamoDB's
  transaction-size limits.

### StreamReader — rehydration

```python
def read_stream(self, stream: StreamId,
                options: StreamReadOptions | None = None) -> AsyncIterator[EventEnvelope]
async def get_stream_version(self, stream: StreamId) -> int   # 0 if absent
```

The `get_events` / `read_stream` duplication collapses into the streaming form;
an eager `collect()` helper covers the list use-case.

### EventLookup — idempotency probe

```python
async def event_exists(self, event_id: UUID) -> bool
```

Kept as its own tiny port (not dropped: `migration/router.py`,
`migration/dual_write.py`, and `sync/adapter.py` consume it in production).
Backed by the `event_id` unique index on SQL; optional for backends that
cannot index by event id.

### GlobalEventFeed — the catch-up side

```python
def read_all(self, from_position: Position | None = None,
             options: FeedReadOptions | None = None) -> AsyncIterator[EventEnvelope]
async def current_position(self) -> Position | None
```

`from_position` is a first-class argument because resumption is the point of
this port. **Resumption is exclusive: iteration starts strictly after
`from_position`** (matches the existing `global_position > :from` behavior in
both SQL stores). `from_position=None` reads from the beginning.
`current_position()` returns `None` when no resumable position exists — an
empty store, or a backend that cannot mint a token before its first event
(Mongo change streams); consumers treat `None` uniformly as "start from the
beginning". Adapters that cannot provide a total order do not implement this
protocol; the type system then prevents wiring them into catch-up
subscriptions.

**Delivery guarantee (no-skip):** a feed must not permanently skip a committed
event when resumed from any position the feed itself produced. This is a real
constraint, not a truism: PostgreSQL sequences commit out of order, so a naive
`WHERE global_position > :from` can race past a still-uncommitted lower
position and lose it forever. Per adapter: memory and sqlite are trivially
safe (serialized writers); **the postgresql adapter must bound feed reads to a
safe horizon** (e.g. `pg_snapshot_xmin(pg_current_snapshot())`-gated reads) —
this is new, scoped work in sub-project 1, and the one place porting is more
than re-homing. The guarantee is what makes exclusive resumption sound.

### CategoryQuery — category-indexed reads

```python
def read_category(self, category: str,
                  options: CategoryReadOptions | None = None) -> AsyncIterator[EventEnvelope]
```

This is today's `get_events_by_type` re-homed **under its true key**: the
current method filters by `aggregate_type` (= `StreamId.category`), ordered by
timestamp, and its production consumers (migration router, dual-writer, sync
adapter) all pass aggregate types. There is no per-event-type port; if a
per-event-type index is ever wanted it is a new capability, designed then.

### Composition

`FullEventStore` — a small Protocol composing all five ports — exists as a
convenience annotation for SQL-grade adapters. Consumers still type-hint the
narrowest port they use, per `.claude/rules/architecture.md`. `EventPublisher`
moves to `ports/bus.py`.

## Adapters

Porting is re-homing plus retyping (the one exception: the postgres safe-horizon
feed, above). Per adapter:

- Methods regroup under the port protocols.
- `StoredEvent` construction becomes `EventEnvelope`.
- Int global positions wrap into `Position` at the boundary via the shared
  codec in `adapters/_sql/` (which also absorbs the existing `_dialect.py`
  machinery).
- `ExpectedVersion` sentinel handling switches to the VO (identity version
  mapping).

memory, sqlite, and postgresql implement all five store ports plus snapshots.
`NotImplementedError` is banned: an unsupported capability is an unimplemented
port.

## Errors

Exceptions stay centralized in `domain/exceptions.py` (re-exported at top level
as today). New: `DuplicateEventError`, `PositionDecodeError`,
`PositionForeignError`. Housing Position's exceptions in `domain/` is a
deliberate, recorded exception to the "VOs live with their owning ring" rule —
centralized exception discovery wins. Each port docstring enumerates exactly
which exceptions its methods may raise; adapters wrap internal/driver errors so
only declared types escape, and conformance suites assert this **on induced
error paths** (duplicate ids, version conflicts, malformed positions, foreign
positions — not on the happy path, where the assertion would be vacuous).

## Observability (explicit consequence)

Opaque positions abolish cross-store position arithmetic. Two current
consumers break by design: `migration/sync_lag_tracker.py` (source-minus-target
position lag) and checkpoint `LagMetrics` (position-delta lag). **Position-delta
lag is abolished; sub-project 2 must re-express lag** in wall-clock terms
(`stored_at` deltas, available on every envelope) and/or an optional
count-behind capability designed then. This contract deliberately does not
smuggle in a numeric-distance method to keep those metrics alive — that would
reintroduce the int assumption everywhere. Recorded in ADR 0019 and in ADR
0014's amendment.

## Testing

### Conformance suites (`testing/`)

One suite per port: `AppenderConformance`, `StreamReaderConformance`,
`EventLookupConformance`, `GlobalFeedConformance`, `CategoryQueryConformance`,
`SnapshotConformance`. An adapter's test module inherits exactly the suites for
the ports it implements. A test-only partitioned in-memory store (no
`GlobalEventFeed`) validates the feed-less path, including `position=None`
envelopes. New cases beyond the ported ones: tenant filtering on
`FeedReadOptions`/`CategoryReadOptions` (a mutant deleting the tenant `WHERE`
clause must die), duplicate-append raising `DuplicateEventError`, exclusive
resumption, and — postgres integration only — a concurrent-writer no-skip test
exercising the safe horizon.

### Property-based tests (hypothesis, already a dev dependency)

Property suites live inside the conformance classes so every future adapter
inherits them.

1. **Position laws**: serialization round-trip over arbitrary payloads;
   ordering laws within a store; foreign-store ordering comparisons always
   raise while `__eq__` returns False; `from_str` fuzzing with garbage strings
   raises `PositionDecodeError`.
2. **StreamId round-trip**: render/parse round-trip; category validator
   fuzzing (the `:`-in-category corruption case is why the validator exists).
3. **Stateful model-based conformance**: a hypothesis `RuleBasedStateMachine`
   drives append/read/feed operations against an adapter with a dict model as
   oracle — stream reads return exactly what was appended, in order; feed
   positions strictly increase; `get_stream_version` equals the count of
   appended events; `ExpectedVersion.exact` conflicts iff the model says so.
   Runs against memory in unit tests, sqlite in integration. The machine
   drives a **new port-shaped sync façade** (scoped task — the existing
   `SyncEventStoreAdapter` wraps the old ABC, not the ports) because
   hypothesis + async interact badly.
4. **Envelope/serialization round-trip**: arbitrary event payloads through the
   JSON encoder and back.

### Mutation testing (mutmut + cosmic-ray, per ADR 0008)

The gate runs on `domain/`, `ports/`, and adapter core logic. Priority targets:

- **The resumption boundary in `read_all(from_position)`** — a `>` → `>=`
  mutant makes every resumed catch-up duplicate or skip one event.
- **The postgres safe-horizon bound** — a mutant widening the horizon
  reintroduces the skip race; the concurrent-writer test must kill it.
- **`ExpectedVersion` dispatch** in adapters (four-way branch; `==` vs `>=`
  off-by-ones).
- **`Position.__lt__`** strictness (`<` → `<=` survivors) and the
  foreign-store guard.
- **Tenant `WHERE` clauses** in feed and category reads.
- **`_sql` dialect conversions** (UUID/timestamp/JSON) — silent-corruption
  territory.

Excluded from mutation: the legacy wrapper classes and re-export modules.

### Regression

The existing unit and integration suites keep passing against the legacy
wrappers for the whole of sub-project 1; the wrappers get their own translation
tests (sentinel mapping, envelope conversion, exception mapping, legacy int
position decoding).

## Out of Scope (deferred, explicitly)

- Application-layer retyping (subscriptions, projections, migration) —
  sub-project 2, including the persisted-checkpoint format upgrade (enabled by
  the codec's legacy-int decoding) and the lag-metric replacement.
- Repositories/bus/locks/multitenancy/GDPR packaging and import-linter ring
  contracts — sub-project 3.
- Event schema versioning / upcasting hooks on the envelope — deliberately
  deferred; the registry is the natural owner and nothing in this contract
  precludes it.
- Any new backend (MySQL, Cassandra, DynamoDB, Mongo, NATS, outbox bus) —
  future work validated against this contract.
- Deleting the legacy wrappers — end of sub-project 2.
