# Legacy Store Surface Retirement

**Date:** 2026-07-31
**Status:** Proposed (design; implementation plans to follow, one per slice)
**Sub-project:** third and final store slice of sub-project 2 (application layer)
in the Clean Architecture redesign (`2026-07-29-core-rings-design.md`), following
`2026-07-30-aggregates-application-ring-design.md` (PR #85) and
`2026-07-30-projections-ring-design.md` (in flight on this branch)

## Context

ADR 0019 established the new store surface: five segregated port protocols in
`ports/store.py` (`EventAppender`, `StreamReader`, `EventLookup`,
`GlobalEventFeed`, `CategoryQuery`, composed as `FullEventStore`), value objects
in `ports/{envelopes,positions}.py`, and three adapters
(`adapters/memory/store.py`, `adapters/sqlite/store.py`,
`adapters/postgresql/store.py`) with conformance suites in
`testing/conformance_ports/`. The legacy surface it replaces still exists in
full and is still what everything actually runs on:

- `stores/interface.py` — the `EventStore` ABC plus `StoredEvent`,
  `ReadOptions`, `EventStream`, `AppendResult`, `ReadDirection`, and the
  int-sentinel `ExpectedVersion` class.
- `stores/{in_memory,postgresql,sqlite}.py` — the original ABC
  implementations. The adapters were ported *from* them; both copies live on.
- `stores/legacy.py` — `LegacyStoreAdapter`, wrapping a `FullEventStore` back
  into the ABC.
- `stores/_compat.py` (`validate_timestamp`), `stores/_type_converter.py`
  (`TypeConverter`, `DefaultTypeConverter`).

Runtime consumers of the ABC: `application/aggregates/repository.py` (and
`multitenancy/repository.py` through it), `sync/adapter.py`,
`testing/{conformance,harness}.py`,
`subscriptions/{lifecycle,transition,manager,error_handling,runners/catchup}.py`,
`migration/{router,dual_write,bulk_copier,consistency,coordinator,sync_lag_tracker}.py`,
`bench/{adapters,scenarios}/*`, and the top-level `__init__.py`, which today
deliberately dual-exports the old and new `ExpectedVersion` / `ReadDirection` /
`AppendResult` (old at top level, new path-only from `eventsource.ports`).
Roughly 44 test files (~37k lines) touch the legacy surface.

The library is unreleased. Per the standing rule, this spec introduces **no
deprecation shims and no back-compat aliases**: consumers are retyped onto the
ports, `stores/` is deleted entirely, and `eventsource` ends with exactly one
blessed set of store names.

## ADR Impact

Per `.claude/rules/definition-of-done.md`:

| ADR | Disposition |
|-----|-------------|
| 0001 async-first design | **Stands.** Every retyped signature stays async; the sync wrapper remains a wrapper. |
| 0014 live-migration cutover semantics | **Stands as amended by 0019.** 0019 already abolished position-delta lag; this work performs that abolition in `migration/sync_lag_tracker.py` (count-behind, bounded — see the migration slice). No further amendment; ADR 0025 records the mechanics. |
| 0015 optional-dependency extras | **Stands.** Extras are unchanged; `aiosqlite` remains the sqlite extra, guarded in `adapters/sqlite/`. |
| 0016 optional tracing no-op | **Amended.** The legacy stores carry per-operation spans (`inmemory_event_store.*`, `postgresql_event_store.*`, `sqlite_event_store.*`); the ports adapters deliberately carry none, and this spec accepts that loss at the store layer (spans survive at the repository, projection, subscription, and migration layers). ADR 0025 records the store-span removal; a ports-level tracing decorator is backlogged, not promised. |
| 0018 tenant isolation model | **Stands.** Tenancy remains a read-option filter (`FeedReadOptions.tenant_id`, `CategoryReadOptions.tenant_id`), never a stream-identity component. |
| 0019 clean-architecture store ports | **Amended.** Its Status says the legacy ABC "remains the default shipped surface, behind that compatibility wrapper, until the application layer is retyped." That condition ends here: add "Amended by ADR 0025" to its Status, and a Consequences line recording that the compat wrapper and the legacy ABC are deleted. Its Decisions all stand and finally become the only surface. |
| 0021 snapshot policy/scheduler | **Stands.** Snapshot collaborators are already ports-typed. |
| 0024 projection persistence ports (in flight, this branch) | **Amended.** `SubscriptionPositions` was just recorded with `position: int`. The subscriptions slice retypes it to the opaque `Position` VO (the int was the legacy store's global position leaking through a port). Add "Amended by ADR 0025" to its Status when the subscriptions slice lands. |

New: **ADR 0025 — Legacy store surface retirement** (see ADR Plan below).
0024 is claimed by the in-flight projections slice
(`docs/superpowers/specs/2026-07-30-projections-ring-design.md` line 546);
verified against `docs/adrs/` (highest committed: 0023). If another ADR lands
before this work's final slice, renumber upward — the number is claimed at
commit time, not at spec time.

---

## 1. Semantic Mapping: Legacy Construct → Ports Equivalent

Signatures below are quoted from the real code
(`stores/interface.py`, `ports/store.py`, `ports/envelopes.py`,
`ports/positions.py`). `LegacyStoreAdapter` (`stores/legacy.py`) is the
executable reference for most translation rules and stays authoritative until
it dies in the last slice.

### 1.1 `EventStore.append_events` → `EventAppender.append`

```python
# legacy
async def append_events(aggregate_id: UUID, aggregate_type: str,
                        events: list[DomainEvent], expected_version: int) -> AppendResult
# ports
async def append(stream: StreamId, events: Sequence[DomainEvent],
                 expected: ExpectedVersion) -> AppendResult
```

Rules:

- `(aggregate_id, aggregate_type)` → `StreamId(aggregate_id=..., category=aggregate_type)`.
- `expected_version` int → VO **by name, not by numeric coincidence**
  (`stores/legacy.py::_expected_from_int`): `ExpectedVersion.ANY` (-1) →
  `.any_()`; `ExpectedVersion.NO_STREAM` (0) → `.no_stream()`;
  `ExpectedVersion.STREAM_EXISTS` (-2) → `.stream_exists()`; `n >= 1` →
  `.exact(n)`. Legacy `0` is ambiguous between "no stream" and "exactly 0
  events"; the two are semantically identical (absent stream = version 0), so
  the `no_stream` mapping is safe.
- Legacy result `AppendResult(success, new_version, global_position: int,
  conflict)` → ports `AppendResult(stream, new_version, position: Position | None)`.
  `success`/`conflict` fields disappear: all three legacy stores *raise*
  `OptimisticLockError` rather than returning `conflicted()` in practice, so
  consumer code checking `result.success` / `result.conflict`
  (`migration/dual_write.py:370`, `migration/router.py`,
  `bench/scenarios/stores.py:126`) is dead-in-practice and is removed, not
  translated.
- **First-vs-last position.** Legacy `global_position` is the position of the
  **last** appended event (`stores/postgresql.py:381`,
  `stores/in_memory.py:214`); ports `AppendResult.position` is the **first**
  appended event's position (all three adapters). `LegacyStoreAdapter` already
  silently changed this and its docstring does not say so — one of two
  undocumented deltas this spec surfaces. Only `migration/bulk_copier.py`
  does arithmetic that depends on last-position semantics (see §3).
- **Empty batch.** Legacy returns `AppendResult.successful(expected_version)`;
  adapters raise `ValueError`. The only src-side caller that could pass an
  empty list, `AggregateRepository.save`, already early-returns on empty
  uncommitted events (`repository.py:497-499`). `DualWriteInterceptor` raises
  `ValueError` itself today, so its behavior is unchanged. Each slice's plan
  must re-verify its callers guard.
- **Duplicate `event_id`.** Legacy `in_memory`/`postgresql` silently *skip*
  already-stored event_ids (idempotent append); adapters raise
  `DuplicateEventError` (ADR 0019 decision 6). Consumers that relied on the
  skip — only `BulkCopier` retries on resume — must catch
  `DuplicateEventError` and treat it as already-copied (§3).

### 1.2 `EventStore.get_events` → `StreamReader.read_stream` + `get_stream_version`

```python
# legacy
async def get_events(aggregate_id: UUID, aggregate_type: str | None = None,
                     from_version: int = 0, from_timestamp: datetime | None = None,
                     to_timestamp: datetime | None = None) -> EventStream
# ports
def read_stream(stream: StreamId,
                options: StreamReadOptions | None = None) -> AsyncIterator[EventEnvelope]
async def get_stream_version(stream: StreamId) -> int
```

Rules:

- Legacy `from_version=n` means "skip the first n events" (in_memory slices
  `[n:]`, postgresql filters `version > n`). Ports `from_version` is an
  **inclusive 1-based** stream version: translate `n > 0` → `from_version=n+1`,
  `n == 0` → `None` (`stores/legacy.py:153`).
- `aggregate_type=None` (cross-type lookup) has **no ports equivalent** — see
  §2 for the resolution.
- `EventStream(aggregate_id, aggregate_type, events, version)` has no
  replacement type. Consumers get `list[DomainEvent]` via
  `[e.event async for e in store.read_stream(...)]` (or
  `ports.collect(...)` when they want envelopes), and the true stream
  version from `get_stream_version` when they need it. Note the legacy
  in-memory quirk `LegacyStoreAdapter` already fixed: legacy
  `EventStream.version` was the length of the *filtered* list; consumers that
  need the real version must call `get_stream_version`, not `len(events)`.
  `AggregateRepository.load` computes the restored version from
  `snapshot.version + len(events)` and needs neither.
- `from_timestamp`/`to_timestamp` on stream reads have no ports pushdown
  (deliberate: point-in-time reconstruction is not a stream-read option). No
  runtime consumer passes them (`AggregateRepository` never does;
  `DualWriteInterceptor`/`TenantStoreRouter` only forward them). The
  parameters die with the ABC.

### 1.3 `EventStore.get_events_by_type` → `CategoryQuery.read_category`

```python
# legacy
async def get_events_by_type(aggregate_type: str, tenant_id: UUID | None = None,
                             from_timestamp: datetime | None = None) -> list[DomainEvent]
# ports
def read_category(category: str,
                  options: CategoryReadOptions | None = None) -> AsyncIterator[EventEnvelope]
```

`CategoryReadOptions(tenant_id=..., from_timestamp=..., limit=...)`.
**Deliberate semantic change**, already documented on `LegacyStoreAdapter`:
legacy filters and orders on the event's own `occurred_at`, **exclusive**
(`occurred_at > from_timestamp`); the port filters and orders on **storage
time** (`EventEnvelope.stored_at` / `created_at`), **inclusive** (`>=`), with
`global_position` (or position key, in memory) as deterministic tie-break.
Naive datetimes are rejected (`ValueError`) rather than silently compared. The
port semantics win everywhere; tests asserting legacy boundary behavior are
rewritten or die with their suites.

### 1.4 `EventStore.read_stream(stream_id: str, ReadOptions)` → `StreamReader.read_stream`

Legacy takes the rendered string `"{aggregate_id}:{aggregate_type}"`; ports
take `StreamId` (whose `render()`/`parse()` use the same wire format —
`domain/stream_id.py:30-38`). `ReadOptions(direction, from_position, limit)` →
`StreamReadOptions(direction, from_version, to_version, limit)` with the same
`+1` from-version shift as §1.2; `ReadDirection` maps name-for-name (both
enums have `FORWARD`/`BACKWARD`; the ports enum is the survivor). BACKWARD
stream reads are native in all three adapters (`ORDER BY version DESC`,
limit applied after reversal — same result as the legacy default
implementation).

### 1.5 `EventStore.read_all` / `get_global_position` → `GlobalEventFeed`

```python
# legacy
async def read_all(options: ReadOptions | None = None) -> AsyncIterator[StoredEvent]
async def get_global_position() -> int
# ports
def read_all(from_position: Position | None = None,
             options: FeedReadOptions | None = None) -> AsyncIterator[EventEnvelope]
async def current_position() -> Position | None
```

Rules:

- `ReadOptions.from_position: int` (0 = start, exclusive resume) →
  `from_position: Position | None` (`None` = start; the port contract is
  strictly-after, matching the legacy exclusive `>` predicate).
- `ReadOptions.tenant_id` / `limit` → `FeedReadOptions(tenant_id, limit)`.
- `get_global_position() == 0` (empty store) ↔ `current_position() is None`.
  Consumers must treat `None` as "empty feed", not as a comparable floor.
- **Positions are opaque**: consumers may compare (`<`, `>=`) and persist
  (`Position.to_str()` / `from_str()`), never subtract. Every
  `target - current` computation in subscriptions/migration is redesigned
  (§Slices b, c) — this is the single largest semantic shift of the
  retirement.
- **BACKWARD feed reads and feed timestamp filters have no ports equivalent.**
  No runtime consumer uses either (catchup, bulk copier, and consistency all
  read FORWARD without timestamps); both capabilities die with `ReadOptions`.
  Tests exercising them (`tests/stores/test_read_all_tenant_filter.py`
  BACKWARD cases) die with the legacy stores.
- **Ordering caveat carried from ADR 0019:** on PostgreSQL the adapter's feed
  is bounded to the xmin safe-horizon, so `current_position()` can lag the
  latest allocated position under concurrent writers. Consumers must not
  assume `append().position <= current_position()` immediately after an
  append. (The memory and sqlite adapters have no such gap.)

### 1.6 `StoredEvent` → `EventEnvelope`

| legacy field/property | ports equivalent |
|---|---|
| `event` | `event` |
| `stream_id: str` | `stream_id: StreamId` (`.render()` for the string) |
| `stream_position: int` | `stream_version: int` (same 1-based meaning) |
| `global_position: int` (0 = unavailable) | `position: Position \| None` |
| `stored_at` | `stored_at` (note: legacy in-memory store fabricated `stored_at=event.occurred_at`; the memory adapter stamps real `datetime.now(UTC)` — tests asserting `stored_at == occurred_at` must change) |
| `.event_id` / `.event_type` | `envelope.event.event_id` / `.event_type` |
| `.aggregate_id` / `.aggregate_type` | `envelope.stream_id.aggregate_id` / `.category` (equal to `event.aggregate_id`/`event.aggregate_type` for all in-tree stores) |

### 1.7 Exceptions

`OptimisticLockError(aggregate_id, expected_version: int, actual_version)`
keeps its int-typed `expected_version` field. The adapters already preserve
the legacy sentinel ints (-1/0/-2) for message fidelity via private module
constants (`_ANY_SENTINEL` etc. in all three adapter stores). **Decision:**
this stays as-is — retyping a widely-caught exception to carry the VO is churn
with no consumer demand; the sentinel constants are an adapter-internal
message-formatting detail. ADR 0025 records this deliberately.

---

## 2. The `dual_write` / `aggregate_type=None` Resolution

`DualWriteInterceptor.get_events` forwards `aggregate_type=None` to the source
store unmodified (`migration/dual_write.py:406-435`), and `LegacyStoreAdapter`
raises `ValueError` for that case because `StreamId` requires a category. The
question was whether the ports surface needs a cross-type lookup (widen
`EventLookup`? a migration-specific query port?).

**Decision: drop the capability. No port is added or widened.**

Evidence, verified in this tree:

- No production caller exercises it. `TenantStoreRouter.get_events` always
  receives a concrete `aggregate_type` from its callers;
  `AggregateRepository` always passes `self._aggregate_type`; migration
  tooling always operates on `(aggregate_id, aggregate_type)` pairs from
  `StoredEvent` metadata.
- Across the entire test tree, exactly **one** call omits the type:
  `tests/unit/migration/test_dual_write.py:731`
  (`TestRouterIntegration::test_interceptor_works_as_eventstore_replacement`),
  and its source store is an `AsyncMock` — it asserts callability, not
  cross-type semantics. `tests/unit/stores/test_legacy_adapter.py:106` asserts
  the *rejection*. No test depends on cross-type lookup succeeding.
- The capability exists only because the legacy ABC's optional parameter made
  it expressible. The legacy PostgreSQL implementation's fallback branch (query
  without type filter, resolve the type from the first row) is dead code in
  practice.

The migration slice reimplements `DualWriteInterceptor` as a ports-shaped
wrapper (structurally satisfying `FullEventStore`): `append` does
source-then-best-effort-target with failure recording
(`FailedWrite.source_position` becomes `Position | None`); the four read ports
delegate to the source verbatim. Its `get_events` method ceases to exist along
with the ABC, and the one mock-based test is rewritten against the ports
shape. If a genuine cross-type need ever appears, the honest design is a new
narrow port (e.g. `StreamDiscovery.find_streams(aggregate_id) ->
list[StreamId]`) — noted in ADR 0025 as the rejected-for-now alternative, not
built speculatively.

---

## 3. Consumer-by-Consumer Map

What each runtime consumer uses today and what it becomes. (Tests: §7.)

### `application/aggregates/repository.py` (+ `multitenancy/repository.py`)

Uses: `append_events`, `get_events(from_version=)`, `get_events` for
existence/version checks. Becomes: constructor takes the narrowest composed
port. Add to `ports/store.py`:

```python
class AggregateStore(EventAppender, StreamReader, Protocol):
    """What an aggregate repository needs: append + stream read/version."""
```

- `save`: `append(StreamId(id, category=self._aggregate_type), events,
  ExpectedVersion.exact(aggregate.version - len(uncommitted)))`; the
  `result.success` branch collapses (failure is exceptional).
- `load`: `read_stream(stream, StreamReadOptions(from_version=snapshot.version + 1))`
  after a snapshot, or no options from scratch; `AggregateNotFoundError` when
  no snapshot and zero envelopes; restored version = `snapshot.version +
  len(events)` (or `len(events)`), replacing `EventStream.version`.
- `exists`/`get_version` paths (lines ~668, ~689) use `get_stream_version`.
- `multitenancy/repository.py` subclasses `AggregateRepository` and touches no
  store method directly — it inherits the retype for free.

### `sync/adapter.py` — `SyncEventStoreAdapter`

Today: wraps the ABC 1:1 (`append_events_sync`, `get_events_sync`,
`get_events_by_type_sync`, `get_stream_version_sync`, `event_exists_sync`,
`read_all_sync`, `get_global_position_sync`), `isinstance(event_store,
EventStore)` guard, `_run_sync` loop machinery (running-loop threadpool,
timeouts).

Becomes: same class name, same `_run_sync` machinery and timeout handling,
port-shaped methods over a `FullEventStore`:

```python
class SyncEventStoreAdapter:
    def __init__(self, store: FullEventStore, timeout: float = 30.0) -> None
    def append(self, stream, events, expected, *, timeout=None) -> AppendResult
    def read_stream(self, stream, options=None, *, timeout=None) -> list[EventEnvelope]
    def get_stream_version(self, stream, *, timeout=None) -> int
    def event_exists(self, event_id, *, timeout=None) -> bool
    def read_all(self, from_position=None, options=None, *, timeout=None) -> list[EventEnvelope]
    def read_category(self, category, options=None, *, timeout=None) -> list[EventEnvelope]
    def current_position(self, *, timeout=None) -> Position | None
    @property def wrapped_store(self) -> FullEventStore
```

The `_sync` suffixes die (no async twin on the same class to disambiguate
from). The `isinstance` ABC guard dies (structural protocol; a `TypeError` on
missing attributes is the honest failure). Iterators are drained to lists —
same as today's `read_all_sync`. `testing/sync_facade.py::SyncStoreFacade`
already provides the ports-shaped *test* facade (dedicated loop, no timeout);
it stays as-is, and the docstring cross-reference in each is updated to
describe the split: facade = owns a private loop, for test machinery; adapter
= per-call `asyncio.run` + running-loop threadpool + timeouts, for
Celery/Django-style sync callers.

### `testing/harness.py` — `InMemoryTestHarness`

Swap `stores.in_memory.InMemoryEventStore` for
`adapters.memory.MemoryEventStore` (constructor: drop `enable_tracing=False`,
which the adapter does not take). The `event_store` property retypes to the
memory adapter class; `create_repository` keeps working because
`AggregateRepository` retypes in the same slice. Harness-level API is
otherwise unchanged.

### `testing/conformance.py`

`EventStoreConformanceSuite` (the ABC-shaped half) is **retired** in the final
slice — `testing/conformance_ports/` (`AppenderConformance`,
`StreamReaderConformance`, `EventLookupConformance`, `GlobalFeedConformance`,
`CategoryQueryConformance`, `SnapshotConformance`, plus the hypothesis
`StoreStateMachine`) is already the richer, per-port successor and is what
third-party adapters should implement against. `EventBusConformanceSuite`
stays; `conformance.py` shrinks to the bus suite. Renaming
`conformance_ports/` → `conformance/` is deliberately **not** done here (it
would collide with the surviving module and churn every adapter test import);
backlogged.

### `subscriptions/` (5 modules)

Uses: `EventStore` type hints (TYPE_CHECKING), `get_global_position()` int
watermarks (`transition.py:202,540`), `read_all(ReadOptions(from_position,
limit, tenant_id))` batches with `target - current` arithmetic
(`runners/catchup.py:294-304,390`), `StoredEvent.global_position` for
checkpoints and DLQ context (`error_handling.py:813`). Becomes: typed against
`GlobalEventFeed` (per ADR 0019: catch-up subscriptions type-require the
feed), `Position` watermarks, no arithmetic. Full redesign in Slice (b).

### `migration/` (6 modules + `subscription_migrator`)

Uses: `EventStore` hints everywhere; `router.py` implements the ABC;
`dual_write.py` implements the ABC (§2); `bulk_copier.py` reads
`read_all(ReadOptions(from_position, tenant_id, limit))` and appends with
last-position arithmetic (`bulk_copier.py:566-590`); `consistency.py` collects
`StoredEvent`s per tenant and compares `stream_position` and event hashes;
`sync_lag_tracker.py` computes `source_position - target_position`
(`sync_lag_tracker.py:266-270`); `coordinator.py` holds `EventStore` refs;
`subscription_migrator.py` translates int checkpoints via the
position-mapping table. Full redesign in Slice (c).

### `bench/`

`bench/adapters/stores.py` constructs all three legacy stores and types
`BenchAdapter[EventStore]`; `bench/scenarios/{stores,aggregate}.py` and
`bench/adapters/e2e.py` drive `append_events`/`get_events`/
`get_stream_version` and read `result.conflict`. Becomes:
`BenchAdapter[FullEventStore]`-shaped, constructing
`adapters.{memory,sqlite,postgresql}` stores (constructor changes per §4.2 —
notably postgres takes the *engine*, and sqlite self-initializes), scenarios
rewritten onto `append`/`read_stream`/`get_stream_version` with
`OptimisticLockError` as the only conflict signal. Benchmark numbers will
move (different code path); the bench harness records the store name, so
before/after comparability is per-run, not cross-era — acceptable, noted in
the slice plan.

### Top-level `__init__.py`

See §4.

---

## 4. Public API Plan

### 4.1 End-state exports

The blessed store surface exported from `eventsource` after the final slice:

| Category | Names |
|---|---|
| Ports (protocols) | `EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`, `CategoryQuery`, `FullEventStore`, `AggregateStore` (new, §3), `collect` |
| Value objects | `EventEnvelope`, `AppendResult`, `Position`, `ExpectedVersion`, `ReadDirection`, `StreamReadOptions`, `FeedReadOptions`, `CategoryReadOptions` — all the `ports` definitions, now bound at top level (the dual-export comment block at `__init__.py:27-38` is deleted) |
| Store adapters | `InMemoryEventStore` (the renamed memory adapter, see below), `SQLiteEventStore`, `PostgreSQLEventStore` (the adapter classes, replacing the legacy classes under the same names), `IntPositionCodec`, `ASYNCPG_AVAILABLE`, `AIOSQLITE_AVAILABLE`, `SQLITE_AVAILABLE` |
| Sync | `SyncEventStoreAdapter` (retyped, §3) |
| Exceptions | unchanged (`OptimisticLockError`, `DuplicateEventError`, `PositionDecodeError`, `PositionForeignError` already exported) |

**Rename:** `adapters/memory/store.py::MemoryEventStore` →
`InMemoryEventStore`, in the final slice, simultaneous with the legacy class's
deletion. Rationale: every sibling memory adapter is `InMemory*`
(`InMemorySnapshotStore`, `InMemoryCheckpointRepository`,
`InMemoryDLQRepository`, `InMemoryEventBus`, `InMemoryTestHarness`); one
`Memory*` outlier would be permanent public-API inconsistency. Doing it in the
same slice as the deletion means the name never has two referents, at the cost
of a mechanical re-touch of files retargeted in earlier slices (accepted; it
is one `sed` pass reviewed in one PR).

### 4.2 Names that die

From `eventsource` and everywhere: `EventStore` (ABC), `EventStream`,
`StoredEvent`, `ReadOptions`, `LegacyStoreAdapter`, `TypeConverter`,
`DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, `DEFAULT_STRING_ID_FIELDS`,
the int-sentinel `ExpectedVersion` class, the legacy `AppendResult` /
`ReadDirection`, the legacy `InMemoryEventStore` / `PostgreSQLEventStore` /
`SQLiteEventStore` classes, `stores.__init__`'s whole namespace
(`eventsource.stores` import path ceases to exist), and
`EventStoreConformanceSuite`. `EventPublisher` keeps its home in `ports/bus`
(the `stores/interface.py` re-export dies with the module).

### 4.3 `SQLITE_AVAILABLE` guards after

Today `__init__.py:223-231` guards `from eventsource.stores.sqlite import
SQLiteEventStore` and conditionally appends to `__all__`. After: the guard
flag comes from the adapter package, which already computes it —
`adapters/sqlite/__init__.py` exports both `AIOSQLITE_AVAILABLE` (store) and
`SQLITE_AVAILABLE` (snapshots); the two are equivalent (both mean "aiosqlite
importable"). Top level re-exports `SQLiteEventStore`, `SQLiteSnapshotStore`,
and the flags unconditionally-importable — note `adapters/sqlite/store.py`
already imports cleanly without aiosqlite (guarded `import aiosqlite`,
constructor raises `ImportError` with the install hint), so the top-level
`try/except ImportError` block becomes a plain import plus the existing
`if SQLITE_AVAILABLE: __all__ += [...]` conditional for the class names.
The `repositories.outbox.SQLiteOutboxRepository` guard at `__init__.py:225`
is untouched (outbox slice's problem).

### 4.4 Constructor differences: adapter classes are NOT drop-in

Every difference between the legacy impls and the adapters, verified against
both files:

**PostgreSQL** — legacy `PostgreSQLEventStore(session_factory:
async_sessionmaker[AsyncSession], *, event_registry=None,
outbox_enabled=False, tracer=None, enable_tracing=True, type_converter=None,
uuid_fields=None, string_id_fields=None, auto_detect_uuid=True)` plus
classmethod `with_strict_uuid_detection(...)` and properties
`session_factory`, `event_registry`, `outbox_enabled`. Adapter
`PostgreSQLEventStore(engine: AsyncEngine, event_registry=None, *,
store_id=None, create_schema=False)` plus `store_id` property, `close()`.

- **`engine` vs `session_factory`**: the adapter builds its own
  `async_sessionmaker` from the engine and owns `close()` (engine dispose).
  Callers holding only a session factory must construct with the engine
  instead (every in-tree caller has one).
- **`outbox_enabled` is missing** and is a real feature, not legacy cruft:
  same-transaction outbox writes are the point of the pattern, and the legacy
  store is the *only* outbox writer in the tree
  (`tests/integration/e2e/test_full_flow.py` and
  `tests/integration/repositories/test_outbox.py` depend on it via the
  `conftest` fixture at `tests/integration/conftest.py:611`). **Decision:**
  port `outbox_enabled: bool = False` and `_write_to_outbox` (verbatim SQL,
  `stores/postgresql.py:417-459`) onto the adapter in the final slice, so the
  guarantee survives the deletion. The wider outbox ring-migration (protocol
  split, repository move) stays out of scope.
- **Tracing params are missing** — accepted loss (ADR 0016 amendment, above).
- **`type_converter`/`uuid_fields`/`string_id_fields`/`auto_detect_uuid` are
  missing** — deliberate (§5.1).
- **`store_id` / `create_schema` are new**: `store_id` defaults to
  `f"pg:{engine.url.database}"`; `create_schema=True` is the tests/dev-only
  lazy-DDL path.

**SQLite** — legacy `SQLiteEventStore(database, event_registry=None, *,
wal_mode=True, busy_timeout=5000, tracer=None, enable_tracing=True,
type_converter=None, uuid_fields=None, string_id_fields=None,
auto_detect_uuid=True)` with explicit lifecycle (`async with` /
`_connect()` + `initialize()`, `RuntimeError` when unconnected,
`is_connected`/`database`/`wal_mode`/`busy_timeout` properties). Adapter
`SQLiteEventStore(database, event_registry=None, *, store_id=None,
wal_mode=True, busy_timeout=5000)`: lazily connects **and applies schema** on
first use, `close()` only. Callers delete their `initialize()` /
`async with` ceremony (bench does both).

**Memory** — legacy `InMemoryEventStore(*, tracer=None, enable_tracing=True)`
with test helpers `clear()`, `get_all_events()`, `get_event_count()`,
`get_aggregate_ids()`. Adapter `MemoryEventStore(store_id="memory", *,
event_registry=None)` has none of them. **Decision:** the four test helpers
are not ported; tests use the ports (`collect(store.read_all(...))` for
all-events, fresh store instead of `clear()` — every in-tree `clear()` use is
fixture-scoped construction that can simply build a new store). If a slice
plan finds a genuinely awkward case, adding `clear()` alone to the adapter is
the fallback; the read helpers stay dead.

---

## 5. Disposition of Support Modules

### 5.1 `stores/_type_converter.py` — dies, not moved

Verified: `adapters/{postgresql,sqlite}/store.py` do **not** replicate it.
They deserialize with `event_class.model_validate(json_loads(payload))` and
rely on pydantic coercing ISO strings/UUID strings into *typed* fields. The
converter's only real effect beyond that was rewriting string values inside
**untyped** `dict[str, Any]` event fields into `UUID`/`datetime` objects by
field-name heuristics (`_id` suffix, `_at` suffix). That guessing is the bug,
not the feature — a `dict[str, Any]` field now round-trips as plain JSON
types, deterministically. This is the second undocumented behavior delta this
spec surfaces; ADR 0025 records it, `CHANGELOG.md` calls it out, and users who
need typed nested data are pointed at typed pydantic sub-models.
`tests/unit/stores/test_type_converter.py` (333 lines) dies with the module.

### 5.2 `stores/_compat.py::validate_timestamp` — dies

Its only importers are the three legacy stores and `LegacyStoreAdapter`, all
deleted. The ports surface type-checks structurally (`CategoryReadOptions`
fields are typed) and the adapters reject naive datetimes where it matters.
`tests/unit/test_timestamp_types.py` (228 lines): the helper-specific cases
die; cases asserting store-level timestamp behavior retarget to
`CategoryQueryConformance` if not already covered.

### 5.3 `testing/conformance.py::EventStoreConformanceSuite` — retired

See §3. `conformance_ports/` is already strictly stronger per port (including
hypothesis stateful testing). One gap check before deletion: the legacy
suite's `test_event_metadata_preserved` and `test_append_and_get_roundtrip`
assertions must have envelope-level equivalents in `StreamReaderConformance`
— the final slice's plan verifies case-by-case and ports any missing
assertion into the ports suites rather than keeping the ABC suite alive.

### 5.4 `testing/harness.py` — retargeted (§3), survives

### 5.5 `sync/adapter.py` — retyped in place (§3), survives

### 5.6 `stores/legacy.py::LegacyStoreAdapter` — deleted last

It is the translation reference while consumers migrate and must remain green
until Slice (d). No consumer in `src/` imports it (verified — only
`__init__.py` and tests); it exists purely as exported API and dies unexported.

---

## 6. Slice Decomposition

Four slices. Each is independently green (full `make check`), reviewable, and
carries its own migration table. **Ordering: (a) and (b) may run in parallel
worktrees (disjoint files: (a) touches application/sync/testing/bench, (b)
touches subscriptions + ports/checkpoints + checkpoint adapters). (c) is
strictly after (b) (it consumes the Position-typed `SubscriptionPositions`
and the checkpoint token column). (d) is strictly after (a), (b), (c).**

### Slice (a) — leaf consumers

Retype `application/aggregates/repository.py` (+`AggregateStore` port),
`sync/adapter.py`, `testing/harness.py`, `bench/`. The legacy stores remain
in place and green; only these consumers stop using them.

| Consumer moved | From | To |
|---|---|---|
| `application/aggregates/repository.py:32` | `stores.interface.EventStore` | `ports.AggregateStore` (new) |
| `multitenancy/repository.py` | (inherited) | (inherited) |
| `sync/adapter.py` | ABC methods | `FullEventStore` methods |
| `sync/__init__.py` docstring | legacy example | ports example |
| `testing/harness.py:32,78` | `stores.in_memory.InMemoryEventStore` | `adapters.memory.MemoryEventStore` |
| `bench/adapters/stores.py`, `bench/adapters/e2e.py`, `bench/scenarios/{stores,aggregate}.py` | ABC + legacy constructors | ports + adapter constructors |

Files deleted: none. `eventsource` exports unchanged (repository's parameter
type is looser structurally; `AggregateStore` is added to `ports` and
exported).

### Slice (b) — subscriptions

The feed goes opaque. Changes:

1. **Typing**: `lifecycle/transition/manager/runners/catchup` type against
   `GlobalEventFeed` (TYPE_CHECKING imports from `eventsource.ports`).
2. **Watermark**: `TransitionCoordinator._watermark` and
   `CatchupRunner.target_position` become `Position | None` from
   `current_position()`; `None` short-circuits catch-up as "nothing to do".
3. **Batch loop redesign** (`catchup.py:281-407`): today
   `remaining = target - current; batch_limit = min(batch_size, remaining)`.
   New loop: read `read_all(from_position=current, FeedReadOptions(tenant_id,
   limit=batch_size))`; deliver envelopes while `envelope.position <=
   watermark` (Position ordering is legal); stop when a batch comes back
   empty or the first past-watermark envelope is seen. No arithmetic, no
   overshoot: the comparison replaces the subtraction exactly.
4. **`SubscriptionPositions` retype** (amends ADR 0024):
   ```python
   async def get_position(self, subscription_id: str) -> Position | None
   async def save_position(self, subscription_id: str, position: Position,
                           event_id: UUID, event_type: str) -> None
   ```
   `CheckpointData.global_position: int | None` → `position: Position | None`.
   Adapters: memory stores the VO; SQL persists `position.to_str()` in a new
   nullable `position_token TEXT` column added by append-only update scripts
   (`migrations/updates/002_add_position_token.sql` + `_sqlite` variant;
   `migrations/schemas/checkpoints.sql` is not modified — Do Not Modify rule).
   Reads prefer `position_token`; a row with only the legacy
   `global_position BIGINT` returns `None` (unreleased software: restart
   catch-up rather than guess a store_id to reconstruct a token —
   `IntPositionCodec.decode`'s bare-int branch stays for *store-side* legacy
   checkpoint strings, not for this table).
5. **`error_handling.py`**: `ErrorInfo.position: int` → `Position | None`
   (`_get_position` returns `stored_event.position`, `None` when absent —
   the `-1` sentinel dies); log `extra` renders `position.to_str()`.
6. **Subscription state/metrics**: `last_processed_position` and friends
   become `Position | None`; any events-behind gauge that subtracted
   positions switches to counting delivered-vs-seen within the current run
   (`subscriptions/health.py` has no position math — verified by grep — the
   changes are confined to `subscription.py`/`metrics.py` field types).

| Consumer moved | From | To |
|---|---|---|
| `subscriptions/lifecycle.py:37,62` | `EventStore` | `GlobalEventFeed` |
| `subscriptions/transition.py:37,136,202,497,540` | `EventStore`, `get_global_position()` | `GlobalEventFeed`, `current_position()` |
| `subscriptions/manager.py:78,115` | `EventStore` | `GlobalEventFeed` |
| `subscriptions/runners/catchup.py:29,43,95,308-407` | `ReadOptions`/`ReadDirection`/`StoredEvent`, int math | `FeedReadOptions`/`EventEnvelope`/`Position` compare |
| `subscriptions/error_handling.py:30,725-816` | `StoredEvent`, int position | `EventEnvelope`, `Position \| None` |
| `ports/checkpoints.py` | `int` positions | `Position` (ADR 0024 amendment) |
| `adapters/memory/checkpoints.py`, `adapters/sql/checkpoints.py` | int column | VO / `position_token` |

Files deleted: none. New: the two update SQL scripts.

### Slice (c) — migration (incl. dual_write)

1. **`TenantStoreRouter`**: drops the ABC base; becomes a ports-shaped
   structural `FullEventStore` routing per tenant, holding
   `dict[str, FullEventStore]`. Method-by-method translation per §1;
   `get_events_by_type` routing becomes `read_category` routing.
2. **`DualWriteInterceptor`**: per §2. `FailedWrite.source_position:
   Position | None`.
3. **`BulkCopier`**: reads `read_all(from_position=resume_token,
   FeedReadOptions(tenant_id, limit=batch))`. Writes: because position
   arithmetic is banned and ports `AppendResult.position` is the *first*
   position of a batch, per-event target positions can no longer be
   estimated from one batched append (`bulk_copier.py:583`). **Decision:**
   when a `position_mapper` is configured, append per event (each append's
   `result.position` is that event's exact target position — strictly more
   correct than today's estimate); without a mapper, keep batched appends
   per aggregate. Resume state and copy checkpoints persist
   `Position.to_str()`. Duplicate handling: catch `DuplicateEventError` and
   count as already-copied (replaces the legacy silent skip).
4. **`ConsistencyVerifier`**: collects `EventEnvelope`s; `stream_position` →
   `stream_version`; hashes unchanged (event payload based).
5. **`SyncLagTracker`**: `source_position - target_position` dies. Lag =
   **bounded count-behind**: read the source feed strictly after the
   target's last-copied source position, counting up to
   `cutover_max_lag_events + 1` (`FeedReadOptions(limit=threshold + 1)`);
   report `>threshold` as not-converged without materializing more. `SyncLag`
   fields become `source_position: Position | None`, `target_position:
   Position | None`, `events: int` (now exact-up-to-bound rather than a
   sequence-number delta, which over-counted across tenants anyway —
   positions are global, the tenant's events are a subset).
6. **`MigrationCoordinator`**: `EventStore` refs → `FullEventStore`.
7. **`SubscriptionMigrator` + `position_mapping` repository**: mapping rows
   gain token TEXT columns via append-only update script(s) (the migration
   repositories' schemas live in `migration/repositories/` DDL — the slice
   plan locates and appends, never edits). Translation logic compares
   `Position` equality/order instead of ints.

| Consumer moved | From | To |
|---|---|---|
| `migration/router.py:61-91` | ABC subclass | ports-shaped wrapper |
| `migration/dual_write.py:65-138` | ABC subclass | ports-shaped wrapper (§2) |
| `migration/bulk_copier.py:47,486-590` | `ReadOptions`/`StoredEvent`, position math | feed + per-event append |
| `migration/consistency.py:59-62` | `StoredEvent` | `EventEnvelope` |
| `migration/sync_lag_tracker.py:59,266` | int delta | bounded count-behind |
| `migration/coordinator.py:100` | `EventStore` | `FullEventStore` |
| `migration/subscription_migrator.py` | int checkpoints | `Position` tokens |

Files deleted: none.

### Slice (d) — deletion, public-API swap, contracts/docs sweep

1. Port `outbox_enabled` + `_write_to_outbox` onto
   `adapters/postgresql/store.py` (§4.4); retarget the outbox integration
   fixture.
2. Rename `MemoryEventStore` → `InMemoryEventStore` (§4.1), sweep all
   importers.
3. Delete `src/eventsource/stores/` entirely (all eight files + README).
4. Rewrite `__init__.py` store section per §4.1-4.3; delete the
   dual-export comment block; update `__all__`.
5. Retire `EventStoreConformanceSuite` (§5.3 gap check first).
6. Contracts + mutmut per §8; docs + ADR per §9.

| Deleted | Lines |
|---|---|
| `stores/interface.py` | 624 |
| `stores/in_memory.py` | 695 |
| `stores/postgresql.py` | 981 |
| `stores/sqlite.py` | 1098 |
| `stores/legacy.py` | 261 |
| `stores/_type_converter.py` | 317 |
| `stores/_compat.py` | 31 |
| `stores/__init__.py` + `README.md` | 54 + docs |

---

## 7. Testing Strategy Per Slice

Inventory audited file-by-file (line counts from the current tree).
Categories: **DELETE** (tests the legacy surface itself), **RETARGET**
(fixture/constructor swap), **REWRITE** (asserts on legacy types/ints).

### Slice (a)

- RETARGET: `tests/conftest.py` (772 — the root `InMemoryEventStore` fixture;
  highest-leverage single change), `tests/unit/application/aggregates/
  test_repository.py` (1000), `test_repository_snapshot.py` (1212),
  `test_repository_tracing.py` (696), `tests/unit/testing/test_harness.py`
  (590), `tests/unit/test_fixtures.py` (448),
  `tests/integration/observability/test_tracing_integration.py` (792),
  `tests/benchmarks/conftest.py` (275), `tests/benchmarks/test_event_store.py`
  (445).
- REWRITE: `tests/unit/sync/test_adapter.py` (563) and
  `tests/unit/sync/test_concurrency.py` (339) — rebuilt against the retyped
  sync adapter (port-shaped calls, `EventEnvelope` asserts).
- New: property test for the sync adapter's loop-scenario dispatch is *not*
  warranted (machinery unchanged); a small unit suite for `AggregateStore`
  conformance of the repository's store double.

### Slice (b)

- RETARGET: `tests/unit/subscriptions/test_manager_pause_resume.py` (678),
  `tests/integration/subscriptions/conftest.py` (371) →
  `test_advanced_features.py` (1505) and `test_resilience.py` (1459) follow
  nearly free.
- REWRITE (position ints → Position): `tests/unit/test_catchup_runner.py`
  (831), `tests/unit/test_transition.py` (971),
  `tests/unit/test_subscription_manager.py` (1490).
- conformance_ports extension: none needed for the store suites; **new**
  conformance cases in the checkpoint suite (projections slice's
  `checkpoints.py` suite) for the Position retype: token round-trip,
  `None` for legacy-int-only rows, cross-store token isolation
  (`PositionForeignError` surfaces on foreign compare, not silent misuse).
- Hypothesis: a **no-skip resumption property** on the memory adapter —
  random event batches, random catch-up batch sizes, random restart points
  persisted through the checkpoint repo → every event delivered exactly once
  in position order. This is the executable form of ADR 0019 decision 5 at
  the subscriptions layer.
- Position codec round-trips are already covered
  (`tests/unit/adapters/` codec tests + `tests/unit/ports/` VO tests);
  extend with `to_str`/`from_str` ↔ checkpoint-table round-trip.

### Slice (c)

- REWRITE: `tests/unit/migration/test_dual_write.py` (888 — incl. the §2
  test), `test_router.py` (1350), `test_bulk_copier.py` (945),
  `test_consistency_verifier.py` (1077), `test_load_benchmarks.py` (1487),
  `test_phase2_integration.py` (1771), `test_chaos.py` (1812 — heaviest:
  `FailureInjectableStore` subclasses the ABC and must become a ports-shaped
  failure-injecting wrapper around `MemoryEventStore`),
  `test_phase3_integration.py` (2193), `test_final_integration.py` (2302).
  Phase3/final are cheap despite size (two store fixtures each); chaos and
  load_benchmarks are the expensive ones (they subclass the legacy classes).
- Hypothesis: **sentinel-mapping property is retired with its subject** —
  `_expected_from_int` lives in `stores/legacy.py` and dies in (d);
  `tests/unit/stores/test_legacy_adapter.py` (269) keeps covering it until
  then. New property worth writing here: bulk-copy resume idempotency
  (random crash points; `DuplicateEventError`-as-skip makes re-runs
  converge to source-equal streams, checked via `ConsistencyVerifier`).

### Slice (d)

- DELETE (dies with the surface): `tests/unit/test_in_memory_event_store.py`
  (1157), `tests/unit/test_event_store_interface.py` (706),
  `tests/unit/test_postgresql_event_store.py` (1473),
  `tests/stores/test_sqlite_event_store.py` (1214),
  `tests/stores/test_read_all_tenant_filter.py` (437 — tenant-filter cases
  are already in `GlobalFeedConformance`; BACKWARD cases die with the
  capability), `tests/unit/stores/test_legacy_adapter.py` (269),
  `tests/unit/stores/test_type_converter.py` (333),
  `tests/unit/stores/test_{memory,sqlite,postgresql}_tracing.py`
  (593/445/538), `tests/unit/test_eventstore_global_position.py` (299),
  `tests/unit/test_conformance.py` (123 — superseded by
  `tests/unit/adapters/test_memory_conformance.py`; keep its
  `EventBusConformanceSuite` half).
- REWRITE: `tests/unit/test_public_api.py` (125) — flips from asserting the
  legacy identities to asserting the ports identities and the absence of
  dead names; this file is the acceptance test for §4.
  `tests/unit/test_edge_cases.py` (621) and
  `tests/unit/test_additional_coverage.py` (495) — drop the legacy-dataclass
  cases, retarget the rest. `tests/unit/test_timestamp_types.py` (228) per
  §5.2.
- Outbox: `tests/integration/repositories/test_outbox.py` and
  `tests/integration/e2e/test_full_flow.py` retarget to the adapter's
  `outbox_enabled` (new unit coverage for `_write_to_outbox` on the adapter).
- Gap check for the retired conformance suite per §5.3.

Coverage safety net across all slices: `tests/unit/adapters/` (~5.7k lines),
`tests/unit/ports/`, and the three conformance suites already cover the
surviving surface; `tests/unit/adapters/test_memory_store.py` is thin (125
lines) but the conformance + stateful suites carry the memory adapter.

### Mutation testing

`only_mutate` already covers `src/eventsource/{domain,ports,adapters,application}` —
slices (a) and (d) need no additions for the surviving code.
Slice-by-slice: (b) adds `src/eventsource/subscriptions` to `only_mutate` and
`tests/unit/test_catchup_runner.py`, `tests/unit/test_transition.py`,
`tests/unit/subscriptions/` to `pytest_add_cli_args_test_selection` (the
position-comparison logic is exactly the kind of off-by-one surface mutmut is
for); (c) optionally adds `src/eventsource/migration/bulk_copier.py` +
`tests/unit/migration/test_bulk_copier.py` (resume/dup-skip logic); (d)
removes nothing (no `stores/` entry exists in the config today — verified).

---

## 8. Contracts End-State (`pyproject.toml`)

**Independence contract** ("Infrastructure backends must not import each
other") — final module list:

```diff
 modules = [
-    "eventsource.stores.postgresql",
-    "eventsource.stores.sqlite",
-    "eventsource.stores.in_memory",
+    "eventsource.adapters.postgresql",
+    "eventsource.adapters.sqlite",
+    "eventsource.adapters.memory",
     "eventsource.bus.redis",
     "eventsource.bus.kafka",
     "eventsource.bus.rabbitmq",
     "eventsource.bus.memory",
 ]
```

(Package-level entries cover their store/snapshot/checkpoint/dlq submodules.
`adapters/_sql/` and `adapters/sql/` are deliberately *not* listed: they are
shared dialect-parameterized infrastructure both SQL backends may import —
same reasoning that keeps `bus/base.py` out of the bus rows. This swap can
land with slice (a) since the adapters already exist and never import each
other; latest with (d).)

**Tier-0 forbidden contract** — remove the rows whose modules die:
`eventsource.stores.interface`, `eventsource.stores.in_memory`. Every other
row survives; `eventsource.sync.adapter` becomes ports-typed but was never
listed (it is Tier-0 by the audit — adding it is a free hardening the (a)
plan should take: `+ "eventsource.sync.adapter"`, `+ "eventsource.testing.sync_facade"`).
The `docs/core-surface.md` rows for `stores/*` (lines 53, 68-70, 179-180 and
the import-chain example at 380-385, which routes through
`stores/__init__` → `stores/postgresql`) are updated in (d) — deleting
`stores/` actually *shortens* the front-door sqlalchemy chain documented
there; the lazy-`__init__` work itself stays out of scope.

**Mutmut** — per §7. No `only_mutate` entry references `stores/` today, so
deletion requires no config change.

---

## 9. ADR Plan and Documentation

**New: `docs/adrs/0025-legacy-store-retirement.md`** (number per ADR Impact
note above). Records: retirement of the ABC surface with no shims; the
by-name sentinel mapping; drop of cross-type `get_events` (rejected
alternative: a `StreamDiscovery` port, not built); first-position
`AppendResult` semantics; duplicate-append raises replacing silent skip;
`stored_at`/inclusive category-read semantics; `TypeConverter` removal (typed
models over field-name guessing); store-span removal (0016 amendment);
`SubscriptionPositions` Position retype (0024 amendment); count-behind lag
(completing 0019's amendment of 0014); `OptimisticLockError` keeping its int
field; `MemoryEventStore` → `InMemoryEventStore`; outbox write support ported
onto the PostgreSQL adapter. Status updates to 0016, 0019, 0024 ("Amended by
ADR 0025") land in slice (d) with the ADR itself; `docs/adrs/index.md` gains
0025.

Docs sweep (slice (d) unless noted): `docs/core-surface.md` (§8);
`docs/api/` store pages (constructor signatures per §4.4, blessed names);
`docs/architecture.md` store narrative; `CLAUDE.md` Project Structure block
(`stores/` row deleted, `sync/` and `testing/` descriptions updated);
`src/eventsource/stores/README.md` content folds into an `adapters/` README;
tutorials/examples grep for `append_events`/`InMemoryEventStore` usage;
`CHANGELOG.md` per-slice entries with the behavior deltas (§1.1, 1.3, 1.6,
5.1) called out loudly.

---

## 10. Out of Scope

- **Outbox ring-migration** — `repositories/outbox.py` protocol split, its
  move to ports/adapters, and the `_connection.py` consolidation (already
  backlogged by the projections slice). Only the `outbox_enabled` write path
  moves here (§4.4), because deleting its only writer without a replacement
  would break the pattern's guarantee.
- **Lazy top-level `__init__`** — the front-door eager-import problem
  (`docs/core-surface.md` §"Why an import-time test does not work") shrinks
  but is not solved here.
- **`locks/`, `readmodels/`, bus verticals** — their own slices.
- **`subscriptions/` package relocation** into `application/` — this spec
  retypes subscriptions' store edge only; the package move is the ADR-0004
  remainder named by the projections spec.
- **`conformance_ports/` → `conformance/` rename** — backlogged (§3).
- **Ports-level tracing decorator** for store spans — backlogged (§ADR 0016).
- **Span renames, engine.py, `protocols.py` consolidation** — untouched.

## 11. Open Risks (experiments a future implementer should run)

1. **Catch-up throughput on PostgreSQL under the xmin horizon.** The adapter's
   `_HORIZON_PREDICATE` runs `pg_current_snapshot()` per batch; the legacy
   catchup path had no horizon. Before slice (b) merges, run
   `make bench` `store.read_all`-family scenarios (and a catchup-loop
   scenario if absent) against postgres with concurrent writers to bound the
   regression. Unresolvable by reading code: plan-time xmin cast cost varies
   by version/load.
2. **Per-event append cost in `BulkCopier` with a position mapper** (§Slice c
   decision 3). Run `tests/unit/migration/test_load_benchmarks.py`'s
   successor against memory + postgres; if the regression is unacceptable,
   the fallback design is batched append + a follow-up feed read to harvest
   actual positions (more code, same correctness) — decide on numbers.
3. **Update-script application path.** `migrations/updates/001_*` exist, but
   whether tests/dev environments actually apply `updates/` scripts (vs. only
   `schemas/`) determines how the `position_token` column reaches sqlite
   `":memory:"` stores whose schema comes from `get_schema("all")`. If
   `schemas/checkpoints.sql` turns out to be the only applied source, the
   append-only rule needs a ruling from Ty (new schema file vs. amended
   schema) before slice (b) — flag early.
4. **Migration-repository DDL location** for the position-mapping token
   columns (§Slice c item 7): confirm where those tables' schemas live and
   whether integration environments migrate them.
5. **Hidden `result.success` consumers.** The dead-in-practice
   `AppendResult.success/conflict` checks are enumerable by grep, but
   downstream *test doubles* returning `AppendResult.conflicted(...)` (e.g.
   chaos/phase suites) encode the returns-not-raises contract; each rewrite
   must convert them to raising doubles or catch silently-passing tests.
6. **`stored_at` assertions.** The memory adapter's real-clock `stored_at`
   (vs. legacy `occurred_at` fabrication) can break time-frozen tests in
   non-obvious places; run the full suite with `-p no:randomly` off (default
   random order) after slice (a) to shake them out early.

## 12. Spec Self-Review

- Placeholder scan: no TODO/TBD/??? remain.
- Signatures checked against `stores/interface.py`, `stores/legacy.py`,
  `ports/{store,envelopes,positions}.py`, all six store implementations,
  `sync/adapter.py`, `migration/*.py`, `subscriptions/*.py` as cited.
- Internal consistency: slice ordering (§6) matches the dependency claims in
  (b)→(c); the rename and outbox decisions appear once each with
  cross-references; ADR numbering contingency stated.
- Ambiguity pass: every "decision" is marked as such with its rejected
  alternative; the three genuinely undecidable items are in §11 with the
  experiment named.
