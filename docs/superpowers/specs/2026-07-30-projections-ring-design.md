# Projections in the Application Ring: Checkpoint and DLQ Ports

**Date:** 2026-07-30
**Status:** Approved (design reviewed in session; pending implementation plan)
**Sub-project:** second slice of sub-project 2 (application layer) in the Clean
Architecture redesign (`2026-07-29-core-rings-design.md`), following
`2026-07-30-aggregates-application-ring-design.md`

## Context

The previous slice moved the aggregates vertical into `domain/` + `application/`
and dissolved `AggregateSnapshotManager` into policy, scheduler, and two pure
module functions (ADR 0021, PR #85). This slice takes the projections vertical
and, with it, the last documented Tier-0 blocker.

`pyproject.toml` carries a KNOWN VIOLATIONS comment block naming the problem
precisely: `repositories/checkpoint.py` and `repositories/dlq.py` each define a
`Protocol` and a sqlalchemy implementation in the same module, so there is no
way to import the contract without importing sqlalchemy. `projections/base.py`,
`projections/checkpoint_manager.py`, and `projections/dlq_manager.py` all import
those modules at module level, which taints the whole projections package.
`docs/core-surface.md` records the same finding from the other side (lines
199-200). Deleting that comment block — because the violation is gone, not
because the contract was relaxed — is the headline outcome of this slice.

Two more DDD problems ride along. `ProjectionCheckpointManager` and
`ProjectionDLQManager` are Manager classes by name and by construction: each is
a stateless wrapper around one repository plus a tracer, holding no invariant of
its own. And `CheckpointRepository` is a seven-method god-interface serving two
disjoint consumer groups — subscription runners call only `get_position` /
`save_position`, projections call only the checkpoint methods.

The library is unreleased. Transition shims are cost without benefit: this slice
deletes rather than aliases.

## ADR Impact

Per `.claude/rules/definition-of-done.md`:

| ADR | Disposition |
|-----|-------------|
| 0001 async-first design | **Stands.** Every relocated method keeps its async signature; the dissolved managers become async module functions, not sync ones. |
| 0007 event-bus delivery semantics | **Stands.** It names the DLQ as the recovery path for swallowed handler errors; the DLQ contract is unchanged, only its module home and the two alias methods change. |
| 0012 event-type auto-derivation | **Stands.** Untouched. |
| 0013 handler-registry composition | **Stands.** `DeclarativeProjection` keeps its `HandlerRegistry` collaborator verbatim; this slice moves the class, it does not restructure handler dispatch. |
| 0015 optional-dependency extras | **Amended.** Its rationale (line 53) argues sqlalchemy is core because "`repositories/_connection.py` normalizes `AsyncConnection \| AsyncEngine` for every repository … the outbox, checkpoint, and DLQ repositories … all go through the same layer." After this slice the checkpoint and DLQ repositories go through `adapters/_sql/connection.py` instead. The *conclusion* (sqlalchemy stays a core dependency) is unaffected — the shared-abstraction argument holds, the file name does not. Add an "Amended by ADR 0024" note to its Status and a Consequences line recording the split. |
| 0016 optional tracing no-op | **Stands.** Every relocated function and adapter keeps its `tracer` / `enable_tracing` parameters and its existing span names and attributes. |
| 0019 clean-architecture store ports | **Stands.** This slice applies its layout to a second family of ports (checkpoints, DLQ) and its ISP guidance to the checkpoint/position split. |
| 0021 snapshot policy + scheduler composition | **Stands.** ADR 0024 is its sibling, applying the same manager-dissolution move to projections; 0021's decision about snapshots is untouched. |

New: **ADR 0024 — Projection persistence ports** (composed checkpoint/position
protocols, manager dissolution into application-ring functions,
`DatabaseProjection` as a SQL adapter, `None` means disabled).

Also unwritten-ADR housekeeping: `docs/adrs/index.md` records at line 95 that
the checkpoint/DLQ/retry machinery ("ADR-0004: Projection Error Handling") has
no record. ADR 0024 covers the *persistence* half of that gap — the ports, their
implementations, and the disabled-by-default semantics. Retry, flow control,
health, and shutdown remain unrecorded. Update the index entry to say so rather
than leaving it claiming the whole area is undocumented.

## Package Changes

```
src/eventsource/
  ports/
    checkpoints.py         # NEW: CheckpointData, LagMetrics value objects;
                           #   ProjectionCheckpoints, SubscriptionPositions,
                           #   CheckpointRepository protocols
    dlq.py                 # NEW: DLQEntry, DLQStats, ProjectionFailureCount;
                           #   DLQRepository protocol
  application/
    projections/           # NEW
      __init__.py
      base.py              # Projection, SyncProjection, EventHandlerBase,
                           #   CheckpointTrackingProjection, DeclarativeProjection,
                           #   TenantFilter  (from projections/base.py)
      checkpoints.py       # record_checkpoint(), read_checkpoint(),
                           #   lag_metrics_dict(), reset_checkpoint()
      dlq.py               # send_to_dlq(), read_failed_events()
      coordinator.py       # ProjectionRegistry, ProjectionCoordinator,
                           #   SubscriberRegistry  (verbatim)
      retry.py             # RetryPolicy, ExponentialBackoffRetryPolicy,
                           #   NoRetryPolicy, FilteredRetryPolicy,
                           #   DEFAULT_RETRY_POLICY  (verbatim)
  adapters/
    _sql/
      connection.py        # NEW: shared connect() helper (DRY)
    sql/                   # NEW subpackage: dialect-parameterized, serves pg+sqlite
      __init__.py
      checkpoints.py       # SQLCheckpointRepository
      dlq.py               # SQLDLQRepository
      projection.py        # DatabaseProjection
    memory/
      checkpoints.py       # NEW: InMemoryCheckpointRepository
      dlq.py               # NEW: InMemoryDLQRepository
```

Deleted outright (no aliases):

- `src/eventsource/projections/` — the entire package: `base.py`,
  `coordinator.py`, `retry.py`, `checkpoint_manager.py`, `dlq_manager.py`,
  `protocols.py` (a two-line re-export of `eventsource.protocols`),
  `README.md` (content folds into `application/projections/README.md`),
  `__init__.py`. The import path `eventsource.projections` ceases to exist.
- `src/eventsource/repositories/checkpoint.py`, `repositories/dlq.py`.
- `src/eventsource/repositories/_dialect.py` — a transition shim re-exporting
  `adapters/_sql/dialect`; after this slice `checkpoint.py` and `dlq.py`, its
  only two importers in `src/`, are gone.
- `CheckpointRepositoryProtocol` and `DLQRepositoryProtocol` back-compat aliases.
- `DLQRepository.list_failed_events` / `.get_failed_event` — pure aliases for
  `get_failed_events` / `get_failed_event_by_id`, removed from the protocol and
  from both implementations.

`src/eventsource/repositories/` survives, holding only `outbox.py`,
`_connection.py`, `_json.py`, and an `__init__.py` reduced to the outbox exports
plus the serialization re-exports.

**The top-level public API is unchanged in names.** Every name currently
re-exported from `eventsource` — `Projection`, `CheckpointTrackingProjection`,
`DeclarativeProjection`, `DatabaseProjection`, `CheckpointRepository`,
`CheckpointData`, `LagMetrics`, `InMemoryCheckpointRepository`,
`SQLCheckpointRepository`, `DLQRepository`, `DLQEntry`, `DLQStats`,
`ProjectionFailureCount`, `InMemoryDLQRepository`, `SQLDLQRepository` — keeps its
`eventsource` re-export. Only source modules change.

## Ports

### `ports/checkpoints.py`

`CheckpointData` and `LagMetrics` move verbatim (frozen dataclasses, field
names and defaults unchanged). The protocol splits along its real consumer
groups, all `@runtime_checkable`:

```python
@runtime_checkable
class ProjectionCheckpoints(Protocol):
    async def get_checkpoint(self, projection_name: str) -> UUID | None: ...
    async def update_checkpoint(
        self, projection_name: str, event_id: UUID, event_type: str
    ) -> None: ...
    async def reset_checkpoint(self, projection_name: str) -> None: ...
    async def get_lag_metrics(
        self, projection_name: str, event_types: list[str] | None = None
    ) -> LagMetrics | None: ...
    async def get_all_checkpoints(self) -> list[CheckpointData]: ...


@runtime_checkable
class SubscriptionPositions(Protocol):
    async def get_position(self, subscription_id: str) -> int | None: ...
    async def save_position(
        self, subscription_id: str, position: int, event_id: UUID, event_type: str
    ) -> None: ...


@runtime_checkable
class CheckpointRepository(ProjectionCheckpoints, SubscriptionPositions, Protocol):
    """Composed convenience protocol: both capabilities in one table."""
```

The split is not speculative — it matches what the code already does.
`subscriptions/{lifecycle,manager,transition,runners/live,runners/catchup}.py`
and `migration/coordinator.py` use position methods only;
`application/projections/*` uses checkpoint methods only;
`migration/subscription_migrator.py` uses positions plus `get_all_checkpoints`
and therefore keeps the composed `CheckpointRepository` annotation. Both
capabilities land in one table (`projection_checkpoints`) in the SQL adapter,
which is why the composed protocol exists at all.

`get_all_checkpoints` sits on `ProjectionCheckpoints` rather than in a third
port: it is a checkpoint-table query, and splitting one method into its own
protocol for one consumer buys nothing.

### `ports/dlq.py`

`DLQEntry` (mutable dataclass — `get_failed_event_by_id` sets `resolved_at` /
`resolved_by` post-construction), `DLQStats`, and `ProjectionFailureCount` move
verbatim. `DLQRepository` keeps its eight real methods —
`add_failed_event`, `get_failed_events`, `get_failed_event_by_id`,
`mark_resolved`, `mark_retrying`, `get_failure_stats`,
`get_projection_failure_counts`, `delete_resolved_events` — with unchanged
signatures, and loses the two aliases. One spelling per operation; the
"backward compatibility" the alias docstrings cite does not apply to unreleased
software.

Both port modules are pure: stdlib, `typing`, `uuid`, `datetime`, `dataclasses`.
No sqlalchemy, no observability import, no implementation code.

## Adapters

### `adapters/_sql/connection.py`

`SQLCheckpointRepository` and `SQLDLQRepository` today define byte-identical
`_connect(self, *, write: bool)` asynccontextmanagers
(`repositories/checkpoint.py:236`, `repositories/dlq.py:311`). One shared helper
replaces both:

```python
@asynccontextmanager
async def sql_connection(
    conn: AsyncConnection | AsyncEngine, *, write: bool
) -> AsyncIterator[AsyncConnection]: ...
```

Semantics are preserved exactly: a live `AsyncConnection` is yielded as-is and
never committed (the caller owns the transaction); an `AsyncEngine` gets
`begin()` for writes and `connect()` for reads. This is deliberately *not*
`repositories/_connection.py::execute_with_connection` — that helper has a
different signature (`transactional=`) and different call sites, and the outbox,
read models, and migration repositories keep using it. Two helpers with the same
job is a wart; consolidating them belongs to the outbox slice, when the last
non-adapter caller moves.

### `adapters/sql/checkpoints.py`, `adapters/sql/dlq.py`

`SQLCheckpointRepository` and `SQLDLQRepository` move with their SQL, their
dialect handling, their tracing spans, and their `nosec` annotations intact.
Three edits: import `Dialect`/`dialect_of`/`*_param`/`*_result` from
`eventsource.adapters._sql.dialect` directly rather than through the deleted
`repositories/_dialect` shim; import the value objects from
`eventsource.ports.{checkpoints,dlq}`; call `sql_connection(self._conn,
write=...)` instead of `self._connect(write=...)`. The `self.conn` public
attribute stays (tests and the harness read it).

The subpackage is `adapters/sql/` (public, dialect-parameterized) beside
`adapters/_sql/` (private helpers) — matching how the existing dialect and
position codecs are already organized.

### `adapters/sql/projection.py`

`DatabaseProjection` moves here unchanged. It takes an
`async_sessionmaker[AsyncSession]` and opens transactions; that is a
framework dependency in a class signature, which makes it an adapter, not a use
case. It continues to subclass `DeclarativeProjection` from the application
ring — an adapter depending inward is exactly the dependency rule.

Its `_handle_with_retry` override (fresh transaction per attempt, because
PostgreSQL aborts a transaction after any error) stays, and is updated to call
the new checkpoint/DLQ functions in place of the manager methods.

### `adapters/memory/checkpoints.py`, `adapters/memory/dlq.py`

`InMemoryCheckpointRepository` and `InMemoryDLQRepository` move verbatim,
including their `clear()` methods (test setup/teardown depends on them) and
their `asyncio.Lock` guards. Both are exported from `adapters/memory/__init__.py`
alongside `InMemorySnapshotStore` and `MemoryEventStore`.

## Application Ring: `application/projections/`

### Manager dissolution

`ProjectionCheckpointManager` and `ProjectionDLQManager` have **zero callers
outside `projections/base.py`** — verified across `src/`, `tests/`, `bench/`,
and `examples/`. The only other references are documentation prose. They become
module-level async functions.

`application/projections/checkpoints.py`:

```python
async def record_checkpoint(
    repo: ProjectionCheckpoints, projection_name: str,
    event: DomainEvent, tracer: Tracer,
) -> None

async def read_checkpoint(
    repo: ProjectionCheckpoints, projection_name: str, tracer: Tracer,
) -> str | None

async def lag_metrics_dict(
    repo: ProjectionCheckpoints, projection_name: str,
    event_types: list[str] | None, tracer: Tracer,
) -> dict[str, Any] | None

async def reset_checkpoint(
    repo: ProjectionCheckpoints, projection_name: str, tracer: Tracer,
) -> None
```

`application/projections/dlq.py`:

```python
async def send_to_dlq(
    repo: DLQRepository, projection_name: str, event: DomainEvent,
    error: Exception, retry_count: int, tracer: Tracer,
) -> bool

async def read_failed_events(
    repo: DLQRepository, projection_name: str, tracer: Tracer, limit: int = 100,
) -> list[DLQEntry]
```

Behavior is preserved to the letter, including the parts that look like
accidents and are not:

- `send_to_dlq` keeps its catch-log-`critical`-return-`False` contract. A DLQ
  write failure must never mask the original processing error, which is about to
  be re-raised.
- `read_failed_events` keeps its swallow-to-empty-list contract.
- `lag_metrics_dict` keeps returning a plain `dict` with the same six keys
  (`projection_name`, `last_event_id`, `latest_event_id`, `lag_seconds`,
  `events_processed`, `last_processed_at`), and `None` when the repository
  returns `None`. The `LagMetrics` → dict conversion is the projection's public
  shape; changing it is out of scope.
- `read_checkpoint` keeps returning `str(event_id)` (not the `UUID`), and `None`
  for a falsy id.
- Span names are unchanged: `eventsource.checkpoint_manager.update`,
  `.get_checkpoint`, `.get_lag_metrics`, `.reset`,
  `eventsource.dlq_manager.send_to_dlq`, `.get_failed_events`. They no longer
  name a class that exists, but renaming spans breaks users' dashboards for no
  functional gain. ADR 0024 records this as deliberate; a rename, if wanted, is
  its own change with its own release note.
- Log messages, levels, and `extra=` payloads are unchanged.

The functions take `tracer` rather than `enable_tracing` — the projection
already owns a `Tracer` and can pass it, which removes one `create_tracer` call
per projection instance (today `CheckpointTrackingProjection.__init__`
constructs three: its own plus one per manager).

`CheckpointTrackingProjection` calls these functions directly, holding
`self._checkpoint_repo` / `self._dlq_repo` as plain attributes. The
`_checkpoint_manager` / `_dlq_manager` attributes disappear.

### `base.py`, `coordinator.py`, `retry.py`

`base.py` receives `Projection`, `SyncProjection`, `EventHandlerBase`,
`CheckpointTrackingProjection`, `DeclarativeProjection`, `TenantFilter`, and
`UnregisteredEventHandling` — everything from `projections/base.py` except
`DatabaseProjection` and the trailing guarded sqlalchemy import block, which go
to the adapter. What remains is pure: `asyncio`, `logging`, `abc`, `uuid`, plus
`eventsource.{events,handlers,observability,protocols,ports}`.

`coordinator.py` and `retry.py` move verbatim.
`eventsource.subscriptions.retry`, which `retry.py` imports, is stdlib-only
(`asyncio`, `logging`, `random`, `time`, `dataclasses`, `enum`, `typing`) —
verified, no transitive sqlalchemy.

`application/projections/__init__.py` re-exports the same names
`projections/__init__.py` does, minus `DatabaseProjection` (which users get from
`eventsource` or from `eventsource.adapters.sql.projection`) and minus the
`AsyncEventHandler` re-export from the deleted `protocols.py` shim —
`AsyncEventHandler` continues to come from `eventsource.protocols`, its
canonical home, and from `eventsource`.

## Behavior Change: `None` Means Disabled

**This is the one behavioral change in the slice, and it is deliberate.**

Today `CheckpointTrackingProjection.__init__` (and `DeclarativeProjection`, and
`DatabaseProjection`) treat `checkpoint_repo=None` and `dlq_repo=None` as
"construct an in-memory one for me":

```python
checkpoint_repo=checkpoint_repo or InMemoryCheckpointRepository()
dlq_repo=dlq_repo or InMemoryDLQRepository()
```

After this slice, `None` means **that concern is disabled**:

| Call | `checkpoint_repo=None` | `dlq_repo=None` |
|------|------------------------|-----------------|
| after successful `_process_event` | no checkpoint write | — |
| `get_checkpoint()` | returns `None` | — |
| `get_lag_metrics()` | returns `None` | — |
| `reset()` | skips checkpoint reset, still calls `_truncate_read_models()` | — |
| after retry exhaustion | — | logs `critical`, then re-raises as before |

**Why.** Two reasons, either sufficient.

The mechanical one: the default lives in the application ring and names a
concrete adapter. `application` importing `adapters` is forbidden by an existing
import-linter contract, which this decision makes load-bearing rather than
decorative.

The design one: the in-memory default is a production footgun. A projection
constructed without a checkpoint repository looks durable — `get_checkpoint()`
returns a value, `get_lag_metrics()` returns numbers — and is not. The
checkpoint dies with the process, so the projection silently reprocesses from
the beginning on restart, and the lag metric that would have shown it is
computed from the same amnesiac store. Making the absence visible converts a
silent wrong answer into an obvious `None`. This matches the precedent the
previous slice set: `AggregateRepository(snapshot_store=None)` turns
snapshotting off rather than inventing a store.

The DLQ side is externally near-identical to today: a per-instance
`InMemoryDLQRepository` nobody holds a reference to is a write to a
process-local dict that no operator will ever read. The observable difference is
one extra `critical` log line on the path where an event fails permanently with
no DLQ configured — and the exception is re-raised either way, so no caller's
control flow changes.

**Test-churn consequence.** Every test that constructs a checkpoint-tracking
projection with no repositories and then asserts on `get_checkpoint()`,
`get_lag_metrics()`, or DLQ contents must now inject
`InMemoryCheckpointRepository()` / `InMemoryDLQRepository()` explicitly. Affected
suites, from the existing tree: `tests/unit/test_projection_base.py`,
`tests/unit/test_projection_decorators.py`,
`tests/unit/projections/test_tenant_filter.py`,
`tests/unit/observability/test_projection_tracing.py`,
`tests/unit/readmodels/test_projection.py`,
`tests/unit/readmodels/test_handler_integration.py`,
`tests/integration/projections/test_database_projection.py`,
`tests/integration/readmodels/test_projection.py`. These are one-line
constructor edits, not assertion rewrites, and each one makes a test's
persistence assumptions explicit — which is the point.

`testing/harness.py` needs no semantic change: it already constructs both
in-memory repositories explicitly (lines ~79-80 and ~191-192) and passes them in.
Only its two import lines move.

## Migration Table

Every runtime consumer, exhaustively:

| File | Current import | New target |
|------|---------------|------------|
| `readmodels/projection.py:20` | `eventsource.projections.base.DatabaseProjection` | `eventsource.adapters.sql.projection` |
| `readmodels/projection.py:22-23` | `repositories.checkpoint.CheckpointRepository`, `repositories.dlq.DLQRepository` | `ports.checkpoints`, `ports.dlq` |
| `testing/harness.py:30-31` | both `InMemory*Repository` | `adapters.memory.checkpoints`, `adapters.memory.dlq` |
| `__init__.py:160-165` | `projections.base` (4 names) | `application.projections.base` (3) + `adapters.sql.projection` (`DatabaseProjection`) |
| `__init__.py:181-198` | `repositories` (checkpoint, DLQ, outbox names) | ports/adapters for checkpoint+DLQ names; `repositories` keeps outbox names |
| `subscriptions/lifecycle.py:36` | TYPE_CHECKING `CheckpointRepository` | `ports.checkpoints.SubscriptionPositions` |
| `subscriptions/manager.py:76-77` | TYPE_CHECKING `CheckpointRepository`, `DLQRepository` | `ports.checkpoints.SubscriptionPositions`, `ports.dlq.DLQRepository` |
| `subscriptions/transition.py:36` | TYPE_CHECKING `CheckpointRepository` | `ports.checkpoints.SubscriptionPositions` |
| `subscriptions/runners/live.py:43` | TYPE_CHECKING `CheckpointRepository` | `ports.checkpoints.SubscriptionPositions` |
| `subscriptions/runners/catchup.py:42` | TYPE_CHECKING `CheckpointRepository` | `ports.checkpoints.SubscriptionPositions` |
| `subscriptions/error_handling.py:29` | TYPE_CHECKING `DLQRepository` | `ports.dlq.DLQRepository` (uses `add_failed_event`; stays composed) |
| `migration/coordinator.py:109` | TYPE_CHECKING `CheckpointRepository` | `ports.checkpoints.SubscriptionPositions` |
| `migration/subscription_migrator.py:67` | TYPE_CHECKING `CheckpointRepository` | `ports.checkpoints.CheckpointRepository` (needs positions **and** `get_all_checkpoints`) |
| `repositories/__init__.py` | re-exports checkpoint + DLQ + outbox | outbox + serialization only |

Each narrowed annotation is checked against actual method use at the call site
before narrowing — a `SubscriptionPositions` hint on a consumer that also calls
`update_checkpoint` would be a type error, not a style choice.

`bench/` and `examples/` import none of these modules — verified, zero hits.

## Contract Changes (`pyproject.toml`)

Tier-0 `forbidden` contract (`source_modules`):

```diff
-    "eventsource.application.aggregates.snapshotting",
+    "eventsource.application",
...
-    "eventsource.projections.protocols",
+    "eventsource.adapters.memory.checkpoints",
+    "eventsource.adapters.memory.dlq",
...
-    "eventsource.application.aggregates.repository",
```

The two per-module `application.aggregates.*` entries collapse into the whole
ring: once `DatabaseProjection` leaves for `adapters/sql/`, nothing under
`eventsource.application` imports sqlalchemy, and import-linter's `forbidden`
contract covers a package's descendants. `eventsource.projections.protocols`
goes because the module goes.

The "Application ring must not import adapters" contract is unchanged in text
(`source_modules = ["eventsource.application"]`, forbidding `adapters`, `bus`,
`repositories`, `locks`) and becomes load-bearing: it is the mechanical reason
the `None`-means-disabled default exists.

**The entire KNOWN VIOLATIONS comment block is deleted.** Every violation it
records is resolved by this slice: `projections/base.py` no longer imports
`repositories.*` (it does not exist); the two manager modules do not exist; and
`checkpoint.py` / `dlq.py` no longer mix protocol with implementation because
the protocols are in `ports/` and the implementations in `adapters/`.

Mutation-testing config, same file:

- `only_mutate` already lists `src/eventsource/{domain,ports,adapters,application}`,
  so every new module is covered with no edit.
- `only_mutate` currently lists `src/eventsource/repositories/_dialect.py`, which
  this slice deletes — **remove that entry**. The dialect helpers themselves are
  still mutated via `src/eventsource/adapters`.
- `pytest_add_cli_args_test_selection` already lists `tests/unit/{adapters,application,domain,ports}/`,
  covering the relocated tests. It also lists
  `tests/unit/repositories/test_dialect.py`, which imports
  `eventsource.repositories._dialect` (line 9) — retarget that test and
  `tests/unit/repositories/test_dialect_properties.py` (line 25) to
  `eventsource.adapters._sql.dialect`. `test_dialect.py:103` already imports the
  new path directly, so the two files end up consistent.

## Testing

- **Port conformance suites** in `src/eventsource/testing/conformance_ports/`,
  following the established pattern (ABC mixin, abstract `store` fixture, imports
  restricted to `eventsource.ports` + stdlib + pytest):
  - `checkpoints.py` — `ProjectionCheckpointsConformance`,
    `SubscriptionPositionsConformance`, and `CheckpointRepositoryConformance`
    (composing both), mirroring the protocol split so a future
    positions-only adapter can run the suite it actually satisfies. Cases:
    absent checkpoint reads `None`; update-then-read round-trip;
    `events_processed` increments across updates; reset makes it absent again;
    `get_all_checkpoints` returns every projection sorted by name;
    `save_position`/`get_position` round-trip; `get_position` is `None` before
    any position is saved even when a checkpoint exists;
    `get_lag_metrics` is `None` without a checkpoint and non-`None` with one.
  - `dlq.py` — `DLQRepositoryConformance`. Cases: add-then-list; the
    `(event_id, projection_name)` upsert key (a second add for the same pair
    updates rather than duplicates, and refreshes `retry_count` /
    `last_failed_at` while preserving `first_failed_at`); status filtering;
    `projection_name` filtering; `limit`; `mark_retrying` and `mark_resolved`
    transitions; `get_failure_stats` and `get_projection_failure_counts`
    aggregates; `delete_resolved_events` deletes only resolved entries past the
    cutoff and returns the count.
  - Exercised by: memory (unit), sqlite (unit, `SQLITE_AVAILABLE`-guarded),
    postgresql (integration, `@pytest.mark.postgres`). Lag-metric cases that
    require the `events` table are postgres/sqlite-only; the in-memory
    implementation documents that it cannot compute real lag, so its suite
    asserts the documented placeholder shape (`latest_event_id is None`,
    `lag_seconds == 0.0`) rather than skipping.
- **Property tests (hypothesis)**, house style — bare `async def` under
  `@given`, `asyncio_mode=auto`, named `test_*_properties.py`:
  - `InMemoryCheckpointRepository`: for any sequence of `update_checkpoint`
    calls, `get_checkpoint` equals the last event id and `events_processed`
    equals the call count; `save_position` leaves `get_position` equal to the
    last position written; `reset_checkpoint` returns the projection to the
    empty state for any prior history; distinct projection names never interfere.
  - `InMemoryDLQRepository`: entry count equals the number of distinct
    `(event_id, projection_name)` pairs added, for any generated add sequence;
    `get_failed_events(limit=n)` returns `min(n, matching)` entries in
    non-increasing `first_failed_at` order; `clear()` empties both the entry map
    and the id counter.
- **Relocated tests**, ring-mirrored:
  `tests/unit/test_checkpoint_repository.py` → `tests/unit/adapters/memory/` +
  `tests/unit/ports/`; `tests/unit/test_dlq_repository.py` likewise;
  `tests/unit/repositories/test_checkpoint_tracing.py` →
  `tests/unit/adapters/sql/`; `tests/unit/test_projection_base.py`,
  `test_projection_decorators.py`, `test_projection_coordinator.py`,
  `test_projection_protocols.py`, `tests/unit/projections/test_tenant_filter.py`
  → `tests/unit/application/projections/`;
  `tests/unit/test_checkpoint_position.py` follows the position methods.
  Import updates plus the `None`-default injections listed above; no assertion
  rewrites beyond the disabled-repository cases.
- `tests/unit/test_public_api.py` must keep passing unmodified — the public name
  set is unchanged. If it needs an edit, the slice has broken compatibility it
  did not intend to break.
- **Process discipline:** implementation agents run only tests targeted at their
  change. The orchestrator runs the full suite (`make check` parity) after each
  agent pass and dispatches fixes. Review agents may fix small findings in place.

## Documentation

- **New:** `docs/adrs/0024-projection-persistence-ports.md`, with the ADR Impact
  table above. Decisions recorded: the ISP split of `CheckpointRepository` into
  `ProjectionCheckpoints` + `SubscriptionPositions` + composed alias; manager
  dissolution into application-ring functions (and why span names were kept);
  `DatabaseProjection` classified as an adapter because
  `async_sessionmaker` appears in its constructor signature; `None` means
  disabled, with the footgun rationale.
- `docs/adrs/0015-optional-dependency-extras.md`: Status gains "Amended by ADR
  0024"; Consequences gains a line noting checkpoint/DLQ now use
  `adapters/_sql/connection.py` while outbox, read models, and migration keep
  `repositories/_connection.py`, and that the sqlalchemy-stays-core conclusion is
  unchanged.
- `docs/adrs/index.md`: add 0024; amend the ADR-0004 entry (line 95) to record
  that the persistence half is now covered by 0024 and only retry/flow-control/
  health/shutdown remain unwritten.
- `docs/architecture.md`: the manager narrative at lines ~343, ~901-904,
  ~1296-1308, ~1441-1495, ~1656-1659 becomes a function-collaborator narrative.
  The ordering point at line ~1480 (process, *then* advance the checkpoint) is
  the load-bearing content and must survive the rewrite intact.
- `docs/api/projections.md`: module table (lines 14-15), the pipeline
  description (lines 53-57), the example imports (lines 136-137), and the
  collaborator table (lines 396-398, 428-429) — the last of which currently
  documents the in-memory defaults and must document the disabled semantics
  instead. The parent-init warning at line ~511 still applies, restated against
  attributes rather than managers.
- `docs/api/repositories.md`: checkpoint and DLQ sections move to ports +
  adapters; outbox stays.
- `docs/core-surface.md`: rows 199-200 and the Tier-0 blocker narrative — the
  blocker is resolved; record what replaced it.
- `docs/guides/multi-tenant.md` line ~621: the `_checkpoint_manager.update` call
  reference.
- `docs/development/code-structure.md` and the `CLAUDE.md` Project Structure
  block: new `ports/`, `adapters/sql/`, `application/projections/` entries;
  `projections/` removed; `repositories/` described as outbox-only.
- `src/eventsource/projections/README.md` → `application/projections/README.md`,
  rewritten for the new module set.
- `CHANGELOG.md` Unreleased: the moves, the deleted alias methods, and the
  `None`-means-disabled change called out as a behavior change.
- `BACKLOG.md`: add **"Migrate outbox repository to ports/adapters (P2)"** —
  `repositories/outbox.py` has the same protocol/implementation mixing this
  slice fixes for checkpoint and DLQ, and moving it is what finally lets
  `repositories/` disappear and the two connection helpers merge. Update the
  "Add CI boundary check for core surface purity (P2)" item: import-linter now
  covers the whole `eventsource.application` ring plus the memory adapters, so
  the remaining question is whether a runtime `sys.modules` assertion adds
  anything over the static contract.

## Out of Scope

- **`repositories/outbox.py`.** It mixes `OutboxRepository` Protocol with
  `PostgreSQLOutboxRepository` / `SQLiteOutboxRepository` / `InMemoryOutboxRepository`
  in one module — the same defect, one slice later. Backlogged above.
- **`repositories/_connection.py`.** Stays where it is; `outbox.py`,
  `readmodels/postgresql.py`, and four `migration/repositories/*` modules still
  import `execute_with_connection`. It merges with
  `adapters/_sql/connection.py` when the outbox slice removes its last
  non-adapter caller.
- **Lazy `__init__.py`.** The top-level module still imports everything eagerly;
  making the public surface lazy is a separate change with its own
  `__getattr__`/`__dir__` design (and the previous slice's gotcha about
  providing both).
- **`SnapshotStore` port redesign.** Untouched by this slice.
- **Retry, flow control, health, and shutdown.** The rest of the ADR-0004 gap;
  `subscriptions/` moves in a later slice.
- **Span renames.** `eventsource.checkpoint_manager.*` and
  `eventsource.dlq_manager.*` keep their names deliberately (see Manager
  dissolution). Renaming them is a separate, user-visible change.
- **The `lag_metrics_dict` return shape.** Returning `LagMetrics` instead of a
  `dict` from `get_lag_metrics()` would be an improvement and is a public API
  change; not here.
