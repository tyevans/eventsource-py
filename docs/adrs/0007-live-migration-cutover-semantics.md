# ADR 0007: Live Migration Cutover Semantics

**Status:** Accepted

**Date:** 2026-07-27

**Deciders:** Library maintainers (architecture owner, migration owner)

---

This record explains the consistency model of `eventsource.migration`: why the
source store stays authoritative for the entire migration, why the cutover
pause is guarded by a PostgreSQL advisory lock *and* bounded by a hard
millisecond timeout, why subscription checkpoints are rewritten through a
recorded position mapping instead of being replayed, and what the system
therefore does **not** guarantee.

The implementation lives in `src/eventsource/migration/` — chiefly
`dual_write.py`, `cutover.py`, `write_pause.py`, `router.py`,
`position_mapper.py`, `subscription_migrator.py`, `consistency.py`, and the
`coordinator.py` that sequences them. Behaviour is pinned by
`tests/unit/migration/` (notably `test_dual_write.py`,
`test_cutover_manager.py`, `test_write_pause.py`, `test_position_mapper.py`,
`test_consistency_verifier.py`, `test_phase2_integration.py`,
`test_phase3_integration.py`, and `test_chaos.py`).

## Status

**Accepted** — implemented and shipped in `eventsource` 0.5.0.

The semantics described here are those of the code as it stands: the phase
model in `migration/models.py`, the source-first interceptor in
`dual_write.py`, the advisory-lock-guarded, timeout-bounded pause in
`cutover.py` and `write_pause.py`, the recorded translation table in
`position_mapper.py`, and the three verification tiers in `consistency.py`.
Nothing below is aspirational; each decision is pinned by tests under
`tests/unit/migration/`.

Scope of the decision:

- It governs `eventsource.migration` only. It does not change the `EventStore`,
  `EventBus`, or subscription contracts — `DualWriteInterceptor` and
  `TenantStoreRouter` both implement `EventStore`, so migration is invisible to
  callers apart from the bounded pause and the `WritePausedError` that can
  escape it.
- The module is imported from `eventsource.migration` and is deliberately *not*
  re-exported from the top-level `eventsource` package. Migration is
  operational tooling, not part of the everyday application surface.
- It inherits, rather than revisits, the choice of PostgreSQL advisory locks as
  the distributed mutual-exclusion primitive. That decision and its scope
  limits belong to ADR-0009 (PostgreSQL Advisory Locks for Distributed
  Coordination); the practical consequence here is that live migration requires
  a PostgreSQL lock manager even when neither event store is PostgreSQL.

This ADR supersedes nothing and is superseded by nothing. Revisiting it would
be warranted if a backend-agnostic lock primitive lands (which would relax the
PostgreSQL dependency), if a background reconciler is added to consume
`FailedWrite` records automatically (which would change the convergence story
in Decision 1), or if a store gained cross-store transactional support (which
would reopen the two-phase-commit alternative). Amend this record rather than
letting the code drift away from it.

## Context

### The problem: moving a tenant between event stores without downtime

A multi-tenant deployment starts tenants on a shared PostgreSQL event store and
later needs to move individual tenants onto dedicated stores — for isolation,
for noisy-neighbour relief, or for data-residency reasons. The tenant's events
are the system of record; there is no "rebuild from a snapshot elsewhere"
escape hatch. Taking the tenant offline for the duration of a copy is not
acceptable, and the copy can be arbitrarily long because it is proportional to
the tenant's entire history.

So the move has to happen underneath live traffic: writers keep appending,
projections keep consuming, and at some instant the routing flips from source
to target without anyone losing an event or seeing one twice.

The unit of migration is a single tenant, not a store:
`MigrationCoordinator.start_migration()` takes a `tenant_id`, a `target_store`,
and a `target_store_id`, and refuses to start a second migration while one is
already active for that tenant (`MigrationAlreadyExistsError`). Other tenants
sharing the source store are unaffected and keep writing to it throughout.

The indirection that makes the flip possible is `TenantStoreRouter`, which is
itself an `EventStore`. Application code holds the router and never learns
which physical store its writes landed in; a migration is, from the caller's
side, a change in what the router decides per operation. That is what makes
"without downtime" achievable at all — there is no client reconfiguration or
redeploy in the critical path, only a routing row that changes value.

### Constraints: append-only stores, optimistic locking, global_position ordering, running subscriptions

Four properties of the store contract shape everything that follows. None of
them are incidental; each one closes off an approach that would otherwise be
obvious.

**Append-only.** `EventStore` exposes `append_events()`, `get_events()`,
`read_all()`, and position/version queries — there is no update and no delete.
A migration is therefore necessarily a *copy plus a routing change*, not an
in-place move. Nothing about the copy can be made atomic with the routing
change, because the copy is thousands of independent appends spread over
however long the history takes to stream. This is what forces the phase model
in the Decision section: the copy has to be a long-running background
operation, and the flip has to be a separate, short, guarded step.

**Optimistic locking.** `append_events(aggregate_id, aggregate_type, events,
expected_version)` rejects the append when `expected_version` does not match
the stream's current version, raising `OptimisticLockError`. Duplicating a
write into a second store therefore cannot simply reuse the version the caller
supplied to the source: the target's version for that stream is whatever the
target has independently accumulated, which lags whenever a dual-write is
dropped. `BulkCopier._write_batch()` handles this by reading the target's own
`get_stream_version()` per aggregate and using *that* as `expected_version`,
falling back to `0` if the query fails. The constraint also means the caller's
returned version must come from exactly one store — two stores at different
versions cannot both be right — which is why Decision 1 makes the source
authoritative.

**`global_position` ordering.** Every `StoredEvent` carries a `stream_position`
(1-based, within the aggregate) and a `global_position` (1-based, across the
whole store), and `read_all()` iterates in global-position order. That counter
is allocated by the store at append time. On a shared source store it is
interleaved with every other tenant's writes; on a freshly provisioned target
it counts only the migrated tenant's events. The two numbering spaces are
unrelated, and no arithmetic offset relates them either, because the copy
re-appends events grouped by aggregate rather than in one global sweep. Any
value that means "position 4,201,338" on the source is meaningless on the
target unless something recorded the correspondence — hence Decision 3.

**Running subscriptions.** Subscriptions are the consumers that make this
awkward. `CatchupSubscriptionRunner` processes events in global order and calls
`checkpoint_repo.save_position(subscription_id, position=stored_event.global_position, ...)`
after each one; on restart it resumes from `get_position()`. Those checkpoints
are live, durable, and expressed purely in the source store's numbering. A
cutover that changes which store a subscription reads from, without touching
its checkpoint, either skips almost the entire stream (target positions are
much smaller) or replays it. And unlike writers, subscriptions cannot simply be
paused for the duration — they are the mechanism by which read models stay
current.

Two further environmental facts complete the picture. There is **no cross-store
transaction**: source and target are separate databases (or a database and
something else entirely), reachable only through the `EventStore` interface,
which offers no prepare/commit hook — so "write to both or neither" is not
expressible. And the deployment is **multi-process**: several application
instances hold their own `TenantStoreRouter`, so any coordination about which
store is authoritative has to be durable and shared rather than in-process,
which is what pushes the cutover onto a PostgreSQL advisory lock in Decision 2.

## Decision

### Phase model and state machine (NORMAL -> BULK_COPY -> DUAL_WRITE -> CUTOVER_PAUSED -> MIGRATED, with CUTOVER_PAUSED -> DUAL_WRITE rollback)

Two enums in `migration/models.py` model the migration, and the split is
deliberate. `MigrationPhase` is the *operator-visible lifecycle* of a migration
record (`PENDING`, `BULK_COPY`, `DUAL_WRITE`, `CUTOVER`, `COMPLETED`,
`ABORTED`, `FAILED`). `TenantMigrationState` is the *routing decision* the
`TenantStoreRouter` consults on every operation:

```
NORMAL ──► BULK_COPY ──► DUAL_WRITE ──► CUTOVER_PAUSED ──► MIGRATED
                              ▲                │
                              └────────────────┘
                                  (rollback)

any state ──► NORMAL   (abort / cleanup)
```

Both enums validate their own transitions via `can_transition_to()`, and both
allow the same two escapes. In the tenant state machine the only non-forward
edge is `CUTOVER_PAUSED -> DUAL_WRITE`, `MIGRATED` is terminal (its transition
list is empty), and *any* state may drop back to `NORMAL` when a migration is
aborted and its routing row cleared. In the phase machine the mirror-image edge
is `CUTOVER -> DUAL_WRITE`, terminal phases (`COMPLETED`, `ABORTED`, `FAILED`,
per `is_terminal`) refuse every transition, and any non-terminal phase may go
straight to `ABORTED` or `FAILED`.

Why two enums rather than one. `MigrationPhase` is durable bookkeeping on the
`Migration` record — what an operator sees in `get_status()`, what the audit log
records, what `wait_for_phase()` waits on. `TenantMigrationState` lives on the
`TenantRouting` row and is read by `TenantStoreRouter` on the hot path of every
append and every read. They advance together, driven by `MigrationCoordinator`,
but they answer different questions, and the routing state is the one that must
be durable and shared across processes because every application instance
consults it independently.

The routing consequences per state (`TenantStoreRouter._get_write_store()` /
`_get_read_store()`):

| State | Writes | Reads |
| --- | --- | --- |
| `NORMAL` | `routing.store_id` | `routing.store_id` |
| `BULK_COPY` | source only | source |
| `DUAL_WRITE` | `DualWriteInterceptor` | source |
| `CUTOVER_PAUSED` | blocked (`allows_writes` is `False`) | source |
| `MIGRATED` | `routing.store_id`, now the target | `routing.store_id` |

Three details of that table are worth spelling out.

The write path checks its per-process `_dual_write_interceptors` map *before*
reading the routing row, so a registered interceptor short-circuits the state
lookup. When the state says `DUAL_WRITE` but no interceptor is registered, the
router logs a warning and falls back to the source store — degrading to
source-only writes rather than failing, consistent with Decision 1's stance that
the source is what must never be missed.

`MIGRATED` is not a special read path. Cutover *rewrites* `routing.store_id` to
the target store id, so post-migration routing is ordinary `NORMAL`-shaped
resolution against a different store. `MIGRATED` records that the move happened;
it does not encode a second lookup rule. This is also why `MIGRATED -> NORMAL`
during cleanup is safe — the routing row already points at the right store.

Reads stay on the source in every state except `MIGRATED`
(`reads_from_target` is true only there), which is what makes a partially
populated target unobservable to application reads. Combined with the write
table, the observable store for a tenant changes exactly once, at the routing
update inside the paused window.

The `allows_writes` and `allows_writes_to_source` properties are declarative
statements of the same fact on either enum: writes are blocked precisely in
`CUTOVER_PAUSED` / `CUTOVER`. Blocking is *enforced* one layer earlier, by
`WritePauseManager` (Decision 2); the router's `CUTOVER_PAUSED` branch raising
`WritePausedError` is a backstop for a write that somehow reached store
resolution while paused.

### Decision 1: Source-first dual-write with best-effort target writes

#### Why the source stays authoritative (`DualWriteInterceptor.append_events` ordering)

`DualWriteInterceptor` implements `EventStore`, so the router can substitute it
transparently. `append_events()` writes to the source **first** and awaits the
result. If the source raises, the exception propagates and the caller's write
simply failed — nothing was written anywhere the application can see. If the
source returns an unsuccessful `AppendResult` (an optimistic-lock conflict), the
interceptor returns that result immediately and never touches the target.

Only after a successful source append does it attempt the target, and it returns
the **source's** `AppendResult` regardless of what the target did. The caller's
version numbers and `global_position` therefore always come from the
authoritative store. Reads during `DUAL_WRITE` are delegated to the source as
well — `get_events`, `get_events_by_type`, `read_all`, `get_global_position`,
`get_stream_version`, and `event_exists` all forward to `self._source`.

The ordering is the whole guarantee: any event visible to a reader is already
durable in the store that is currently authoritative for the tenant.

#### Why target failures are recorded (`FailedWrite` / `FailureStats`) rather than raised

The target append is wrapped in a bare `except Exception`. A failure logs a
warning and calls `_record_sync_failure()`, which appends a `FailedWrite`
(timestamp, aggregate id and type, the failed event ids, the error message, and
the source `global_position` reached by the successful source write) and adds
the aggregate to an affected-aggregates set. The list is trimmed to
`max_failure_history` (default 1000) to bound memory, and the affected set is
rebuilt when trimming occurs.

Raising instead would be strictly worse. The source write has already
committed; propagating a target error would report failure for an operation
that in fact succeeded, and would invite the caller to retry an append that
would then fail the optimistic-lock check. Making the target's availability a
precondition for the tenant's writes would also mean the migration reduces
availability, which defeats the purpose of a live migration.

`get_failure_stats()` aggregates the records into `FailureStats`
(`total_failures`, `total_events_failed`, `first_failure_at`, `last_failure_at`,
`unique_aggregates_affected`) for monitoring, and `clear_failure_history()`
resets them once the divergence is known to be resolved.

#### How eventual convergence is reached via `BulkCopier` catch-up

Convergence is a property of the copy path, not of a repair daemon. `BulkCopier`
is resumable: `run()` starts from `migration.last_source_position` and
`migration.events_copied`, streams the tenant's events from that point, and
writes them to the target in batches. `_write_batch()` groups a batch by
`(aggregate_id, aggregate_type)`, reads the target's *current* stream version
with `get_stream_version()`, and appends with that version as
`expected_version` — so the copy adapts to whatever the target already has
rather than assuming source version numbers apply.

That means a gap left by a dropped dual-write is closed by re-running the copy
from the recorded source position: the events reappear in the source scan and
are appended to the target at its own next version. Convergence is also what the
`SyncLagTracker` gate measures — `calculate_lag()` compares
`source.get_global_position()` to `target.get_global_position()` and reports
`max(0, source - target)`.

Two limits are worth stating plainly: there is **no** background reconciliation
loop that consumes `FailedWrite` records automatically — they are a monitoring
and diagnosis surface, and closing the gap is a re-copy (or an operator action)
triggered by the lag gate refusing to pass.

### Decision 2: Advisory-lock-guarded write pause with a hard cutover timeout

#### Why a PostgreSQL advisory lock (`migration_lock_key`) rather than in-process coordination

`CutoverManager.execute_cutover()` wraps the entire cutover sequence in
`self._lock_manager.acquire(migration_lock_key(tenant_id, "cutover"),
timeout=self._lock_acquisition_timeout)` — a `PostgreSQLLockManager` advisory
lock keyed `"cutover:{tenant_id}"`, with a default acquisition timeout of 0.5s.

An in-process lock would be sufficient only in a single-process deployment. The
cutover mutates shared, durable state — the tenant routing row — and the library
targets multi-instance deployments where several application processes hold
their own routers. The lock lives in the same PostgreSQL instance that holds the
routing table, needs no extra infrastructure, and is released by the database if
the holding session dies. If the lock cannot be acquired,
`execute_cutover()` does not raise: it returns
`CutoverResult(success=False, duration_ms=0.0, rolled_back=False)` with the
acquisition error in `error_message`, so a contended cutover is a retryable
no-op rather than a half-applied change. See ADR-0009 for why that lock
primitive is PostgreSQL-only.

#### The two-layer pause: distributed lock plus per-process `WritePauseManager` (`asyncio.Event`, `WritePausedError`)

The advisory lock grants exclusivity between *cutover attempts*; it does not
stop application writers. That is the second layer. `TenantStoreRouter`
delegates to a `WritePauseManager`, and every `append_events()` call passes
through `_wait_if_paused()` before a write store is selected.

`WritePauseManager` keeps a per-tenant `PauseState` holding an `asyncio.Event`,
the pause start time, and a waiting-writer count, all guarded by an
`asyncio.Lock`. `pause_writes()` and `resume_writes()` are idempotent;
`wait_if_paused()` blocks on the event with a timeout (`default_timeout`, 5.0s
unless overridden) and raises `WritePausedError(tenant_id, timeout, waited_ms)`
when the timeout expires. `resume_writes()` returns `PauseMetrics`
(duration, start/end timestamps, max and total waiters) for observability, and
`force_resume_all()` exists as an operator escape hatch.

Writers thus experience the pause as added latency, not as an error, unless the
pause outlives their tolerance. `TenantStoreRouter._get_write_store()` also
raises `WritePausedError` if it is ever reached while routing state is
`CUTOVER_PAUSED` — a defence-in-depth path that should be unreachable because
`_wait_if_paused()` runs first.

#### Why `cutover_timeout_ms` is a hard bound (default 500ms) and `cutover_max_lag_events` a precondition

`MigrationConfig.cutover_timeout_ms` defaults to 500 and is validated to be at
least 100. It is not advisory: `_execute_cutover_locked()` records
`time.perf_counter()` at entry and re-checks elapsed time at three points —
after the routing state moves to `CUTOVER_PAUSED`, after the drain wait, and
before the routing switch — raising `CutoverTimeoutError` the moment the budget
is exhausted. The drain step itself is deliberately tiny: it sleeps for the
remaining budget minus a 10ms reserve, capped at 10ms.

The bound is only affordable because the *work* was already done. Convergence is
a **precondition**, not part of the pause: with writes paused, the manager
calls `lag_tracker.calculate_lag()` and rejects the cutover with
`CutoverLagError` unless the lag is within `cutover_max_lag_events` (default
100). If the target still has real catching-up to do, the cutover is refused
before the pause can become expensive. `validate_cutover_readiness()` runs the
same checks as a non-mutating pre-flight — lag within threshold, routing state
is exactly `DUAL_WRITE`, and the cutover lock is currently free (probed with
`try_acquire()` and released immediately).

The remaining steps inside the budget are cheap by construction: a second lag
measurement, one `get_global_position()` health check against the target store,
and the routing update.

#### Why timeout and failure trigger automatic rollback to `DUAL_WRITE` instead of failing forward

Every failure path — `CutoverTimeoutError`, `CutoverLagError`, `CutoverError`,
and any unexpected `Exception` — calls `_rollback()`, which sets the tenant's
routing state back to `DUAL_WRITE`, and returns a `CutoverResult` with
`success=False` and `rolled_back` reflecting whether that write succeeded. A
`finally` block calls `router.resume_writes(tenant_id)` unconditionally, so
writers are never left blocked by a crashed cutover.

Failing forward is not an option here because the target is not yet known to be
complete: routing a tenant to a store that may be missing recent events would
make those events invisible to reads and let subsequent appends build on a
truncated history. `DUAL_WRITE` is, by contrast, a state the system was already
operating in safely a millisecond earlier, and one from which cutover can simply
be retried. `MigrationCoordinator.trigger_cutover()` mirrors this: it refuses to
start unless the migration is in `DUAL_WRITE`, and on failure calls
`_rollback_cutover()` rather than advancing the phase.

### Decision 3: Position remapping for subscription checkpoint continuity

#### Why source and target `global_position` values cannot be assumed equal

`global_position` is assigned by the store on append. On a shared source store
it is interleaved with every other tenant's writes; on a fresh dedicated target
it counts only the migrated tenant's events, starting near zero. Even for a
single-tenant source the values diverge, because `BulkCopier` re-appends events
grouped by aggregate rather than in a single global order, and the target
allocates its own positions.

A subscription that checkpointed at source position 4,201,338 therefore has no
meaningful position on the target. Carrying the number across unchanged would
either skip nearly the whole stream or replay it.

#### `PositionMapper` as the source-of-truth translation table (forward and reverse)

The mapping is recorded, not computed. During bulk copy, `_write_batch()` calls
`position_mapper.record_mapping(migration_id, source_position, target_pos,
event_id)` for each copied event (the per-event target position is derived from
the batch's resulting `global_position`; the code notes this is an estimate for
multi-event appends). `record_mappings_batch()` exists for bulk recording.
Recording is controlled by `MigrationConfig.position_mapping_enabled` (default
`True`).

`translate_position()` attempts an exact lookup first
(`find_by_source_position`). Missing an exact hit and with `use_nearest=True`
(the default), it falls back to `find_nearest_source_position()` — the greatest
recorded mapping at or below the requested position — and returns a
`TranslationResult` with `is_exact=False` plus the `nearest_source_position` it
actually used. With no candidate at all it raises `PositionMappingError`.
Nearest-below is the conservative direction: it may re-deliver a few events, it
cannot skip any. `translate_position_reverse()` provides target-to-source
lookup for debugging and verification, and is exact-only.

#### How `SubscriptionMigrator` uses translation to rewrite checkpoints (plan, migrate, verify)

`SubscriptionMigrator` exposes three steps. `plan_migration()` is a dry run
producing a `MigrationPlan` of `PlannedMigration` entries so an operator can see
which checkpoints move and where before anything is written.
`migrate_subscriptions()` performs the rewrite and returns a `MigrationSummary`
with per-subscription `SubscriptionMigrationResult`s (including
`is_exact_translation` and `nearest_source_position`). `verify_migration()`
checks afterwards that each named subscription still has a checkpoint.

Per subscription, `_migrate_single_subscription()` reads the current position,
skips the subscription entirely if it has no checkpoint or incomplete checkpoint
data, translates the position with `use_nearest=True`, and saves the translated
position back via `checkpoint_repo.save_position()`, preserving the recorded
`last_event_id` and `last_event_type`. A `PositionMappingError` or a failed
checkpoint write is captured as an unsuccessful result rather than aborting the
batch.

The coordinator runs this after a successful cutover when
`MigrationConfig.migrate_subscriptions` is true (default) and both a position
mapper and a checkpoint repository were supplied; failures are logged and the
migration still completes.

### Decision 4: COUNT / HASH / FULL verification tiers

#### What each tier compares and its cost profile

`ConsistencyVerifier` collects the tenant's events from both stores, groups them
by stream, and compares at the requested `VerificationLevel`:

- **`COUNT`** — per-stream event counts and versions only. No event bodies are
  compared. Cheapest; catches missing or duplicated events, nothing else.
- **`HASH`** — everything `COUNT` does, plus SHA-256 hashes of paired events.
  Detects content divergence without a field-by-field comparison.
- **`FULL`** — direct comparison of complete event data, reporting
  `mismatched_positions`. Most thorough, slowest.

Results come back as a `VerificationReport` with `is_consistent`,
`consistency_percentage`, the violations found, the level used, and the
`sample_percentage` applied. `verify_event_checksums()` and
`verify_aggregate_versions()` are thin conveniences over `HASH` and `COUNT`
respectively.

#### Why `HASH` is the default and where sampling applies

`verify_tenant_consistency()` defaults to `level=VerificationLevel.HASH` and
`sample_percentage=100.0`. Hashing is the tier where the marginal cost buys the
most: count-only verification cannot distinguish "the right number of wrong
events", while `FULL` pays for exhaustive field comparison to find divergences
that a content hash already surfaces.

Sampling applies only to content comparison (`HASH` and `FULL`), and only when
counts already match — `_sample_events()` returns nothing to compare if the
per-stream counts differ, because a count mismatch is already a violation and
pairing events would be meaningless. `sample_percentage` must be in `(0, 100]`;
anything at or above 100 compares every pair.

#### Where verification sits in the cutover sequence (readiness gate vs. post-migration audit)

Verification is **not** in the cutover pause. The readiness gate is the cheap
positional one — `SyncLagTracker` lag against `cutover_max_lag_events`, checked
by `validate_cutover_readiness()` and again inside the paused window. Content
verification runs *after* the switch: `MigrationCoordinator._complete_cutover()`
calls `verify_consistency()` at `HASH` / 100% when
`MigrationConfig.verify_consistency` is true (the default), logs any violations,
and proceeds regardless — the call is explicitly non-fatal, and even an
exception from the verifier is caught and logged. The report is retained and
retrievable via `get_consistency_report()`.

## Consequences

### Guarantees provided

- No acknowledged event is lost: the source is authoritative until the routing
  row flips, and a write is only acknowledged after the source append commits.
- Writers never observe a partially populated target: reads route to the source
  in every state except `MIGRATED`.
- Optimistic-locking semantics are unchanged for callers — the version and
  position they receive always come from the authoritative store.
- The write pause is bounded by `cutover_timeout_ms` (default 500ms, minimum
  100ms) and is always lifted, including on crash paths, via the `finally`
  block in `_execute_cutover_locked()`.
- Concurrent cutover attempts for one tenant are mutually excluded by the
  `"cutover:{tenant_id}"` advisory lock; a losing attempt is a clean no-op.
- Any cutover failure leaves the tenant in `DUAL_WRITE`, a state the system was
  already running in, from which retry is safe.
- Subscription checkpoints are translated through recorded mappings, and
  inexact translations resolve backwards (re-delivery), never forwards.

### Guarantees explicitly NOT provided

#### No cross-store atomicity or distributed transaction

There is no two-phase commit. A source append and a target append are two
independent operations, and the window between them is real. The system chooses
which store is right (the source) rather than pretending both are.

#### Target may lag or be temporarily incomplete during `DUAL_WRITE`

`SyncLag.events` is expected to be non-zero. Dropped target writes are recorded
as `FailedWrite`s and closed by re-copying, not by an automatic repair loop. A
target inspected mid-migration may be missing events, and that is not an error
condition.

#### Pause is bounded, not infinite: writers may observe `WritePausedError`

If a writer waits past `WritePauseManager`'s timeout, it receives
`WritePausedError` — a failed write, from the application's perspective, that it
must retry. The design trades a small number of such failures for a bounded
blast radius, rather than letting writers block indefinitely on a stalled
cutover.

#### Verification is a consistency check, not a repair mechanism

`ConsistencyVerifier` reports violations. It does not copy missing events, does
not roll back, and does not block completion — `_complete_cutover()` logs an
inconsistent report and finishes the migration anyway. Acting on a failed
report is an operator decision.

### Operational implications (monitoring, metrics, status streaming, abort paths)

`MigrationMetrics` records events copied, sync lag, per-phase durations, cutover
duration, failed target writes, and verification failures, with no-op
instruments when OpenTelemetry is absent; `get_snapshot()` returns a
`MigrationMetricSnapshot`, and `ActiveMigrationsTracker` tracks concurrency.
`StatusStreamer` / `StatusStreamManager` push `MigrationStatus` updates to
subscribers, and the coordinator notifies them on every phase change and bulk
copy progress tick.

Operators drive the migration through `MigrationCoordinator`:
`start_migration()`, `get_status()`, `pause_migration()` / `resume_migration()`,
`wait_for_phase()`, `is_cutover_ready()`, `get_sync_lag()`,
`trigger_cutover()`, and `abort_migration()`. Cutover is operator-triggered, not
automatic — the coordinator transitions to `DUAL_WRITE` on its own but waits for
`trigger_cutover()`.

`abort_migration()` cancels the active copier and background task, clears the
tenant's migration state and dual-write interceptor, releases lag trackers and
status queues, and marks the migration `ABORTED`. It is refused for terminal
phases. Note that abort does **not** delete events already copied to the target;
cleaning up the abandoned target data is a separate operator task. Migration
lifecycle events are written to an audit log (`AuditEventType`, including
`CUTOVER_INITIATED`, `CUTOVER_COMPLETED`, `CUTOVER_ROLLED_BACK`, and the
verification events) for compliance and post-hoc debugging.

The dashboard-worthy signals are: sync lag versus `cutover_max_lag_events`,
`FailureStats.total_failures` during dual-write, cutover duration versus
`cutover_timeout_ms`, `PauseMetrics.max_waiters`, and the count of inexact
translations in the subscription `MigrationSummary`.

## Alternatives Considered

### Target-first or two-phase-commit dual-write

Writing to the target first would make the target's availability a hard
dependency of every tenant write, converting a background migration into an
availability risk — and would leave orphaned target events whenever the source
append subsequently failed its optimistic-lock check. A genuine two-phase commit
across two event stores is not expressible through the `EventStore` interface
and would require a transaction coordinator plus prepared-transaction support in
every backend. Source-first with recorded failures gets the same end state at a
fraction of the cost, at the price of a transient lag the lag gate already
measures.

### Stop-the-world offline migration

Pausing the tenant for the whole copy is simple and needs no interceptor, no
position mapping, and no lag tracking. It was rejected because copy time scales
with total history: for a large tenant the outage is minutes to hours, which is
exactly the outcome this module exists to avoid. The chosen design confines the
unavailable window to a sub-second, bounded, automatically-rolled-back pause.

### Replaying subscriptions from zero instead of remapping checkpoints

Resetting every checkpoint to zero after cutover would remove `PositionMapper`
and `SubscriptionMigrator` entirely and needs no mapping table. It was rejected
because it forces a full re-projection of the tenant's history at exactly the
moment the system is under migration stress, and because handlers that are not
idempotent would double-apply every historical event. Remapping restarts each
subscription at (or just before) the event it had genuinely reached.

### Always-FULL verification

Running `FULL` unconditionally would give the strongest post-migration
assurance. It was rejected as a default because it compares every field of every
event for the tenant's entire history while the system is live, and because
`HASH` detects the same content divergence at materially lower cost. `FULL`
remains available for the cases that warrant it, and `sample_percentage` lets
operators trade coverage for time on very large tenants.

## References

- `src/eventsource/migration/` — implementation (`README.md` in that directory
  gives the component walkthrough)
- `src/eventsource/migration/models.py` — `MigrationPhase`,
  `TenantMigrationState`, `MigrationConfig`, `SyncLag`, `CutoverResult`
- `src/eventsource/migration/dual_write.py` — `DualWriteInterceptor`,
  `FailedWrite`, `FailureStats`
- `src/eventsource/migration/cutover.py` — `CutoverManager`
- `src/eventsource/migration/write_pause.py` — `WritePauseManager`,
  `WritePausedError`, `PauseMetrics`
- `src/eventsource/migration/position_mapper.py` — `PositionMapper`
- `src/eventsource/migration/subscription_migrator.py` — `SubscriptionMigrator`
- `src/eventsource/migration/consistency.py` — `ConsistencyVerifier`,
  `VerificationLevel`, `VerificationReport`
- `src/eventsource/locks/postgresql.py` — `PostgreSQLLockManager`,
  `migration_lock_key`
- ADR-0009: PostgreSQL Advisory Locks for Distributed Coordination
- ADR-0009: Multi-Instance Subscription Coordination
- `tests/unit/migration/` — behavioural pinning for every claim above
