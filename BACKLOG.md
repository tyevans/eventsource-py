# Backlog

Open work items, carried over from the retired `bd` (beads) tracker.

## Live-phase lag has no signal (P2)

`Subscription.lag` is derived from `_events_seen - _events_delivered`, but only the
catch-up runner increments `_events_seen` (`runners/catchup.py`); the live runner
increments `_events_delivered` without ever recording events as seen, so lag is
structurally zero during live processing and the accumulated surplus also masks lag
across a later LIVE → CATCHING_UP transition. An operator's lag dashboard cannot
distinguish a healthy live subscription from a stalled one. Give the live path a
seen-recording point (bus delivery receipt) or an explicit "lag unavailable in live"
marker on SubscriptionStatus, and make record_events_seen/record_events_unseen
accounting symmetric across phases. Surfaced by the slice-(b) final review (2026-07-31).

## Investigate making sqlalchemy an optional dependency (P3)

Investigate whether sqlalchemy can be moved from core deps to optional extras. Its
current importers, per `grep -rlE '^(from|import) sqlalchemy' src/eventsource/`:
`adapters/postgresql/outbox.py`, `adapters/postgresql/snapshots.py`,
`adapters/postgresql/store.py`, `adapters/sql/checkpoints.py`,
`adapters/_sql/connection.py`, `adapters/_sql/dialect.py`, `adapters/sql/dlq.py`,
`engine.py`, `locks/postgresql.py`, `adapters/sql/migration/audit_log.py`,
`adapters/sql/migration/migration.py`, `adapters/sql/migration/position_mapping.py`,
`adapters/sql/migration/routing.py`, and `readmodels/postgresql.py`. (`stores/` and
`repositories/` no longer exist — both were deleted by the store retirement and
outbox ring migration slices; `migration/repositories/*.py` moved to
`adapters/sql/migration/` when `eventsource.migration` joined the ring map,
ADR 0034.) The key question: do any core interfaces import
sqlalchemy at module level? If the interfaces are clean (pydantic-only), sqlalchemy
can become optional. If not, identify what needs to change. This further lightens
the base install toward the Tier 0 goal.

Prerequisite (done): drop redis from core dependencies.

## Add CI boundary check for core surface purity (P2)

Add a test that imports only core surface modules (events, aggregates, protocols,
`stores/interface`, `application/projections/base`, handlers, exceptions, types) and
asserts no sqlalchemy/redis modules are in `sys.modules` afterward. This prevents
accidental coupling from creeping in and makes a future Tier 0 extraction cheap.

Note: import-linter contracts were added in commit 260c662 — check whether they
already cover this before doing more work here. As of ADR 0024, import-linter now
covers the whole `eventsource.application` ring plus the memory adapters, so the
remaining question is narrower than it was: whether a runtime `sys.modules` assertion
adds anything over the static contract, or whether the static contract alone is
sufficient and this item can be closed. Task 3 Step 9 of the outbox ring migration
(2026-07-31) added exactly this kind of check for one module —
`uv run python -c "import sys, eventsource.ports.outbox; assert 'sqlalchemy' not in
sys.modules"` — as a working example of the runtime assertion this item proposes
generalizing.

Prerequisite (done): document core surface boundary for future Tier 0 extraction
(`docs/core-surface.md`).

## Deterministic or scheduled coverage for bus performance assertions (P3)

The kafka/rabbitmq metrics-overhead and duration tests assert wall-clock thresholds
and were excluded from the blocking broker-tests CI job (marked `@pytest.mark.benchmark`)
because shared runners make elapsed-time assertions nondeterministic (observed 55.79%
vs a 20% threshold on a green code path). Nothing in CI watches metrics overhead now.
Either rewrite the assertions as deterministic proxies (count instrumentation calls
rather than elapsed time) or add a scheduled, non-blocking benchmark workflow that
runs `-m benchmark` and reports results.

## Remove bus facade compat shims (P2) -- DONE via ADR 0031

0.8.0: remove bus facade compat shims -- migrate ~90 white-box test call sites to
collaborator access (`bus._connection_manager.*` etc.), delete the facade property
shims and thin delegations on both backends. (The scheduled
`record_reconnection`/`record_rebalance` removal landed with the
aggregates-application-ring branch.)

Resolved by the bus ring split (ADR 0031, 2026-07-31): `bus/` and its facade
`__init__.py` are deleted outright, so there is no facade left to shim -- the
~90 call sites are retargeted onto `eventsource.adapters._bus` and the
per-backend collaborator modules directly, in the same pass as the ring
move, rather than as a separate 0.8.0 migration.

## Document store_id uniqueness expectations (P3)

Default `store_id`s are not unique across distinct stores: `pg:{database}` collides
for same-named databases on different servers, `sqlite::memory:` for every in-memory
store, `"memory"` for every `MemoryEventStore`. The `PositionForeignError` guard
silently passes for colliding ids. Document that `store_id` must be set explicitly
when two distinct stores could share a name; consider deriving the pg default from
host+port+database.

## SQLite adapter: reads share the writer's connection (P3)

Reads run on the same aiosqlite connection as `append` and outside the write lock,
so a read scheduled between two of an append's INSERTs can observe uncommitted rows
of the in-flight batch. Inherited from the legacy sqlite store design. Take reads on
a separate connection (or under the write lock) so partial appends are never visible.

## Share ExpectedVersion dispatch across store adapters (P3)

All three adapters reimplement `_check_expected`/`_expected_sentinel` verbatim; the
read_category batch-timestamp tie-break divergence showed what this duplication
invites. Hoist into a shared `adapters/_common/` helper, and add a rules note that
behavior asserted by a conformance suite should be implemented once.

## Reconcile DLQ delete_resolved_events cutoff semantics (P3)

`delete_resolved_events(older_than_days)` computes its cutoff differently per
backend: `adapters/memory/dlq.py` truncates to midnight UTC before subtracting
days, while `adapters/sql/dlq.py` subtracts from `now()` directly — so at
`older_than_days=0` a moments-ago resolution is deleted by SQL backends but kept
by the memory backend. The port docstring (`ports/dlq.py`, `delete_resolved_events`)
does not specify cutoff semantics, which is why two conforming-looking adapters
diverged. Pick one semantic, make both conform, update the conformance suite's
per-backend day-zero tests back into the shared suite, and tighten the port
docstring. Found by the Task 7 conformance review (projections-ring slice).

## Catch-up can end early with completed=False when a batch is entirely filtered (P3)

`_process_batch` in `subscriptions/runners/catchup.py` returns a delivered-event
count of 0 when every envelope in a read batch is excluded by the subscription's
event-type filter, and the outer catch-up loop breaks on a zero-delivered batch —
even though the store position advanced and more of the feed remains unread. The
loop conflates "empty read" (genuinely caught up) with "all filtered" (not caught
up, just nothing this batch matched); catch-up exits with `completed=False` and the
subscription is left short of the actual watermark. The live runner happens to
cover the gap once it takes over, so this does not currently drop events end to
end, but it means catch-up's own completion signal is unreliable for
heavily-filtered subscriptions. Pre-existing behavior, surfaced by the store
retirement slice (b) Task 3 review. Suggested fix direction: distinguish "empty
read" from "all filtered" in `_process_batch`'s return contract (e.g. report
envelopes-read alongside envelopes-delivered) so the loop can tell the two cases
apart.

## Re-benchmark pg catch-up horizon predicate at scale (P2)

The slice-(b) completion bench (50k events, batch 500, no concurrent writers) measured
the ports pg adapter's read_all at +27% wall time vs the legacy no-horizon path
(0.8 ms/batch median) against the *inline* `xmin::text::bigint <
pg_snapshot_xmin(...)::text::bigint` predicate — acceptable at that scale, but
EXPLAIN ANALYZE showed that predicate defeating the `global_position` index
(Seq Scan + top-N heapsort instead of Index Scan), so per-batch cost was
O(table size). ADR 0027 part (b) has since replaced that predicate: it was also
wraparound-unsafe (32-bit `xmin` cast compared against a 64-bit epoch-extended
`xid8`, universally true past the first xid epoch), and the fix is exactly the
mitigation this entry proposed — `(txid IS NULL OR txid <
CAST(:txid_horizon AS text)::xid8)` against a new `events.txid` column, with the
horizon computed once per read in a separate query rather than inlined per row.

The re-bench is still warranted at 1M+ rows under concurrent writers — the exact
conditions spec §11 risk 1 (legacy-store-retirement design) names — but must be
re-run against the new predicate shape, not re-read from the old numbers: a bound
parameter against an indexed column is a materially different query plan than a
volatile inline expression, and whether the planner now keeps the `global_position`
index path is exactly what needs re-measuring. Prior methodology + numbers (now
describing a predicate that no longer exists): session artifact
pg-catchup-bench.md (2026-07-31).

## Wire position_mapper into BulkCopier's coordinator call site (P2)

`MigrationCoordinator` never passes a `position_mapper` to `BulkCopier` on the default
path, so an ordinary migration records no position mappings and subscription
checkpoint translation (`SubscriptionMigrator` / `PositionMapper.translate_position`)
has nothing to translate against. Spec §11 risk 2 (per-event append cost in
`BulkCopier` with a position mapper attached) is therefore unmeasured in production
use — the only place it gets exercised is tests that construct a mapper explicitly.
Either wire `position_mapper` into the coordinator's default `BulkCopier`
construction, or document the opt-in explicitly (a migration that wants checkpoint
translation must pass a mapper itself) so the gap is a documented choice rather than
an oversight. Surfaced by the store retirement slice (c) Task 6 review (2026-07-31).

## No in-phase resync for dual-write mirror failures after bulk copy completes (P2)

Once `BULK_COPY` finishes, `SyncLagTracker`'s lag anchor only advances through the
dual-write mirror; a mirror failure during `DUAL_WRITE` clamps the anchor in place
(fail-closed) via `first_failed_source_position`, and nothing in the `DUAL_WRITE`
phase can run an absorbing copy pass to clear it — only `mark_copy_pass_complete`
does, and that only runs during `BULK_COPY`. Today's remedy is abort-and-restart
the migration. This should fold into the catch-up-cap "one more pass" operator API
need: an operator-triggered resync entry point that runs a bounded catch-up copy
pass while already in `DUAL_WRITE`, so a transient mirror failure late in a long
migration doesn't force starting over. Surfaced by the store retirement slice (c)
Task 6 review (2026-07-31); noted in `docs/guides/live-migration.md`.

## Migrate bus/ interface and backends to ports/adapters (P2) -- DONE via ADR 0031

`bus/` colocates the EventBus interface with InMemory, Redis, RabbitMQ, and
Kafka backends. Ring migration: EventBus port into `ports/`, backends into
`adapters/<backend>/bus.py`, guarded optional imports preserved, conformance
suite already exists to anchor behavior. Coordinate with the existing "Remove
bus facade compat shims (P2)" entry — shim removal scheduled for 0.8.0 should
land first or together to avoid double-moving. Campaign residue item (2026-07-31).

Resolved by ADR 0031 (2026-07-31): `EventBus` moved to `ports/bus.py`,
`BaseEventBus`/`SubscriptionRegistry` to adapters-internal `adapters/_bus/`,
and the four backends to `adapters/memory/bus.py`, `adapters/redis/`,
`adapters/kafka/`, `adapters/rabbitmq/` (guarded optional imports preserved).
Landed together with "Remove bus facade compat shims" above, per the
coordination note.

## Relocate subscriptions/ into the application ring (P2)

`subscriptions/` (manager, runners, retry, health, flow control) is use-case
orchestration operating purely on ports since the store retirement, but still
lives as a top-level package outside `application/`. Relocate under
`application/subscriptions/` with deprecation shims for public names, and
extend the Tier-0 import-linter contract to cover it. Campaign residue item
(2026-07-31).

## Resolve the duplicate `OptimisticLockError` name (readmodels vs core) (P2)

Two unrelated classes share the name: `eventsource.ports.readmodels.OptimisticLockError(ReadModelError)`
with `(model_id, expected_version, actual_version=None)`, and
`eventsource.exceptions.OptimisticLockError(EventSourceError)` with
`(aggregate_id, expected_version, actual_version)`. They do not catch each
other. Proposed resolution: rename the read-model one to
`ReadModelVersionConflictError` with a deprecation alias. **The collision
predates the structure slice A (locks/readmodels/engine ring migration,
2026-07-31)** — record that, so a future reader does not attribute it to that
slice.

## Remove the `eventsource.locks` and `eventsource.readmodels` deprecation shims (P3) — Done (ADR 0030)

Originally scheduled for 0.8.0. Removed ahead of schedule as part of the
top-level module ring consolidation (ADR 0030, 2026-07-31), when the project
adopted a no-deprecation-shim policy pre-1.0: `src/eventsource/locks/__init__.py`
and `src/eventsource/readmodels/__init__.py` are deleted; `import
eventsource.locks` / `import eventsource.readmodels` now raise
`ModuleNotFoundError`. The `eventsource.locks` entry in the "Application
ring must not import adapters" `forbidden_modules` contract in
`pyproject.toml` is removed alongside it (owned by a separate task in the
same slice).

## InMemoryReadModelRepository aliases live objects (P3)

`get()`/`get_many()` return the live dict entry rather than a copy, and
`save()`/`save_with_version_check()` mutate the caller's object in place
(bumps `.version`, sets `.updated_at`) — while `PostgreSQLReadModelRepository`
and `SQLiteReadModelRepository` always hydrate fresh instances from the
database. A caller holding a reference to a model it previously saved or
fetched can have it silently mutated by a later, unrelated write against the
same repository. Fix direction: `model_copy()` on read, copy-before-mutate on
save, so all three backends present the same aliasing contract. Found by the
readmodels conformance work (structure slice A, 2026-07-31).

## Decide engine.py's ring placement (P3) — Done (ADR 0029, structure slice A)

`engine.py` moved to `src/eventsource/adapters/_sql/engine.py` — `adapters/_sql/`
rather than dissolving into the existing connection helpers (a distinct
concern, not naturally absorbed by `connection.py` or `dialect.py`) and rather
than `adapters/sql/engine.py` (whose package `__init__` eagerly imports
`projection.py`, reaching into `application/projections/` — see ADR 0029 §3.2
for the full rejected-alternatives argument). The lazy-init backlog item below
is updated with the new import-chain shape.

## Small ring-consistency cleanups (P3)

Batch of low-risk campaign leftovers (2026-07-31): (a) consider renaming
`testing/conformance_ports/` now that it is the only conformance package —
folding it into `testing/conformance.py`'s namespace or documenting the split;
(b) move the tracing decorator used by adapters up to a ports-level helper so
adapters stop importing `observability/` internals directly; (c) consolidate
`protocols.py`'s remaining ABCs/Protocols with their ring homes (several now
duplicate `ports/` definitions in spirit) with deprecation re-exports; (d) the
readmodels port-purity `ast`-based test (verifying `ports/readmodels/` imports
no sqlalchemy) is blind to relative imports — none exist in that package
today, so this is not a live gap, but tighten the test if `ports/` ever
adopts relative imports (structure slice A, 2026-07-31); (e)
`scripts/_mutmut_configure.py`'s `dialect`, `checkpoint`, and `json`
(actually `dlq`) selector entries point at `src/eventsource/repositories/...`,
a package deleted in an earlier slice — those selectors are dead until
retargeted at the modules' current locations or removed outright. Found by
the A6 engine-move task (structure slice A, 2026-07-31), out of scope there
by the task's own boundary.

## Cutover can switch routing with up to cutover_max_lag_events missing (P2)

`cutover_max_lag_events` defaults to 100 (migration/models.py:391); cutover.py:321
allows cutover to succeed with up to that many source events absent from the
target — a real loss window at routing switch, caught only by the non-fatal
post-cutover consistency check. This undercuts dual_write.py's documented
"stuck-until-recopied, never cutover over missing data" stance. Decide: default
to 0 (strict), or document the loss window prominently in the live-migration
guide. Pre-existing default, not a campaign regression; needs its own small
spec. Surfaced by the whole-campaign final review (2026-07-31).

## given_events supports one aggregate per scenario (P3)

The testing harness's `given_events` seeds a single aggregate stream per
scenario; multi-aggregate scenarios need manual store setup. Behavior is
documented in docs/tutorials/08-testing.md (~line 323); this entry exists so
the limitation is tracked as improvable, closing a ledger note from the
aggregates slice (2026-07-31).

## Differentiate run_resync_pass's two zero-return shapes in logs (P3)

`MigrationCoordinator.run_resync_pass` returns 0 both when the migration is
converged and when the coordinator restarted and has no interceptor registered
(coordinator.py ~1010-1017). Operator outcome is identical (retry cutover), but
a differentiating log line would make the restart shape visible. Deferred minor
from the correctness-fixes slice final review (2026-07-31).

## Buffered live events dropped on stop/transition-failure leave lag inflated (P3)

`subscriptions/transition.py` `_cleanup` (~392-402) and the live runner's
`stop()` drop the transition/pause buffers without calling
`record_events_unseen` for the buffered events' seen-receipts (recorded at bus
receipt since the live-lag fix), violating the reconciliation contract stated
in `Subscription.lag`'s docstring. Bounded consequence: no in-tree path
auto-restarts the same Subscription object, and nonzero lag on a stopped/ERROR
subscription is directionally truthful — but re-calling manager.start() on the
same registry objects inherits permanently inflated lag from the re-read. Fix:
unsee `qsize()` on buffer drop, or reset both counters at transition start.
Found by the correctness-fixes slice final review (2026-07-31).
