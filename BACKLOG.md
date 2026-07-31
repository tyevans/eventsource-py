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

Investigate whether sqlalchemy can be moved from core deps to optional extras. It's
used in `stores/postgresql.py`, `stores/sqlite.py`, `repositories/`,
`snapshots/postgresql.py`, `locks/postgresql.py`, and `migration/`. The key question:
do any core interfaces import sqlalchemy at module level? If the interfaces are clean
(pydantic-only), sqlalchemy can become optional. If not, identify what needs to change.
This further lightens the base install toward the Tier 0 goal.

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
sufficient and this item can be closed.

Prerequisite (done): document core surface boundary for future Tier 0 extraction
(`docs/core-surface.md`).

## Migrate outbox repository to ports/adapters (P2)

`src/eventsource/repositories/outbox.py` still mixes the `OutboxRepository` Protocol
with its three implementations (PostgreSQL, SQLite, in-memory) in one file — the same
defect ADR 0024 just fixed for the checkpoint and DLQ repositories. Moving it the same
way (Protocol + dataclasses to `ports/outbox.py`, SQL implementation to
`adapters/sql/outbox.py`, in-memory implementation to `adapters/memory/outbox.py`) is
what finally lets `repositories/` disappear as a package, and lets the two SQL
connection helpers (`adapters/_sql/connection.py` and `repositories/_connection.py`)
merge into one.

## Deterministic or scheduled coverage for bus performance assertions (P3)

The kafka/rabbitmq metrics-overhead and duration tests assert wall-clock thresholds
and were excluded from the blocking broker-tests CI job (marked `@pytest.mark.benchmark`)
because shared runners make elapsed-time assertions nondeterministic (observed 55.79%
vs a 20% threshold on a green code path). Nothing in CI watches metrics overhead now.
Either rewrite the assertions as deterministic proxies (count instrumentation calls
rather than elapsed time) or add a scheduled, non-blocking benchmark workflow that
runs `-m benchmark` and reports results.

## Remove bus facade compat shims (P2)

0.8.0: remove bus facade compat shims -- migrate ~90 white-box test call sites to
collaborator access (`bus._connection_manager.*` etc.), delete the facade property
shims and thin delegations on both backends. (The scheduled
`record_reconnection`/`record_rebalance` removal landed with the
aggregates-application-ring branch.)

## Redesign SnapshotStore port as composed Protocols (P2)

`ports/snapshots.py` now permanently hosts `SnapshotStore`, but it moved verbatim
as an ABC with concrete default bodies (`snapshot_exists`) and a
`delete_snapshots_by_type` that raises `NotImplementedError` by default -- both
violate the settled ports rules ("no implementation code, ever"; optional
capability = not implementing a port, never NotImplementedError). Split into small
composed Protocols (save/get/delete; bulk invalidation as a separate optional
capability port), update the three snapshot adapters and the conformance suite,
and drop the NotImplementedError default. Flagged by the
aggregates-application-ring final review (deliberately out of scope there).

## Lazy top-level eventsource/__init__ (P3)

`import eventsource` eagerly loads sqlalchemy through the public front door
(`application/aggregates/repository.py` -> `stores/__init__` -> `stores/postgresql`).
Correctness is unaffected (sqlalchemy is a core dep) but import time and the Tier 0
story would benefit from a PEP 562 lazy `__getattr__` front door. Pairs with the
"Investigate making sqlalchemy an optional dependency" item above.

## Define store lifecycle in the ports layer (P2)

`close()` is not part of any store port, yet consumers duck-type it:
`SyncStoreFacade.close()` calls `getattr(store, "close", None)`, `MemoryEventStore`
has no `close()`, and `PostgreSQLEventStore.close()` disposes an engine the caller
injected and still owns — `SyncStoreFacade(PostgreSQLEventStore(shared_engine))`
quietly tears down the caller's pool. Add an optional close/lifecycle port with
documented ownership semantics; make engine ownership an explicit constructor flag
on the postgres adapter (or stop disposing caller-provided engines).

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

## Reconcile events.tenant_id schema drift (P2)

`tests/integration/conftest.py` provisions `events.tenant_id VARCHAR(255)` but
`migrations/schemas/events.sql` declares `UUID`; the legacy postgres store binds
`str(...)` and fails with DatatypeMismatchError against a migrations-provisioned
database (surfaced when the ports conformance tests recreated the shared table from
the canonical schema — they now use a private `ports_conformance` database instead).
Verify the legacy store against the real migrations schema and reconcile.

## Make the postgres safe-horizon predicate wraparound-safe (P2)

`xmin::text::bigint < pg_snapshot_xmin(pg_current_snapshot())::text::bigint`
compares a 32-bit xid against an epoch-extended xid8: after the cluster's first xid
epoch, the predicate becomes universally true and no-skip protection silently
disappears. Add an xid8 insert-time column or compare with age(); today it degrades
silently.

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
(0.8 ms/batch median) — acceptable at that scale. But EXPLAIN ANALYZE shows the
safe-horizon predicate defeats the `global_position` index (Seq Scan + top-N heapsort
instead of Index Scan), so per-batch cost is O(table size) and the regression is
untested at 1M+ rows and under concurrent writers — the exact conditions spec §11
risk 1 (legacy-store-retirement design) names. Before a high-throughput production
catch-up path ships on this adapter: rerun at 1M+ events with concurrent writers, and
if the Seq Scan persists, restructure the predicate or index so the planner keeps the
index path (e.g. computing the horizon once per batch in a separate query rather than
inline). Methodology + numbers: session artifact pg-catchup-bench.md (2026-07-31).
