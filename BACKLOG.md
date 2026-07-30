# Backlog

Open work items, carried over from the retired `bd` (beads) tracker.

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
`stores/interface`, `projections/base`, handlers, exceptions, types) and asserts no
sqlalchemy/redis modules are in `sys.modules` afterward. This prevents accidental
coupling from creeping in and makes a future Tier 0 extraction cheap.

Note: import-linter contracts were added in commit 260c662 — check whether they
already cover this before doing more work here.

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

## Remove bus facade compat shims (P2)

0.8.0: remove bus facade compat shims -- migrate ~90 white-box test call sites to
collaborator access (`bus._connection_manager.*` etc.), delete the facade property
shims and thin delegations on both backends, alongside the scheduled
`record_reconnection`/`record_rebalance` removal.

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
