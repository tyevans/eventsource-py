# Live Tenant Store Migration

Move a single tenant's events from one event store to another -- for example, off a
shared PostgreSQL store and onto a dedicated one -- while the application keeps
reading and writing. The source store stays authoritative until the moment of
cutover, so at no point is there a window where events can be lost.

The work is driven by `MigrationCoordinator` from `eventsource.application.migration`, which
walks a tenant through five phases (`PENDING` -> `BULK_COPY` -> `DUAL_WRITE` ->
`CUTOVER` -> `COMPLETED`), and by `TenantStoreRouter`, a transparent proxy you put
in front of your event store so application code never has to know a migration is
running. Historical events stream across in the background, new events are written
to both stores while the copy catches up, and the final switch is a sub-second
write pause that rolls back automatically if it overruns its budget.

Follow the steps below in order the first time you run a migration. Each step is
self-contained, so once you are familiar with the flow you can jump straight to the
one you need -- monitoring sync lag, gating a cutover, verifying consistency, or
aborting.

## What this guide covers

By the end you will have moved one tenant's events onto a new store with the
application still serving traffic, and you will know how to observe, gate, and
undo each stage of that move.

The steps walk through:

- Applying the migration control-plane schema and putting `TenantStoreRouter` in
  front of your event store so routing can change underneath running code.
- Wiring a `MigrationCoordinator` with its repositories, a PostgreSQL advisory
  lock manager, and a tuned `MigrationConfig`.
- Starting a migration and watching `BULK_COPY` progress with `stream_status()`,
  including pausing and resuming the copy.
- Monitoring `DUAL_WRITE` and reading sync lag with `get_sync_lag()`.
- Gating the switch on `is_cutover_ready()`, calling `trigger_cutover()`, and
  reading the rollback result when a cutover overruns its timeout.
- Verifying the result with `verify_consistency()` and moving subscription
  checkpoints across with `migrate_subscriptions()`.
- Aborting or rolling back, reading migration metrics and the audit log, and
  interpreting the error classification when something goes wrong.

This guide assumes you already have a working multi-tenant setup: tenant context
is propagated on every operation, and both the source and target stores are
reachable from the process running the coordinator. It does not cover choosing a
tenant-isolation strategy or provisioning the target database -- see the
Multi-Tenancy and Production Deployment guides for those.

Two things are deliberately out of scope. Migrating more than one tenant at a
time is possible (each migration is independent and keyed by tenant), but this
guide follows a single tenant end to end. And the terminal `ABORTED` and
`FAILED` phases are covered only as escape hatches, not as a routine path -- a
healthy migration ends in `COMPLETED`.

## Prerequisites

Before you start, you need a source store, a target store, a PostgreSQL
control-plane database, and the `postgresql` extra installed:

```bash
uv add "eventsource-py[postgresql]"   # pulls in asyncpg
```

The control plane is PostgreSQL-only. `get_schema("migration")` ships a
PostgreSQL template only (there is no SQLite equivalent), and cutover
coordination goes through `PostgreSQLLockManager` from `eventsource.adapters.postgresql.locks`.
The stores you migrate between can be anything implementing `FullEventStore`
(the combined ports surface: append, stream read, event lookup, global feed,
and category query); the database that holds migration state cannot.

### Required components

**Source store** -- the `FullEventStore` the tenant lives on today. It is the
first positional argument to `MigrationCoordinator(source_store=...)`,
alongside the keyword-only `source_store_id` string (default `"default"`) used
as its key in routing records.

**Target store** -- a second `FullEventStore` instance, already provisioned with the
events schema and reachable from the coordinator process. You pass it to
`start_migration(tenant_id, target_store, target_store_id, ...)`. It does not
need to be the same backend as the source, but it should hold none of this
tenant's events: nothing validates emptiness, and pre-existing events will skew
the bulk-copy position mappings and the consistency check.

**PostgreSQL advisory lock manager** -- `PostgreSQLLockManager(session_factory)`
from `eventsource.adapters.postgresql.locks`, built from a SQLAlchemy
`async_sessionmaker[AsyncSession]`. It is a keyword-only, optional constructor
argument, but the coordinator raises `MigrationError` when it needs to build its
`CutoverManager` without one, so treat it as required for any migration you
intend to finish. Each held lock occupies its own session, because PostgreSQL
advisory locks are session-scoped -- size your connection pool accordingly.

The coordinator also takes two required repositories, `MigrationRepository` and
`TenantRoutingRepository`, plus the `TenantStoreRouter` from Step 2. Three more
arguments are optional: a `PositionMapper` wrapping a `PositionMappingRepository`
and a `CheckpointRepository` (both needed for Step 10), and a
`MigrationAuditLogRepository` (read in Step 9). The PostgreSQL repository
implementations each take an `AsyncConnection` or `AsyncEngine`. Steps 2 and 3
wire all of these up.

Passing a `PositionMapper` is not by itself enough to get position mappings
recorded: mappings are only written when the coordinator was constructed with
one **and** `MigrationConfig.position_mapping_enabled` is `True`, which is
the default. That gate has a cost -- with a mapper attached, the bulk copier
appends one event at a time so each target position can be recorded, where
it otherwise batches. Set `position_mapping_enabled=False` to keep the
batched path if you do not need checkpoint translation (`migrate_subscriptions`
will then have nothing to translate against).

Two application-side assumptions carry through the whole guide. Tenant context
must be propagated on every store operation, since routing decisions are keyed
by tenant. And application code must reach the event store through
`TenantStoreRouter` rather than holding a direct reference to the source store
-- otherwise writes will bypass dual-write and be lost at cutover.

### Migration phases at a glance

`MigrationPhase` is a strict state machine. The happy path is five phases, and
each one determines where `TenantStoreRouter` sends application traffic:

| Phase | What is happening | Reads | Writes |
| --- | --- | --- | --- |
| `PENDING` | Migration record created, not started | Source | Source |
| `BULK_COPY` | Historical events streaming to target | Source | Source |
| `DUAL_WRITE` | New events written to both stores | Source | Both, source first and authoritative |
| `CUTOVER` | Brief pause while routing flips | Source | Blocked (`WritePausedError` past the pause timeout) |
| `COMPLETED` | Migration finished | Target | Target |

Note that reads keep coming from the source right up until cutover completes --
the target is never read-served while it is still catching up.

Two properties on `MigrationPhase` are worth knowing, because later steps use
them: `is_active` is true for `BULK_COPY`, `DUAL_WRITE`, and `CUTOVER`, and
`is_terminal` is true for `COMPLETED`, `ABORTED`, and `FAILED`. A status loop
that exits on `status.phase.is_terminal` stops on success and on failure alike.

The forward transitions the coordinator drives are:

- `PENDING -> BULK_COPY` on `start_migration()`
- `BULK_COPY -> DUAL_WRITE` once the historical copy is done
- `DUAL_WRITE -> CUTOVER` once sync lag is within `cutover_max_lag_events` (0 by default -- exactly zero)
- `CUTOVER -> COMPLETED` once routing has flipped

`MigrationRepository` enforces this with a `VALID_TRANSITIONS` table, so an
out-of-order phase update is rejected rather than corrupting state.

Sync lag is anchored to the last source position the target has actually
copied; once `BULK_COPY` completes, that anchor only advances through the
dual-write mirror, and a mirror failure during `DUAL_WRITE` clamps it in
place rather than letting the reported lag drift wrong.

When that happens, run an in-phase resync rather than aborting:

    remaining = await coordinator.run_resync_pass(migration.id)
    while remaining:
        remaining = await coordinator.run_resync_pass(migration.id)

Each call runs one bounded catch-up copy pass while the migration stays in
`DUAL_WRITE`, and returns the number of unabsorbed mirror failures left. A
return of 0 means the lag anchor is unclamped and cutover can proceed once
lag drains. Bounding the retries is your policy, not the library's: a count
that stops falling is a mirror problem to investigate, not a pass to repeat.

> **Warning — a nonzero `cutover_max_lag_events` accepts event loss.** The lag
> it tolerates is not optimistic slack: `safe_lag_anchor` guarantees every
> counted event is provably absent from the target. Writes are paused for the
> whole cutover and nothing in the sequence copies the residue, so any lag
> remaining at the routing switch is events the target never receives while it
> becomes authoritative. The default is 0. If a cutover refuses because lag
> will not drain, the remedy is `run_resync_pass` (above), not a higher
> threshold.

`ABORTED` and `FAILED` are the two off-ramps. `abort_migration()` is available
from `PENDING`, `BULK_COPY`, and `DUAL_WRITE`. It raises `MigrationError` in
`CUTOVER`. `FAILED` is reachable from `BULK_COPY`, `DUAL_WRITE`, and `CUTOVER`.
In `CUTOVER`, it is the outcome of an unrecoverable error or of a failed
post-switch check (see [Cutover safety rules](#cutover-safety-rules)). Neither
off-ramp is reachable once a migration is `COMPLETED` -- terminal phases have
no outgoing transitions at all, so a finished migration stays finished.

One transition is a rollback rather than a failure: `CUTOVER -> DUAL_WRITE`. A
cutover that overruns `cutover_timeout_ms` does not land in `FAILED`; it returns
to `DUAL_WRITE` with the source still authoritative, and you can retry once
whatever caused the delay clears (Step 8).

Pausing is not a phase. `pause_migration()` sets an `is_paused` flag on the
migration record while the phase stays `BULK_COPY`, so a paused copy still
reports its phase normally and routing is undisturbed. It raises
`MigrationError` if the migration has already reached a terminal phase.

Alongside `MigrationPhase`, each tenant's routing record carries a
`TenantMigrationState` (`NORMAL`, `BULK_COPY`, `DUAL_WRITE`, `CUTOVER_PAUSED`,
`MIGRATED`). That is the value the router actually reads to pick a store; the
coordinator keeps it in step with the phase, so in practice you observe the
phase and let routing look after itself.

### Cutover safety rules

These rules hold for every migration. Read them before you call
`trigger_cutover()` or `abort_migration()`.

- **Abort is refused during cutover.** While the phase is `CUTOVER`,
  `abort_migration()` raises `MigrationError`. If a lock manager is configured,
  abort takes the tenant's cutover lock and checks the phase again. It waits up
  to 5 seconds for that lock. If a cutover holds the lock for longer, abort
  raises `MigrationError`. Retry after the cutover returns.
- **The cutover stops if the phase changed.** The cutover checks the migration
  phase after writes pause and again just before routing switches. If the
  migration is no longer `CUTOVER`, the cutover does not switch routing. It
  clears the pause and the dual-write interceptor, and it returns a failed
  `CutoverResult`. It does not roll back to `DUAL_WRITE`.
- **The write pause is per process.** `WritePauseManager` holds the pause in
  process memory. Writers in the same process wait for the pause to end, up to
  the write pause timeout. In another process, the routing record shows
  `CUTOVER_PAUSED`, so its writes raise `WritePausedError` at once. They do not
  wait. A write that passed the routing check before the pause began can still
  land in the old store. The wait itself is not stored in the database. Run one
  writer per tenant during cutover, or stop the other writers before you call
  `trigger_cutover()`.
- **A failed verification or subscription migration ends in `FAILED`.** The
  switch to the target has already happened when verification runs. A failed
  check therefore ends the migration in `FAILED`. It never ends in `COMPLETED`.
  The failure counts when:
  - `verify_consistency` is `True` and the target store is not registered,
    verification raises, or it finds violations;
  - `migrate_subscriptions` is `True`, position mapping and a checkpoint
    repository are configured, and subscription migration raises or reports
    at least one failed subscription.
- **The tenant stays on the target after such a failure.** Routing is not moved
  back. Writes made on the target after the switch would be missing from the
  source. The tenant keeps reading and writing the target. The migration cannot
  be retried in place. Use the audit log and the consistency report to decide
  how to repair the target.

When a check fails, the outcome is visible in three places:

- The `CutoverResult` returned by `trigger_cutover()` has `success=False`,
  `rolled_back=False`, `error_message` set to the failure, and
  `consistency_verified` set to `False`. It is `None` when verification is off.
- The migration has `phase == MigrationPhase.FAILED` and `last_error` set to
  the same message.
- The audit log has one `MIGRATION_FAILED` entry. Its `details` include
  `consistency_verified`, `subscriptions_migrated`, and `target_store_id`.
  The log has no `CUTOVER_COMPLETED` entry for that migration.

A clean cutover returns `success=True` with `consistency_verified=True`. Its
phase becomes `COMPLETED`.

## Step 1: Apply the migration control-plane schema

The coordinator keeps all of its state in four PostgreSQL tables. Create them
before you construct anything else -- the repositories in Steps 2 and 3 assume
the tables already exist and do not create them on demand.

The DDL ships with the library:

```python
from eventsource.adapters.sql.schemas import get_schema

schema_sql = get_schema("migration")
```

The script is a multi-statement PostgreSQL script that also defines two
`plpgsql` trigger functions, so most SQLAlchemy execution paths will reject it
(`exec_driver_sql()` runs a single statement at a time). Hand the whole script
to the raw asyncpg connection instead:

```python
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("postgresql+asyncpg://.../control_plane")

async with engine.begin() as conn:
    raw_conn = await conn.get_raw_connection()
    await raw_conn.driver_connection.execute(schema_sql)
```

Every statement is `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`
/ `CREATE OR REPLACE FUNCTION`, so re-running the script is safe. If you manage
schema with Alembic, paste the script into a revision's `upgrade()` rather than
calling `get_schema()` at runtime.

### `get_schema("migration")` and the four tables

`get_schema("migration")` returns the PostgreSQL template only. There is no
SQLite variant -- `get_schema("migration", backend="sqlite")` raises
`ValueError`, because the schema relies on `JSONB`, partial indexes, and
`plpgsql` triggers. The migration tables are also *not* part of
`get_all_schemas()`; that combined script covers events, outbox, checkpoints,
DLQ, and snapshots. You have to apply the migration schema explicitly.

The four tables it creates:

| Table | Holds | Read by |
| --- | --- | --- |
| `tenant_migrations` | One row per migration: phase, progress counters, per-phase timestamps, JSONB config, error and pause state | `MigrationRepository` |
| `tenant_routing` | One row per tenant: current `store_id`, `migration_state`, and a nullable `active_migration_id` | `TenantRoutingRepository`, and through it `TenantStoreRouter` |
| `migration_position_mappings` | Source position -> target position, with the event ID for correlation | `PositionMappingRepository` / `PositionMapper` |
| `migration_audit_log` | Append-only trail of migration events, with `old_phase`/`new_phase`, JSONB `details`, and `operator` | `MigrationAuditLogRepository` |

Three details worth knowing before you start:

- `tenant_migrations` has a **partial unique index on `tenant_id`** covering
  every non-terminal phase. The database enforces one active migration per
  tenant; a second `start_migration()` for the same tenant fails at the insert
  rather than silently forking. Once a migration is `completed`, `aborted`, or
  `failed`, that tenant is free again, and the historical rows stay for audit.
- `phase` and `migration_state` are `CHECK`-constrained to the lowercase enum
  values (`pending`/`bulk_copy`/... and `normal`/`bulk_copy`/`dual_write`/
  `cutover_paused`/`migrated`). An unknown value is rejected by the database,
  not just by the Python state machine.
- `migration_audit_log.event_type` is `CHECK`-constrained to the fifteen values
  of the `AuditEventType` enum (`migration_started`, `phase_changed`,
  `cutover_rolled_back`, `verification_failed`, `progress_checkpoint`, and so
  on). Writing a custom audit type outside that set is rejected.
- `migration_position_mappings` and `migration_audit_log` both cascade-delete on
  `tenant_migrations(id)`, while `tenant_routing.active_migration_id` is
  `ON DELETE SET NULL`. Deleting an old migration row therefore discards its
  audit trail -- prefer keeping completed rows, and archive out of band if the
  audit log grows.

`updated_at` on `tenant_migrations` and `tenant_routing` is maintained by
`BEFORE UPDATE` triggers, so the timestamps are trustworthy even for writes that
do not go through the repositories.

### Where to apply it

Apply this schema to a **control-plane database**, not to the per-tenant event
stores you are migrating between. The four tables describe migrations *about*
stores; nothing in them is part of any store's event data, and no store
implementation reads them. Only the repositories and the coordinator do.

Concretely:

- **One control-plane database per deployment**, shared by every tenant
  migration. Its connection is what you hand to `PostgreSQLMigrationRepository`
  and its three siblings -- each takes an `AsyncConnection` or `AsyncEngine` as
  its first argument, so in practice you create one engine for the control plane
  and pass it to all four.
- **PostgreSQL 12 or newer.** The schema header states the requirement, and it
  is real: the partial unique index on `tenant_migrations`, the `JSONB` columns,
  and the `plpgsql` `updated_at` triggers all need a modern PostgreSQL.
- **The event stores get only their normal schema.** Apply
  `get_schema("events")` (or `get_all_schemas()`) to the target store before you
  start, exactly as you did for the source. Do not apply `get_schema("migration")`
  there -- `get_all_schemas()` does not include it, so the default path already
  does the right thing.

The advisory lock manager is a separate decision. `PostgreSQLLockManager` takes
an `async_sessionmaker[AsyncSession]`, and pointing it at the control-plane
database is the obvious choice, since that database is already PostgreSQL and
already in the coordinator's dependency set. It does not have to be, though --
advisory locks are namespaced by key within whichever PostgreSQL instance holds
them, so any PostgreSQL database that every coordinator process can reach will
do. What matters is that *all* processes that might run a migration for the same
tenant use the **same** lock database; two coordinators locking in different
databases are not mutually exclusive at all.

Two placements to avoid:

- **Inside the source store's database.** The source is the database you are
  moving off. When you eventually decommission it, you take the migration
  history and routing table with it -- including the `tenant_routing` rows that
  tell the router where every tenant now lives.
- **Inside the target store's database.** Less obviously wrong, but it makes
  migration state unreadable exactly when the target is unhealthy, which is one
  of the situations where you most want to read it and abort.

Co-locating the control plane with an existing application or metadata database
is fine and common. The requirements are only that it is PostgreSQL 12+, that it
is reachable from every process that constructs a `MigrationCoordinator`, and
that its lifecycle is independent of the stores being migrated.

One further consequence of the split: subscription checkpoints stay wherever
your `CheckpointRepository` already keeps them. Step 10 rewrites checkpoint
positions in place using the position mappings from the control plane; it does
not move the checkpoint table itself.

## Step 2: Put the router in front of the event store

`TenantStoreRouter` wraps your event store and sends each call to the store that
the tenant uses now. Application code calls the router. It never calls the source
store directly.

```python
from eventsource.adapters.sql.migration import PostgreSQLTenantRoutingRepository
from eventsource.application.migration import TenantStoreRouter

routing_repo = PostgreSQLTenantRoutingRepository(control_engine)

router = TenantStoreRouter(
    source_store,
    routing_repo,
    stores={"default": source_store},
    default_store_id="default",
    write_pause_timeout=5.0,
)
```

- `default_store_id` must equal the `source_store_id` you pass in Step 3. A tenant
  with no routing record uses this store.
- `write_pause_timeout` is the number of seconds a write waits during the cutover
  pause. A write that waits longer raises `WritePausedError`.
- Give the router to every component that writes events. A write that goes to
  `source_store` directly skips dual-write, so the event is lost at cutover.

## Step 3: Create the coordinator

`MigrationCoordinator` runs every phase. Build it once per process.

```python
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
from eventsource.adapters.sql.migration import (
    PostgreSQLMigrationAuditLogRepository,
    PostgreSQLMigrationRepository,
    PostgreSQLPositionMappingRepository,
)
from eventsource.application.migration import MigrationCoordinator, PositionMapper

session_factory = async_sessionmaker(control_engine, class_=AsyncSession)

coordinator = MigrationCoordinator(
    source_store,
    PostgreSQLMigrationRepository(control_engine),
    routing_repo,
    router,
    source_store_id="default",
    lock_manager=PostgreSQLLockManager(session_factory),
    position_mapper=PositionMapper(PostgreSQLPositionMappingRepository(control_engine)),
    checkpoint_repo=SQLCheckpointRepository(projection_engine),
    audit_log_repo=PostgreSQLMigrationAuditLogRepository(control_engine),
)
```

- `lock_manager` is required to run a cutover. Without it, `trigger_cutover()`
  raises `MigrationError`.
- `position_mapper` and `checkpoint_repo` are required only for Step 10.
- `checkpoint_repo` must point at the database that holds your projection
  checkpoints. That database can differ from the control plane.
- `audit_log_repo` is optional. Without it, the audit log is empty.

## Step 4: Start the migration and watch the bulk copy

`start_migration()` creates the migration record, sets routing to `BULK_COPY`,
and starts the copy as a background task. The call returns before the copy
finishes.

```python
from eventsource.application.migration import MigrationConfig, MigrationPhase
from eventsource.adapters.postgresql.store import PostgreSQLEventStore

target_store = PostgreSQLEventStore(target_engine)  # schema already applied

migration = await coordinator.start_migration(
    tenant_id=tenant_id,
    target_store=target_store,
    target_store_id="dedicated",
    config=MigrationConfig(batch_size=1000, max_bulk_copy_rate=5000),
)
```

Watch the copy progress until it leaves `BULK_COPY`:

```python
async for status in coordinator.stream_status(migration.id, update_interval=2.0):
    print(status.phase.value, f"{status.progress_percent:.1f}%", status.events_copied)
    if status.phase != MigrationPhase.BULK_COPY:
        break
```

Pause and resume the copy when you need to. A pause takes effect after the
current batch:

```python
await coordinator.pause_migration(migration.id)
await coordinator.resume_migration(migration.id)
```

The coordinator moves the migration to `DUAL_WRITE` when the copy finishes. To
block until then, call `wait_for_phase(migration.id, MigrationPhase.DUAL_WRITE,
timeout=3600)`. It also returns early on a terminal phase, so check the returned
phase.

`start_migration()` raises `MigrationAlreadyExistsError` when the tenant already
has an active migration.

## Step 5: Monitor dual-write and sync lag

In `DUAL_WRITE`, the router writes each new event to the source first and then to
the target. The source stays authoritative. A failed target write does not fail
the application write. The interceptor records the failed write, and sync lag
grows until the event reaches the target.

```python
lag = await coordinator.get_sync_lag(migration.id)
if lag is not None:
    print(lag.events)
```

- `get_sync_lag()` returns `None` when no lag tracker exists.
- Cutover needs lag at or below `cutover_max_lag_events`, which is 0 by default.
- When a mirror write failed, run resync passes until the return value is 0. See
  [Migration phases at a glance](#migration-phases-at-a-glance).

## Step 6: Gate the cutover and trigger it

Before the cutover, stop other writers for this tenant, or make sure they use
the router. The write pause is held in process memory (see
[Cutover safety rules](#cutover-safety-rules)).

```python
ready, reason = await coordinator.is_cutover_ready(migration.id)
if not ready:
    raise RuntimeError(f"Not ready for cutover: {reason}")

result = await coordinator.trigger_cutover(migration.id)
```

`trigger_cutover()` pauses writes, checks lag, switches routing to the target,
and then runs the post-switch checks. It does not raise on a failed cutover. It
returns a `CutoverResult`:

- `success` is `True` only when the migration reaches `COMPLETED`.
- `duration_ms` and `events_synced` describe the cutover sequence.
- `error_message` names the failure. `rolled_back` is `True` when routing was
  restored to the source.
- `consistency_verified` is set when verification ran.

With the default configuration, the post-switch checks are verification
(Step 7) and subscription migration (Step 10). A failed check ends the migration
in `FAILED`, not `COMPLETED`.

## Step 7: Verify consistency

Verify before you trigger the cutover. The target store must be registered,
which `start_migration()` does for you.

```python
from eventsource.application.migration import VerificationLevel

report = await coordinator.verify_consistency(
    migration.id,
    level=VerificationLevel.HASH,
    sample_percentage=100.0,
)
if not report.is_consistent:
    for violation in report.violations:
        print(violation)
```

- `VerificationLevel.HASH` compares event hashes per stream. `sample_percentage`
  sets the share of streams that are checked. `100.0` checks all of them.
- Each run writes a `VERIFICATION_COMPLETED` or `VERIFICATION_FAILED` audit
  entry.
- `coordinator.get_consistency_report(migration.id)` returns the most recent
  report.

## Step 8: Handle a cutover timeout and retry

`cutover_timeout_ms` limits how long writes stay paused. Its default is 500 ms.
A cutover also rolls back when sync lag is too high. In both cases the result
has `success=False` and `rolled_back=True`, and the migration returns to
`DUAL_WRITE`. The source stays authoritative. No exception reaches your code.

```python
result = await coordinator.trigger_cutover(migration.id)
if not result.success and result.rolled_back:
    print(result.error_message)
```

To retry, first find the cause:

- For a timeout, give the next attempt more time with `timeout_ms`:

  ```python
  result = await coordinator.trigger_cutover(migration.id, timeout_ms=1000)
  ```

- For high lag, run `run_resync_pass()` until it returns 0, then trigger the
  cutover again.

The migration keeps its `DUAL_WRITE` phase between attempts, so you can retry.
The migration record stores the failure in `last_error` as `Cutover failed: ...`.
Set a limit on retries in your own code.

## Step 9: Abort, read metrics, and read the audit log

`abort_migration()` stops a migration and restores routing to the source. It is
allowed in `PENDING`, `BULK_COPY`, and `DUAL_WRITE`. It raises `MigrationError`
in `CUTOVER`.

```python
abort_result = await coordinator.abort_migration(
    migration.id, reason="operator stopped rollout"
)
```

The result has `final_phase=MigrationPhase.ABORTED`. The target keeps the events
it already received. Nothing is deleted from it.

### Metrics

Read the in-process metrics while the migration is active:

```python
from eventsource.application.migration import get_migration_metrics

snapshot = get_migration_metrics(str(migration.id), str(tenant_id)).get_snapshot()
print(snapshot.events_copied, snapshot.sync_lag_events)
print(snapshot.cutover_durations, snapshot.cutover_failures)
print(snapshot.verification_failures, snapshot.failed_target_writes)
```

The OpenTelemetry instruments are:

- `migration.cutover.duration` (histogram, ms). The `success` attribute is
  `"true"` only for a cutover that reaches `COMPLETED`.
- `migration.cutover.failures` (counter). It counts a cutover that switched
  routing and then ended in `FAILED`.
- `migration.verification.failures` and `migration.target.writes.failed`
  (counters).
- `migration.sync.lag` (gauge).

The coordinator drops the in-process instance when a migration reaches
`COMPLETED`, `FAILED`, or `ABORTED`. After that, `get_migration_metrics()`
returns a new, empty instance. Read the snapshot before the migration ends, or
export the OpenTelemetry instruments for long-term records.

### Audit log

Read the audit trail with the repository that you passed in Step 3:

```python
audit_repo = PostgreSQLMigrationAuditLogRepository(control_engine)

for entry in await audit_repo.get_by_migration(migration.id):
    old = entry.old_phase.value if entry.old_phase else "-"
    new = entry.new_phase.value if entry.new_phase else "-"
    print(entry.occurred_at.isoformat(), entry.event_type.value, old, "->", new)
```

Entries are ordered by `occurred_at`, oldest first. A failed post-switch check
writes a `MIGRATION_FAILED` entry. A clean cutover writes `CUTOVER_COMPLETED`.

## Step 10: Move subscription checkpoints

A subscription stores its position in the source store's position space. The
target store uses different positions. During `DUAL_WRITE`, the coordinator
records a position mapping for each event. Subscription migration uses those
mappings to rewrite each checkpoint in place.

Subscription migration needs:

- `position_mapper` and `checkpoint_repo` from Step 3.
- `MigrationConfig.position_mapping_enabled=True`, which is the default.

With the default configuration, `trigger_cutover()` runs subscription migration
after the switch. Read the result with `get_subscription_summary()`:

```python
summary = coordinator.get_subscription_summary(migration.id)
if summary is not None:
    print(summary.successful_count, summary.failed_count, summary.skipped_count)
```

To run it yourself for selected subscriptions, start with a dry run. A dry run
computes the new positions and writes nothing:

```python
summary = await coordinator.migrate_subscriptions(
    migration.id,
    subscription_names=["OrderSummaryProjection"],
    dry_run=True,
)
```

A checkpoint that is older than every recorded mapping cannot be translated. It
counts as a failed subscription. When this happens during cutover, the migration
ends in `FAILED`.
