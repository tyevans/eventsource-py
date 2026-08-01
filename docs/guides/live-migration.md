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
- Monitoring `DUAL_WRITE`, reading sync lag with `get_sync_lag()`, and staying
  inside the dual-write timeout budget.
- Gating the switch on `is_cutover_ready()`, calling `trigger_cutover()`, and
  handling `CutoverTimeoutError` with its automatic rollback to `DUAL_WRITE`.
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
`TenantRoutingRepository`, plus the `TenantStoreRouter` from Step 2. Two more
keyword-only arguments are needed for Step 10: a `PositionMapper` wrapping a
`PositionMappingRepository`, and a `CheckpointRepository`. The PostgreSQL
repository implementations each take an `AsyncConnection` or `AsyncEngine`. The
fourth control-plane repository, `MigrationAuditLogRepository`, is not passed to
the coordinator -- you construct it separately and query it directly (see
Observability). Steps 1-3 wire all of these up.

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
from `PENDING`, `BULK_COPY`, and `DUAL_WRITE`; `FAILED` is reachable from
`BULK_COPY`, `DUAL_WRITE`, and `CUTOVER` on an unrecoverable error. Neither is
reachable once a migration is `COMPLETED` -- terminal phases have no outgoing
transitions at all, so a finished migration stays finished.

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

## Step 1: Apply the migration control-plane schema

The coordinator keeps all of its state in four PostgreSQL tables. Create them
before you construct anything else -- the repositories in Steps 2 and 3 assume
the tables already exist and do not create them on demand.

The DDL ships with the library:

```python
from eventsource.migrations import get_schema

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
