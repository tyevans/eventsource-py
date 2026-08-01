# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **`DomainEvent.__init_subclass__` no longer mutates the parent class's shared `event_type` FieldInfo.** Subclassing a concrete event corrupted the parent's registry key: `register_event(Parent)` after `class Child(Parent)` filed Parent under "Child", making stored `"Parent"` events undeserializable (or raising a spurious `DuplicateEventTypeError`). Event-type derivation is now unified on the new `DomainEvent.event_type_name()` classmethod, used by both instance construction and `EventRegistry`.
- **`clear_tenant_context()` now actually clears.** Previously it left the token stack intact, so any enclosing `tenant_scope()` exit silently resurrected the "cleared" tenant — a cross-tenant leakage vector. It now invalidates all outstanding tokens in the current context; a subsequent `reset_tenant_context()` (including a scope exit) raises `TenantContextResetError` instead of restoring a stale tenant.
- Two `@handles` methods for the same event type in one class now raise the new `DuplicateHandlerError` at class-definition time instead of silently dropping one handler (discovery order used to decide the winner alphabetically).

### Changed
- **BREAKING: `DomainEvent` now uses `extra="forbid"`.** Unknown constructor kwargs (typically typos) raise `pydantic.ValidationError` instead of being silently dropped and persisting an event with missing data. Arbitrary payload data belongs in the `metadata` field.
- **BREAKING: `EventTypeNotFoundError`, `DuplicateEventTypeError`, and `HandlerSignatureError` no longer subclass `KeyError`/`ValueError`.** `except KeyError`/`except ValueError` will no longer catch them; catch the specific type or `EventSourceError`. Their `str()` output is no longer re-quoted by `KeyError.__str__`.
- **BREAKING: 13 infrastructure exceptions moved from `eventsource.domain.exceptions` to `eventsource.ports.exceptions`** (ADR 0041, no shims): `CheckpointError`, `CheckpointNotFoundError`, `EventBusConnectionError`, `EventStoreConnectionError`, `LockAcquisitionError`, `LockNotHeldError`, `PositionDecodeError`, `PositionForeignError`, `SubscriptionError`, `SubscriptionConfigError`, `SubscriptionStateError`, `SubscriptionAlreadyExistsError`, `TransitionError`. Top-level `from eventsource import ...` re-exports are unchanged.
- `DeciderAggregate` stamping now applies the ambient tenant-context fallback for every command type, not only `DomainCommand` (unified with `create_event()` semantics via the shared `_provenance_updates()` helper).
- `DeclarativeAggregate` validates handler signatures at class-definition time: async handlers and wrong parameter counts raise `HandlerSignatureError` (previously only projections validated).
- `DeciderAggregate.state` raises `RuntimeError` on a `None` state instead of using a bare `assert` (which `python -O` strips).

### Added
- `DomainEvent.event_type_name()` classmethod — the canonical wire name for an event class.
- `eventsource.domain.decorators.discover_handlers()` — shared @handles discovery used by aggregates and projections.
- `DuplicateHandlerError` exception; `HandlerSignatureError` gains an optional `reason` parameter.
- `domain/__init__` now exports `HandlerSignatureError`, `DuplicateHandlerError`, and the three tenant-context exceptions (surface sync).

## [0.9.0] - 2026-08-01

This release contains the multitenancy dissolution and out-of-ring settlement wave (ADRs 0038-0040, PR #101). These changes were written for 0.8.0 but never reached it: their PR was stacked and merged into its parent branch seconds *after* the parent merged to main, so GitHub marked it merged while the commits were orphaned. The published 0.8.0/0.8.1 wheels still contain `eventsource.multitenancy` and `eventsource.migrations`; this release is where they are actually removed.

### Added

- **Two new `import-linter` forbidden contracts formalize `eventsource.observability` and `eventsource.testing` as settled out-of-ring packages** (ADR 0040): "Domain and ports must not import observability" and "Rings must not import the testing toolkit." Both assert properties that already held; neither required a code change. With ADRs 0038 and 0039 dissolving `eventsource.multitenancy` and `eventsource.migrations`, every top-level package under `src/eventsource/` is now one of the four rings or one of these two settled exceptions -- the ring-migration campaign's completion criterion.

### Removed

- **BREAKING: `eventsource.multitenancy` no longer exists** (ADR 0038, dissolving the last transitional package alongside `eventsource.migrations` below). `import eventsource.multitenancy` now raises `ModuleNotFoundError`. No shim, no deprecation warning: the same pre-1.0, no-external-consumers standing rule ADR 0025/0026/0029-0034 already applied without qualification. Replacements: `eventsource.multitenancy.context` (`tenant_context`, `TenantContextToken`, `get_current_tenant`, `get_required_tenant`, `set_current_tenant`, `reset_tenant_context`, `clear_tenant_context`, `tenant_scope`, `tenant_scope_sync`) -> `eventsource.domain.tenant_context`; `eventsource.multitenancy.events.TenantDomainEvent` -> `eventsource.domain.tenant_events`; `eventsource.multitenancy.exceptions` (`TenantContextNotSetError`, `TenantContextResetError`, `TenantMismatchError`) -> merged into `eventsource.domain.exceptions` (already `EventSourceError`-rooted, no rebase); `eventsource.multitenancy.repository.TenantAwareRepository` -> `eventsource.application.aggregates.tenant_repository`. Top-level `from eventsource import ...` imports are unaffected -- `__all__` is byte-identical, the barrel re-exports from the new homes directly. `TenantAwareRepository` was never re-exported from the top-level package and still is not; import it from its new module path. As part of this move, the `importlib`-based soft dependency `AggregateRoot._get_tenant_from_context()` used while reaching for an out-of-ring package is replaced by a direct import of `eventsource.domain.tenant_context.get_current_tenant`, now that the target is a same-ring sibling shipped unconditionally.
- **BREAKING: `eventsource.migrations` (plural, the schema-DDL package) no longer exists** (ADR 0039). `import eventsource.migrations` now raises `ModuleNotFoundError`. No shim, no deprecation warning, same standing rule. The whole package relocates as one unit -- `__init__.py`, `SCHEMA_DESIGN.md`, `additive/`, `schemas/`, `templates/`, `updates/` -- to `eventsource.adapters.sql.schemas`, unchanged in every other respect: `from eventsource.migrations import get_schema` becomes `from eventsource.adapters.sql.schemas import get_schema`, and every other name (`get_all_schemas`, `get_template_path`, `list_schemas`, `list_backends`, `get_alembic_template`, `list_alembic_templates`, the seven `*_SCHEMA` constants, `SchemaName`, `BackendName`) moves the same way. None of these were ever re-exported from top-level `eventsource`. Packaging is unaffected: the `.sql`/`.md`/template files are wheel-verified to ship at the new path (`uv build` + `unzip -l` on the built wheel), with no `pyproject.toml` change needed.

## [0.8.1] - 2026-08-01

### Fixed

- **The 0.8.0 wheel published to PyPI was missing the entire `eventsource.adapters.memory` package** -- `pip install eventsource-py==0.8.0` failed on `from eventsource import InMemoryEventStore` (and every other memory-adapter name) with `ModuleNotFoundError: No module named 'eventsource.adapters.memory'`. Root cause: `.gitignore` carried an unanchored `memory/` pattern (intended for machine-local agent-team memory at the repo root), and hatchling applies `.gitignore` patterns when selecting wheel contents even for files git tracks -- so the adapter package was silently dropped from the build. The pattern is now anchored to `/memory/`. 0.8.0 is yanked on PyPI; this release is identical except for the packaging fix.
- The release workflow now smoke-tests the built wheel (install + import `DomainEvent`, `InMemoryEventStore`, `InMemoryEventBus`) before anything is published, so an incomplete wheel fails the build instead of reaching PyPI.

## [0.8.0] - 2026-08-01

### Added

- **New ports/value-object surface (`eventsource.ports`)** -- `StreamId`, `Position`, `ExpectedVersion`, `EventEnvelope`, `AppendResult`, `StreamReadOptions`, `FeedReadOptions`, `CategoryReadOptions`, `ReadDirection`, and the five composable store ports (`EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`, `CategoryQuery`, composed as `FullEventStore`) plus the `collect` helper. `StreamId`, `Position`, `EventEnvelope`, `StreamReadOptions`, `FeedReadOptions`, `CategoryReadOptions`, `ExpectedVersion`, `ReadDirection`, `AppendResult`, and the five ports are re-exported from top-level `eventsource`.
- **Three new backend adapters** implementing the ports above: `eventsource.adapters.memory.InMemoryEventStore` (re-exported as `eventsource.InMemoryEventStore`), `eventsource.adapters.sqlite.SQLiteEventStore`, `eventsource.adapters.postgresql.PostgreSQLEventStore`. All three expose a public `store_id` property; `PostgreSQLEventStore` additionally takes `outbox_enabled=False` to write to the transactional outbox in the same transaction as the append. The outbox reader the drain side of the pattern needs -- the `OutboxRepository` contract and its `memory`/`postgresql`/`sqlite` adapters -- lives in `eventsource.ports.outbox` and `eventsource.adapters.{memory,postgresql,sqlite}` (see the outbox ring migration entry below).
- **PostgreSQL global feed no-skip guarantee**: the PostgreSQL adapter's feed reader no longer risks skipping events committed out of insertion order under concurrent writers.
- **Conformance suites for the new ports** (`eventsource.testing.conformance_ports`) -- `AppenderConformance`, `StreamReaderConformance`, `EventLookupConformance`, `GlobalFeedConformance`, `CategoryQueryConformance`, and `SnapshotConformance`, run against the memory, sqlite, and (integration) postgresql adapters.
- New exceptions `DuplicateEventError`, `PositionDecodeError`, `PositionForeignError`, and the `IntPositionCodec` position codec are re-exported from top-level `eventsource`.
- **`eventsource.application.aggregates`** -- `SnapshotPolicy` (`EveryNEvents`, `Never`), `SnapshotScheduler` (`ImmediateScheduler`, `BackgroundScheduler`), and the `take_snapshot` / `read_valid_snapshot` helpers, composed by `AggregateRepository` to decide and schedule snapshotting. `AggregateRepository` gained `snapshot_policy=` / `snapshot_scheduler=` constructor parameters for injecting custom policy/scheduler implementations.
- ADR 0021, documenting the snapshot composition design (policy + scheduler replacing the monolithic snapshot manager/strategy) and superseding ADR 0017.
- **Command objects and the decider aggregate style** -- `eventsource.commands.DomainCommand` (frozen pydantic base for commands), `eventsource.domain.decider.DeciderAggregate` (decide/evolve aggregate style), `CommandRejectedError`, and `create_event(command=...)` provenance stamping on `AggregateRoot`. All re-exported from top-level `eventsource`; documented in ADR 0022. (Landed on main via PR #82; recorded here on merge since it shipped without a changelog entry.)
- **`eventsource.ports.outbox`** -- `OutboxRepository` (the transactional outbox Protocol), `OutboxEntry`, `OutboxStats`, and `outbox_event_data()` (the single authority for the JSON-safe payload dict stored in `event_outbox.event_data`, replacing four independent constructions of the same shape). Plus the three adapter modules (`eventsource.adapters.memory.outbox`, `eventsource.adapters.postgresql.outbox`, `eventsource.adapters.sqlite.outbox`) and `OutboxRepositoryConformance` in `eventsource.testing.conformance_ports`, exercised against all three. See ADR 0026.
- **`AggregateStore`, `ASYNCPG_AVAILABLE`, `AIOSQLITE_AVAILABLE`** are now re-exported from top-level `eventsource`.
- **`MigrationCoordinator.run_resync_pass(migration_id) -> int`** -- runs one bounded catch-up copy pass while a migration is in `DUAL_WRITE`, returning the number of unabsorbed dual-write mirror failures remaining (0 means the sync-lag anchor is unclamped and cutover can proceed). Previously a mirror failure after the bulk copy finished clamped the lag anchor permanently and the only remedy was to abort and restart the migration. The migration's phase is never touched. See ADR 0028.
- **`eventsource.ports.locks`** -- `DistributedLock` and `LockRegistry` (small Protocols, ISP-split along the two real consumer groups: acquire/release individual locks vs. bulk lifecycle over everything one manager holds), `LockInfo`, and `migration_lock_key`. See ADR 0029.
- **`eventsource.adapters.memory.locks.InMemoryLockManager`** -- a second `DistributedLock`/`LockRegistry` implementation, test-scoped only: single-process, no crash release, no fairness. Its docstring leads with what it does not guarantee. See ADR 0029.
- **`eventsource.ports.readmodels`** -- a subpackage (not a flat module) holding `ReadModel`, `Query`, `Filter`, `ReadModelRepository`, and the read-model exception family (`ReadModelError`, `OptimisticLockError`, `ReadModelNotFoundError`). See ADR 0029.
- **`DistributedLockConformance` and `ReadModelRepositoryConformance`** in `eventsource.testing.conformance_ports`, exercised against the memory and postgresql (locks) and memory/postgresql/sqlite (read models) adapters.
- **`eventsource.ports.migration`** -- a new subpackage holding `models.py` (`Migration`, `MigrationConfig`, `MigrationPhase`, `MigrationStatus`, `MigrationResult`, `TenantRouting`, `TenantMigrationState`, `PositionMapping`, `SyncLag`, `CutoverResult`, `MigrationAuditEntry`, `AuditEventType`) and `repositories.py` (`MigrationRepository`, `TenantRoutingRepository`, `PositionMappingRepository`, `MigrationAuditLogRepository` Protocols), extracted from the migration ring migration below. See ADR 0034.
- **`eventsource.ports.snapshots.SnapshotTypeInvalidation`** -- optional capability Protocol for bulk snapshot invalidation by aggregate type (`delete_snapshots_by_type`), split out of `SnapshotStore`. See ADR 0036.
- **`eventsource.ports.lifecycle.SupportsClose`** -- optional capability Protocol for releasing resources an adapter owns (a single `close()` method), with a documented ownership contract: never tears down a resource injected by and still owned by the caller. `SyncStoreFacade.close` uses it via `isinstance` instead of duck-typing `getattr`. See ADR 0037.
- **PEP 562 lazy front door for `eventsource/__init__.py`.** `import eventsource` no longer imports sqlalchemy, asyncpg, aiosqlite, redis, aiokafka, or aio-pika -- every public name resolves on first `__getattr__` access and is cached. `__all__` is unchanged (same names, same order). Payoff: `import eventsource, sys; assert "sqlalchemy" not in sys.modules` now holds, and runtime Tier-0 purity checks (previously only possible via static `ast` analysis, see `tests/unit/ports/test_readmodels_port_surface.py`) are now directly verifiable at import time. See ADR 0035.

### Changed

- **`MigrationConfig.cutover_max_lag_events` now defaults to `0` (strict), was `100`.** Cutover no longer proceeds while any source event is provably absent from the target. Writes are paused for the entire cutover and nothing in the sequence copies the residue, so lag remaining at the routing switch was events the target never received while it became authoritative — caught only by a non-fatal post-cutover consistency check. **Behavior change:** a cutover that previously succeeded with residual lag now raises `CutoverLagError` and rolls back to `DUAL_WRITE`. To restore the old behavior, pass `MigrationConfig(cutover_max_lag_events=100)` explicitly — and understand it as accepting up to 100 lost events at the switch. When lag will not drain, the remedy is the new `MigrationCoordinator.run_resync_pass` rather than a higher threshold. See ADR 0028.
- **PostgreSQL deployments MUST apply `migrations/updates/004_add_events_txid.sql` before upgrading.** The global feed read path (`read_all`, `current_position`) now filters on a new `events.txid xid8` column instead of the `xmin` system column, and fails loudly with an undefined-column error against a database that has not applied it. The old predicate compared a 32-bit `xmin` against an epoch-extended 64-bit `pg_snapshot_xmin(...)`, so it became universally true — silently dropping the no-skip guarantee — once a cluster crossed its first xid epoch. Fresh provisioning via `get_schema`/`get_all_schemas` needs nothing: the column arrives as an additive fragment. Rows left with a NULL `txid` (those predating the migration) are always read; no backfill is needed. Requires PostgreSQL 13+, the same floor as before. See ADR 0027.
- **`PostgreSQLEventStore`'s constructor now takes `engine: AsyncEngine` as its primary argument instead of `session_factory: async_sessionmaker`.** The adapter builds its own internal session factory from the engine; callers that previously constructed and passed a `session_factory` now pass the `AsyncEngine` directly.
- **`tracer=` / `enable_tracing=` constructor kwargs are gone from all three store adapters.** `InMemoryEventStore`, `SQLiteEventStore`, and `PostgreSQLEventStore` (`eventsource.adapters.memory` / `.sqlite` / `.postgresql`) all accepted these on `eventsource.stores`; none of the ports adapters do.
- **Snapshot store implementations re-homed** into their adapters: `InMemorySnapshotStore` -> `eventsource.adapters.memory.snapshots`, `SQLiteSnapshotStore` -> `eventsource.adapters.sqlite.snapshots`, `PostgreSQLSnapshotStore` -> `eventsource.adapters.postgresql.snapshots`. `Snapshot` and `SnapshotStore` now live in `eventsource.ports.snapshots`; snapshot exceptions live in `eventsource.exceptions`. The `eventsource.snapshots` package itself has been **deleted** -- see Removed. Top-level `eventsource` re-exports of `Snapshot`, `SnapshotStore`, and the snapshot store adapters are unchanged.
- **`ExpectedVersion`, `ReadDirection`, and `AppendResult` are the ports-layer classes, full stop.** With the legacy `EventStore` ABC and `eventsource.stores` deleted (see Removed), the naming collision these three names used to have with `eventsource.stores.interface.ExpectedVersion` / `.ReadDirection` / `.AppendResult` no longer exists. They are re-exported from top-level `eventsource` as well as `eventsource.ports`. Likewise `eventsource.adapters.sqlite.SQLiteEventStore` and `eventsource.adapters.postgresql.PostgreSQLEventStore` are the only `SQLiteEventStore` / `PostgreSQLEventStore` in the library and are re-exported from top-level `eventsource`.
- **`AggregateRoot` and `DeclarativeAggregate` re-homed** to `eventsource.domain.aggregate`; the `eventsource.aggregates` import path is gone (see Removed). Top-level `eventsource` imports are unaffected.
- **`AggregateRepository` re-homed** to `eventsource.application.aggregates`. Top-level `eventsource` imports are unaffected.
- **`EventPublisher` re-homed** to `eventsource.ports.bus`; the legacy re-export path from `eventsource.stores.interface` was removed with the stores package.
- **Outbox ring migration** (ADR 0026, completing the split ADR 0024 made for checkpoints and DLQ): `OutboxRepository`, `OutboxEntry`, `OutboxStats`, and `outbox_event_data` moved from `eventsource.repositories` to `eventsource.ports.outbox`; `InMemoryOutboxRepository` moved to `eventsource.adapters.memory`; `PostgreSQLOutboxRepository` moved to `eventsource.adapters.postgresql`; `SQLiteOutboxRepository` moved to `eventsource.adapters.sqlite`. Unlike checkpoints and DLQ, the outbox backends are per-technology modules rather than one dialect-parameterized module -- `SQLiteOutboxRepository` is written against a raw `aiosqlite.Connection`, and unifying it onto sqlalchemy would have meant rewriting a working adapter's driver layer with no caller requesting it. Top-level `eventsource` re-exports are unaffected in name, only in the module they resolve to.
- **`sql_connection(conn, *, write=...)`** (`eventsource.adapters._sql.connection`, introduced by ADR 0024) is now the single SQL connection-normalization helper in the codebase. `PostgreSQLOutboxRepository`'s seven call sites, plus five callers outside `adapters/` (`eventsource.readmodels.postgresql` and four `eventsource.migration.repositories` modules), moved onto it from the retired `execute_with_connection`.
- **Behavior change: the in-memory outbox adapter's `event_data` JSON formatting changed from orjson's compact separators to stdlib `json.dumps`'s default spaced separators** (`", "` / `": "` instead of `","` / `":"`). This is cosmetic for any consumer that parses the field back into a dict -- round-trip equality is unaffected -- and breaking for a consumer that compares the stored string byte-for-byte. Only `InMemoryOutboxRepository` is affected; PostgreSQL and SQLite serialize through their own drivers' JSON handling, unchanged. This swap also drops one non-stdlib import from a Tier 0 adapter: `eventsource.adapters.memory.outbox` no longer imports `eventsource.serialization` for orjson.
- **Projection persistence re-homed** (ADR 0024): `eventsource.projections` -> `eventsource.application.projections`; the checkpoint and DLQ Protocols -> `eventsource.ports.checkpoints` (`ProjectionCheckpoints`, `SubscriptionPositions`, `CheckpointRepository`) and `eventsource.ports.dlq` (`DLQRepository`); the checkpoint and DLQ implementations -> `eventsource.adapters.sql` (`SQLCheckpointRepository`, `SQLDLQRepository`, dialect-parameterized for PostgreSQL and SQLite) and `eventsource.adapters.memory` (`InMemoryCheckpointRepository`, `InMemoryDLQRepository`); `DatabaseProjection` -> `eventsource.adapters.sql.projection`. Top-level `eventsource` imports are unaffected.
- **Behavior change: `checkpoint_repo=None` / `dlq_repo=None` now disable the concern** instead of constructing a per-instance in-memory repository. A `CheckpointTrackingProjection` (and subclasses `DeclarativeProjection`, `DatabaseProjection`) built with no `checkpoint_repo` no longer checkpoints at all -- `get_checkpoint()` / `get_lag_metrics()` return `None` -- and one built with no `dlq_repo` no longer captures failed events to a DLQ; a permanently failed event is logged at `critical` and re-raised either way. To keep the old vanish-on-restart behavior, pass `InMemoryCheckpointRepository()` / `InMemoryDLQRepository()` (from `eventsource`) explicitly.
- **Breaking: subscription and checkpoint positions are now opaque `Position` value objects, not integers** (legacy store retirement, slice (b), amending ADR 0024 -- see ADR 0025). `SubscriptionPositions.get_position` / `.save_position` and `CheckpointData` carry `Position` rather than an integer global position. `SubscriptionConfig.start_from` (`eventsource.subscriptions`) no longer accepts a bare `int`; its type is now `Literal["beginning", "end", "checkpoint"] | Position`. Checkpoint rows are stored under a new additive `position_token` column -- a row that carries only the legacy `global_position` column (written before this change, or by a store that has not been migrated) reads back as **no position** and causes catch-up to restart from the beginning rather than resuming; this is a deliberate fail-safe, not a bug. `Subscription.lag` is redefined: it is now a **count of events not yet delivered within the current run** (undelivered envelopes still pending in the current catch-up window), not a store-wide distance between two global positions -- the old integer subtraction (`max_position - last_processed_position`) is gone because opaque positions cannot be subtracted.
- **Breaking: `eventsource.migration` runs on the ports store surface, not the legacy `EventStore` ABC** (legacy store retirement, slice (c)). `MigrationCoordinator`, `TenantStoreRouter`, `BulkCopier`, `DualWriteInterceptor`, `ConsistencyVerifier`, and `SyncLagTracker` all take/return `FullEventStore` (source and target) instead of the old ABC, and their positions are opaque `Position` tokens end to end -- the migration's position mappings and its `tenant_migrations.last_source_position` / `last_target_position` progress are now persisted as tokens (`*_position_token` columns), not integers. `MigrationCoordinator` and `SubscriptionMigrator` no longer take a `position_store_id`: the token-keyed position mapping table needs nothing to convert.
  - The dropped cross-type capability: `get_events(aggregate_type=None)` (querying across all aggregate types in one call) is gone with the legacy store; the bulk copier reads per-stream through the ports surface instead.
  - A duplicate append during bulk copy (e.g. on resume after a crash) is now counted as **already copied** rather than silently skipped -- resuming a bulk copy is idempotent in outcome, not just in effect.
  - `SyncLagTracker.calculate_lag()` reports a **bounded count** of source events not yet copied to the target, not a position delta -- opaque positions cannot be subtracted. The count is exact up to `cutover_max_lag_events + 1`; beyond that, `SyncLag.count_is_bounded` is `True` and the number is a floor, not an exact count. The anchor it counts from is clamped by any unabsorbed dual-write mirror failure (fail-closed), so a mirror error after `BULK_COPY` completes freezes the reported lag rather than letting it read as more caught-up than reality; frozen lag is recovered with `MigrationCoordinator.run_resync_pass` (see above).
  - `find_nearest_source_position` (checkpoint translation) is now a binary search over the position-mapping table's surrogate row order rather than a `source_position DESC` index scan, since opaque tokens have no SQL-orderable representation. See `docs/api/migration-schema.md` for the constraint this rests on.
- **Locks ring migration** (ADR 0029, completing the split ADR 0024/0026 applied to checkpoints/DLQ and outbox): `PostgreSQLLockManager` moved from `eventsource.locks` to `eventsource.adapters.postgresql.locks`; `LockInfo` and `migration_lock_key` moved to `eventsource.ports.locks`. Top-level `eventsource` imports are unaffected -- none of these names were ever re-exported from `eventsource`.

  | Old import | New import |
  | --- | --- |
  | `eventsource.locks.PostgreSQLLockManager` | `eventsource.adapters.postgresql.locks.PostgreSQLLockManager` |
  | `eventsource.locks.LockInfo` | `eventsource.ports.locks.LockInfo` |
  | `eventsource.locks.migration_lock_key` | `eventsource.ports.locks.migration_lock_key` |
  | `eventsource.locks.LockAcquisitionError` | `eventsource.exceptions.LockAcquisitionError` |
  | `eventsource.locks.LockNotHeldError` | `eventsource.exceptions.LockNotHeldError` |

- **Read-model ring migration** (ADR 0029): the contract half moved from `eventsource.readmodels` to `eventsource.ports.readmodels`; the backend half split across three adapter modules plus `eventsource.adapters.sql`. Top-level `eventsource` imports are unaffected -- `ReadModelProjection` remains the only name re-exported from `eventsource`, unchanged.

  | Old import | New import |
  | --- | --- |
  | `eventsource.readmodels.ReadModel` | `eventsource.ports.readmodels.ReadModel` |
  | `eventsource.readmodels.Query` | `eventsource.ports.readmodels.Query` |
  | `eventsource.readmodels.Filter` | `eventsource.ports.readmodels.Filter` |
  | `eventsource.readmodels.ReadModelRepository` | `eventsource.ports.readmodels.ReadModelRepository` |
  | `eventsource.readmodels.ReadModelError` | `eventsource.ports.readmodels.ReadModelError` |
  | `eventsource.readmodels.OptimisticLockError` | `eventsource.ports.readmodels.OptimisticLockError` |
  | `eventsource.readmodels.ReadModelNotFoundError` | `eventsource.ports.readmodels.ReadModelNotFoundError` |
  | `eventsource.readmodels.InMemoryReadModelRepository` | `eventsource.adapters.memory.readmodels.InMemoryReadModelRepository` |
  | `eventsource.readmodels.PostgreSQLReadModelRepository` | `eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository` |

  (`SQLiteReadModelRepository` moved to `eventsource.adapters.sqlite.readmodels`; `ReadModelProjection` to `eventsource.adapters.sql.readmodel_projection`; `generate_schema`/`generate_indexes`/`generate_full_schema`/`POSTGRESQL_TYPE_MAP`/`SQLITE_TYPE_MAP` to `eventsource.adapters.sql.readmodel_schema` -- nine rows moved in total.)

- **`eventsource/engine.py` moved to `eventsource/adapters/_sql/engine.py`.** `eventsource.create_async_engine` (the canonical public name) is unchanged in signature and behavior. Anyone importing `eventsource.engine` directly -- which the docs never told them to do -- should import from `eventsource` instead. See ADR 0029.
- **`LockAcquisitionError` and `LockNotHeldError` now subclass `EventSourceError` and live in `eventsource.exceptions`** (ADR 0029). This is the one semantic change in the locks/readmodels/engine ring-migration slice, and it is widening only: every existing `except LockAcquisitionError` and `except Exception` still catches exactly as before; the newly-catching clause is `except EventSourceError`, which caught nothing lock-related before this change. Previously both derived directly from `Exception`, defined in `eventsource/locks/postgresql.py`.
- **BREAKING: `eventsource.migration` no longer exists** (ADR 0034, the last top-level package to join the ring map). `import eventsource.migration` now raises `ModuleNotFoundError`. No shim, no deprecation warning: the same pre-1.0, no-external-consumers standing rule ADR 0025/0026/0029/0030/0031/0032/0033 already applied without qualification.

  | Old import | New import |
  | --- | --- |
  | `eventsource.migration.{coordinator,cutover,router,bulk_copier,dual_write,exceptions,metrics,position_mapper,status_streamer,subscription_migrator,sync_lag_tracker,write_pause}` | `eventsource.application.migration.*` (same names) |
  | `eventsource.migration.models` | `eventsource.ports.migration.models` |
  | `eventsource.migration.repositories.{migration,routing,position_mapping,audit_log}` (the four `Protocol` classes) | `eventsource.ports.migration.repositories` |
  | `eventsource.migration.repositories.{migration,routing,position_mapping,audit_log}` (the four `PostgreSQL*` implementations, plus `VALID_TRANSITIONS`) | `eventsource.adapters.sql.migration` |

  Moving the package onto a ring the `import-linter` layers contract actually covers surfaced a latent violation: five application-ring orchestration modules (`coordinator.py`, `cutover.py`, `router.py`, `position_mapper.py`, `bulk_copier.py`) imported the repository *implementations* directly, with no Protocol indirection. The fix was completing the Protocol/implementation split those modules should always have had, not an exception to the contract. **`MigrationError` now subclasses `EventSourceError`** (previously a bare `Exception`), widening only -- every existing `except MigrationError` still catches, and `except EventSourceError` newly catches migration failures too. The two targeted `import-linter` forbidden contracts guarding this boundary ("Application ring must not import adapters" and "Ports must not import adapters, application, or migration") are replaced by one full `type = "layers"` contract (`adapters > application > ports > domain`), adding domain-ring coverage neither predecessor contract had. See ADR 0034.
- **BREAKING: `eventsource.ports.snapshots.SnapshotStore` is a `Protocol`, not an `ABC`.** Subclassing it no longer enforces abstractness (no `TypeError` on missing methods), and instantiating it directly now raises `TypeError: Protocols cannot be instantiated` rather than an abstract-class error. `snapshot_exists` is now one of the four core (bodyless) Protocol methods rather than a concrete default implemented via `get_snapshot`; every shipped adapter already implemented it natively. `delete_snapshots_by_type` moves to a new, separate `SnapshotTypeInvalidation` Protocol (see Added) and no longer raises `NotImplementedError` by default -- there is no default at all. `InMemorySnapshotStore`, `SQLiteSnapshotStore`, and `PostgreSQLSnapshotStore` no longer inherit from `SnapshotStore`; they satisfy it structurally. **`SnapshotConformance` (`eventsource.testing.conformance_ports`) is now core-only** (7 tests); the combined suite exercising both capabilities is renamed `SnapshotStoreConformance`, with the 2 bulk-invalidation tests split into a new `SnapshotTypeInvalidationConformance`. See ADR 0036.
- **BREAKING: `PostgreSQLEventStore.close()` no longer disposes the underlying engine by default.** The engine is always caller-supplied to the constructor; `close()` previously disposed it unconditionally, which could silently tear down a connection pool the caller still held or shared with other consumers (e.g. `SyncStoreFacade(PostgreSQLEventStore(shared_engine)).close()`). A new keyword-only `owns_engine: bool = False` constructor parameter controls this: `close()` disposes only when `owns_engine=True`. To restore the old behavior, pass `owns_engine=True` explicitly at construction. `SyncStoreFacade.close` now checks `isinstance(store, SupportsClose)` (see Added) instead of `getattr(store, "close", None)` duck-typing. See ADR 0037.

### Removed

- **`eventsource.snapshots` package deleted**, including the `eventsource.aggregates` and `eventsource.snapshots` import paths themselves (the `# TRANSITION` re-export shims planned for these modules were never shipped -- the package was dissolved directly). Import `Snapshot` / `SnapshotStore` from `eventsource.ports.snapshots` or continue using the top-level `eventsource` re-exports.
- **`AggregateSnapshotManager`** and the strategy classes it composed -- `SnapshotStrategy`, `ThresholdSnapshotStrategy`, `BackgroundSnapshotStrategy`, `NoSnapshotStrategy`, `create_snapshot_strategy` (formerly `eventsource.snapshots.strategies`) -- replaced by the `SnapshotPolicy` / `SnapshotScheduler` composition on `AggregateRepository` (see Added).
- **`KafkaEventBus.record_reconnection()` / `record_rebalance()`**, deprecated in 0.7.0 with removal planned for 0.8.0. Use their replacements directly.
- **`InMemoryEventBus.published_events` / `clear_published_events()`**, deprecated in 0.6.0 with removal planned for 0.8.0. Use `eventsource.testing.RecordingEventBus` instead.
- **`eventsource.repositories._json`** internal module.
- **`CheckpointRepositoryProtocol`, `DLQRepositoryProtocol`, `DLQRepository.list_failed_events`, `DLQRepository.get_failed_event`, `ProjectionCheckpointManager`, `ProjectionDLQManager`, `eventsource.repositories._dialect`** -- removed as part of the projection persistence ports split (ADR 0024). `list_failed_events` / `get_failed_event` were pure aliases for `get_failed_events` / `get_failed_event_by_id`, which remain. `ProjectionCheckpointManager` and `ProjectionDLQManager` are replaced by module-level functions in `eventsource.application.projections.checkpoints` and `.dlq` (see Changed).
- **`eventsource.repositories` -- the whole package -- is gone** (ADR 0026, completing the outbox ring migration). `import eventsource.repositories` now raises `ModuleNotFoundError`. Its `EventSourceJSONEncoder`/`json_dumps`/`json_loads` re-exports go with it -- import them from `eventsource.serialization` instead, which is where they are actually defined. No shim, no deprecation warning: the library is unreleased, so the standing rule applies without qualification.
- **`OutboxRepositoryProtocol`** -- a bare alias for `OutboxRepository`, kept "for compatibility." One name per thing; use `OutboxRepository`.
- **`OutboxRepository.list_pending_events`** -- a second name that delegated to `get_pending_events`. Use `get_pending_events` directly.
- **`eventsource.repositories._connection.execute_with_connection`** -- the SQL connection-normalization helper. Every former caller uses `sql_connection` from `eventsource.adapters._sql.connection` instead (see Changed).
- **The legacy `EventStore` ABC surface is retired: `eventsource.stores` no longer exists** (legacy store retirement, slice (d); see ADR 0025). `import eventsource.stores` now raises `ModuleNotFoundError`. No shim, no deprecation warning, no back-compat alias -- the library is unreleased, so the standing rule applies without qualification. Every name that used to live under `eventsource.stores` or the legacy ABC surface is gone: `EventStore` (the ABC), `EventStream`, `StoredEvent`, `ReadOptions`, `LegacyStoreAdapter`, `TypeConverter`, `DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, `DEFAULT_STRING_ID_FIELDS`, the int-sentinel `ExpectedVersion` class, the legacy `AppendResult` / `ReadDirection` classes, the legacy `InMemoryEventStore` (spelled `MemoryEventStore`) / `PostgreSQLEventStore` / `SQLiteEventStore` classes, and `EventStoreConformanceSuite` (`eventsource.testing.conformance`). The ports-layer `ExpectedVersion`, `ReadDirection`, and `AppendResult` (`eventsource.ports`) are the only classes with those names now, and are re-exported from top-level `eventsource` (see Changed above). Third-party backend authors validate against `eventsource.testing.conformance_ports` instead of subclassing `EventStoreConformanceSuite`.
- **`eventsource.adapters.memory.MemoryEventStore` renamed `InMemoryEventStore`**, for sibling-naming consistency with `PostgreSQLEventStore` / `SQLiteEventStore`. `MemoryEventStore` is not kept as an alias.
- **Behavior change: `AppendResult.position` is the position of the first appended event, not the last.** The legacy stores' `global_position` was the position of the last event in the batch; every ports adapter (memory, postgresql, sqlite) returns the first event's position. A caller doing its own arithmetic on the returned position across a multi-event append must account for this.
- **Behavior change: duplicate `event_id` appends now raise `DuplicateEventError`.** The legacy in-memory and PostgreSQL stores silently skipped a duplicate append and returned as if it had succeeded; every ports adapter raises instead.
- **Behavior change: category reads (`CategoryQuery.read_category`) filter and order on storage time, inclusive, not event time, exclusive.** The legacy `get_events_by_type(..., from_timestamp=...)` filtered and ordered on the event's own `occurred_at`, exclusive (`>`). The ports equivalent filters and orders on `EventEnvelope.stored_at`, inclusive (`>=`), with position as a deterministic tie-break, and rejects naive datetimes with `ValueError` rather than silently comparing them against timezone-aware ones.
- **Behavior change: the in-memory adapter's `stored_at` is a real timestamp, not a fabrication.** The legacy in-memory store fabricated `stored_at=event.occurred_at`; `InMemoryEventStore` stamps `datetime.now(UTC)` at append time. Tests asserting `stored_at == occurred_at` against the in-memory store no longer hold.
- **Behavior change: appending an empty event list now raises `ValueError`.** The legacy stores returned a no-op successful `AppendResult`; every ports adapter raises instead.
- **Behavior change: `current_position()` on an empty store returns `None`, not `0`.** The legacy `get_global_position()` returned `0` for an empty store; the ports equivalent's `None` must be treated as "empty feed," not as a comparable floor.
- **Removed capability: BACKWARD feed reads and feed-level timestamp filters have no ports equivalent.** No in-tree consumer used either (catch-up, bulk copy, and consistency checking all read FORWARD without timestamp filters); both died with `ReadOptions`. Per-stream BACKWARD reads (`StreamReader.read_stream(..., direction=ReadDirection.BACKWARD)`) are unaffected and remain native in all three adapters.
- **Removed capability: cross-type `get_events(aggregate_type=None)` has no ports equivalent.** No production caller exercised it. A narrow `StreamDiscovery.find_streams(aggregate_id) -> list[StreamId]` port was considered and rejected as unbuilt speculation; see ADR 0025.
- **`TypeConverter` and its field-name-guessing behavior are gone, not replaced.** Structured payload fields on a `DomainEvent` should be declared as typed pydantic sub-models; pydantic's own coercion handles datetimes, UUIDs, and decimals at the JSON boundary. See `docs/explanation/sql-backend-type-handling.md`.
- **`OptimisticLockError` keeps its int-typed `expected_version` field, deliberately.** It is not retyped to carry the ports `ExpectedVersion` VO -- see ADR 0025.
- **The legacy BIGINT position columns (`projection_checkpoints.global_position`, `migration_position_mappings.source_position` / `.target_position`, `tenant_migrations.last_source_position` / `.last_target_position`) are frozen, not dropped.** They are neither written nor read by the library after this release; they remain in the schema and die with their own schema revision, not this one (dropping a column is destructive, and `schemas/checkpoints.sql` is under the Do Not Modify rule).
- Nearest-position lookup in migration checkpoint translation (`find_nearest_source_position`) is now a binary search over the position-mapping table's surrogate row order, resting on a documented monotonicity precondition, since opaque `Position` tokens have no SQL-orderable representation.
- **BREAKING: top-level module ring consolidation -- `eventsource.types`, `eventsource.exceptions`, `eventsource.protocols`, `eventsource.commands`, `eventsource.sync`, and `eventsource.serialization` no longer exist** (ADR 0030, completing the ring migration). `import eventsource.types`, `.exceptions`, `.protocols`, `.commands`, `.sync`, and `.serialization` now all raise `ModuleNotFoundError`. No shim, no deprecation warning: the library is unreleased, so the standing rule already applied to `eventsource.stores` (ADR 0025) and `eventsource.repositories` (ADR 0026) applies here without qualification. Replacements: `eventsource.types` -> `eventsource.domain.types`; `eventsource.exceptions` -> `eventsource.domain.exceptions`; `eventsource.protocols` -> `eventsource.ports.handlers`; `eventsource.commands` (`DomainCommand`, from ADR 0022) -> `eventsource.domain.command` -- this move also fixes a dependency-rule violation, since `domain/aggregate.py` and `domain/decider.py` had been importing `DomainCommand` from a top-level package the ring map placed nowhere; `eventsource.sync` -> `eventsource.adapters.sync`; `eventsource.serialization` -> `eventsource.adapters.serialization`. Top-level `from eventsource import ...` imports are unaffected -- the barrel re-exports from the new homes directly and always did.
- **BREAKING: `eventsource.locks` and `eventsource.readmodels` no longer exist, ahead of the 0.8.0 removal ADR 0029 originally scheduled** (ADR 0030). `import eventsource.locks` and `import eventsource.readmodels` now raise `ModuleNotFoundError`. The two deprecation shims ADR 0029 introduced are deleted as part of the same pre-1.0 "no shims" decision applied to the six modules above. Replacements unchanged from ADR 0029: `eventsource.ports.locks` / `eventsource.adapters.{memory,postgresql}.locks` and `eventsource.ports.readmodels` / `eventsource.adapters.{memory,postgresql,sqlite}.readmodels`.
- **`eventsource.config` deleted.** ADR 0030: a seven-line placeholder module (docstring + one trailing comment, zero imports, zero classes, zero functions, no `__all__`) with no importer anywhere in `src/` or `tests/`. There was no old import path to keep working, because nothing imported it.
- **BREAKING: `eventsource.bus` -- the whole package, including its facade `__init__.py` -- no longer exists** (ADR 0031, completing the ring migration's last multi-backend top-level package). `import eventsource.bus` and every `eventsource.bus.*` submodule import now raise `ModuleNotFoundError`. No shim, no deprecation warning: the same pre-1.0, no-external-consumers standing rule ADR 0025/0026/0029/0030 already applied without qualification. Top-level `from eventsource import ...` imports are unaffected -- the barrel re-exports from the new homes directly.

  | Old import | New import |
  | --- | --- |
  | `eventsource.bus.interface.EventBus` | `eventsource.ports.bus.EventBus` |
  | `eventsource.bus.base.BaseEventBus` | `eventsource.adapters._bus.BaseEventBus` |
  | `eventsource.bus.registry.SubscriptionRegistry` | `eventsource.adapters._bus.SubscriptionRegistry` |
  | `eventsource.bus.memory.InMemoryEventBus` | `eventsource.adapters.memory.bus.InMemoryEventBus` |
  | `eventsource.bus.redis.RedisEventBus` / `.RedisEventBusConfig` | `eventsource.adapters.redis.bus.RedisEventBus` / `.RedisEventBusConfig` |
  | `eventsource.bus.kafka.*` (`KafkaEventBus` + consumer/publisher/connection/config/dlq/metrics/models collaborators) | `eventsource.adapters.kafka.*` |
  | `eventsource.bus.rabbitmq.*` (`RabbitMQEventBus` + consumer/publisher/connection/config/dlq/topology/serialization/models/death_headers collaborators) | `eventsource.adapters.rabbitmq.*` |

  `REDIS_AVAILABLE`, `KAFKA_AVAILABLE`, and `RABBITMQ_AVAILABLE` move with their respective adapter packages, unchanged in behavior. This deletion also completes the "Remove bus facade compat shims" backlog entry -- there is no facade left to shim, so the ~90 white-box test call sites that reached through `bus._connection_manager.*`-style properties are retargeted onto `eventsource.adapters._bus` and the per-backend collaborator modules in the same pass. See ADR 0031, which amends ADR 0007, ADR 0010, ADR 0011, and ADR 0020 for module locations only -- none of those four ADRs' Decisions change.
- **BREAKING: `eventsource.subscriptions` no longer exists** (ADR 0032, completing the ring migration). `import eventsource.subscriptions` now raises `ModuleNotFoundError`. No shim, no deprecation warning: the same "the library is unreleased" standing rule ADR 0025, ADR 0026, and ADR 0030 already applied. Replacements: seventeen orchestration modules (`manager.py`, `lifecycle.py`, `registry.py`, `pause_resume.py`, `health_provider.py`, `health.py`, `shutdown.py`, `metrics.py`, `transition.py`, `subscription.py`, `config.py`, `filtering.py`, `flow_control.py`, `retry.py`, `error_handling.py`, `runners/{catchup,live}.py`, plus the concrete subscriber base classes in `subscriber.py`) -> `eventsource.application.subscriptions` (same names); the `Subscriber`/`SyncSubscriber`/`BatchSubscriber` Protocols and their two helper functions -> `eventsource.ports.subscribers`; the `LeaderElector`/`LeaderElectorWithLease` Protocols and `LeaderChangeCallback` -> `eventsource.ports.coordination`; the coordination message types, topic constants, and `WorkRedistributionCoordinator` -> `eventsource.application.subscriptions` (still, via `coordination.py`); **`InMemoryLeaderElector` and `SharedLeaderState` move to `eventsource.adapters.memory`** (`adapters.memory.coordination`), not `application.subscriptions` -- the one name-availability break beyond the package path itself, since the application ring may not import adapters; `EventHandlerFunc` -> `eventsource.ports.bus` (relocated together with `EventBus` by the bus ring split, ADR 0031). A new `SubscribableEventBus` port (`eventsource.ports.bus`), a two-method Protocol `EventBus` satisfies structurally, replaces the `TYPE_CHECKING`-only `EventBus` imports the runners previously depended on. **The subscription exception hierarchy (`SubscriptionError` and its eight subclasses) moves from its own module into `eventsource.domain.exceptions`, and `SubscriptionError` is rebased onto `EventSourceError`** (previously a bare `Exception` subclass) -- widening only: every existing `except SubscriptionError` still catches, and `except EventSourceError` newly catches subscription failures too. Top-level `from eventsource import ...` imports are unaffected -- the top-level package never exported subscription names, so nothing there changes.

- **BREAKING: `eventsource.events` no longer exists** (ADR 0033, dissolving the last transitional entities-ring package). `import eventsource.events` and every `eventsource.events.*` submodule import now raise `ModuleNotFoundError`. No shim, no deprecation warning: the same pre-1.0, no-external-consumers standing rule ADR 0025/0026/0029/0030/0031/0032 already applied without qualification. Replacements: `eventsource.events.base.DomainEvent` -> `eventsource.domain.event.DomainEvent`; `eventsource.events.registry.EventRegistry` / `.register_event` / `.default_registry` / `.get_event_class` / `.get_event_class_or_none` / `.is_event_registered` / `.list_registered_events` -> `eventsource.domain.event_registry` (same names); `eventsource.events.registry.EventTypeNotFoundError` / `.DuplicateEventTypeError` -> `eventsource.domain.exceptions` (both rebased onto `EventSourceError`, widening only -- their `KeyError` / `ValueError` mixins are retained). Top-level `from eventsource import ...` imports are unaffected -- the barrel re-exports from the new homes directly.
- **BREAKING: `eventsource.handlers` no longer exists** (ADR 0033). `import eventsource.handlers` now raises `ModuleNotFoundError`. No shim, no deprecation warning, same standing rule. Replacements: `eventsource.handlers.decorators.handles` / `.get_handled_event_type` / `.is_event_handler` -> `eventsource.domain.decorators` (domain-ring, since `DeclarativeAggregate` is their only consumer); `eventsource.handlers.registry.HandlerRegistry` / `.HandlerInfo` / `.UnregisteredEventHandling` -> `eventsource.application.projections.handlers` (the ADR-0013 collaborator extracted out of `DeclarativeProjection`); `eventsource.handlers.registry.HandlerSignatureError` -> `eventsource.domain.exceptions` (rebased onto `EventSourceError`, `ValueError` mixin retained, widening only); `eventsource.handlers.adapter.HandlerAdapter` / `.get_handler_name` -> `eventsource.adapters._bus.handler_adapter` (every importer is a bus adapter). **The `AsyncEventHandler` / `SyncEventHandler` compatibility re-export that `eventsource.handlers.adapter` used to carry is dropped, not repointed** -- import them from their canonical home, `eventsource.ports.handlers`, which already worked. Top-level `from eventsource import ...` imports are unaffected; `handles` is still re-exported from the barrel.
- **BREAKING: `eventsource._internal` no longer exists** (ADR 0033). `import eventsource._internal` now raises `ModuleNotFoundError`. Replacement: `eventsource._internal.background_tasks.BackgroundTaskManager` -> `eventsource.application.background_tasks.BackgroundTaskManager`. `BackgroundTaskManager` is shared by `application/aggregates/`'s background snapshot scheduling and `adapters/_bus/`'s shutdown drain; it lands in `application/` because the Dependency Rule lets an outer ring (`adapters/`) depend inward on an inner ring (`application/`) but never the reverse, so the innermost of its two consumers is the only dependency-rule-compatible owner. No code inside the class changed -- this is a pure relocation.
- **`MigrationRepositoryProtocol`, `TenantRoutingRepositoryProtocol`, `PositionMappingRepositoryProtocol`, `MigrationAuditLogRepositoryProtocol`** -- bare aliases for the corresponding Protocol classes, kept "for compatibility." One name per thing; no consumers were found anywhere in the codebase. Use `MigrationRepository`, `TenantRoutingRepository`, `PositionMappingRepository`, `MigrationAuditLogRepository` directly from `eventsource.ports.migration.repositories`. See ADR 0034.

### Fixed

- **`InMemoryEventStore`/SQLite test fixtures no longer hang the test process on shutdown.** `tests/unit/test_sqlite_read_isolation.py` left `:memory:` stores open across tests, and aiosqlite's background writer thread is non-daemon, so an unclosed connection kept the interpreter alive after the test run finished. Stores are now closed in a fixture teardown.
- **SQLite reads can no longer observe a partially committed append.** All read paths shared the writer's aiosqlite connection but ran outside the write lock, and `append` is multi-statement — so a read scheduled between two of its INSERTs ran inside the open transaction and could yield a torn batch from `read_all`, or mint a `Position` from `current_position` for a row that was then rolled back. Reads now take the same lock; the connection stays shared, which `":memory:"` databases require.
- **SQLite outbox schema corrected: `event_outbox.id` is now `TEXT PRIMARY KEY`, was `INTEGER PRIMARY KEY AUTOINCREMENT`.** `SQLiteOutboxRepository.add_event` inserts a `str(uuid4())` into that column, which SQLite's strictly-typed rowid alias rejects with `sqlite3.IntegrityError: datatype mismatch` -- so every insert against the shipped schema failed, and the table has never held a row written by this library. **Migration note:** an existing SQLite database provisioned from `migrations/templates/sqlite/outbox.sql` or `migrations/schemas/sqlite_all.sql` carries an empty, unusable `event_outbox` table. `CREATE TABLE IF NOT EXISTS` will not replace it -- run `DROP TABLE event_outbox;` and re-provision from the corrected schema. No data can be lost: none can have existed. See ADR 0027.
- **`SQLiteOutboxRepository.cleanup_published` no longer silently deletes nothing at `days=0`.** It compared `published_at` (written as `datetime.now(UTC).isoformat()`, `'T'`-separated with microseconds and a UTC offset) against SQLite's `datetime('now', '-N days')` (space-separated, no microseconds) as raw TEXT; `'T'` (0x54) sorts after `' '` (0x20), so the comparison never matched for a cutoff computed within the same wall-clock second, and an entry published moments ago was never eligible for cleanup regardless of `days`. The cutoff is now computed in Python (`datetime.now(UTC) - timedelta(days=days)`) and bound as a parameter in the same `isoformat()` shape `published_at` is written in, so both sides of the comparison share one format. Only surfaced once the `id` column fix above made `add_event` succeed against the real schema -- the outbox conformance suite could not previously reach this code path.
- **Default-path migrations now record position mappings.** `MigrationCoordinator` accepted a `position_mapper` and `MigrationConfig.position_mapping_enabled` documented a default of `True`, but the flag was read nowhere and the coordinator never passed the mapper to its `BulkCopier` -- so an ordinary migration recorded nothing and subscription checkpoint translation (`migrate_subscriptions=True`, also a documented default) silently skipped. Mappings are now recorded whenever the coordinator was given a mapper and the flag is True. Note the cost: with a mapper attached the bulk copier appends one event at a time so each target position can be recorded, where it otherwise batches; set `position_mapping_enabled=False` to keep the batched path.
- **Live-phase subscription lag now reports events received but not yet delivered; it was previously always 0.** `Subscription.lag` is `events_seen - events_delivered`, and the live runner counted deliveries without ever counting receipts -- so a stalled subscriber with events arriving was indistinguishable from a healthy idle one, and the accumulated delivered-surplus made a later return to catch-up under-report real backlog. Live lag now includes the catch-up->live transition buffer and the pause buffer, so a paused or stalled subscription shows growing lag.
- **Catch-up no longer terminates early with `completed=False` when a read batch is entirely filtered out.** The loop broke on a zero *delivered* count, which conflated "the feed is exhausted" with "nothing in this batch matched the event-type filter" — so a heavily-filtered subscription reported failure despite having advanced its position with more feed behind it. Termination is now exactly reaching the target position (or a stop request). `CatchUpResult.events_processed` is unchanged and still counts events delivered to the subscriber.
- **`InMemoryDLQRepository.delete_resolved_events` now uses the same rolling cutoff as the SQL adapter.** It truncated `now` to midnight UTC before subtracting `older_than_days`, so `older_than_days=0` kept entries resolved earlier the same day while PostgreSQL and SQLite deleted them. The port now specifies the cutoff — exactly `datetime.now(UTC) - timedelta(days=older_than_days)`, with an entry deleted iff it is resolved and `resolved_at` is strictly before it — and the conformance suite pins it for every backend. This is a behavior change on a public class, though in practice the in-memory DLQ is a test and development backend.

## [0.7.0] - 2026-07-30

### Changed

- **RabbitMQ and Kafka backends decomposed into internal collaborator packages.** `eventsource.bus.rabbitmq` and `eventsource.bus.kafka` are now packages of internal, state-owning collaborators (connection, topology/config, publisher, consumer, DLQ admin, serialization) composed by a facade; imports are unchanged. See ADR 0020.
- **Kafka `background=True` publishes are now scheduled as tracked background tasks** per ADR 0010 -- send/serialization errors are logged and recorded in stats rather than raised to the caller, stats settle asynchronously, and `shutdown()` drains outstanding background publishes.

### Removed

- `KafkaEventBus.get_handlers_for_event` (deprecated in 0.6.0).

### Deprecated

- `KafkaEventBus.record_reconnection` / `record_rebalance` are deprecated; they warn and delegate to their replacements. Removal planned for 0.8.0.

## [0.6.0] - 2026-07-29

### Added

- **`eventsource.bus.base.BaseEventBus`** - Shared concrete base class for all four `EventBus` backends. Centralizes subscription management, event-class resolution, and fire-and-forget background-task tracking/draining so `interface.py` can stay a pure ABC.
- **`eventsource.bus.registry.SubscriptionRegistry`** - Thread-safe registry of event handlers keyed by event class, with a cached specific-then-wildcard handler tuple per event type so dispatch allocates nothing per event. Used internally by all four bus backends, replacing four independent (and inconsistent) implementations.
- **`eventsource.bus.retry.RetryPolicy`** - Shared retry/backoff policy with symmetric jitter, used by the broker-backed buses for consume-side redelivery and publish retries.
- **`eventsource.testing.RecordingEventBus`** - Purpose-built in-memory bus for tests that need to assert on published events, replacing the ad hoc `InMemoryEventBus.published_events` / `clear_published_events` attributes.
- **`eventsource.HandlerDispatchError`** - New public exception. Broker consume paths (Kafka, RabbitMQ, Redis) now run every registered handler for a delivered event, aggregate any failures into a `HandlerDispatchError`, and withhold the ack so the broker redelivers -- instead of aborting on the first handler failure and silently dropping the rest.
- CI now runs the Kafka and RabbitMQ integration suites, run against real brokers via testcontainers in a blocking CI job.
- ADR 0010 and ADR 0011, documenting the event bus contract decisions behind this release (shared base/registry/retry, uniform handler-error isolation, broker CI).

### Changed

- **`EventBusConformanceSuite` gained a new abstract method `create_subscriber`** (and an overridable `await_delivery` hook). Existing third-party subclasses of the conformance suite must implement `create_subscriber` to keep passing on upgrade.

- **`RedisEventBus.publish` now honors `background=True`.** Previously the parameter was accepted but silently ignored for Redis (documented as "Ignored for Redis"); background publishes are now genuinely fire-and-forget, matching the other backends.
- **Uniform handler-error isolation across all backends.** Every registered handler now runs for each delivered event, regardless of whether an earlier handler raised. Previously Redis and RabbitMQ aborted dispatch on the first handler failure, silently skipping any handlers registered after it.
- **Kafka retry jitter is now symmetric.** Previously jitter was one-sided positive, which meant effective backoff could exceed `retry_max_delay`. Jitter is now applied symmetrically, so backoff never exceeds the configured maximum.
- **Kafka publishes are now batched** rather than awaiting one broker round-trip per event, improving publish throughput for multi-event batches.
- **Kafka handler dispatch is now keyed by event class** rather than by class name. Previously, an event class whose `event_type` field differed from its class name would silently fail to reach its handlers; dispatch now resolves handlers the same way as the other backends.
- **Kafka background publishes no longer crash.** A misuse of aiokafka's `Future` API in the background-publish path is fixed.
- Subscription management (subscribe/unsubscribe/wildcard/clear/count) is now genuinely thread-safe in all four backends, via the shared `SubscriptionRegistry`.

### Deprecated

- `KafkaEventBus.get_handlers_for_event` is deprecated. It remains available as a shim for existing callers but new code should not depend on it.
- `InMemoryEventBus.published_events` and `InMemoryEventBus.clear_published_events` are deprecated in favor of `eventsource.testing.RecordingEventBus`.

## [0.5.0] - 2025-12-15

### Added

- **Multi-Tenancy Module** (`eventsource.multitenancy`) - First-class multi-tenant support
  - `tenant_context` ContextVar for managing tenant context across async boundaries
  - `tenant_scope` async context manager for scoped tenant operations
  - `tenant_scope_sync` sync context manager for synchronous code paths
  - Helper functions: `get_current_tenant()`, `get_required_tenant()`, `set_current_tenant()`, `clear_tenant_context()`
  - `TenantDomainEvent` base class with required `tenant_id` field and `with_tenant_context()` class method
  - `TenantAwareRepository` wrapper that enforces tenant isolation on load/save operations
  - `TenantContextNotSetError` and `TenantMismatchError` exceptions for clear error handling
  - Tenant-aware projections with automatic filtering via `tenant_filter` parameter
  - Public exports from `eventsource`: `tenant_context`, `tenant_scope`, `tenant_scope_sync`, `get_current_tenant`, `get_required_tenant`, `set_current_tenant`, `clear_tenant_context`, `TenantDomainEvent`, `TenantContextNotSetError`, `TenantMismatchError`

- **Sync Adapter** (`eventsource.sync`) - Synchronous wrappers for async components
  - `SyncEventStoreAdapter` for using async event stores in synchronous contexts
  - Ideal for Celery tasks, Django management commands, RQ workers, and other sync environments
  - Configurable timeout for operations (default: 30 seconds)
  - Public export from `eventsource`: `SyncEventStoreAdapter`

- **Testing Module** (`eventsource.testing`) - Comprehensive testing utilities
  - `EventBuilder` - Fluent builder for creating test events with minimal boilerplate
    - `with_aggregate_id()`, `with_version()`, `with_tenant_id()`, `with_timestamp()` chainable methods
    - `build()` for single events, `build_sequence()` for event chains
  - `InMemoryTestHarness` - Pre-configured in-memory infrastructure for fast tests
    - Includes event store, event bus, checkpoint repository, and DLQ
    - `setup()` and `teardown()` lifecycle methods
    - `clear()` to reset state between tests
  - `EventAssertions` - Domain-specific test assertions with clear error messages
    - `assert_event_published()`, `assert_no_events_published()`, `assert_event_count()`
    - `assert_event_sequence()` for verifying event ordering
    - `assert_aggregate_version()`, `assert_aggregate_state()`
  - BDD-style helpers for readable tests:
    - `given_events()` - Set up initial event history
    - `when_command()` - Execute a command/action
    - `then_event_published()` - Assert expected event was published
    - `then_no_events_published()` - Assert no events were published
    - `then_event_sequence()` - Assert specific sequence of events
    - `then_event_count()` - Assert number of events
  - Public exports from `eventsource.testing`: `EventBuilder`, `InMemoryTestHarness`, `EventAssertions`, `given_events`, `when_command`, `then_event_published`, `then_no_events_published`, `then_event_sequence`, `then_event_count`

- **Aggregate `create_event()` Method** - Reduced boilerplate for event creation
  - Auto-populates `aggregate_id`, `aggregate_type`, and `aggregate_version`
  - Auto-populates `tenant_id` from context when available
  - Explicit kwargs always override auto-populated values
  - Example: `self.create_event(OrderShipped, tracking_number="TRACK-001")` instead of manually setting all aggregate fields

- **Deferred State Pattern** - Aggregates without upfront initial state
  - `requires_creation_event` class attribute on `DeclarativeAggregate`
  - When `True`, `_get_initial_state()` returns `None` and state is set by first event handler
  - `AggregateNotCreatedError` raised when accessing `state` before creation event applied
  - Useful for aggregates where initial state depends entirely on creation event data

- **Automatic Type Inference** - Less boilerplate for events and aggregates
  - `DomainEvent.event_type` now auto-infers from class name if not explicitly set
  - `DomainEvent.aggregate_type` auto-infers from aggregate's `aggregate_type` when created via `create_event()`
  - Aggregate state type (`TState`) auto-detected from Generic parameter

### Changed

- **InMemoryEventBus** - Now thread-safe with proper locking for concurrent access
- **AggregateRoot._get_initial_state()** - Return type changed from `TState` to `TState | None` to support deferred state pattern

### Tests

- Added multi-tenancy module tests (`tests/unit/multitenancy/`)
  - Context management tests (`test_context.py`)
  - TenantDomainEvent tests (`test_events.py`)
  - TenantAwareRepository tests (`test_repository.py`)
  - Projection tenant filtering tests (`tests/unit/projections/test_tenant_filter.py`)
- Added sync adapter tests (`tests/unit/sync/`)
  - Adapter functionality tests (`test_adapter.py`)
  - Concurrency tests (`test_concurrency.py`)
- Added testing module tests (`tests/unit/testing/`)
  - EventBuilder tests (`test_builder.py`)
  - InMemoryTestHarness tests (`test_harness.py`)
  - EventAssertions tests (`test_assertions.py`)
  - BDD helpers tests (`test_bdd.py`)
  - Module structure tests (`test_module_structure.py`)
- Added aggregate improvement tests
  - `create_event()` tests (`tests/unit/aggregates/test_create_event.py`)
  - Deferred state tests (`tests/unit/aggregates/test_deferred_state.py`)
  - Type inference tests (`tests/unit/aggregates/test_aggregate_type_inference.py`)
- Added automatic event type inference tests (`tests/unit/test_event_type_auto.py`)
- Added InMemoryEventBus threading tests (`tests/unit/bus/test_memory.py`)

## [0.4.0] - 2025-12-13

### Added

- **Tracer Protocol & Implementations** (`eventsource.observability.tracer`) - Composition-based tracing
  - `Tracer` protocol defining the contract for tracing implementations
  - `NullTracer` - No-op implementation for when tracing is disabled
  - `OpenTelemetryTracer` - Full OpenTelemetry integration when OTEL is available
  - `MockTracer` - Testing implementation for verifying trace calls
  - `create_tracer()` factory function for automatic tracer selection based on configuration
- **Serialization Module** (`eventsource.serialization`) - Centralized JSON utilities
  - `EventSourceJSONEncoder` for consistent JSON serialization across the library
  - `json_dumps()` and `json_loads()` helper functions
  - Proper handling of UUID, datetime, Enum, dataclass, and Pydantic model serialization
- **Handler Decorators** (`eventsource.handlers.decorators`) - Relocated and enhanced decorator
  - `@handles` decorator now in canonical location with full backward compatibility
  - `HandlerSignatureError` exception with detailed validation messages for invalid handler signatures
- **Repository Method Aliases** - Consistent naming conventions
  - `list_pending()` alias for `get_pending_events()` in OutboxRepository
  - `list_failed()` alias for `get_failed_events()` in DLQRepository
  - `get_by_id()` alias for `get_failed_event_by_id()` in DLQRepository
- **AsyncEventHandler ABC** - Consolidated to single definition in `eventsource.protocols`

### Changed

- **Tracing Architecture** - Migrated from inheritance to composition pattern
  - All 47+ traced classes now use `Tracer` composition instead of `TracingMixin` inheritance
  - Components accept optional `tracer` parameter for dependency injection
  - Enables easier testing with `MockTracer` and better separation of concerns
- **Handler Registry** - Improved validation and error messages
  - Better detection of invalid handler signatures
  - More descriptive error messages for common mistakes

### Tests

- Added comprehensive Tracer protocol tests (`tests/unit/observability/test_tracer.py`)
- Added handler decorator tests (`tests/unit/handlers/test_decorators.py`)
- Added handler registry tests (`tests/unit/handlers/test_registry.py`)
- Added serialization module tests (`tests/unit/serialization/test_json.py`)
- Added protocol consolidation tests (`tests/unit/test_protocols.py`)
- Added import compatibility tests (`tests/integration/test_imports.py`)
- Updated all existing tracing tests to use new composition pattern

## [0.3.1] - 2025-12-13

### Changed

- **Schema: `global_position` replaces `id` as primary key** - Events table now uses `global_position` as the primary key for strict sequential ordering, while `event_id` (UUID) remains as a unique constraint for deduplication and idempotency
  - PostgreSQL: `global_position BIGSERIAL PRIMARY KEY` with `event_id UUID NOT NULL UNIQUE`
  - SQLite: `global_position INTEGER PRIMARY KEY AUTOINCREMENT` with `event_id TEXT NOT NULL UNIQUE`
  - Updated all SQL templates, Alembic migration templates, and store implementations
  - Consistent naming across PostgreSQL and SQLite backends

### Fixed

- **SQLite store consistency** - SQLite event store now uses `global_position` column naming consistent with PostgreSQL, fixing column name mismatch between backends

## [0.3.0] - 2025-12-12

### Added

- **ReadModel Persistence Tooling** - Standardized read model persistence infrastructure (`eventsource.readmodels`)
  - **Phase 1 - Core Components**:
    - `ReadModel` base class with standard fields (id, timestamps, version, deleted_at)
    - `ReadModelRepository` protocol with 13 methods for CRUD, querying, and lifecycle management
    - `Query` and `Filter` classes for flexible, type-safe querying with operators (eq, ne, lt, gt, le, ge, in_, contains, startswith)
    - `InMemoryReadModelRepository` implementation for testing and development
  - **Phase 2 - SQL Backends**:
    - `PostgreSQLReadModelRepository` with full async support via asyncpg
    - `SQLiteReadModelRepository` with async support via aiosqlite
    - Schema generation utilities (`generate_postgresql_schema()`, `generate_sqlite_schema()`) for automatic table creation from ReadModel classes
  - **Phase 3 - Projection Integration**:
    - `ReadModelProjection` base class integrating with `DatabaseProjection`
    - `HandlerRegistry` integration with `@handles` decorator for event-driven updates
    - Automatic repository injection into event handlers
  - **Phase 4 - Enhanced Features**:
    - Soft delete support with `get_deleted()` and `find_deleted()` methods
    - Optimistic locking via `save_with_version_check()` for concurrent update safety
    - `OptimisticLockError` and `ReadModelNotFoundError` exceptions
  - Public exports from `eventsource.readmodels`: `ReadModel`, `ReadModelRepository`, `ReadModelProjection`, `Query`, `Filter`, `InMemoryReadModelRepository`, `PostgreSQLReadModelRepository`, `SQLiteReadModelRepository`
  - New observability attributes: `ATTR_READ_MODEL_TYPE`, `ATTR_READ_MODEL_ID`
- **Multi-Tenant Live Migration** - Zero-downtime tenant migration between event stores (`eventsource.migration`)
  - `MigrationCoordinator` orchestrating full migration lifecycle with pause/resume/abort controls
  - `BulkCopier` for streaming historical event migration with checkpointing and configurable batch sizes
  - `DualWriteInterceptor` for simultaneous writes to source and target stores during migration
  - `CutoverManager` for sub-100ms atomic tenant routing switch with rollback capability
  - `ConsistencyVerifier` for data integrity validation with COUNT, HASH, and FULL verification modes
  - `SubscriptionMigrator` for checkpoint position translation between stores
  - `TenantStoreRouter` for tenant-aware read/write routing during and after migration
  - `WritePauseManager` for coordinated write pausing during cutover
  - `SyncLagTracker` for monitoring replication lag between stores
  - Real-time status streaming via `StatusStreamer` for migration monitoring
  - Position mapping for checkpoint translation between source and target stores
  - Comprehensive error classification with retry policies and circuit breaker pattern
  - Audit logging for all migration operations
  - OpenTelemetry metrics integration (`eventsource.migration.metrics`)
- **PostgreSQL Advisory Locks** - Distributed locking for migration coordination (`eventsource.locks`)
  - `PostgreSQLAdvisoryLock` for session-level and transaction-level advisory locks
  - Lock context managers for safe acquisition and release
  - Lock timeout and retry configuration
- **Migration Exceptions** - Comprehensive exception hierarchy (`eventsource.migration.exceptions`)
  - `MigrationError`, `MigrationStateError`, `MigrationNotFoundError`
  - `BulkCopyError`, `DualWriteError`, `CutoverError`
  - `ConsistencyError`, `RoutingError`, `LockError`
  - Error classification with `ErrorCategory` and `ErrorSeverity` enums
- **Migration Documentation** - Comprehensive guides in `docs/migration/`:
  - Architecture overview and component documentation
  - Step-by-step migration guide
  - API reference for all migration components
  - Operational runbooks and troubleshooting guides
  - Monitoring and alerting setup

- **Subscription Tracing** - OpenTelemetry tracing for all subscription components
  - `SubscriptionManager` tracing for subscription lifecycle operations:
    - `subscribe`, `unsubscribe`, `start_subscription`, `stop`, `stop_subscription`
    - `pause_subscription`, `resume_subscription`
  - `TransitionCoordinator` tracing for catch-up to live transitions:
    - `execute` span with phase tracking (initial_catchup → live_subscribed → final_catchup → processing_buffer → live)
    - Watermark and buffer size attributes
  - `CatchUpRunner` tracing for historical event processing:
    - `run_until_position` span with batch progress
    - `deliver_event` span for individual event delivery
  - `LiveRunner` tracing for real-time event processing:
    - `start`, `stop`, `process_event` spans
    - `process_buffer`, `process_pause_buffer` for transition buffers
  - New subscription trace attributes in `eventsource.observability.attributes`:
    - `ATTR_SUBSCRIPTION_NAME`, `ATTR_SUBSCRIPTION_STATE`, `ATTR_SUBSCRIPTION_PHASE`
    - `ATTR_FROM_POSITION`, `ATTR_TO_POSITION`, `ATTR_BATCH_SIZE`
    - `ATTR_BUFFER_SIZE`, `ATTR_EVENTS_PROCESSED`, `ATTR_EVENTS_SKIPPED`, `ATTR_WATERMARK`
  - All components support `enable_tracing` parameter (default: `True`)
  - Graceful degradation when OpenTelemetry is not installed

- **Subscription Manager** - New `eventsource.subscriptions` module for building event-driven projections with catch-up subscriptions and live event streaming
  - `SubscriptionManager` class for coordinating subscriptions with unified lifecycle management
    - Automatic catch-up from event store historical data
    - Seamless transition to live event streaming via event bus
    - Multiple subscriber support with concurrent processing
    - Graceful shutdown with SIGTERM/SIGINT signal handling (`run_until_shutdown()`)
    - Pause/resume functionality for individual subscriptions
  - `Subscription` class with state machine for subscription lifecycle (idle → starting → catching_up → live → pausing → paused → resuming → stopping → stopped → failed)
  - `SubscriptionConfig` for configurable subscription behavior:
    - `batch_size`: Events per batch during catch-up (default: 100)
    - `checkpoint_interval`: Events between checkpoints (default: 100)
    - `checkpoint_strategy`: "batch" or "interval" checkpointing
    - `start_from`: Start position ("beginning", "end", or specific position)
    - `filter_event_types`: Optional event type filtering
  - Subscriber protocols and base classes:
    - `Subscriber` and `SyncSubscriber` protocols for event handlers
    - `BatchSubscriber` protocol for batch event processing
    - `BaseSubscriber`, `BatchAwareSubscriber`, and `FilteringSubscriber` base classes
  - Catch-up and live runner implementations:
    - `CatchupRunner` for reading historical events from event store with batching
    - `LiveRunner` for streaming real-time events from event bus
    - `TransitionCoordinator` for seamless handoff between modes
  - Comprehensive error handling (`eventsource.subscriptions.error_handling`):
    - `SubscriptionErrorHandler` with configurable retry policies
    - `ErrorSeverity` levels: low, medium, high, critical
    - `ErrorCategory` classification: event_processing, checkpoint, transition, infrastructure
    - Error callbacks: `on_error()` and `on_critical_error()` hooks
    - Circuit breaker pattern for failing subscriptions
  - Retry system (`eventsource.subscriptions.retry`):
    - Configurable retry with exponential backoff
    - Jitter support for distributed systems
    - Max retries and timeout limits
  - Health monitoring (`eventsource.subscriptions.health`):
    - `ManagerHealthChecker` for overall system health
    - `SubscriptionHealthChecker` for per-subscription health
    - Kubernetes-compatible liveness/readiness probes
    - `HealthStatus`, `LivenessStatus`, `ReadinessStatus` enums
    - Configurable health check thresholds
  - Metrics collection (`eventsource.subscriptions.metrics`):
    - Events processed, errors, lag, and processing duration metrics
    - Per-subscription and aggregate statistics
  - Flow control (`eventsource.subscriptions.flow_control`):
    - Backpressure handling for slow consumers
    - Rate limiting support
  - Graceful shutdown (`eventsource.subscriptions.shutdown`):
    - `ShutdownCoordinator` with phased shutdown sequence
    - Configurable shutdown timeout
    - In-flight event completion before shutdown
    - `FlowController.wait_for_drain()` for tracking in-flight events during shutdown
    - `ShutdownReason` enum for tracking shutdown triggers (SIGNAL_SIGTERM, SIGNAL_SIGINT, PROGRAMMATIC, HEALTH_CHECK, TIMEOUT, DOUBLE_SIGNAL)
    - Pre-shutdown hooks (`on_pre_shutdown()`) for cleanup before shutdown (e.g., load balancer deregistration)
    - Post-shutdown hooks (`on_post_shutdown()`) for actions after shutdown completes
    - Shutdown deadline support (`set_shutdown_deadline()`) for Kubernetes `terminationGracePeriodSeconds` compliance
    - Periodic checkpoint saves during drain phase (`checkpoint_interval` parameter)
    - Shutdown metrics with OpenTelemetry integration (`ShutdownMetricsSnapshot`)
  - Multi-instance coordination (`eventsource.subscriptions.coordination`):
    - `LeaderElector` protocol for distributed leadership election
    - `LeaderElectorWithLease` extended protocol for lease-based leadership
    - `InMemoryLeaderElector` implementation for single-instance and testing scenarios
    - `WorkRedistributionCoordinator` for coordinating work handoff during shutdown
    - `ShutdownNotification` and `HeartbeatMessage` for peer-to-peer coordination
    - Support for graceful work redistribution when instances shut down
  - Event filtering (`eventsource.subscriptions.filtering`):
    - Filter events by type, aggregate, or custom predicates
  - Global position support in event stores:
    - `PostgreSQLEventStore.subscribe_all_from_position()` for ordered event streaming
    - `SQLiteEventStore.subscribe_all_from_position()` for ordered event streaming
    - `global_position` field in stored events for total ordering
  - Database migrations for `checkpoints` table with position tracking
  - Comprehensive documentation: API reference, user guide, migration guide, and examples
  - Exception hierarchy: `SubscriptionError`, `SubscriptionConfigError`, `SubscriptionStateError`, `SubscriptionAlreadyExistsError`, `CheckpointNotFoundError`
- **TypeConverter Extraction** - Refactored serialization logic from event stores into a dedicated component
  - New `TypeConverter` protocol defining the contract for type conversion during event deserialization
  - `DefaultTypeConverter` implementation with configurable UUID and datetime field detection
  - `DEFAULT_UUID_FIELDS` and `DEFAULT_STRING_ID_FIELDS` constants for common field patterns
  - `DefaultTypeConverter.strict()` factory method for explicit-only UUID field configuration
  - SQLiteEventStore now has full configuration parity with PostgreSQLEventStore:
    - Added `uuid_fields`, `string_id_fields`, and `auto_detect_uuid` constructor parameters
    - Added `with_strict_uuid_detection()` factory method
  - Public exports from `eventsource.stores`: `TypeConverter`, `DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, `DEFAULT_STRING_ID_FIELDS`
  - 37 unit tests for comprehensive TypeConverter coverage

### Changed

- Removed ~120 lines of duplicate serialization code from PostgreSQL and SQLite event stores by extracting to shared `TypeConverter`

### Fixed

- **SQLiteOutboxRepository**: `get_pending_events()` now returns `list[OutboxEntry]` instead of `list[dict]`, matching the protocol specification
- **SQLiteOutboxRepository**: `add_event()` now properly stores and returns a UUID as the outbox ID, matching PostgreSQL behavior
- **SQLiteDLQRepository**: `get_failed_events()` now properly parses timestamp fields (`first_failed_at`, `last_failed_at`) from ISO 8601 strings to `datetime` objects
- **SQLiteDLQRepository**: `get_failed_event_by_id()` now properly parses timestamp fields (`first_failed_at`, `last_failed_at`, `resolved_at`) to `datetime` objects
- **SQLite schema**: Event outbox table now uses `TEXT PRIMARY KEY` for the `id` column (UUID as string) instead of `INTEGER PRIMARY KEY AUTOINCREMENT` to match PostgreSQL schema

### Breaking Changes (Internal)

- Internal methods `_is_uuid_field()` and `_convert_types()` on event stores have been removed
  - Users who were calling these internal methods directly should migrate to `store._type_converter.is_uuid_field()` and `store._type_converter.convert_types()`

### Documentation

- Added subscription manager user guide (`docs/guides/subscriptions.md`) covering:
  - Getting started with catch-up and live subscriptions
  - Basic usage patterns and configuration
  - Resilience patterns and error handling
  - Advanced patterns for production deployments
  - Troubleshooting guide
- Added subscription API reference (`docs/api/subscriptions.md`) with complete class and method documentation
- Added subscription migration guide (`docs/guides/subscription-migration.md`) for migrating from manual projection processing
- Added subscription examples (`examples/subscriptions/`) with:
  - Basic projection example
  - Multi-subscriber example
  - Resilient projection with error handling
- **Updated all projection examples to use SubscriptionManager pattern**:
  - `examples/projection_example.py` now demonstrates SubscriptionManager with catch-up, live subscriptions, and checkpoint tracking
  - `docs/getting-started.md` updated with SubscriptionManager as the recommended approach
  - `docs/examples/projections.md` now recommends SubscriptionManager over direct `event_bus.subscribe_all()`
  - `docs/examples/sqlite-usage.md` integration tests updated to use SubscriptionManager
- Added comparison table showing benefits of SubscriptionManager vs direct EventBus subscription
- Added Kubernetes deployment guide (`docs/guides/kubernetes-deployment.md`) covering:
  - Pod lifecycle integration and graceful shutdown
  - Health probe configuration (liveness, readiness, startup)
  - `terminationGracePeriodSeconds` configuration with shutdown deadline
  - Example Deployment, Service, and PodDisruptionBudget manifests
  - Spot instance and preemptible VM considerations (AWS, GCP, Azure)
  - Shutdown metrics and observability
  - Troubleshooting guide for common Kubernetes issues

### Tests

- Added comprehensive ReadModel persistence test suite:
  - Unit tests for all ReadModel components (`tests/unit/readmodels/`)
    - Base class and field validation tests
    - Query and Filter class tests
    - Repository protocol compliance tests
    - In-memory repository tests
    - PostgreSQL and SQLite repository tests
    - Schema generation tests
    - Projection integration tests
    - Handler registry integration tests
  - Integration tests (`tests/integration/readmodels/`)
    - Repository CRUD operations across all backends
    - Projection event handling flows
    - Enhanced features (soft delete, optimistic locking)
- Added comprehensive migration test suite (~950 new tests):
  - Unit tests for all migration components (`tests/unit/migration/`)
  - Integration tests for PostgreSQL locks and migration schema
  - Chaos tests for failure scenarios and recovery
  - Load testing benchmarks for performance validation
  - Phase integration tests for bulk copy, dual write, and cutover
- Added comprehensive subscription manager test suite:
  - Unit tests for all subscription components (`tests/unit/subscriptions/`)
  - Integration tests for catch-up, live, and transition flows (`tests/integration/subscriptions/`)
  - Resilience tests for error handling, retries, and recovery
  - Health check and metrics tests
  - Pause/resume functionality tests
  - Backpressure and flow control tests
  - Drain functionality tests (`test_drain.py`) for shutdown coordination
  - Coordination protocol tests (`test_coordination.py`) for leader election and work redistribution
  - Shutdown tests for pre/post hooks, deadline, metrics, and reason tracking

## [0.2.0] - 2025-12-08

### Added

#### Observability & Telemetry

- **Observability Module** (`eventsource.observability`) - Reusable OpenTelemetry utilities
  - `OTEL_AVAILABLE` constant for checking OpenTelemetry availability
  - `get_tracer()` and `should_trace()` helper functions
  - `@traced` decorator for method-level tracing
  - `TracingMixin` class for consistent tracing across components
- **Kafka Event Bus Metrics** - Comprehensive OpenTelemetry metrics
  - Counters: `messages.published`, `messages.consumed`, `handler.invocations`, `handler.errors`, `messages.dlq`, `connection.errors`, `reconnections`, `rebalances`, `publish.errors`
  - Histograms: `publish.duration`, `consume.duration`, `handler.duration`, `batch.size`
  - Gauges: `connections.active`, `consumer.lag` (per partition)
  - New `KafkaEventBusMetrics` class with `enable_metrics` config option
  - Less than 5% performance overhead
- **SQLiteEventStore Tracing** - `enable_tracing` parameter for `append_events` and `get_events` operations
- **InMemoryEventBus Tracing** - `enable_tracing` parameter for event dispatch and handler execution

#### Aggregate Snapshotting

- `Snapshot` dataclass for point-in-time aggregate state capture
- `SnapshotStore` interface with `InMemorySnapshotStore`, `PostgreSQLSnapshotStore`, and `SQLiteSnapshotStore` implementations
- `AggregateRepository` snapshot support: `snapshot_store`, `snapshot_threshold`, and `snapshot_mode` parameters
- `AggregateRoot.schema_version` for snapshot schema evolution with automatic invalidation
- `create_snapshot()` and `await_pending_snapshots()` methods
- Snapshot exceptions: `SnapshotError`, `SnapshotDeserializationError`, `SnapshotSchemaVersionError`, `SnapshotNotFoundError`
- Database migrations for `snapshots` table (PostgreSQL and SQLite)

#### Event Handling & Validation

- `EventVersionError` exception and configurable version validation via `AggregateRoot.validate_versions`
- `UnhandledEventError` exception with configurable handling via `unregistered_event_handling` attribute ("ignore", "warn", "error")
- `FlexibleEventHandler` and `FlexibleEventSubscriber` protocols for sync/async handler signatures
- Consolidated `eventsource.protocols` module as canonical location for protocol definitions

#### Database & Repository

- `DatabaseProjection` class for projections requiring raw database connection access
- `execute_with_connection` helper for consistent connection handling
- Configurable UUID field detection in `PostgreSQLEventStore` via `uuid_fields`, `string_id_fields`, and `auto_detect_uuid` parameters
- `PostgreSQLEventStore.with_strict_uuid_detection()` class method

#### Developer Experience

- Pre-commit hooks with ruff, mypy, and bandit
- GitHub Actions workflow for performance benchmarks with baseline tracking and PR comparison
- Shared test fixtures module (`tests/fixtures/`) with reusable components

### Changed

- Improved type annotations for better mypy compatibility
- Consolidated `@handles` decorator to `eventsource.projections.decorators` (old location deprecated)
- Consolidated protocol definitions to `eventsource.protocols` (old locations deprecated)
- Repository methods `get_pending_events`, `get_failed_events`, `get_failed_event_by_id` now return typed dataclasses (`OutboxEntry`, `DLQEntry`) instead of dicts
- Unified `get_events_by_type()` timestamp parameters to use `datetime` instead of `float`
- Refactored repositories to use `execute_with_connection` helper
- In-memory repositories now use `asyncio.Lock` for proper async concurrency

### Removed

- `SyncEventStore` abstract class (use `asyncio.run()` for sync access; see ADR-0007)

### Fixed

- Broken documentation links in ADRs and guides
- Mypy type errors in projections, repositories, and event bus modules
- `DeclarativeProjection` connection handling for proper transaction sharing

## [0.1.3] - 2025-12-07

### Documentation

- Added documentation badge linking to GitHub Pages
- Updated all documentation URLs to point to https://tyevans.github.io/eventsource-py
- Fixed mkdocs.yml site configuration with correct repository URLs
- Simplified README documentation section with links to hosted docs

## [0.1.2] - 2025-12-07

### Fixed

- Fixed release workflow version validation

## [0.1.1] - 2025-12-07

### Fixed

- Release infrastructure corrections

## [0.1.0] - 2025-12-07

### Added

- Initial release of eventsource-py library
- Event Store with PostgreSQL, SQLite, and In-Memory backends
- Domain Events with Pydantic validation
- Aggregate base class with optimistic concurrency control
- Projection system for building read models with checkpoint tracking
- Dead Letter Queue (DLQ) for failed event handling
- Snapshot support for aggregate state caching
- Multi-tenant support with tenant isolation
- Async-first API design throughout
- Comprehensive type hints and mypy compatibility
- Event registry for type-safe event deserialization
- Event Bus with In-Memory and Redis Streams backends
- Transactional Outbox pattern implementation
- `DatabaseProjection` class for projections requiring raw database connection access
- Pre-commit hooks configuration with ruff, mypy, and bandit

### Infrastructure

- PostgreSQL backend with connection pooling (asyncpg)
- SQLite backend for lightweight deployments, development, and testing
- Redis Streams backend for distributed event bus
- In-Memory backends for testing and development
- Automatic schema creation and migrations
- GitHub Actions CI/CD pipeline

[Unreleased]: https://github.com/tyevans/eventsource-py/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/tyevans/eventsource-py/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/tyevans/eventsource-py/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/tyevans/eventsource-py/compare/v0.5.0...v0.8.0
[0.5.0]: https://github.com/tyevans/eventsource-py/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/tyevans/eventsource-py/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/tyevans/eventsource-py/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/tyevans/eventsource-py/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/tyevans/eventsource-py/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/tyevans/eventsource-py/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/tyevans/eventsource-py/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/tyevans/eventsource-py/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tyevans/eventsource-py/releases/tag/v0.1.0
