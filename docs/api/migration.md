# Migration API Reference

Technical reference for `eventsource.application.migration`, the live migration system that
moves a single tenant's event data from one event store to another while the
application keeps reading and writing.

The package is organised around one orchestrator and a set of collaborators it
drives:

| Area | Module | Key names |
| --- | --- | --- |
| Lifecycle model | `eventsource.ports.migration.models` | `MigrationPhase`, `TenantMigrationState`, `Migration`, `MigrationConfig`, `MigrationStatus`, `MigrationResult`, `TenantRouting`, `PositionMapping`, `SyncLag`, `CutoverResult`, `MigrationAuditEntry`, `AuditEventType` |
| Orchestration | `eventsource.application.migration.coordinator` | `MigrationCoordinator` |
| Routing | `eventsource.application.migration.router` | `TenantStoreRouter` |
| Phase components | `bulk_copier`, `dual_write`, `cutover`, `write_pause`, `position_mapper`, `sync_lag_tracker`, `subscription_migrator` | `BulkCopier`, `DualWriteInterceptor`, `CutoverManager`, `WritePauseManager`, `PositionMapper`, `SyncLagTracker`, `SubscriptionMigrator` |
| Verification | `eventsource.application.migration.consistency` | `ConsistencyVerifier`, `VerificationLevel`, `VerificationReport` |
| Observability | `status_streamer`, `metrics` | `StatusStreamer`, `StatusStreamManager`, `MigrationMetrics`, `ActiveMigrationsTracker` |
| Errors | `eventsource.application.migration.exceptions` | `MigrationError` and subclasses, `classify_exception()`, `RetryConfig`, `CircuitBreaker`, `ErrorHandler` |

A migration advances through the phases `PENDING → BULK_COPY → DUAL_WRITE →
CUTOVER → COMPLETED`, with `ABORTED` and `FAILED` as terminal escapes from any
active phase. The source store stays authoritative until cutover completes, so
an abort at any point leaves reads and writes on the original store.

Everything documented on this page is exported from the package root:

```python
from eventsource.application.migration import (
    MigrationCoordinator,
    MigrationConfig,
    MigrationPhase,
    TenantStoreRouter,
)
```

`eventsource.application.migration.__all__` is the authoritative export list; submodule
paths are given below only to show where each name is defined. Persistence
*interfaces* (Protocols) for migration records, routing rows, position
mappings, and audit entries live in `eventsource.ports.migration.repositories`;
the PostgreSQL implementations live in `eventsource.adapters.sql.migration`.

Every enum value, dataclass field, default, validation rule, and coordinator
method below is described as it behaves in the current implementation.

## Overview

Live migration moves one tenant at a time. A migration is created against a
`tenant_id` (a `UUID`) and a target `FullEventStore`, and the coordinator refuses to
create a second migration for a tenant that already has an active one
(`MigrationAlreadyExistsError`). The unit of work is therefore
"this tenant, from its current store to that store" -- not a whole-database
copy.

Three objects have to be in place before anything can run:

- A `TenantStoreRouter`, which is itself a `FullEventStore`. The application uses
  the router as its store, and the router decides per operation whether to hit
  the source store, the target store, or both, based on the tenant's
  `TenantMigrationState`.
- Repositories for persistence: `MigrationRepository` and
  `TenantRoutingRepository` (both `Protocol`s under
  `eventsource.ports.migration.repositories`), plus the position-mapping and audit-log
  repositories used by the later phases.
- A `MigrationCoordinator`, constructed with the source store, those
  repositories, and the router.

```python
router = TenantStoreRouter(default_store=shared_store, routing_repo=routing_repo)

coordinator = MigrationCoordinator(
    source_store=shared_store,
    migration_repo=migration_repo,
    routing_repo=routing_repo,
    router=router,
)

migration = await coordinator.start_migration(
    tenant_id=tenant_id,
    target_store=dedicated_store,
    target_store_id="dedicated-tenant-abc",
)
```

`start_migration()` returns as soon as the migration record exists; the bulk
copy runs in the background. Progress is observed with `get_status()`,
`wait_for_phase()`, or `stream_status()`, and the switch to the target store is
an explicit, separately triggered step (`trigger_cutover()`) rather than
something the coordinator decides on its own.

### When to use live migration

Use this package when a tenant's events must move between stores while the
application keeps serving traffic -- the canonical case being promotion from a
shared PostgreSQL event store to a dedicated one. It is designed so that:

- the source store stays authoritative until cutover succeeds, so
  `abort_migration()` at any earlier phase cancels the copy and restores the
  tenant to normal routing (copied events left in the target store are yours to
  clean up; a migration that has already reached a terminal phase cannot be
  aborted and raises `MigrationError`);
- writes are only blocked during the cutover pause itself, bounded by
  `MigrationConfig.cutover_timeout_ms` (default 500 ms, minimum 100), and
  cutover only proceeds when sync lag is within `cutover_max_lag_events`
  (default 0 -- strict) -- otherwise `CutoverManager` raises rather than switching;
- application code is unchanged, because routing is hidden behind
  `TenantStoreRouter`, which is itself a `FullEventStore`;
- subscription checkpoints are translated to target-store positions by
  `SubscriptionMigrator` via `PositionMapper`, so consumers resume from the
  equivalent position rather than replaying from zero.

Do not reach for it when downtime is acceptable or the data is not
tenant-partitioned. A maintenance-window copy needs none of the dual-write,
position-mapping, or lag-tracking machinery here, and this package has no mode
for migrating all tenants as a single unit -- migrate them one at a time.

### Import surface (`eventsource.application.migration`)

Every public name is re-exported from the package root; `__all__` in
`eventsource/migration/__init__.py` is the authoritative list. Import from the
package, not from submodules:

```python
from eventsource.application.migration import (
    # lifecycle model
    Migration, MigrationConfig, MigrationPhase, MigrationStatus,
    MigrationResult, MigrationAuditEntry, AuditEventType,
    TenantRouting, TenantMigrationState, PositionMapping, SyncLag, CutoverResult,
    # orchestration and routing
    MigrationCoordinator, TenantStoreRouter,
    # phase components
    BulkCopier, DualWriteInterceptor, CutoverManager, PositionMapper,
    SyncLagTracker, LagSample, LagStats,
    WritePauseManager, WritePausedError, PauseState, PauseMetrics,
    SubscriptionMigrator, MigrationPlan, PlannedMigration,
    SubscriptionMigrationResult, SubscriptionMigrationError, MigrationSummary,
    # verification
    ConsistencyVerifier, VerificationLevel, VerificationReport,
    StreamConsistency, ConsistencyViolation,
    # observability
    StatusStreamer, StatusStreamManager,
    MigrationMetrics, MigrationMetricSnapshot, ActiveMigrationsTracker,
    get_migration_metrics, release_migration_metrics, clear_metrics_registry,
    # errors and classification
    MigrationError, MigrationNotFoundError, MigrationAlreadyExistsError,
    MigrationStateError, CutoverError, CutoverTimeoutError, ConsistencyError,
    PositionMappingError, RoutingError,
    ErrorSeverity, ErrorRecoverability, ErrorClassification, classify_exception,
    RetryConfig, TRANSIENT_RETRY_CONFIG, CONNECTIVITY_RETRY_CONFIG,
    CUTOVER_RETRY_CONFIG,
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerOpenError, CircuitState,
    ErrorHandler,
)
```

Persistence interfaces are the one part of the surface not re-exported at the
package root. The four Protocols live in `eventsource.ports.migration.repositories`;
the PostgreSQL implementations live in `eventsource.adapters.sql.migration`:

```python
from eventsource.ports.migration.repositories import (
    MigrationRepository,
    TenantRoutingRepository,
    PositionMappingRepository,
    MigrationAuditLogRepository,
)
from eventsource.adapters.sql.migration import (
    PostgreSQLMigrationRepository,
    PostgreSQLTenantRoutingRepository,
    PostgreSQLPositionMappingRepository,
    PostgreSQLMigrationAuditLogRepository,
    VALID_TRANSITIONS,
)
```

`MigrationRepository`, `TenantRoutingRepository`, `PositionMappingRepository`,
and `MigrationAuditLogRepository` are `Protocol` classes -- implement them
structurally to back migration state with something other than PostgreSQL.
`VALID_TRANSITIONS` is the legal `MigrationPhase` transition map enforced by
the repository layer, documented under
[Legal transition table](#legal-transition-table).

## Migration Lifecycle

Two enums, both in `eventsource.ports.migration.models`, describe the lifecycle from
two angles. `MigrationPhase` is the state of the *migration record* -- what the
coordinator is doing. `TenantMigrationState` is the state of the *tenant's
routing row* -- what `TenantStoreRouter` does with each operation. The
coordinator keeps them in step; they are separate because routing has to be
readable by every request without loading the migration.

### MigrationPhase

`MigrationPhase` is a plain `Enum` whose members carry lowercase string values
(`MigrationPhase.BULK_COPY.value == "bulk_copy"`); it is not a `str` subclass,
so compare members, not strings, and use `.value` when persisting or
serialising. It is stored on the `Migration` record and is what
`get_status()`, `wait_for_phase()`, and the audit log report.

#### Phase values

| Phase | `.value` | Meaning |
| --- | --- | --- |
| `PENDING` | `"pending"` | Migration record created; no work started. |
| `BULK_COPY` | `"bulk_copy"` | `BulkCopier` is copying historical events to the target. |
| `DUAL_WRITE` | `"dual_write"` | New events are written to both stores; lag is tracked. |
| `CUTOVER` | `"cutover"` | Writes paused briefly while routing switches to the target. |
| `COMPLETED` | `"completed"` | Cutover succeeded; the tenant reads and writes the target. |
| `ABORTED` | `"aborted"` | Operator cancelled via `abort_migration()`; routing restored to normal. |
| `FAILED` | `"failed"` | Unrecoverable error; routing restored to normal. |

The source store is authoritative through `PENDING`, `BULK_COPY`, and
`DUAL_WRITE`. Only the `CUTOVER → COMPLETED` step makes the target
authoritative.

Notes on the individual values:

- `PENDING` is the phase the `Migration` record is constructed with, before any
  routing row is touched. It is transient in practice: `start_migration()`
  creates the record and then immediately advances to `BULK_COPY` as part of the
  same call, so it is mostly visible in the audit trail rather than in a status
  poll.
- `CUTOVER` is the only phase in which `allows_writes_to_source` is `False`, and
  the only one that is not simply "keep serving from the source". It is bounded
  by `MigrationConfig.cutover_timeout_ms`; a cutover that cannot finish inside
  that budget rolls back to `DUAL_WRITE` rather than sticking.
- `COMPLETED` is set by `_complete_migration()` after `CutoverManager` has
  already moved the tenant's routing state to `MIGRATED`, so the phase update is
  the last step of a successful migration, not the trigger for the switch.
- `ABORTED` and `FAILED` differ only in who caused them. Both clear the tenant's
  routing state back to `NORMAL` (`TenantRoutingRepository.clear_migration_state()`),
  tear down the dual-write interceptor, cancel the running `BulkCopier`, and
  release lag trackers and target-store references. `FAILED` additionally
  records the error message via `record_error()`; `ABORTED` records one only if
  a `reason` was passed. Neither deletes events already copied into the target
  store -- cleaning those up is the operator's job.
- All three terminal phases are final. `abort_migration()` on a migration whose
  phase `is_terminal` raises `MigrationError` rather than transitioning.

#### Legal transition table

Two transition maps exist and they are not identical. `MigrationPhase.can_transition_to()`
is the in-memory check; `VALID_TRANSITIONS` in
`eventsource.adapters.sql.migration.migration` is what
`MigrationRepository.update_phase()` actually enforces, raising
`InvalidPhaseTransitionError` on a violation.

| From | `VALID_TRANSITIONS` (enforced by the repository) | `can_transition_to()` additionally allows |
| --- | --- | --- |
| `PENDING` | `BULK_COPY`, `ABORTED` | `FAILED` |
| `BULK_COPY` | `DUAL_WRITE`, `ABORTED`, `FAILED` | -- |
| `DUAL_WRITE` | `CUTOVER`, `ABORTED`, `FAILED` | -- |
| `CUTOVER` | `COMPLETED`, `DUAL_WRITE` (rollback), `FAILED` | `ABORTED` |
| `COMPLETED` | none | none |
| `ABORTED` | none | none |
| `FAILED` | none | none |

`CUTOVER → DUAL_WRITE` is the rollback edge: if `CutoverManager` cannot complete
the switch inside `cutover_timeout_ms`, the migration returns to dual-write and
can be retried. Every other edge is forward-only, and the three terminal phases
map to the empty set -- once a migration is `COMPLETED`, `ABORTED`, or `FAILED`,
no `update_phase()` call will move it again.

The two maps disagree in exactly two cells, both of them cases where the
in-memory predicate is more permissive than the store:

- `PENDING → FAILED`: `can_transition_to()` returns `True` (it short-circuits on
  `ABORTED`/`FAILED` from any non-terminal phase), but `VALID_TRANSITIONS[PENDING]`
  is `{BULK_COPY, ABORTED}`, so persisting it raises. A migration that has not
  started work is cancelled, not failed.
- `CUTOVER → ABORTED`: likewise allowed in memory, absent from
  `VALID_TRANSITIONS[CUTOVER]`. Once writes are paused there is no clean abort
  edge -- the migration either completes, rolls back to `DUAL_WRITE`, or fails.

When the two disagree the repository wins: treat `VALID_TRANSITIONS` as
authoritative for anything that will be persisted, and read
`can_transition_to()` as a cheap pre-filter rather than a guarantee.

`VALID_TRANSITIONS` is importable if you are implementing `MigrationRepository`
yourself and want to enforce the same rules:

```python
from eventsource.adapters.sql.migration import VALID_TRANSITIONS
from eventsource.ports.migration.models import MigrationPhase

assert VALID_TRANSITIONS[MigrationPhase.CUTOVER] == {
    MigrationPhase.COMPLETED,
    MigrationPhase.DUAL_WRITE,
    MigrationPhase.FAILED,
}
assert VALID_TRANSITIONS[MigrationPhase.COMPLETED] == set()
```

The exception raised on a violation is `InvalidPhaseTransitionError`, a subclass
of `MigrationStateError` (and therefore of `MigrationError`); it carries the
migration id and both phases.

#### Phase predicates

Four read-only properties on the enum:

| Property | True for |
| --- | --- |
| `is_terminal` | `COMPLETED`, `ABORTED`, `FAILED` |
| `is_active` | `BULK_COPY`, `DUAL_WRITE`, `CUTOVER` |
| `allows_writes_to_source` | every phase except `CUTOVER` |
| `requires_dual_write` | `DUAL_WRITE` only |

Note that `PENDING` is neither terminal nor active, so `is_terminal` and
`is_active` are not complements. `list_active_migrations()` uses the
non-terminal notion, not `is_active`.

#### can_transition_to()

```python
def can_transition_to(self, target: MigrationPhase) -> bool
```

Returns `False` unconditionally when `self.is_terminal`. Otherwise returns
`True` for `ABORTED` and `FAILED` from any non-terminal phase, and otherwise
consults the forward map (`PENDING → BULK_COPY`, `BULK_COPY → DUAL_WRITE`,
`DUAL_WRITE → CUTOVER`, `CUTOVER → {COMPLETED, DUAL_WRITE}`).

```python
assert MigrationPhase.DUAL_WRITE.can_transition_to(MigrationPhase.CUTOVER)
assert MigrationPhase.CUTOVER.can_transition_to(MigrationPhase.DUAL_WRITE)  # rollback
assert not MigrationPhase.COMPLETED.can_transition_to(MigrationPhase.DUAL_WRITE)
```

### TenantMigrationState

`TenantMigrationState` lives on the tenant's `TenantRouting` row and is the only
thing `TenantStoreRouter` consults per operation.

#### State values

| State | `.value` | Router behaviour |
| --- | --- | --- |
| `NORMAL` | `"normal"` | Reads and writes go to the tenant's configured store (or the router's default). |
| `BULK_COPY` | `"bulk_copy"` | Reads and writes still go to the source; the copier works in the background. |
| `DUAL_WRITE` | `"dual_write"` | Writes go through the tenant's `DualWriteInterceptor`; reads still come from the source. |
| `CUTOVER_PAUSED` | `"cutover_paused"` | Writes are blocked (`WritePausedError` / callers wait via `WritePauseManager`); reads still come from the source. |
| `MIGRATED` | `"migrated"` | Reads and writes go to the target, which is now the tenant's `store_id`. |

Reads never move on their own: `_get_read_store()` resolves `routing.store_id`
in every state, and it is `_complete_migration()` writing the target
`store_id` at completion that redirects them.

#### State transitions and rollback

`TenantMigrationState.can_transition_to()` allows:

| From | To |
| --- | --- |
| `NORMAL` | `BULK_COPY` |
| `BULK_COPY` | `DUAL_WRITE` |
| `DUAL_WRITE` | `CUTOVER_PAUSED` |
| `CUTOVER_PAUSED` | `MIGRATED`, `DUAL_WRITE` (rollback) |
| `MIGRATED` | (none forward) |

Additionally, **any** state may transition to `NORMAL` -- including `MIGRATED`.
That edge is the cleanup path: `TenantRoutingRepository.clear_migration_state()`
sets `NORMAL` and clears `active_migration_id`, and the coordinator calls it on
both abort and failure. `MIGRATED` is terminal only with respect to forward
progress.

#### State predicates

| Property | True for |
| --- | --- |
| `is_migrating` | `BULK_COPY`, `DUAL_WRITE`, `CUTOVER_PAUSED` |
| `allows_writes` | every state except `CUTOVER_PAUSED` |
| `reads_from_target` | `MIGRATED` only |

#### Phase-to-state correspondence

The coordinator drives both, in this order:

| Migration phase | Tenant state | Set by |
| --- | --- | --- |
| `PENDING` | `NORMAL` | record created before routing is touched |
| `BULK_COPY` | `BULK_COPY` | `start_migration()` sets the routing state, then the phase |
| `DUAL_WRITE` | `DUAL_WRITE` | phase update followed by `set_migration_state()` |
| `CUTOVER` | `CUTOVER_PAUSED` | phase set by `trigger_cutover()`; state set by `CutoverManager` during the pause |
| `COMPLETED` | `MIGRATED` | `_complete_migration()`, which also repoints `store_id` |
| `ABORTED` / `FAILED` | `NORMAL` | `clear_migration_state()` plus interceptor teardown |

The two are updated in separate calls, so a status read can briefly observe a
phase whose corresponding state has not landed yet. Treat
`TenantRouting.migration_state` as the truth about where an individual read or
write will go, and `Migration.phase` as the truth about how far the migration
has progressed.
