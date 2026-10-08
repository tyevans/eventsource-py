---
id: '0006'
title: Migrate Event Stores Zero-Downtime
status: Accepted
created: 2026-10-07
persona: Chris (The SRE / Resilience & Cutover Operator)
target_bc: migration
feature: FEAT-LIVE-MIGRATION
governing_prd: PRD-0001
scenarios:
- Coordinator advances migration through deterministic five-state phase lifecycle
- Source-first dual writing secures authoritative store before mirroring
- Bounded cutover write pause automatically rolls back to dual-write upon timeout
- Strict zero-lag cutover gate enforces complete sync before traffic switchover
- On-demand resync pass reconciles clamped lag anchors during dual-write
- PositionMapper translates subscription checkpoints across heterogeneous stores
governing_adrs:
- ADR-0007
- ADR-0114
- ADR-0123
- ADR-0127
- ADR-0128
- ADR-0134
- ADR-0144
---

# US-0006 — Migrate Event Stores Zero-Downtime

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** SRE and resilience operator (Chris),
**I want** to execute zero-downtime event store migrations using `MigrationCoordinator`,
**So that** live tenant traffic continues uninterrupted with source-first dual writing, bounded write pauses roll back safely under timeouts, and subscription checkpoints seamlessly map to target store positions.

## Acceptance Criteria

```gherkin
Scenario: Coordinator advances migration through deterministic five-state phase lifecycle
  Given a migration request for a tenant from source store to target store
  When the operator initiates the migration via "MigrationCoordinator.start_migration()"
  Then the migration transitions sequentially through PENDING, BULK_COPY, DUAL_WRITE, CUTOVER, and COMPLETED
  And status streaming emits real-time progress updates for each phase transition
  And terminal state prevents any subsequent forward phase transitions.
```

```gherkin
Scenario: Source-first dual writing secures authoritative store before mirroring
  Given an active migration in DUAL_WRITE phase with DualWriteInterceptor attached
  When a client appends new domain events through TenantStoreRouter
  Then the events are committed to the authoritative source store first
  And the events are mirrored asynchronously to the target store
  And a transient error in target mirroring does not abort or roll back the primary source transaction.
```

```gherkin
Scenario: Bounded cutover write pause automatically rolls back to dual-write upon timeout
  Given an active migration in DUAL_WRITE phase with "cutover_timeout_ms" set to 500
  When cutover is triggered but lock acquisition or synchronization exceeds the 500ms deadline
  Then CutoverTimeoutError is raised and write pause is released
  And the migration phase automatically rolls back to DUAL_WRITE
  And client appends to the source store resume immediately without lost writes.
```

```gherkin
Scenario: Strict zero-lag cutover gate enforces complete sync before traffic switchover
  Given a migration configured with default "cutover_max_lag_events=0"
  And the target store is lagging behind the source safe horizon by 3 events
  When the operator attempts to trigger cutover
  Then the cutover gate rejects the operation with CutoverLagError
  And no routing switch or write pause occurs until target lag reaches exactly 0.
```

```gherkin
Scenario: On-demand resync pass reconciles clamped lag anchors during dual-write
  Given an active migration in DUAL_WRITE phase where intermittent network drops created residual lag
  When the operator invokes "MigrationCoordinator.run_resync_pass(migration_id)"
  Then the coordinator copies all unmirrored historical events up to the source safe horizon
  And the synchronization lag anchor advances to 0 without restarting the bulk copy phase.
```

```gherkin
Scenario: PositionMapper translates subscription checkpoints across heterogeneous stores
  Given completed bulk copy and dual-write recording position mappings in PositionMapper
  And a projection subscription with a recorded checkpoint at source position 1250
  When the operator executes subscription migration during cutover
  Then the subscription checkpoint is remapped to the corresponding target position
  And the projection runner resumes on the target store without event loss or duplicate dispatch.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0114, ADR-0123, ADR-0127, ADR-0128, ADR-0134, ADR-0144
- **Verified Test Suites**:
  - `tests/unit/application/migration/test_chaos.py`: Verifies network partition simulation, failure recovery, and timeout rollbacks.
  - `tests/unit/application/migration/test_router.py`: Verifies `TenantStoreRouter` concurrent store registration and traffic switching.
  - `tests/unit/application/migration/test_bulk_copy_resume_property.py`: Verifies bulk copy property invariants and historical event parity.
  - `tests/unit/application/migration/test_migration_audit_log.py`: Verifies phase transition logging and audit trail integrity.
  - `tests/integration/migrations/`: Verifies end-to-end live migration lifecycle, dual-writing, cutover, and position mapping.
- **Architectural Invariants Verified**:
  - *Deterministic 5-Phase Lifecycle*: Strict state progression (PENDING -> BULK_COPY -> DUAL_WRITE -> CUTOVER -> COMPLETED).
  - *Source-First Dual Writing*: Authoritative store appends succeed independently of target mirror latency.
  - *Strict Zero-Lag Cutover Gate*: Cutover enforces `cutover_max_lag_events=0` and rolls back to dual-write upon timeout.
