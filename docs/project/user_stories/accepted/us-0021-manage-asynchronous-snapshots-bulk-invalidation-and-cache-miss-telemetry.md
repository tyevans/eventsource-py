---
id: '0021'
title: Manage Asynchronous Snapshots, Bulk Invalidation, and Cache Miss Telemetry
status: Accepted
created: 2026-10-09
persona: Alex (The Event-Sourced Domain Architect)
target_bc: snapshots
feature: FEAT-SNAPSHOT-LIFECYCLE
governing_prd: PRD-0001
scenarios:
- BackgroundScheduler executes snapshot writes asynchronously with flushes
- Bulk snapshot invalidation purges outdated schema versions
- Snapshot miss reasons record granular operational metrics
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0007
- ADR-0106
---

# US-0021: Manage Asynchronous Snapshots, Bulk Invalidation, and Cache Miss Telemetry

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** event-sourced domain architect (Alex),
**I want** to execute snapshots asynchronously using background schedulers, bulk-invalidate obsolete snapshot schemas, and monitor snapshot miss telemetry,
**So that** aggregate commands never block on snapshot I/O, evolving aggregate schemas can purge stale mementos, and observability pipelines track cache hit/miss health.

## Acceptance Criteria

```gherkin
Scenario: BackgroundScheduler executes snapshot writes asynchronously with flushes
  Given an aggregate repository configured with BackgroundScheduler
  When a command save triggers an automatic snapshot
  Then the command returns immediately without blocking on snapshot disk writes
  And awaiting "await_pending()" drains in-flight snapshot tasks before teardown.
```

```gherkin
Scenario: Bulk snapshot invalidation purges outdated schema versions
  Given stored snapshots across multiple aggregate types with legacy schema_version 1
  When "delete_snapshots_by_type('Order', schema_version_below=2)" is invoked
  Then all legacy Order snapshots are purged while newer version snapshots remain intact
  And aggregates rehydrate cleanly from immutable event streams.
```

```gherkin
Scenario: Snapshot miss reasons record granular operational metrics
  Given aggregate loads encountering missing snapshots, schema mismatches, or store errors
  When the snapshot load falls back to event replay
  Then the exact SnapshotMissReason is categorized and the OTel counter is incremented.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified (100% test pass rate, 0 backdoor mocks)
- **Implementation Modules**:
  - `src/eventsource/application/aggregates/snapshotting.py`: `BackgroundScheduler`, `SnapshotMissReason`.
  - `src/eventsource/ports/snapshots.py`: `SnapshotTypeInvalidation` protocol.
  - `src/eventsource/adapters/postgresql/snapshots.py`, `sqlite/snapshots.py`, `memory/snapshots.py`.
- **Verified Test Suites**:
  - `tests/unit/application/aggregates/test_snapshotting.py`: Scheduler execution and miss classification.
  - `tests/unit/adapters/test_postgresql_snapshots.py`: PostgreSQL invalidation and bounds.
  - `tests/unit/adapters/test_sqlite_snapshots.py`: SQLite invalidation and persistence.
