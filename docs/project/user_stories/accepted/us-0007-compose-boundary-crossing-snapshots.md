---
id: '0007'
title: Compose Boundary-Crossing Snapshots for Efficient Aggregate Rehydration
status: Accepted
created: 2026-10-08
persona: Alex (The Event-Sourced Domain Architect)
target_bc: snapshots
feature: FEAT-SNAPSHOTS
governing_prd: PRD-0001
scenarios:
- EveryNEvents policy triggers snapshot when save crosses stride boundary
- Aggregate state is rehydrated from valid snapshot and tail event stream
- Automatic snapshot persistence failure degrades gracefully without aborting event
  save
- Explicit create_snapshot strictly validates and persists aggregate memento
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0106
---

# US-0007 — Compose Boundary-Crossing Snapshots for Efficient Aggregate Rehydration

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** event sourced domain architect (Alex),
**I want** to compose `SnapshotPolicy` predicates and `SnapshotScheduler` executors on `AggregateRepository`,
**So that** aggregates with long stream histories rehydrate efficiently from boundary-crossing snapshots while events remain the immutable ground truth.

## Acceptance Criteria

```gherkin
Scenario: EveryNEvents policy triggers snapshot when save crosses stride boundary
  Given an aggregate repository configured with an "EveryNEvents(100)" snapshot policy
  When a save operation commits a batch of 3 events advancing version from 98 to 101
  Then the policy recognizes an interval boundary crossing
  And a snapshot at version 101 is scheduled for persistence.
```

```gherkin
Scenario: Aggregate state is rehydrated from valid snapshot and tail event stream
  Given a stored aggregate snapshot at version 100 with matching schema version
  And subsequent domain events in the stream from version 101 to 105
  When the aggregate is loaded through the repository
  Then the aggregate state is initialized directly from the snapshot
  And only events 101 through 105 are replayed to reach current state.
```

```gherkin
Scenario: Automatic snapshot persistence failure degrades gracefully without aborting event save
  Given an aggregate repository configured with an ImmediateScheduler and a failing snapshot store
  When a command execution saves events that trigger an automatic snapshot
  Then the committed events are durably persisted to the event store
  And a warning is logged while returning success to the caller without raising an exception.
```

```gherkin
Scenario: Explicit create_snapshot strictly validates and persists aggregate memento
  Given a loaded aggregate instance at version 45
  When the caller invokes create_snapshot explicitly on the repository
  Then the aggregate state is serialized into a Snapshot memento
  And saved synchronously to the snapshot store with any failure raised directly.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0121, ADR-0149, ADR-0153
- **Verified Test Suites**:
  - `tests/unit/application/snapshots/`: Verifies snapshot policy evaluation (`EveryNEvents`), boundary stride crossing, and scheduler execution.
  - `tests/unit/adapters/sqlite/snapshots.py` tests: Verifies SQLite snapshot store schema, connection ownership, and memento persistence.
  - `tests/unit/adapters/postgresql/snapshots.py` tests: Verifies PostgreSQL snapshot store schema, JSONB serialization, and version tracking.
  - `tests/unit/bench/test_adapters_memory.py`: Verifies in-memory snapshot roundtrip and aggregate rehydration from snapshot + tail events.
- **Architectural Invariants Verified**:
  - *Boundary-Crossing Policy*: `EveryNEvents` stride detection triggers snapshots reliably on batch saves across boundaries.
  - *Non-Blocking Degradation*: Automatic snapshot failures log warnings without rolling back committed domain events.
  - *Snapshots as Caches*: Aggregate rehydration verifies events remain ground truth while snapshots optimize replay latency.
