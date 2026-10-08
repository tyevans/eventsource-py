---
id: 0008
title: Rebuild Projections Deterministically with Typed Read Models
status: Accepted
created: 2026-10-08
persona: Alex (The Event-Sourced Domain Architect)
target_bc: projections
feature: FEAT-PROJECTIONS-REPLAY
governing_prd: PRD-0001
scenarios:
- Replay global feed into projections with bounded failure tracking
- Replay filters feed at adapter level by aggregate type and tenant
- StoreProjection forwards typed constructor options and mutates underlying store
- Read model save with version conflict raises ReadModelVersionConflictError
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0109
---

# US-0008 — Rebuild Projections Deterministically with Typed Read Models

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** event sourced domain architect (Alex),
**I want** to rebuild projections deterministically using `replay()` and author type-safe projections with `StoreProjection[TStore]`,
**So that** read models can be reconstructed from historical logs with explicit failure accounting and optimistic locking guards against lost updates.

## Acceptance Criteria

```gherkin
Scenario: Replay global feed into projections with bounded failure tracking
  Given a GlobalEventFeed containing historical events and a poison event that causes projection error
  When replay is executed against registered projections with non-strict failure mode
  Then all valid events are processed into the projection read models
  And a ReplayReport is returned detailing failed event IDs without aborting the rebuild run.
```

```gherkin
Scenario: Replay filters feed at adapter level by aggregate type and tenant
  Given a multi-tenant event feed containing mixed aggregate categories across multiple tenants
  When replay is invoked specifying aggregate_type "Order" and a designated tenant ID
  Then the storage adapter receives FeedReadOptions to filter at the database query layer
  And only events matching both category and tenant are read into memory.
```

```gherkin
Scenario: StoreProjection forwards typed constructor options and mutates underlying store
  Given a StoreProjection subclass typed with an OrderStore and initialized with Unpack projection options
  When event batches are delivered to the projection
  Then parent configuration such as retry policy and tracer are preserved
  And handlers mutate the encapsulated read model store via self._store.
```

```gherkin
Scenario: Read model save with version conflict raises ReadModelVersionConflictError
  Given a read model record loaded from store at version 2
  When a concurrent update attempts to save the record expecting version 1
  Then a ReadModelVersionConflictError is raised
  And the conflicting write is rejected without overwriting stored state.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0150, ADR-0154, ADR-0155
- **Verified Test Suites**:
  - `tests/unit/application/projections/test_rebuild.py`: Verifies foreground projection replay, progress tracking, and batch processing.
  - `tests/unit/application/projections/test_replay.py`: Verifies error retention in `ReplayReport` and pushdown query filtering.
  - `tests/unit/readmodels/`: Verifies `StoreProjection[TStore]` typed constructor forwarding and `ReadModelVersionConflictError` on optimistic lock collisions.
  - `tests/benchmarks/test_projections.py`: Verifies high-throughput handler lookup and dispatch performance.
- **Architectural Invariants Verified**:
  - *Bounded Failure Tracking*: Poison events record failure details into `ReplayReport` without aborting rebuild passes.
  - *Pushdown Feed Filtering*: Replay filters aggregate type and tenant at the adapter storage layer.
  - *Generic Store Base*: `StoreProjection[TStore]` provides type-safe encapsulation of read model repositories.
