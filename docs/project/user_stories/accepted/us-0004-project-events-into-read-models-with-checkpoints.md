---
id: '0004'
title: Project Events into Read Models with Checkpoints
status: Accepted
created: 2026-10-07
persona: Alex (The Event-Sourced Domain Architect)
target_bc: projections
feature: FEAT-PROJECTIONS
governing_prd: PRD-0001
scenarios:
- Fold stream events through DeclarativeProjection handlers
- Resume projection from persisted checkpoint after interruption
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0109
---

# US-0004 — Project Events into Read Models with Checkpoints

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** event sourced systems architect (Alex),
**I want** to build CQRS read models using `DeclarativeProjection` with persistent checkpoints,
**So that** query representations stay synchronized with event streams and projection restarts resume accurately from the last acknowledged sequence position.

## Acceptance Criteria

```gherkin
Scenario: Fold stream events through DeclarativeProjection handlers
  Given a "DeclarativeProjection" handling "OrderCreated" and "OrderPaid"
  When a series of order lifecycle events are delivered to the projection
  Then the projection read model reflects the mutated order status and computed total.
```

```gherkin
Scenario: Resume projection from persisted checkpoint after interruption
  Given a projection running on a catchup runner interrupted at position 42
  When the subscription runner restarts with the checkpoint repository
  Then processing resumes from position 43 without duplicating previous event applications.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0121, ADR-0124, ADR-0126, ADR-0129, ADR-0147, ADR-0150, ADR-0154, ADR-0155, ADR-0166
- **Verified Test Suites**:
  - `tests/unit/application/projections/test_projection_decorators.py`: Verifies `@handles` registration and type mapping on projections.
  - `tests/unit/readmodels/test_projection.py`: Verifies `DeclarativeProjection` and `DatabaseProjection` read model mutation and lifecycle.
  - `tests/unit/readmodels/test_handler_integration.py`: Verifies end-to-end event folding into typed read model repositories.
  - `tests/integration/projections/`: Verifies checkpoint persistence across interruptions and gapless replay.
- **Architectural Invariants Verified**:
  - *Checkpoint Isolation*: Segregated `CheckpointRepository` port tracks stream positions independently of read model payloads.
  - *Deterministic Resumption*: Interrupted runners resume accurately from `last_checkpoint + 1` without duplicate execution.
  - *Read Model Error Isolation*: Version conflicts raise explicit `ReadModelVersionConflictError` rather than silent overwrites.
