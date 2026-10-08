---
id: '0004'
title: Project Events into Read Models with Checkpoints
status: Accepted
created: 2026-10-07
persona: Alex (The Event Sourced Systems Architect)
target_bc: application
feature: FEAT-PROJECTIONS
governing_prd: PRD-0001
scenarios:
  - Fold stream events through DeclarativeProjection handlers
  - Resume projection from persisted checkpoint after interruption
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
