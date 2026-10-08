---
id: '0001'
title: Define Aggregates and Record Committed Events
status: Accepted
created: 2026-10-07
persona: Alex (The Event Sourced Systems Architect)
target_bc: domain
feature: FEAT-CORE-AGGREGATE
governing_prd: PRD-0001
scenarios:
  - Command execution on DeciderAggregate emits domain events
  - Concurrent command on stale aggregate version raises ExpectedVersionError
---

# US-0001 — Define Aggregates and Record Committed Events

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** event sourced systems architect (Alex),
**I want** to model state machines using `DeciderAggregate` or `DeclarativeAggregate`,
**So that** business invariants are enforced synchronously and domain events are committed with optimistic concurrency control.

## Acceptance Criteria

```gherkin
Scenario: Command execution on DeciderAggregate emits domain events
  Given an uncommitted Order aggregate in draft state
  When the caller issues a "ShipOrder" command with valid tracking info
  Then an "OrderShipped" domain event is generated
  And the uncommitted version is incremented by 1.
```

```gherkin
Scenario: Concurrent command on stale aggregate version raises ExpectedVersionError
  Given an aggregate committed at version 5
  When another worker attempts to commit a change expecting version 4
  Then an "ExpectedVersionError" is raised
  And no uncommitted events are appended to the event store.
```
