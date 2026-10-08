---
id: '0006'
title: Migrate Event Stores Zero-Downtime
status: Accepted
created: 2026-10-07
persona: Riley (The Open-Source Library Maintainer)
target_bc: application
feature: FEAT-LIVE-MIGRATION
governing_prd: PRD-0001
scenarios:
  - Dual-write events to source and target event stores during migration
  - Bulk copy historical events and cutover cleanly
---

# US-0006 — Migrate Event Stores Zero-Downtime

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As a** system maintainer (Riley),
**I want** to execute zero-downtime event store migrations using `LiveMigrationCoordinator`,
**So that** historical events copy in the background while new stream appends dual-write to both stores until final cutover.

## Acceptance Criteria

```gherkin
Scenario: Dual-write events to source and target event stores during migration
  Given an active migration in dual-write phase
  When new events are appended to the migration router store
  Then events are written to both source and target stores with identical global sequences.
```

```gherkin
Scenario: Bulk copy historical events and cutover cleanly
  Given a bulk copier streaming historical events from source to target
  When all historical ranges are copied and sync lag drops below cutover threshold
  Then the router cuts over primary reads and writes to target without dropping concurrent appends.
```
