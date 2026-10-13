---
id: "0185"
title: Atomic Routing Switch for Migration Cutover
status: Proposed
target_bc: application
governing_adrs:
  - ADR-0007
  - ADR-0161
  - ADR-0174
priority: P2
---

# TASK-0185: Atomic Routing Switch for Migration Cutover

## Summary
Ensure tenant cutover executes routing table updates and migration state transitions in a single atomic database transaction to prevent inconsistent states during unexpected process failure.

## Problem Statement & Context
- Tenant cutover currently executes `set_routing()` and `set_migration_state(MIGRATED)` as two separate database writes.
- A process failure or crash between these operations causes state inconsistency that requires manual operator recovery.
- While ADR-0048 introduced compensating rollbacks (`_rollback`) to revert route changes on failure, `TenantRoutingRepository` currently lacks multi-statement transaction support.

## Required Actions
1. Extend the `TenantRoutingRepository` port to support multi-operation database transactions or atomic batch execution.
2. Update the migration coordinator to execute routing updates (`set_routing`) and migration state transitions (`set_migration_state(MIGRATED)`) in a single atomic transaction.
3. Update adapter implementations (PostgreSQL, SQLite, In-Memory) to implement the atomic transaction capability.
4. Add failure-injection tests verifying atomicity under abrupt coordinator failure.

## Definition of Done (Blackbox Frontdoor TDD)
1. Cutover transitions are fully atomic; simulated failure during cutover leaves zero dangling routing states.
2. Conformance tests across all repository backends verify transactional commit and rollback contracts.
3. All source files strictly comply with ADR-0002 (<500 lines limit).
