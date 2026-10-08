---
id: '0106'
title: Snapshot Policies, Scheduling, and Boundary-Crossing Rehydration
status: Accepted
target_bc: snapshots
governing_prds:
- PRD-0001
governing_stories:
- US-0007
---

# ADR-0106: Snapshot Policies, Scheduling, and Boundary-Crossing Rehydration

## Summary
Composed snapshot policies decoupled from stores with boundary-crossing rehydration.

## Context
Rehydrating long-lived aggregates from hundreds or thousands of events degrades performance. Earlier designs tightly coupled snapshot generation to the event store or mutated aggregate state.

## Decision
1. Decompose snapshotting into `SnapshotPolicy` (when to snapshot) and `SnapshotScheduler` (how to schedule).
2. Support composable policies like `EveryNEvents(n)` and compound policies.
3. Snapshots cross boundaries cleanly: they record pure state models without binding to specific stream instances.
4. Aggregate repository rehydration automatically loads the nearest valid snapshot and replays subsequent events forward from the snapshot version.

## Consequences
- Sub-millisecond aggregate rehydration even over massive stream histories.
- Snapshot policies can be tuned per aggregate type without changing storage backends.
- Clean isolation between domain state models and persistent snapshot schemas.
