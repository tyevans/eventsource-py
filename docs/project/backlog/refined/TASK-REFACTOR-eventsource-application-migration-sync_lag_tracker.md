---
id: REFACTOR-eventsource-application-migration-sync_lag_tracker
title: Refactor and Decompose Legacy File sync_lag_tracker.py
status: Refined
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-sync_lag_tracker: Refactor Legacy File sync_lag_tracker.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/sync_lag_tracker.py` contains 591 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (sync_lag_tracker_sample.py, sync_lag_tracker_stats.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/sync_lag_tracker/` with submodules:
- `sync_lag_tracker_sample.py`: LagSample, SyncLagTracker
- `sync_lag_tracker_stats.py`: LagStats

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/migration/sync_lag_tracker.py (591 lines):
  Submodule 'sync_lag_tracker_sample.py' (~470 lines):
    - [class] LagSample (lines 72-82)
    - [class] SyncLagTracker (lines 126-584)
  Submodule 'sync_lag_tracker_stats.py' (~38 lines):
    - [class] LagStats (lines 86-123)
  Suggested barrel exports:
    from .sync_lag_tracker_sample import LagSample, SyncLagTracker
    from .sync_lag_tracker_stats import LagStats

    __all__ = ["LagSample", "SyncLagTracker", "LagStats"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
