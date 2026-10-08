---
id: REFACTOR-tests-unit-application-migration-test_sync_lag_tracker
title: Refactor and Decompose Legacy File test_sync_lag_tracker.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_sync_lag_tracker: Refactor Legacy File test_sync_lag_tracker.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_sync_lag_tracker.py` contains 1230 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_sync_lag_tracker_store.py, test_sync_lag_tracker_config.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_sync_lag_tracker/` with submodules:
- `test_sync_lag_tracker_store.py`: source_store, target_store, LagTestEvent, sid, seed_events, tracker, TestLagSample, TestLagStats, TestSyncLagTrackerInit, TestCalculateLag, TestConvergenceDetection, TestTrackerLagStats, TestSampleHistoryManagement, TestManualLagRecording, TestEdgeCases, TestWithTenantId, TestIntegrationScenarios
- `test_sync_lag_tracker_config.py`: config, strict_config

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_sync_lag_tracker.py (1230 lines):
  Submodule 'test_sync_lag_tracker_store.py' (~1083 lines):
    - [function] source_store (lines 74-76)
    - [function] target_store (lines 80-82)
    - [class] LagTestEvent (lines 41-45)
    - [function] sid (lines 48-50)
    - [function] seed_events (lines 53-65)
    - [function] tracker (lines 98-109)
    - [class] TestLagSample (lines 117-144)
    - [class] TestLagStats (lines 152-216)
    - [class] TestSyncLagTrackerInit (lines 224-315)
    - [class] TestCalculateLag (lines 323-516)
    - [class] TestConvergenceDetection (lines 524-691)
    - [class] TestTrackerLagStats (lines 699-844)
    - [class] TestSampleHistoryManagement (lines 852-934)
    - [class] TestManualLagRecording (lines 942-1000)
    - [class] TestEdgeCases (lines 1008-1099)
    - [class] TestWithTenantId (lines 1107-1143)
    - [class] TestIntegrationScenarios (lines 1151-1230)
  Submodule 'test_sync_lag_tracker_config.py' (~6 lines):
    - [function] config (lines 86-88)
    - [function] strict_config (lines 92-94)
  Suggested barrel exports:
    from .test_sync_lag_tracker_store import source_store, target_store, LagTestEvent, sid, seed_events, tracker, TestLagSample, TestLagStats, TestSyncLagTrackerInit, TestCalculateLag, TestConvergenceDetection, TestTrackerLagStats, TestSampleHistoryManagement, TestManualLagRecording, TestEdgeCases, TestWithTenantId, TestIntegrationScenarios
    from .test_sync_lag_tracker_config import config, strict_config

    __all__ = ["source_store", "target_store", "LagTestEvent", "sid", "seed_events", "tracker", "TestLagSample", "TestLagStats", "TestSyncLagTrackerInit", "TestCalculateLag", "TestConvergenceDetection", "TestTrackerLagStats", "TestSampleHistoryManagement", "TestManualLagRecording", "TestEdgeCases", "TestWithTenantId", "TestIntegrationScenarios", "config", "strict_config"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
