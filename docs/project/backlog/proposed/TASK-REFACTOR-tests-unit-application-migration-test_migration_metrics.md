---
id: REFACTOR-tests-unit-application-migration-test_migration_metrics
title: Refactor and Decompose Legacy File test_migration_metrics.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_migration_metrics: Refactor Legacy File test_migration_metrics.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_migration_metrics.py` contains 964 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_migration_metrics_no.py, test_migration_metrics_tel.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_migration_metrics/` with submodules:
- `test_migration_metrics_no.py`: TestNoOpInstruments, TestMigrationMetricsNoOTel, TestOTELMetricsAvailable, TestMigrationMetricSnapshot, TestMigrationMetrics, TestActiveMigrationsTracker, TestMetricsRegistry, TestPhaseTimer, TestCutoverTimer, TestObservableGaugeCallbacks, TestImports
- `test_migration_metrics_tel.py`: TestMetricsWithMockedOTel

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_migration_metrics.py (964 lines):
  Submodule 'test_migration_metrics_no.py' (~791 lines):
    - [class] TestNoOpInstruments (lines 40-67)
    - [class] TestMigrationMetricsNoOTel (lines 399-453)
    - [class] TestOTELMetricsAvailable (lines 23-37)
    - [class] TestMigrationMetricSnapshot (lines 70-131)
    - [class] TestMigrationMetrics (lines 134-396)
    - [class] TestActiveMigrationsTracker (lines 587-672)
    - [class] TestMetricsRegistry (lines 675-769)
    - [class] TestPhaseTimer (lines 772-808)
    - [class] TestCutoverTimer (lines 811-845)
    - [class] TestObservableGaugeCallbacks (lines 848-914)
    - [class] TestImports (lines 917-964)
  Submodule 'test_migration_metrics_tel.py' (~129 lines):
    - [class] TestMetricsWithMockedOTel (lines 456-584)
  Suggested barrel exports:
    from .test_migration_metrics_no import TestNoOpInstruments, TestMigrationMetricsNoOTel, TestOTELMetricsAvailable, TestMigrationMetricSnapshot, TestMigrationMetrics, TestActiveMigrationsTracker, TestMetricsRegistry, TestPhaseTimer, TestCutoverTimer, TestObservableGaugeCallbacks, TestImports
    from .test_migration_metrics_tel import TestMetricsWithMockedOTel

    __all__ = ["TestNoOpInstruments", "TestMigrationMetricsNoOTel", "TestOTELMetricsAvailable", "TestMigrationMetricSnapshot", "TestMigrationMetrics", "TestActiveMigrationsTracker", "TestMetricsRegistry", "TestPhaseTimer", "TestCutoverTimer", "TestObservableGaugeCallbacks", "TestImports", "TestMetricsWithMockedOTel"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
