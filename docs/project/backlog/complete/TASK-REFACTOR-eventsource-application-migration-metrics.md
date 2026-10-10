---
id: REFACTOR-eventsource-application-migration-metrics
title: Refactor and Decompose Legacy File metrics.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-metrics: Refactor Legacy File metrics.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/metrics.py` contains 818 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (metrics_migration.py, metrics_op.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/metrics/` with submodules:
- `metrics_migration.py`: MigrationMetricSnapshot, MigrationMetrics, get_migration_metrics, release_migration_metrics, _get_meter, reset_meter, _PhaseTimer, _CutoverTimer, ActiveMigrationsTracker, clear_metrics_registry
- `metrics_op.py`: NoOpCounter, NoOpHistogram, NoOpGauge

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/migration/metrics.py (818 lines):
  Submodule 'metrics_migration.py' (~668 lines):
    - [class] MigrationMetricSnapshot (lines 132-167)
    - [class] MigrationMetrics (lines 171-542)
    - [function] get_migration_metrics (lines 743-773)
    - [function] release_migration_metrics (lines 776-788)
    - [function] _get_meter (lines 59-72)
    - [function] reset_meter (lines 75-82)
    - [class] _PhaseTimer (lines 545-580)
    - [class] _CutoverTimer (lines 583-619)
    - [class] ActiveMigrationsTracker (lines 626-736)
    - [function] clear_metrics_registry (lines 791-800)
  Submodule 'metrics_op.py' (~40 lines):
    - [class] NoOpCounter (lines 85-99)
    - [class] NoOpHistogram (lines 102-116)
    - [class] NoOpGauge (lines 119-128)
  Suggested barrel exports:
    from .metrics_migration import MigrationMetricSnapshot, MigrationMetrics, get_migration_metrics, release_migration_metrics, _get_meter, reset_meter, _PhaseTimer, _CutoverTimer, ActiveMigrationsTracker, clear_metrics_registry
    from .metrics_op import NoOpCounter, NoOpHistogram, NoOpGauge

    __all__ = ["MigrationMetricSnapshot", "MigrationMetrics", "get_migration_metrics", "release_migration_metrics", "_get_meter", "reset_meter", "_PhaseTimer", "_CutoverTimer", "ActiveMigrationsTracker", "clear_metrics_registry", "NoOpCounter", "NoOpHistogram", "NoOpGauge"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
