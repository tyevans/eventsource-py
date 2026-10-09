---
id: REFACTOR-eventsource-application-migration-subscription_migrator
title: Refactor and Decompose Legacy File subscription_migrator.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-subscription_migrator: Refactor Legacy File subscription_migrator.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/subscription_migrator.py` contains 905 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (subscription_migrator_migration.py, subscription_migrator_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/subscription_migrator/` with submodules:
- `subscription_migrator_migration.py`: SubscriptionMigrationError, SubscriptionMigrationResult, PlannedMigration, MigrationPlan, MigrationSummary
- `subscription_migrator_core.py`: SubscriptionMigrator

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/migration/subscription_migrator.py (905 lines):
  Submodule 'subscription_migrator_migration.py' (~188 lines):
    - [class] SubscriptionMigrationError (lines 74-100)
    - [class] SubscriptionMigrationResult (lines 104-144)
    - [class] PlannedMigration (lines 148-183)
    - [class] MigrationPlan (lines 187-222)
    - [class] MigrationSummary (lines 226-273)
  Submodule 'subscription_migrator_core.py' (~620 lines):
    - [class] SubscriptionMigrator (lines 276-895)
  Suggested barrel exports:
    from .subscription_migrator_migration import SubscriptionMigrationError, SubscriptionMigrationResult, PlannedMigration, MigrationPlan, MigrationSummary
    from .subscription_migrator_core import SubscriptionMigrator

    __all__ = ["SubscriptionMigrationError", "SubscriptionMigrationResult", "PlannedMigration", "MigrationPlan", "MigrationSummary", "SubscriptionMigrator"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
