---
id: REFACTOR-tests-unit-application-migration-test_subscription_migrator
title: Refactor and Decompose Legacy File test_subscription_migrator.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_subscription_migrator: Refactor Legacy File test_subscription_migrator.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_subscription_migrator.py` contains 1166 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_subscription_migrator_migration.py, test_subscription_migrator_pos.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_subscription_migrator/` with submodules:
- `test_subscription_migrator_migration.py`: TestSubscriptionMigratorPlanMigration, TestSubscriptionMigratorVerifyMigration, TestSubscriptionMigrationResultDataclass, TestPlannedMigrationDataclass, TestMigrationPlanDataclass, TestMigrationSummaryDataclass, TestSubscriptionMigrationErrorException, TestSubscriptionMigratorInit, TestSubscriptionMigratorMigrateSubscriptions, TestSubscriptionMigratorMigrateTenantSubscriptions, TestSubscriptionMigratorWorkflows
- `test_subscription_migrator_pos.py`: source_pos, target_pos

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_subscription_migrator.py (1166 lines):
  Submodule 'test_subscription_migrator_migration.py' (~1097 lines):
    - [class] TestSubscriptionMigratorPlanMigration (lines 93-275)
    - [class] TestSubscriptionMigratorVerifyMigration (lines 666-732)
    - [class] TestSubscriptionMigrationResultDataclass (lines 735-798)
    - [class] TestPlannedMigrationDataclass (lines 801-846)
    - [class] TestMigrationPlanDataclass (lines 849-894)
    - [class] TestMigrationSummaryDataclass (lines 897-975)
    - [class] TestSubscriptionMigrationErrorException (lines 978-1008)
    - [class] TestSubscriptionMigratorInit (lines 50-90)
    - [class] TestSubscriptionMigratorMigrateSubscriptions (lines 278-549)
    - [class] TestSubscriptionMigratorMigrateTenantSubscriptions (lines 552-663)
    - [class] TestSubscriptionMigratorWorkflows (lines 1011-1166)
  Submodule 'test_subscription_migrator_pos.py' (~10 lines):
    - [function] source_pos (lines 36-42)
    - [function] target_pos (lines 45-47)
  Suggested barrel exports:
    from .test_subscription_migrator_migration import TestSubscriptionMigratorPlanMigration, TestSubscriptionMigratorVerifyMigration, TestSubscriptionMigrationResultDataclass, TestPlannedMigrationDataclass, TestMigrationPlanDataclass, TestMigrationSummaryDataclass, TestSubscriptionMigrationErrorException, TestSubscriptionMigratorInit, TestSubscriptionMigratorMigrateSubscriptions, TestSubscriptionMigratorMigrateTenantSubscriptions, TestSubscriptionMigratorWorkflows
    from .test_subscription_migrator_pos import source_pos, target_pos

    __all__ = ["TestSubscriptionMigratorPlanMigration", "TestSubscriptionMigratorVerifyMigration", "TestSubscriptionMigrationResultDataclass", "TestPlannedMigrationDataclass", "TestMigrationPlanDataclass", "TestMigrationSummaryDataclass", "TestSubscriptionMigrationErrorException", "TestSubscriptionMigratorInit", "TestSubscriptionMigratorMigrateSubscriptions", "TestSubscriptionMigratorMigrateTenantSubscriptions", "TestSubscriptionMigratorWorkflows", "source_pos", "target_pos"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
