---
id: REFACTOR-tests-unit-application-migration-test_coordinator_subscriptions
title: Refactor and Decompose Legacy File test_coordinator_subscriptions.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_coordinator_subscriptions: Refactor Legacy File test_coordinator_subscriptions.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_coordinator_subscriptions.py` contains 1081 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_coordinator_subscriptions_pos.py, test_coordinator_subscriptions_make.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_coordinator_subscriptions/` with submodules:
- `test_coordinator_subscriptions_pos.py`: source_pos, target_pos, TestVerifyConsistency, TestMigrateSubscriptions, TestGetConsistencyReport, TestGetSubscriptionSummary, TestCompleteCutoverWithP3005, TestCleanupMigrationResourcesP3005, TestVerificationLevel
- `test_coordinator_subscriptions_make.py`: make_verification_report, make_migration_summary

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/migration/test_coordinator_subscriptions.py (1081 lines):
  Submodule 'test_coordinator_subscriptions_pos.py' (~971 lines):
    - [function] source_pos (lines 44-46)
    - [function] target_pos (lines 49-51)
    - [class] TestVerifyConsistency (lines 105-342)
    - [class] TestMigrateSubscriptions (lines 345-613)
    - [class] TestGetConsistencyReport (lines 616-658)
    - [class] TestGetSubscriptionSummary (lines 661-701)
    - [class] TestCompleteCutoverWithP3005 (lines 704-930)
    - [class] TestCleanupMigrationResourcesP3005 (lines 933-975)
    - [class] TestVerificationLevel (lines 978-1081)
  Submodule 'test_coordinator_subscriptions_make.py' (~47 lines):
    - [function] make_verification_report (lines 54-80)
    - [function] make_migration_summary (lines 83-102)
  Suggested barrel exports:
    from .test_coordinator_subscriptions_pos import source_pos, target_pos, TestVerifyConsistency, TestMigrateSubscriptions, TestGetConsistencyReport, TestGetSubscriptionSummary, TestCompleteCutoverWithP3005, TestCleanupMigrationResourcesP3005, TestVerificationLevel
    from .test_coordinator_subscriptions_make import make_verification_report, make_migration_summary

    __all__ = ["source_pos", "target_pos", "TestVerifyConsistency", "TestMigrateSubscriptions", "TestGetConsistencyReport", "TestGetSubscriptionSummary", "TestCompleteCutoverWithP3005", "TestCleanupMigrationResourcesP3005", "TestVerificationLevel", "make_verification_report", "make_migration_summary"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
