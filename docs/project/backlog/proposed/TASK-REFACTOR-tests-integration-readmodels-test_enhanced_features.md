---
id: REFACTOR-tests-integration-readmodels-test_enhanced_features
title: Refactor and Decompose Legacy File test_enhanced_features.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-readmodels-test_enhanced_features: Refactor Legacy File test_enhanced_features.py

## Summary
The grandfathered debt file `tests/integration/readmodels/test_enhanced_features.py` contains 851 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_enhanced_features_repo.py, test_enhanced_features_model.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/readmodels/test_enhanced_features/` with submodules:
- `test_enhanced_features_repo.py`: enhanced_inmemory_repo, enhanced_sqlite_repo, enhanced_postgresql_repo, enhanced_repo_type, enhanced_repo, TestSoftDeleteHelpers, TestOptimisticLocking, TestExceptions, TestCombinedFeatures, TestInMemoryEnhancedFeatures, TestSQLiteEnhancedFeatures, TestPostgreSQLEnhancedFeatures
- `test_enhanced_features_model.py`: EnhancedTestModel, enhanced_model_factory

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/integration/readmodels/test_enhanced_features.py (851 lines):
  Submodule 'test_enhanced_features_repo.py' (~715 lines):
    - [function] enhanced_inmemory_repo (lines 63-69)
    - [function] enhanced_sqlite_repo (lines 73-93)
    - [function] enhanced_postgresql_repo (lines 97-114)
    - [function] enhanced_repo_type (lines 140-142)
    - [function] enhanced_repo (lines 146-164)
    - [class] TestSoftDeleteHelpers (lines 172-327)
    - [class] TestOptimisticLocking (lines 335-466)
    - [class] TestExceptions (lines 474-502)
    - [class] TestCombinedFeatures (lines 510-665)
    - [class] TestInMemoryEnhancedFeatures (lines 673-721)
    - [class] TestSQLiteEnhancedFeatures (lines 724-764)
    - [class] TestPostgreSQLEnhancedFeatures (lines 768-851)
  Submodule 'test_enhanced_features_model.py' (~26 lines):
    - [class] EnhancedTestModel (lines 48-54)
    - [function] enhanced_model_factory (lines 118-136)
  Suggested barrel exports:
    from .test_enhanced_features_repo import enhanced_inmemory_repo, enhanced_sqlite_repo, enhanced_postgresql_repo, enhanced_repo_type, enhanced_repo, TestSoftDeleteHelpers, TestOptimisticLocking, TestExceptions, TestCombinedFeatures, TestInMemoryEnhancedFeatures, TestSQLiteEnhancedFeatures, TestPostgreSQLEnhancedFeatures
    from .test_enhanced_features_model import EnhancedTestModel, enhanced_model_factory

    __all__ = ["enhanced_inmemory_repo", "enhanced_sqlite_repo", "enhanced_postgresql_repo", "enhanced_repo_type", "enhanced_repo", "TestSoftDeleteHelpers", "TestOptimisticLocking", "TestExceptions", "TestCombinedFeatures", "TestInMemoryEnhancedFeatures", "TestSQLiteEnhancedFeatures", "TestPostgreSQLEnhancedFeatures", "EnhancedTestModel", "enhanced_model_factory"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
