---
id: REFACTOR-tests-unit-readmodels-test_soft_delete
title: Refactor and Decompose Legacy File test_soft_delete.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-readmodels-test_soft_delete: Refactor Legacy File test_soft_delete.py

## Summary
The grandfathered debt file `tests/unit/readmodels/test_soft_delete.py` contains 525 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_soft_delete_deleted.py, test_soft_delete_model.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/readmodels/test_soft_delete/` with submodules:
- `test_soft_delete_deleted.py`: TestGetDeleted, TestFindDeleted, TestGetExcludesSoftDeleted, TestGetManyExcludesSoftDeleted, TestFindExcludesSoftDeleted, TestCountExcludesSoftDeleted, TestExistsExcludesSoftDeleted, TestTruncateRemovesSoftDeleted, repo, TestSoftDelete, TestRestore, TestSoftDeleteCycle
- `test_soft_delete_model.py`: TestModel

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/readmodels/test_soft_delete.py (525 lines):
  Submodule 'test_soft_delete_deleted.py' (~476 lines):
    - [class] TestGetDeleted (lines 153-212)
    - [class] TestFindDeleted (lines 215-311)
    - [class] TestGetExcludesSoftDeleted (lines 314-330)
    - [class] TestGetManyExcludesSoftDeleted (lines 333-351)
    - [class] TestFindExcludesSoftDeleted (lines 354-406)
    - [class] TestCountExcludesSoftDeleted (lines 409-442)
    - [class] TestExistsExcludesSoftDeleted (lines 445-461)
    - [class] TestTruncateRemovesSoftDeleted (lines 464-483)
    - [function] repo (lines 28-30)
    - [class] TestSoftDelete (lines 33-89)
    - [class] TestRestore (lines 92-150)
    - [class] TestSoftDeleteCycle (lines 486-525)
  Submodule 'test_soft_delete_model.py' (~6 lines):
    - [class] TestModel (lines 19-24)
  Suggested barrel exports:
    from .test_soft_delete_deleted import TestGetDeleted, TestFindDeleted, TestGetExcludesSoftDeleted, TestGetManyExcludesSoftDeleted, TestFindExcludesSoftDeleted, TestCountExcludesSoftDeleted, TestExistsExcludesSoftDeleted, TestTruncateRemovesSoftDeleted, repo, TestSoftDelete, TestRestore, TestSoftDeleteCycle
    from .test_soft_delete_model import TestModel

    __all__ = ["TestGetDeleted", "TestFindDeleted", "TestGetExcludesSoftDeleted", "TestGetManyExcludesSoftDeleted", "TestFindExcludesSoftDeleted", "TestCountExcludesSoftDeleted", "TestExistsExcludesSoftDeleted", "TestTruncateRemovesSoftDeleted", "repo", "TestSoftDelete", "TestRestore", "TestSoftDeleteCycle", "TestModel"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
