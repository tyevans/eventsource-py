---
id: REFACTOR-tests-unit-application-migration-test_bulk_copier
title: Refactor and Decompose Legacy File test_bulk_copier.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_bulk_copier: Refactor Legacy File test_bulk_copier.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_bulk_copier.py` contains 1393 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_bulk_copier_copy.py, test_bulk_copier_progress.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_bulk_copier/` with submodules:
- `test_bulk_copier_copy.py`: TestBulkCopyProgress, TestBulkCopyResult, TestEvent, _pos, TestRateLimiter, TestBulkCopierInit, TestBulkCopierPauseResumeCancel, TestBulkCopierRun, TestBulkCopierWriteBatch, TestBulkCopierOverlapWithLiveMirror, TestBulkCopierCountTenantEvents, TestBulkCopierStreamTenantEvents, TestBulkCopierWaitIfPaused
- `test_bulk_copier_progress.py`: TestBulkCopierRunProgress

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/migration/test_bulk_copier.py (1393 lines):
  Submodule 'test_bulk_copier_copy.py' (~1264 lines):
    - [class] TestBulkCopyProgress (lines 55-113)
    - [class] TestBulkCopyResult (lines 116-143)
    - [class] TestEvent (lines 44-48)
    - [function] _pos (lines 51-52)
    - [class] TestRateLimiter (lines 146-192)
    - [class] TestBulkCopierInit (lines 195-246)
    - [class] TestBulkCopierPauseResumeCancel (lines 249-297)
    - [class] TestBulkCopierRun (lines 300-765)
    - [class] TestBulkCopierWriteBatch (lines 768-1072)
    - [class] TestBulkCopierOverlapWithLiveMirror (lines 1137-1265)
    - [class] TestBulkCopierCountTenantEvents (lines 1268-1313)
    - [class] TestBulkCopierStreamTenantEvents (lines 1316-1348)
    - [class] TestBulkCopierWaitIfPaused (lines 1351-1393)
  Submodule 'test_bulk_copier_progress.py' (~60 lines):
    - [class] TestBulkCopierRunProgress (lines 1075-1134)
  Suggested barrel exports:
    from .test_bulk_copier_copy import TestBulkCopyProgress, TestBulkCopyResult, TestEvent, _pos, TestRateLimiter, TestBulkCopierInit, TestBulkCopierPauseResumeCancel, TestBulkCopierRun, TestBulkCopierWriteBatch, TestBulkCopierOverlapWithLiveMirror, TestBulkCopierCountTenantEvents, TestBulkCopierStreamTenantEvents, TestBulkCopierWaitIfPaused
    from .test_bulk_copier_progress import TestBulkCopierRunProgress

    __all__ = ["TestBulkCopyProgress", "TestBulkCopyResult", "TestEvent", "_pos", "TestRateLimiter", "TestBulkCopierInit", "TestBulkCopierPauseResumeCancel", "TestBulkCopierRun", "TestBulkCopierWriteBatch", "TestBulkCopierOverlapWithLiveMirror", "TestBulkCopierCountTenantEvents", "TestBulkCopierStreamTenantEvents", "TestBulkCopierWaitIfPaused", "TestBulkCopierRunProgress"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
