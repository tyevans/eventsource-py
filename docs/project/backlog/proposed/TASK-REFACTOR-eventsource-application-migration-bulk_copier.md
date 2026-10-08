---
id: REFACTOR-eventsource-application-migration-bulk_copier
title: Refactor and Decompose Legacy File bulk_copier.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-bulk_copier: Refactor Legacy File bulk_copier.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/bulk_copier.py` contains 727 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (bulk_copier_copy.py, bulk_copier_rate.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/bulk_copier/` with submodules:
- `bulk_copier_copy.py`: BulkCopyProgress, BulkCopyResult, BulkCopier
- `bulk_copier_rate.py`: RateLimiter

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/migration/bulk_copier.py (727 lines):
  Submodule 'bulk_copier_copy.py' (~598 lines):
    - [class] BulkCopyProgress (lines 66-103)
    - [class] BulkCopyResult (lines 107-125)
    - [class] BulkCopier (lines 187-727)
  Submodule 'bulk_copier_rate.py' (~57 lines):
    - [class] RateLimiter (lines 128-184)
  Suggested barrel exports:
    from .bulk_copier_copy import BulkCopyProgress, BulkCopyResult, BulkCopier
    from .bulk_copier_rate import RateLimiter

    __all__ = ["BulkCopyProgress", "BulkCopyResult", "BulkCopier", "RateLimiter"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
