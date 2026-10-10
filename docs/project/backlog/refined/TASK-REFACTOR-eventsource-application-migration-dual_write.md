---
id: REFACTOR-eventsource-application-migration-dual_write
title: Refactor and Decompose Legacy File dual_write.py
status: Refined
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-dual_write: Refactor Legacy File dual_write.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/dual_write.py` contains 747 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (dual_write_failed.py, dual_write_stats.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/dual_write/` with submodules:
- `dual_write_failed.py`: FailedWrite, DualWriteInterceptor
- `dual_write_stats.py`: FailureStats

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/migration/dual_write.py (747 lines):
  Submodule 'dual_write_failed.py' (~622 lines):
    - [class] FailedWrite (lines 83-106)
    - [class] DualWriteInterceptor (lines 143-740)
  Submodule 'dual_write_stats.py' (~31 lines):
    - [class] FailureStats (lines 110-140)
  Suggested barrel exports:
    from .dual_write_failed import FailedWrite, DualWriteInterceptor
    from .dual_write_stats import FailureStats

    __all__ = ["FailedWrite", "DualWriteInterceptor", "FailureStats"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
