---
id: REFACTOR-eventsource-adapters-sqlite-readmodels
title: Refactor and Decompose Legacy File readmodels.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-sqlite-readmodels: Refactor Legacy File readmodels.py

## Summary
The grandfathered debt file `src/eventsource/adapters/sqlite/readmodels.py` contains 844 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (readmodels_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/sqlite/readmodels/` with submodules:
- `readmodels_core.py`: SQLiteReadModelRepository

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/adapters/sqlite/readmodels.py (844 lines):
  Submodule 'readmodels_core.py' (~799 lines):
    - [class] SQLiteReadModelRepository (lines 46-844)
  Suggested barrel exports:
    from .readmodels_core import SQLiteReadModelRepository

    __all__ = ["SQLiteReadModelRepository"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
