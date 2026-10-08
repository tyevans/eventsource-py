---
id: REFACTOR-eventsource-adapters-postgresql-store
title: Refactor and Decompose Legacy File store.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-postgresql-store: Refactor Legacy File store.py

## Summary
The grandfathered debt file `src/eventsource/adapters/postgresql/store.py` contains 639 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (store_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/postgresql/store/` with submodules:
- `store_core.py`: PostgreSQLEventStore

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/adapters/postgresql/store.py (639 lines):
  Submodule 'store_core.py' (~520 lines):
    - [class] PostgreSQLEventStore (lines 117-636)
  Suggested barrel exports:
    from .store_core import PostgreSQLEventStore

    __all__ = ["PostgreSQLEventStore"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
