---
id: REFACTOR-eventsource-adapters-memory-readmodels
title: Refactor and Decompose Legacy File readmodels.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-memory-readmodels: Refactor Legacy File readmodels.py

## Summary
The grandfathered debt file `src/eventsource/adapters/memory/readmodels.py` contains 547 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (readmodels_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/memory/readmodels/` with submodules:
- `readmodels_core.py`: InMemoryReadModelRepository

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/memory/readmodels.py (547 lines):
  Submodule 'readmodels_core.py' (~518 lines):
    - [class] InMemoryReadModelRepository (lines 30-547)
  Suggested barrel exports:
    from .readmodels_core import InMemoryReadModelRepository

    __all__ = ["InMemoryReadModelRepository"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
