---
id: REFACTOR-eventsource-application-migration-coordinator
title: Refactor and Decompose Legacy File coordinator.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-coordinator: Refactor Legacy File coordinator.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/coordinator.py` contains 2132 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (coordinator_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/coordinator/` with submodules:
- `coordinator_core.py`: MigrationCoordinator

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/migration/coordinator.py (2132 lines):
  Submodule 'coordinator_core.py' (~1996 lines):
    - [class] MigrationCoordinator (lines 131-2126)
  Suggested barrel exports:
    from .coordinator_core import MigrationCoordinator

    __all__ = ["MigrationCoordinator"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
