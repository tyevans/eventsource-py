---
id: REFACTOR-eventsource-application-migration-write_pause
title: Refactor and Decompose Legacy File write_pause.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-write_pause: Refactor Legacy File write_pause.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/write_pause.py` contains 535 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (write_pause_paused.py, write_pause_state.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/write_pause/` with submodules:
- `write_pause_paused.py`: WritePausedError, PauseMetrics, WritePauseManager
- `write_pause_state.py`: PauseState

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/application/migration/write_pause.py (535 lines):
  Submodule 'write_pause_paused.py' (~439 lines):
    - [class] WritePausedError (lines 63-98)
    - [class] PauseMetrics (lines 123-166)
    - [class] WritePauseManager (lines 169-527)
  Submodule 'write_pause_state.py' (~18 lines):
    - [class] PauseState (lines 102-119)
  Suggested barrel exports:
    from .write_pause_paused import WritePausedError, PauseMetrics, WritePauseManager
    from .write_pause_state import PauseState

    __all__ = ["WritePausedError", "PauseMetrics", "WritePauseManager", "PauseState"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
