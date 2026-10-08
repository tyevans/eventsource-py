---
id: REFACTOR-eventsource-application-migration-status_streamer
title: Refactor and Decompose Legacy File status_streamer.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-status_streamer: Refactor Legacy File status_streamer.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/status_streamer.py` contains 533 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (status_streamer_manager.py, status_streamer_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/status_streamer/` with submodules:
- `status_streamer_manager.py`: StatusStreamManager
- `status_streamer_core.py`: StatusStreamer

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/application/migration/status_streamer.py (533 lines):
  Submodule 'status_streamer_manager.py' (~161 lines):
    - [class] StatusStreamManager (lines 367-527)
  Submodule 'status_streamer_core.py' (~306 lines):
    - [class] StatusStreamer (lines 59-364)
  Suggested barrel exports:
    from .status_streamer_manager import StatusStreamManager
    from .status_streamer_core import StatusStreamer

    __all__ = ["StatusStreamManager", "StatusStreamer"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
