---
id: REFACTOR-eventsource-application-projections-base
title: Refactor and Decompose Legacy File base.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-projections-base: Refactor Legacy File base.py

## Summary
The grandfathered debt file `src/eventsource/application/projections/base.py` contains 759 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (base_projection.py, base_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/projections/base/` with submodules:
- `base_projection.py`: Projection, SyncProjection, CheckpointTrackingProjection, DeclarativeProjection
- `base_event.py`: EventHandlerBase

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/application/projections/base.py (759 lines):
  Submodule 'base_projection.py' (~647 lines):
    - [class] Projection (lines 64-103)
    - [class] SyncProjection (lines 106-129)
    - [class] CheckpointTrackingProjection (lines 175-523)
    - [class] DeclarativeProjection (lines 526-759)
  Submodule 'base_event.py' (~41 lines):
    - [class] EventHandlerBase (lines 132-172)
  Suggested barrel exports:
    from .base_projection import Projection, SyncProjection, CheckpointTrackingProjection, DeclarativeProjection
    from .base_event import EventHandlerBase

    __all__ = ["Projection", "SyncProjection", "CheckpointTrackingProjection", "DeclarativeProjection", "EventHandlerBase"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
