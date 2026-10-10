---
id: REFACTOR-eventsource-application-subscriptions-runners-live
title: Refactor and Decompose Legacy File live.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-runners-live: Refactor Legacy File live.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/runners/live.py` contains 1172 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (live_runner.py, live_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/runners/live/` with submodules:
- `live_runner.py`: LiveRunnerStats, LiveRunner
- `live_event.py`: _LiveEventHandler

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/runners/live.py (1172 lines):
  Submodule 'live_runner.py' (~1073 lines):
    - [class] LiveRunnerStats (lines 62-76)
    - [class] LiveRunner (lines 80-1137)
  Submodule 'live_event.py' (~27 lines):
    - [class] _LiveEventHandler (lines 1140-1166)
  Suggested barrel exports:
    from .live_runner import LiveRunnerStats, LiveRunner
    from .live_event import _LiveEventHandler

    __all__ = ["LiveRunnerStats", "LiveRunner", "_LiveEventHandler"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
