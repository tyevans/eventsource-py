---
id: REFACTOR-eventsource-application-subscriptions-runners-catchup
title: Refactor and Decompose Legacy File catchup.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-runners-catchup: Refactor Legacy File catchup.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/runners/catchup.py` contains 1051 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (catchup_outcome.py, catchup_result.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/runners/catchup/` with submodules:
- `catchup_outcome.py`: _BatchOutcome, CatchUpRunner
- `catchup_result.py`: CatchUpResult

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/runners/catchup.py (1051 lines):
  Submodule 'catchup_outcome.py' (~960 lines):
    - [class] _BatchOutcome (lines 58-68)
    - [class] CatchUpRunner (lines 97-1045)
  Submodule 'catchup_result.py' (~23 lines):
    - [class] CatchUpResult (lines 72-94)
  Suggested barrel exports:
    from .catchup_outcome import _BatchOutcome, CatchUpRunner
    from .catchup_result import CatchUpResult

    __all__ = ["_BatchOutcome", "CatchUpRunner", "CatchUpResult"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
