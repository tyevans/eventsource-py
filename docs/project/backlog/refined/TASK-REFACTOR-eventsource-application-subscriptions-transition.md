---
id: REFACTOR-eventsource-application-subscriptions-transition
title: Refactor and Decompose Legacy File transition.py
status: Refined
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-transition: Refactor Legacy File transition.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/transition.py` contains 613 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (transition_phase.py, transition_result.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/transition/` with submodules:
- `transition_phase.py`: TransitionPhase, TransitionCoordinator, StartFromResolver
- `transition_result.py`: TransitionResult

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/transition.py (613 lines):
  Submodule 'transition_phase.py' (~531 lines):
    - [class] TransitionPhase (lines 44-64)
    - [class] TransitionCoordinator (lines 94-528)
    - [class] StartFromResolver (lines 531-605)
  Submodule 'transition_result.py' (~24 lines):
    - [class] TransitionResult (lines 68-91)
  Suggested barrel exports:
    from .transition_phase import TransitionPhase, TransitionCoordinator, StartFromResolver
    from .transition_result import TransitionResult

    __all__ = ["TransitionPhase", "TransitionCoordinator", "StartFromResolver", "TransitionResult"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
