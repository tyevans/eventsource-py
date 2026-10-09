---
id: REFACTOR-eventsource-application-subscriptions-manager
title: Refactor and Decompose Legacy File manager.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-manager: Refactor Legacy File manager.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/manager.py` contains 1260 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (manager_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/manager/` with submodules:
- `manager_core.py`: SubscriptionManager

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/manager.py (1260 lines):
  Submodule 'manager_core.py' (~1172 lines):
    - [class] SubscriptionManager (lines 84-1255)
  Suggested barrel exports:
    from .manager_core import SubscriptionManager

    __all__ = ["SubscriptionManager"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
