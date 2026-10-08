---
id: REFACTOR-eventsource-application-subscriptions-health_provider
title: Refactor and Decompose Legacy File health_provider.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-health_provider: Refactor Legacy File health_provider.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/health_provider.py` contains 590 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (health_provider_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/health_provider/` with submodules:
- `health_provider_core.py`: HealthCheckProvider

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/health_provider.py (590 lines):
  Submodule 'health_provider_core.py' (~541 lines):
    - [class] HealthCheckProvider (lines 47-587)
  Suggested barrel exports:
    from .health_provider_core import HealthCheckProvider

    __all__ = ["HealthCheckProvider"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
