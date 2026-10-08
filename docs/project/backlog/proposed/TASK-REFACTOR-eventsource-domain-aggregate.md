---
id: REFACTOR-eventsource-domain-aggregate
title: Refactor and Decompose Legacy File aggregate.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-domain-aggregate: Refactor Legacy File aggregate.py

## Summary
The grandfathered debt file `src/eventsource/domain/aggregate.py` contains 951 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (aggregate_root.py, aggregate_declarative.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/domain/aggregate/` with submodules:
- `aggregate_root.py`: AggregateRoot
- `aggregate_declarative.py`: DeclarativeAggregate

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/domain/aggregate.py (951 lines):
  Submodule 'aggregate_root.py' (~636 lines):
    - [class] AggregateRoot (lines 44-679)
  Submodule 'aggregate_declarative.py' (~262 lines):
    - [class] DeclarativeAggregate (lines 682-943)
  Suggested barrel exports:
    from .aggregate_root import AggregateRoot
    from .aggregate_declarative import DeclarativeAggregate

    __all__ = ["AggregateRoot", "DeclarativeAggregate"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
