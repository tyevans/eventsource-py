---
id: REFACTOR-eventsource-application-projections-coordinator
title: Refactor and Decompose Legacy File coordinator.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-projections-coordinator: Refactor Legacy File coordinator.py

## Summary
The grandfathered debt file `src/eventsource/application/projections/coordinator.py` contains 636 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (coordinator_registry.py, coordinator_projection.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/projections/coordinator/` with submodules:
- `coordinator_registry.py`: ProjectionRegistry, SubscriberRegistry
- `coordinator_projection.py`: ProjectionCoordinator

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/projections/coordinator.py (636 lines):
  Submodule 'coordinator_registry.py' (~388 lines):
    - [class] ProjectionRegistry (lines 33-279)
    - [class] SubscriberRegistry (lines 496-636)
  Submodule 'coordinator_projection.py' (~212 lines):
    - [class] ProjectionCoordinator (lines 282-493)
  Suggested barrel exports:
    from .coordinator_registry import ProjectionRegistry, SubscriberRegistry
    from .coordinator_projection import ProjectionCoordinator

    __all__ = ["ProjectionRegistry", "SubscriberRegistry", "ProjectionCoordinator"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
