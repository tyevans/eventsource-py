---
id: REFACTOR-eventsource-application-subscriptions-coordination
title: Refactor and Decompose Legacy File coordination.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-coordination: Refactor Legacy File coordination.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/coordination.py` contains 990 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (coordination_shutdown.py, coordination_work.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/coordination/` with submodules:
- `coordination_shutdown.py`: ShutdownIntent, ShutdownNotification, HeartbeatMessage, PeerInfo
- `coordination_work.py`: WorkAssignment, WorkRedistributionCoordinator

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/application/subscriptions/coordination.py (990 lines):
  Submodule 'coordination_shutdown.py' (~247 lines):
    - [class] ShutdownIntent (lines 97-118)
    - [class] ShutdownNotification (lines 127-227)
    - [class] HeartbeatMessage (lines 236-320)
    - [class] PeerInfo (lines 403-441)
  Submodule 'coordination_work.py' (~560 lines):
    - [class] WorkAssignment (lines 329-370)
    - [class] WorkRedistributionCoordinator (lines 445-962)
  Suggested barrel exports:
    from .coordination_shutdown import ShutdownIntent, ShutdownNotification, HeartbeatMessage, PeerInfo
    from .coordination_work import WorkAssignment, WorkRedistributionCoordinator

    __all__ = ["ShutdownIntent", "ShutdownNotification", "HeartbeatMessage", "PeerInfo", "WorkAssignment", "WorkRedistributionCoordinator"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
