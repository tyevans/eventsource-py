---
id: REFACTOR-eventsource-adapters-redis-bus
title: Refactor and Decompose Legacy File bus.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-redis-bus: Refactor Legacy File bus.py

## Summary
The grandfathered debt file `src/eventsource/adapters/redis/bus.py` contains 1389 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (bus_event.py, bus_available.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/redis/bus/` with submodules:
- `bus_event.py`: RedisEventBusConfig, RedisEventBusStats, RedisEventBus
- `bus_available.py`: RedisNotAvailableError

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/redis/bus.py (1389 lines):
  Submodule 'bus_event.py' (~1275 lines):
    - [class] RedisEventBusConfig (lines 101-165)
    - [class] RedisEventBusStats (lines 169-190)
    - [class] RedisEventBus (lines 193-1380)
  Submodule 'bus_available.py' (~7 lines):
    - [class] RedisNotAvailableError (lines 91-97)
  Suggested barrel exports:
    from .bus_event import RedisEventBusConfig, RedisEventBusStats, RedisEventBus
    from .bus_available import RedisNotAvailableError

    __all__ = ["RedisEventBusConfig", "RedisEventBusStats", "RedisEventBus", "RedisNotAvailableError"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
