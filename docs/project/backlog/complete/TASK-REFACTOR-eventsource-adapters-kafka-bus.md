---
id: REFACTOR-eventsource-adapters-kafka-bus
title: Refactor and Decompose Legacy File bus.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-kafka-bus: Refactor Legacy File bus.py

## Summary
The grandfathered debt file `src/eventsource/adapters/kafka/bus.py` contains 1023 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (bus_meter.py, bus_kafka.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/kafka/bus/` with submodules:
- `bus_meter.py`: _get_meter
- `bus_kafka.py`: KafkaEventBus

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/kafka/bus.py (1023 lines):
  Submodule 'bus_meter.py' (~15 lines):
    - [function] _get_meter (lines 151-165)
  Submodule 'bus_kafka.py' (~843 lines):
    - [class] KafkaEventBus (lines 168-1010)
  Suggested barrel exports:
    from .bus_meter import _get_meter
    from .bus_kafka import KafkaEventBus

    __all__ = ["_get_meter", "KafkaEventBus"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
