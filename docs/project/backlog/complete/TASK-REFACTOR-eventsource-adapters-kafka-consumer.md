---
id: REFACTOR-eventsource-adapters-kafka-consumer
title: Refactor and Decompose Legacy File consumer.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-kafka-consumer: Refactor Legacy File consumer.py

## Summary
The grandfathered debt file `src/eventsource/adapters/kafka/consumer.py` contains 1136 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (consumer_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/kafka/consumer/` with submodules:
- `consumer_core.py`: KafkaConsumerLoop

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/kafka/consumer.py (1136 lines):
  Submodule 'consumer_core.py' (~1062 lines):
    - [class] KafkaConsumerLoop (lines 75-1136)
  Suggested barrel exports:
    from .consumer_core import KafkaConsumerLoop

    __all__ = ["KafkaConsumerLoop"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
