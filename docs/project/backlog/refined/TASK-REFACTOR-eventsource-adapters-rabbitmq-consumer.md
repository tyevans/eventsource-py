---
id: REFACTOR-eventsource-adapters-rabbitmq-consumer
title: Refactor and Decompose Legacy File consumer.py
status: Refined
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-rabbitmq-consumer: Refactor Legacy File consumer.py

## Summary
The grandfathered debt file `src/eventsource/adapters/rabbitmq/consumer.py` contains 883 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (consumer_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/rabbitmq/consumer/` with submodules:
- `consumer_core.py`: RabbitMQConsumer

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/rabbitmq/consumer.py (883 lines):
  Submodule 'consumer_core.py' (~808 lines):
    - [class] RabbitMQConsumer (lines 76-883)
  Suggested barrel exports:
    from .consumer_core import RabbitMQConsumer

    __all__ = ["RabbitMQConsumer"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
