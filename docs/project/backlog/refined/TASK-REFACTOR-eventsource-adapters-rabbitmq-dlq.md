---
id: REFACTOR-eventsource-adapters-rabbitmq-dlq
title: Refactor and Decompose Legacy File dlq.py
status: Refined
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-rabbitmq-dlq: Refactor Legacy File dlq.py

## Summary
The grandfathered debt file `src/eventsource/adapters/rabbitmq/dlq.py` contains 505 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (dlq_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/rabbitmq/dlq/` with submodules:
- `dlq_core.py`: RabbitMQDLQAdmin

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/rabbitmq/dlq.py (505 lines):
  Submodule 'dlq_core.py' (~460 lines):
    - [class] RabbitMQDLQAdmin (lines 46-505)
  Suggested barrel exports:
    from .dlq_core import RabbitMQDLQAdmin

    __all__ = ["RabbitMQDLQAdmin"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
