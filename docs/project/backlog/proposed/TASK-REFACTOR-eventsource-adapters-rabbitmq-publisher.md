---
id: REFACTOR-eventsource-adapters-rabbitmq-publisher
title: Refactor and Decompose Legacy File publisher.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-rabbitmq-publisher: Refactor Legacy File publisher.py

## Summary
The grandfathered debt file `src/eventsource/adapters/rabbitmq/publisher.py` contains 584 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (publisher_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/rabbitmq/publisher/` with submodules:
- `publisher_core.py`: RabbitMQPublisher

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/adapters/rabbitmq/publisher.py (584 lines):
  Submodule 'publisher_core.py' (~524 lines):
    - [class] RabbitMQPublisher (lines 58-581)
  Suggested barrel exports:
    from .publisher_core import RabbitMQPublisher

    __all__ = ["RabbitMQPublisher"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
