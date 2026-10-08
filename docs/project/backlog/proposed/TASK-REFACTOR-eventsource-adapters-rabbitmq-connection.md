---
id: REFACTOR-eventsource-adapters-rabbitmq-connection
title: Refactor and Decompose Legacy File connection.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-adapters-rabbitmq-connection: Refactor Legacy File connection.py

## Summary
The grandfathered debt file `src/eventsource/adapters/rabbitmq/connection.py` contains 537 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (connection_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/adapters/rabbitmq/connection/` with submodules:
- `connection_core.py`: RabbitMQConnectionManager

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/adapters/rabbitmq/connection.py (537 lines):
  Submodule 'connection_core.py' (~499 lines):
    - [class] RabbitMQConnectionManager (lines 39-537)
  Suggested barrel exports:
    from .connection_core import RabbitMQConnectionManager

    __all__ = ["RabbitMQConnectionManager"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
