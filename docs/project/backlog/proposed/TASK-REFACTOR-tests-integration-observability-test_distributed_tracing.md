---
id: REFACTOR-tests-integration-observability-test_distributed_tracing
title: Refactor and Decompose Legacy File test_distributed_tracing.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-observability-test_distributed_tracing: Refactor Legacy File test_distributed_tracing.py

## Summary
The grandfathered debt file `tests/integration/observability/test_distributed_tracing.py` contains 634 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_distributed_tracing_mq.py, test_distributed_tracing_kafka.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/observability/test_distributed_tracing/` with submodules:
- `test_distributed_tracing_mq.py`: TestRabbitMQDistributedTracing, TestCrossProcessTraceContext, TestRedisDistributedTracing
- `test_distributed_tracing_kafka.py`: TestKafkaDistributedTracing

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/integration/observability/test_distributed_tracing.py (634 lines):
  Submodule 'test_distributed_tracing_mq.py' (~341 lines):
    - [class] TestRabbitMQDistributedTracing (lines 74-271)
    - [class] TestCrossProcessTraceContext (lines 483-545)
    - [class] TestRedisDistributedTracing (lines 555-634)
  Submodule 'test_distributed_tracing_kafka.py' (~193 lines):
    - [class] TestKafkaDistributedTracing (lines 282-474)
  Suggested barrel exports:
    from .test_distributed_tracing_mq import TestRabbitMQDistributedTracing, TestCrossProcessTraceContext, TestRedisDistributedTracing
    from .test_distributed_tracing_kafka import TestKafkaDistributedTracing

    __all__ = ["TestRabbitMQDistributedTracing", "TestCrossProcessTraceContext", "TestRedisDistributedTracing", "TestKafkaDistributedTracing"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
