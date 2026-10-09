---
id: REFACTOR-tests-integration-bus-test_redis
title: Refactor and Decompose Legacy File test_redis.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-integration-bus-test_redis: Refactor Legacy File test_redis.py

## Summary
The grandfathered debt file `tests/integration/bus/test_redis.py` contains 701 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_redis_connection.py, test_redis_publishing.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/bus/test_redis/` with submodules:
- `test_redis_connection.py`: TestRedisEventBusConnection, TestRedisEventBusSubscription, TestRedisEventBusConsumerGroups, TestRedisEventBusDLQ, TestRedisEventBusEdgeCases, TestRedisEventBusPerformance, TestRedisEventBusConformance
- `test_redis_publishing.py`: TestRedisEventBusPublishing

## AST Decomposition Blueprint
Decomposition Blueprint for tests/integration/bus/test_redis.py (701 lines):
  Submodule 'test_redis_connection.py' (~533 lines):
    - [class] TestRedisEventBusConnection (lines 58-103)
    - [class] TestRedisEventBusSubscription (lines 205-367)
    - [class] TestRedisEventBusConsumerGroups (lines 370-415)
    - [class] TestRedisEventBusDLQ (lines 418-487)
    - [class] TestRedisEventBusEdgeCases (lines 490-569)
    - [class] TestRedisEventBusPerformance (lines 572-601)
    - [class] TestRedisEventBusConformance (lines 604-701)
  Submodule 'test_redis_publishing.py' (~97 lines):
    - [class] TestRedisEventBusPublishing (lines 106-202)
  Suggested barrel exports:
    from .test_redis_connection import TestRedisEventBusConnection, TestRedisEventBusSubscription, TestRedisEventBusConsumerGroups, TestRedisEventBusDLQ, TestRedisEventBusEdgeCases, TestRedisEventBusPerformance, TestRedisEventBusConformance
    from .test_redis_publishing import TestRedisEventBusPublishing

    __all__ = ["TestRedisEventBusConnection", "TestRedisEventBusSubscription", "TestRedisEventBusConsumerGroups", "TestRedisEventBusDLQ", "TestRedisEventBusEdgeCases", "TestRedisEventBusPerformance", "TestRedisEventBusConformance", "TestRedisEventBusPublishing"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
