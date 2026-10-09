---
id: REFACTOR-tests-unit-test_redis_event_bus_tracing
title: Refactor and Decompose Legacy File test_redis_event_bus_tracing.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-test_redis_event_bus_tracing: Refactor Legacy File test_redis_event_bus_tracing.py

## Summary
The grandfathered debt file `tests/unit/test_redis_event_bus_tracing.py` contains 701 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_redis_event_bus_tracing_span.py, test_redis_event_bus_tracing_handler.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/test_redis_event_bus_tracing/` with submodules:
- `test_redis_event_bus_tracing_span.py`: TestRedisEventBusPublishSpanCreation, TestRedisEventBusProcessMessageSpanCreation, TestRedisEventBusDispatchSpanCreation, TestRedisEventBusHandlerSpanCreation, TestRedisEventBusSpanNaming, RedisTracingTestEvent, event_registry, config, mock_redis, mock_tracer, bus, TestRedisEventBusTracerComposition, TestRedisEventBusTracingDisabled, TestRedisEventBusStandardAttributes
- `test_redis_event_bus_tracing_handler.py`: TracingTestHandler, FailingTracingHandler

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/test_redis_event_bus_tracing.py (701 lines):
  Submodule 'test_redis_event_bus_tracing_span.py' (~562 lines):
    - [class] TestRedisEventBusPublishSpanCreation (lines 230-291)
    - [class] TestRedisEventBusProcessMessageSpanCreation (lines 299-366)
    - [class] TestRedisEventBusDispatchSpanCreation (lines 374-425)
    - [class] TestRedisEventBusHandlerSpanCreation (lines 433-519)
    - [class] TestRedisEventBusSpanNaming (lines 672-701)
    - [class] RedisTracingTestEvent (lines 42-46)
    - [function] event_registry (lines 77-81)
    - [function] config (lines 85-100)
    - [function] mock_redis (lines 104-132)
    - [function] mock_tracer (lines 136-139)
    - [function] bus (lines 143-160)
    - [class] TestRedisEventBusTracerComposition (lines 168-222)
    - [class] TestRedisEventBusTracingDisabled (lines 527-593)
    - [class] TestRedisEventBusStandardAttributes (lines 601-664)
  Submodule 'test_redis_event_bus_tracing_handler.py' (~13 lines):
    - [class] TracingTestHandler (lines 54-61)
    - [class] FailingTracingHandler (lines 64-68)
  Suggested barrel exports:
    from .test_redis_event_bus_tracing_span import TestRedisEventBusPublishSpanCreation, TestRedisEventBusProcessMessageSpanCreation, TestRedisEventBusDispatchSpanCreation, TestRedisEventBusHandlerSpanCreation, TestRedisEventBusSpanNaming, RedisTracingTestEvent, event_registry, config, mock_redis, mock_tracer, bus, TestRedisEventBusTracerComposition, TestRedisEventBusTracingDisabled, TestRedisEventBusStandardAttributes
    from .test_redis_event_bus_tracing_handler import TracingTestHandler, FailingTracingHandler

    __all__ = ["TestRedisEventBusPublishSpanCreation", "TestRedisEventBusProcessMessageSpanCreation", "TestRedisEventBusDispatchSpanCreation", "TestRedisEventBusHandlerSpanCreation", "TestRedisEventBusSpanNaming", "RedisTracingTestEvent", "event_registry", "config", "mock_redis", "mock_tracer", "bus", "TestRedisEventBusTracerComposition", "TestRedisEventBusTracingDisabled", "TestRedisEventBusStandardAttributes", "TracingTestHandler", "FailingTracingHandler"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
