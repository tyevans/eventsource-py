---
id: REFACTOR-tests-integration-observability-test_tracing_integration
title: Refactor and Decompose Legacy File test_tracing_integration.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-observability-test_tracing_integration: Refactor Legacy File test_tracing_integration.py

## Summary
The grandfathered debt file `tests/integration/observability/test_tracing_integration.py` contains 525 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_tracing_integration_aggregate.py, test_tracing_integration_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/observability/test_tracing_integration/` with submodules:
- `test_tracing_integration_aggregate.py`: TracingTestAggregateState, TracingTestAggregate, TestRepositoryTracing, TestSpanHierarchy, TestTracingGracefulDegradation, TestTraceIdConsistency, TestStandardAttributes
- `test_tracing_integration_event.py`: TestEventBusTracing

## AST Decomposition Blueprint
Decomposition Blueprint for tests/integration/observability/test_tracing_integration.py (525 lines):
  Submodule 'test_tracing_integration_aggregate.py' (~336 lines):
    - [class] TracingTestAggregateState (lines 45-50)
    - [class] TracingTestAggregate (lines 53-115)
    - [class] TestRepositoryTracing (lines 231-289)
    - [class] TestSpanHierarchy (lines 297-331)
    - [class] TestTracingGracefulDegradation (lines 339-403)
    - [class] TestTraceIdConsistency (lines 411-454)
    - [class] TestStandardAttributes (lines 462-525)
  Submodule 'test_tracing_integration_event.py' (~101 lines):
    - [class] TestEventBusTracing (lines 123-223)
  Suggested barrel exports:
    from .test_tracing_integration_aggregate import TracingTestAggregateState, TracingTestAggregate, TestRepositoryTracing, TestSpanHierarchy, TestTracingGracefulDegradation, TestTraceIdConsistency, TestStandardAttributes
    from .test_tracing_integration_event import TestEventBusTracing

    __all__ = ["TracingTestAggregateState", "TracingTestAggregate", "TestRepositoryTracing", "TestSpanHierarchy", "TestTracingGracefulDegradation", "TestTraceIdConsistency", "TestStandardAttributes", "TestEventBusTracing"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
