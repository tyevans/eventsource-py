---
id: "0182"
title: Deterministic or Scheduled Coverage for Bus Performance Assertions
status: Proposed
target_bc: adapters
governing_adrs:
  - ADR-0003
  - ADR-0009
priority: P3
---

# TASK-0182: Deterministic or Scheduled Coverage for Bus Performance Assertions

## Summary
Replace non-deterministic wall-clock execution limits in message bus performance tests with deterministic proxies (such as counting instrumentation invocations), or introduce a dedicated scheduled non-blocking CI trend workflow.

## Problem Statement & Context
- Message bus performance tests currently assert wall-clock execution limits.
- Shared CI runners experience variable CPU throttling and multi-tenant noise, causing non-deterministic timing measurements and false failures.
- These tests are excluded from blocking CI checks (`-m "not benchmark"`).
- Consequently, CI does not monitor metric overhead regressions reliably (e.g. CI measured 55.79% overhead against a 20% limit on valid code paths).

## Required Actions
1. Audit Kafka and RabbitMQ duration and metric overhead tests using `@pytest.mark.benchmark`.
2. Replace wall-clock duration assertions with deterministic proxies (e.g. counting event dispatch loops, allocations, or instrumentation hook invocations).
3. Alternatively or in addition, configure a scheduled non-blocking GitHub Actions workflow that executes `-m benchmark` and records trend data over time.

## Definition of Done (Blackbox Frontdoor TDD)
1. Performance tests in `tests/benchmarks/` and unit suites execute deterministically without flaky failures on shared runners.
2. Verified via automated tests run across multiple iterations.
3. All source files strictly comply with ADR-0002 (<500 lines limit).
