---
id: REFACTOR-tests-unit-application-subscriptions-test_metrics
title: Refactor and Decompose Legacy File test_metrics.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_metrics: Refactor Legacy File test_metrics.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_metrics.py` contains 689 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_metrics_no.py, test_metrics_subscription.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_metrics/` with submodules:
- `test_metrics_no.py`: TestNoOpInstruments, TestSubscriptionMetricsNoOTel, TestOTELMetricsAvailable, TestStateValue, TestMetricSnapshot, TestMetricsWithMockedOTel, TestMetricsRegistry, TestTimer, TestImports, TestObservableGaugeCallbacks
- `test_metrics_subscription.py`: TestSubscriptionMetrics

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/subscriptions/test_metrics.py (689 lines):
  Submodule 'test_metrics_no.py' (~477 lines):
    - [class] TestNoOpInstruments (lines 66-93)
    - [class] TestSubscriptionMetricsNoOTel (lines 325-373)
    - [class] TestOTELMetricsAvailable (lines 22-36)
    - [class] TestStateValue (lines 39-63)
    - [class] TestMetricSnapshot (lines 96-152)
    - [class] TestMetricsWithMockedOTel (lines 376-445)
    - [class] TestMetricsRegistry (lines 448-524)
    - [class] TestTimer (lines 530-566)
    - [class] TestImports (lines 569-626)
    - [class] TestObservableGaugeCallbacks (lines 629-689)
  Submodule 'test_metrics_subscription.py' (~168 lines):
    - [class] TestSubscriptionMetrics (lines 155-322)
  Suggested barrel exports:
    from .test_metrics_no import TestNoOpInstruments, TestSubscriptionMetricsNoOTel, TestOTELMetricsAvailable, TestStateValue, TestMetricSnapshot, TestMetricsWithMockedOTel, TestMetricsRegistry, TestTimer, TestImports, TestObservableGaugeCallbacks
    from .test_metrics_subscription import TestSubscriptionMetrics

    __all__ = ["TestNoOpInstruments", "TestSubscriptionMetricsNoOTel", "TestOTELMetricsAvailable", "TestStateValue", "TestMetricSnapshot", "TestMetricsWithMockedOTel", "TestMetricsRegistry", "TestTimer", "TestImports", "TestObservableGaugeCallbacks", "TestSubscriptionMetrics"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
