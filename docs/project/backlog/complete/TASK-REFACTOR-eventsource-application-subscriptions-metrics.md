---
id: REFACTOR-eventsource-application-subscriptions-metrics
title: Refactor and Decompose Legacy File metrics.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-metrics: Refactor Legacy File metrics.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/metrics.py` contains 571 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (metrics_op.py, metrics_meter.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/metrics/` with submodules:
- `metrics_op.py`: NoOpCounter, NoOpHistogram, NoOpGauge, StateValue, MetricSnapshot, SubscriptionMetrics, _Timer, get_metrics, clear_metrics_registry
- `metrics_meter.py`: _get_meter, reset_meter

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/metrics.py (571 lines):
  Submodule 'metrics_op.py' (~442 lines):
    - [class] NoOpCounter (lines 102-116)
    - [class] NoOpHistogram (lines 119-133)
    - [class] NoOpGauge (lines 136-145)
    - [class] StateValue (lines 49-58)
    - [class] MetricSnapshot (lines 149-181)
    - [class] SubscriptionMetrics (lines 185-478)
    - [class] _Timer (lines 481-516)
    - [function] get_metrics (lines 523-542)
    - [function] clear_metrics_registry (lines 545-553)
  Submodule 'metrics_meter.py' (~22 lines):
    - [function] _get_meter (lines 76-89)
    - [function] reset_meter (lines 92-99)
  Suggested barrel exports:
    from .metrics_op import NoOpCounter, NoOpHistogram, NoOpGauge, StateValue, MetricSnapshot, SubscriptionMetrics, _Timer, get_metrics, clear_metrics_registry
    from .metrics_meter import _get_meter, reset_meter

    __all__ = ["NoOpCounter", "NoOpHistogram", "NoOpGauge", "StateValue", "MetricSnapshot", "SubscriptionMetrics", "_Timer", "get_metrics", "clear_metrics_registry", "_get_meter", "reset_meter"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
