---
id: REFACTOR-examples-subscriptions-resilient_projection
title: Refactor and Decompose Legacy File resilient_projection.py
status: Refined
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-examples-subscriptions-resilient_projection: Refactor Legacy File resilient_projection.py

## Summary
The grandfathered debt file `examples/subscriptions/resilient_projection.py` contains 535 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (resilient_projection_payment.py, resilient_projection_error.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `examples/subscriptions/resilient_projection/` with submodules:
- `resilient_projection_payment.py`: PaymentReceived, PaymentFailed, PaymentRefunded, PaymentState, PaymentAggregate, PaymentAnalyticsProjection, main
- `resilient_projection_error.py`: on_any_error, on_transient_error, on_critical_error

## AST Decomposition Blueprint
Decomposition Blueprint for examples/subscriptions/resilient_projection.py (535 lines):
  Submodule 'resilient_projection_payment.py' (~412 lines):
    - [class] PaymentReceived (lines 64-71)
    - [class] PaymentFailed (lines 75-81)
    - [class] PaymentRefunded (lines 85-92)
    - [class] PaymentState (lines 100-106)
    - [class] PaymentAggregate (lines 109-160)
    - [class] PaymentAnalyticsProjection (lines 168-275)
    - [function] main (lines 310-531)
  Submodule 'resilient_projection_error.py' (~14 lines):
    - [function] on_any_error (lines 284-286)
    - [function] on_transient_error (lines 289-294)
    - [function] on_critical_error (lines 297-301)
  Suggested barrel exports:
    from .resilient_projection_payment import PaymentReceived, PaymentFailed, PaymentRefunded, PaymentState, PaymentAggregate, PaymentAnalyticsProjection, main
    from .resilient_projection_error import on_any_error, on_transient_error, on_critical_error

    __all__ = ["PaymentReceived", "PaymentFailed", "PaymentRefunded", "PaymentState", "PaymentAggregate", "PaymentAnalyticsProjection", "main", "on_any_error", "on_transient_error", "on_critical_error"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
