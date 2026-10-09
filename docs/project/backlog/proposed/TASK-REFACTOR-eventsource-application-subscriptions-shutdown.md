---
id: REFACTOR-eventsource-application-subscriptions-shutdown
title: Refactor and Decompose Legacy File shutdown.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-shutdown: Refactor Legacy File shutdown.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/shutdown.py` contains 1537 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (shutdown_record.py, shutdown_metrics.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/shutdown/` with submodules:
- `shutdown_record.py`: record_shutdown_initiated, record_shutdown_completed, record_drain_duration, record_events_drained, record_in_flight_at_shutdown, _get_meter, get_in_flight_at_shutdown, ShutdownPhase, ShutdownReason, ShutdownResult, ShutdownCoordinator
- `shutdown_metrics.py`: _init_shutdown_metrics, reset_shutdown_metrics, ShutdownMetricsSnapshot

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/shutdown.py (1537 lines):
  Submodule 'shutdown_record.py' (~1326 lines):
    - [function] record_shutdown_initiated (lines 126-134)
    - [function] record_shutdown_completed (lines 137-153)
    - [function] record_drain_duration (lines 156-167)
    - [function] record_events_drained (lines 170-181)
    - [function] record_in_flight_at_shutdown (lines 184-196)
    - [function] _get_meter (lines 57-72)
    - [function] get_in_flight_at_shutdown (lines 199-206)
    - [class] ShutdownPhase (lines 270-299)
    - [class] ShutdownReason (lines 302-326)
    - [class] ShutdownResult (lines 330-379)
    - [class] ShutdownCoordinator (lines 383-1516)
  Submodule 'shutdown_metrics.py' (~105 lines):
    - [function] _init_shutdown_metrics (lines 75-123)
    - [function] reset_shutdown_metrics (lines 209-226)
    - [class] ShutdownMetricsSnapshot (lines 230-267)
  Suggested barrel exports:
    from .shutdown_record import record_shutdown_initiated, record_shutdown_completed, record_drain_duration, record_events_drained, record_in_flight_at_shutdown, _get_meter, get_in_flight_at_shutdown, ShutdownPhase, ShutdownReason, ShutdownResult, ShutdownCoordinator
    from .shutdown_metrics import _init_shutdown_metrics, reset_shutdown_metrics, ShutdownMetricsSnapshot

    __all__ = ["record_shutdown_initiated", "record_shutdown_completed", "record_drain_duration", "record_events_drained", "record_in_flight_at_shutdown", "_get_meter", "get_in_flight_at_shutdown", "ShutdownPhase", "ShutdownReason", "ShutdownResult", "ShutdownCoordinator", "_init_shutdown_metrics", "reset_shutdown_metrics", "ShutdownMetricsSnapshot"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
