---
id: REFACTOR-eventsource-observability-tracer
title: Refactor and Decompose Legacy File tracer.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-observability-tracer: Refactor Legacy File tracer.py

## Summary
The grandfathered debt file `src/eventsource/observability/tracer.py` contains 507 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (tracer_enum.py, tracer_null.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/observability/tracer/` with submodules:
- `tracer_enum.py`: SpanKindEnum, Tracer, OpenTelemetryTracer, MockTracer, create_tracer
- `tracer_null.py`: NullTracer

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/observability/tracer.py (507 lines):
  Submodule 'tracer_enum.py' (~390 lines):
    - [class] SpanKindEnum (lines 47-66)
    - [class] Tracer (lines 70-204)
    - [class] OpenTelemetryTracer (lines 259-395)
    - [class] MockTracer (lines 398-463)
    - [function] create_tracer (lines 466-497)
  Submodule 'tracer_null.py' (~50 lines):
    - [class] NullTracer (lines 207-256)
  Suggested barrel exports:
    from .tracer_enum import SpanKindEnum, Tracer, OpenTelemetryTracer, MockTracer, create_tracer
    from .tracer_null import NullTracer

    __all__ = ["SpanKindEnum", "Tracer", "OpenTelemetryTracer", "MockTracer", "create_tracer", "NullTracer"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
