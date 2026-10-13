---
id: "0186"
title: Decompose Oversized Modules Exceeding Line Limits in Application Ring
status: Proposed
target_bc: core
governing_adrs:
  - ADR-0002
priority: P3
---

# TASK-0186: Decompose Oversized Modules Exceeding Line Limits in Application Ring

## Summary
Decompose monolithic operational modules in the application ring (including `SubscriptionManager`, `shutdown.py`, and `migration/coordinator.py`) into cohesive submodules strictly complying with ADR-0002 (<500 lines limit).

## Problem Statement & Context
Multiple operational production modules in `src/eventsource/application/` exceed project file length limits (governed by ADR-0002):
- `SubscriptionManager` in `src/eventsource/application/subscriptions/manager.py` (~500+ lines).
- `shutdown.py` in `src/eventsource/application/subscriptions/shutdown.py`.
- `migration/coordinator.py` in `src/eventsource/application/migration/coordinator.py` (2,132 lines).
ADR-0002 enforces a hard 500-line ceiling and triggers proactive warnings at 400 lines.

## Required Actions
1. Refactor `SubscriptionManager` into focused single-responsibility submodules under 400 lines (e.g. registration, lifecycle, state queries).
2. Refactor `shutdown.py` into smaller lifecycle management components.
3. Decompose `migration/coordinator.py` into separate migration phase coordinators and handlers (<400 lines each).
4. Establish clean public barrel export facades (`__init__.py`) ensuring complete backward compatibility.
5. Update `.specops/grandfathered_debt.json` and `specops.toml` upon completion.

## Definition of Done (Blackbox Frontdoor TDD)
1. Every newly created/decomposed module is strictly under 400 lines.
2. 100% of application unit, integration, and BDD test suites pass without regression.
3. Codebase health reports 0 file limit violations.
