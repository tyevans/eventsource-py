---
id: TASK-0278
title: "Slim conformance readmodels, ports snapshots/readmodels, shutdown, sqlite/store, subscription_migrator_execution, and transition_executor below 265 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0278: Slim conformance readmodels, ports snapshots/readmodels, shutdown, sqlite/store, subscription_migrator_execution, and transition_executor below 265 lines

## Summary
Compact `src/eventsource/testing/conformance_ports/readmodels.py`, `src/eventsource/ports/snapshots.py`, `src/eventsource/ports/readmodels/repository.py`, `src/eventsource/application/subscriptions/shutdown.py`, `src/eventsource/adapters/sqlite/store.py`, `src/eventsource/application/migration/subscription_migrator_execution.py`, and `src/eventsource/application/subscriptions/transition_executor.py` strictly below 265 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
