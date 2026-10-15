---
id: TASK-0286
title: "Compact live_delivery, scenario, pg snapshots, kafka metrics, and shutdown_runner below 250 lines"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0004
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0286: Compact live_delivery, scenario, pg snapshots, kafka metrics, and shutdown_runner below 250 lines

## Summary
Compact `src/eventsource/application/subscriptions/runners/live_delivery.py` (279 lines), `src/eventsource/testing/scenario.py` (278 lines), `src/eventsource/adapters/postgresql/snapshots.py` (278 lines), `src/eventsource/adapters/kafka/metrics.py` (276 lines), and `src/eventsource/application/subscriptions/shutdown_runner.py` (274 lines) strictly below 250 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 275` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
