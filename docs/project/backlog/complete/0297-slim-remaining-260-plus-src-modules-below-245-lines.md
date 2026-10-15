---
id: TASK-0297
title: "Compact remaining 260+ line src/eventsource modules below 245 lines"
status: Complete
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0006
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0297: Compact remaining 260+ line src/eventsource modules below 245 lines

## Summary
Compact `src/eventsource/application/subscriptions/runners/catchup_runner.py` (263 lines), `src/eventsource/application/migration/cutover_execution.py` (263 lines), `src/eventsource/application/aggregates/repository.py` (263 lines), `src/eventsource/adapters/sql/checkpoints.py` (263 lines), `src/eventsource/application/projections/base_checkpoint.py` (262 lines), `src/eventsource/adapters/rabbitmq/config.py` (262 lines), `src/eventsource/ports/migration/models/phases.py` (261 lines), and `src/eventsource/application/subscriptions/coordination_shutdown.py` (261 lines) strictly below 245 lines so 100% of `src/eventsource/` is strictly `< 260` lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
