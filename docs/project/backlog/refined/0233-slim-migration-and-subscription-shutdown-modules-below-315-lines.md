---
id: TASK-0233
title: "Slim migration error_classification, status_streamer_core, coordinator_cutover, and subscription shutdown modules below 315 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0233: Slim migration error_classification, status_streamer_core, coordinator_cutover, and subscription shutdown modules below 315 lines

## Summary
Compact `src/eventsource/application/migration/error_classification.py` (334 lines), `src/eventsource/application/migration/status_streamer_core.py` (330 lines), `src/eventsource/application/migration/coordinator_cutover.py` (330 lines), `src/eventsource/application/subscriptions/shutdown.py` (329 lines), and `src/eventsource/application/subscriptions/coordination_shutdown.py` (328 lines) below 315 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
