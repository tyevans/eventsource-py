---
id: TASK-0296
title: "Compact top 8 src/eventsource modules at 264-269 lines below 245 lines"
status: Refined
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

# TASK-0296: Compact top 8 src/eventsource modules at 264-269 lines below 245 lines

## Summary
Compact `src/eventsource/testing/conformance_ports/dlq.py` (269 lines), `src/eventsource/application/subscriptions/runners/live_runner.py` (269 lines), `src/eventsource/adapters/kafka/bus.py` (267 lines), `src/eventsource/application/projections/base_declarative.py` (266 lines), `src/eventsource/adapters/sql/migration/position_mapping_query.py` (266 lines), `src/eventsource/testing/conformance_ports/snapshots.py` (264 lines), `src/eventsource/application/subscriptions/lifecycle.py` (264 lines), and `src/eventsource/adapters/sql/dlq.py` (264 lines) strictly below 245 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 265` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
