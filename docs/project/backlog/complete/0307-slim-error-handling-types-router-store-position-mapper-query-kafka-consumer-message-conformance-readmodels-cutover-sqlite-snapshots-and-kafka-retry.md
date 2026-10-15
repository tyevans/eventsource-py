---
id: TASK-0307
title: "Compact 8 src/eventsource modules at 256-257 lines below 235 lines"
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

# TASK-0307: Compact 8 src/eventsource modules at 256-257 lines below 235 lines

## Summary
Compact `src/eventsource/application/subscriptions/error_handling_types.py` (257 lines), `src/eventsource/application/migration/router_store.py` (257 lines), `src/eventsource/application/migration/position_mapper_query.py` (257 lines), `src/eventsource/adapters/kafka/consumer_message.py` (257 lines), `src/eventsource/testing/conformance_ports/readmodels.py` (256 lines), `src/eventsource/application/migration/coordinator_cutover.py` (256 lines), `src/eventsource/adapters/sqlite/snapshots.py` (256 lines), and `src/eventsource/adapters/kafka/consumer_retry.py` (256 lines) strictly below 235 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 260` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
