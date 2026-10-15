---
id: TASK-0318
title: "Compact 8 src modules at 243-248 lines below 225 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0318: Compact 8 src modules at 243-248 lines below 225 lines

## Summary
Compact `src/eventsource/application/aggregates/snapshotting.py` (248 lines), `src/eventsource/adapters/memory/dlq.py` (248 lines), `src/eventsource/testing/scenario.py` (247 lines), `src/eventsource/application/subscriptions/retry_types.py` (247 lines), `src/eventsource/adapters/sql/migration/migration_progress.py` (247 lines), `src/eventsource/application/migration/coordinator_post_cutover.py` (246 lines), `src/eventsource/application/subscriptions/error_handling_handler.py` (245 lines), and `src/eventsource/application/subscriptions/health_provider_probes.py` (244 lines) strictly below 225 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
