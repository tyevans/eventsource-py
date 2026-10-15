---
id: TASK-0248
title: "Slim rabbitmq/dlq.py, exceptions_base.py, coordinator_cutover.py, bulk_copier.py, and sqlite/snapshots.py below 300 lines"
status: Complete
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

# TASK-0248: Slim rabbitmq/dlq.py, exceptions_base.py, coordinator_cutover.py, bulk_copier.py, and sqlite/snapshots.py below 300 lines

## Summary
Compact `src/eventsource/adapters/rabbitmq/dlq.py`, `src/eventsource/application/migration/exceptions_base.py`, `src/eventsource/application/migration/coordinator_cutover.py`, `src/eventsource/application/migration/bulk_copier.py`, and `src/eventsource/adapters/sqlite/snapshots.py` strictly below 300 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
