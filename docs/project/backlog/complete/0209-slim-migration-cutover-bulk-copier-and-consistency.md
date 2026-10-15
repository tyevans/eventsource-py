---
id: TASK-0209
title: "Slim migration cutover_execution, bulk_copier, and consistency modules below 340 lines"
status: Complete
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

# TASK-0209: Slim migration cutover_execution, bulk_copier, and consistency modules below 340 lines

## Summary
Compact `src/eventsource/application/migration/cutover_execution.py` (376 lines), `src/eventsource/application/migration/bulk_copier.py` (369 lines), `src/eventsource/application/migration/consistency_stream_checker.py` (356 lines), and `src/eventsource/application/migration/consistency.py` (354 lines) below 340 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
