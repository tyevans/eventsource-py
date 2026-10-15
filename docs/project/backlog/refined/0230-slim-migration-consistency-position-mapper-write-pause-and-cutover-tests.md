---
id: TASK-0230
title: "Slim migration consistency verifier, position mapper, write pause, and cutover manager tests below 320 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0230: Slim migration consistency verifier, position mapper, write pause, and cutover manager tests below 320 lines

## Summary
Compact `tests/unit/application/migration/test_consistency_verifier/test_consistency_verifier_versions_and_helpers.py` (363 lines), `tests/unit/application/migration/test_position_mapper/test_position_mapper_translation.py` (360 lines), `tests/unit/application/migration/test_write_pause/test_write_pause_operations.py` (359 lines), and `tests/unit/application/migration/test_cutover_manager/test_cutover_manager_init_and_flow.py` (359 lines) below 320 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
