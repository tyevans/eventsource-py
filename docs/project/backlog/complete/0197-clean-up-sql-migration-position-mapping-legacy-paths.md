---
id: TASK-0197
title: "Clean Up Legacy Int-Only Position Mapping Comments and Slim Query Helpers"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0102
dependencies: []
---

# TASK-0197: Clean Up Legacy Int-Only Position Mapping Comments and Slim Query Helpers

## Summary
Clean up legacy integer-only position mapping comments and slim src/eventsource/adapters/sql/migration/position_mapping_query.py below 350 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
