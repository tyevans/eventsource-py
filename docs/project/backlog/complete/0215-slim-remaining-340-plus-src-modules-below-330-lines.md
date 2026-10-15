---
id: TASK-0215
title: "Slim sql/dlq.py, aggregates/repository.py, domain/event.py, and migration/coordinator_copy.py below 330 lines"
status: Complete
priority: High
slice_type: domain
target_bc: domain
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0215: Slim sql/dlq.py, aggregates/repository.py, domain/event.py, and migration/coordinator_copy.py below 330 lines

## Summary
Compact the final 4 modules in `src/eventsource/` over 340 lines (`adapters/sql/dlq.py` 348, `application/aggregates/repository.py` 344, `domain/event.py` 343, `application/migration/coordinator_copy.py` 340) below 330 lines so 100% of `src/eventsource/` is strictly `< 340` lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
