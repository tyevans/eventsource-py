---
id: TASK-0227
title: "Slim projection tracing order, coordinator lifecycle, and testing module structure tests below 320 lines"
status: Refined
priority: High
slice_type: domain
target_bc: observability
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0227: Slim projection tracing order, coordinator lifecycle, and testing module structure tests below 320 lines

## Summary
Compact `tests/unit/observability/test_projection_tracing/test_projection_tracing_order.py` (372 lines), `tests/unit/application/migration/test_coordinator/test_coordinator_lifecycle.py` (371 lines), and `tests/unit/testing/test_module_structure.py` (368 lines) below 320 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
