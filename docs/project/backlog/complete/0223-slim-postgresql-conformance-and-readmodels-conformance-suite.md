---
id: TASK-0223
title: "Slim test_postgresql_conformance.py and conformance_ports/readmodels.py below 330 lines"
status: Complete
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
dependencies: []
---

# TASK-0223: Slim test_postgresql_conformance.py and conformance_ports/readmodels.py below 330 lines

## Summary
Compact `tests/integration/adapters/test_postgresql_conformance.py` (380 lines) and `src/eventsource/testing/conformance_ports/readmodels.py` (338 lines) below 330 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
