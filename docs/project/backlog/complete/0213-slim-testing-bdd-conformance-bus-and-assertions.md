---
id: TASK-0213
title: "Slim testing/bdd.py, testing/conformance_ports/bus.py, and testing/assertions.py below 340 lines"
status: Complete
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0213: Slim testing/bdd.py, testing/conformance_ports/bus.py, and testing/assertions.py below 340 lines

## Summary
Compact `src/eventsource/testing/bdd.py` (373 lines), `src/eventsource/testing/conformance_ports/bus.py` (357 lines), and `src/eventsource/testing/assertions.py` (355 lines) below 340 lines while preserving all BDD step definitions, bus conformance tests, and fluent assertions.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
