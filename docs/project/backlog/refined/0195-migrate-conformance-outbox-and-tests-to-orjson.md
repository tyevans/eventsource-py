---
id: TASK-0195
title: "Migrate Outbox Conformance Suite and Test Helpers to orjson"
status: Refined
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

# TASK-0195: Migrate Outbox Conformance Suite and Test Helpers to orjson

## Summary
Replace the final remaining stdlib json import in src/eventsource/testing/conformance_ports/outbox.py and test JSON helpers with orjson (json_dumps / json_loads).

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
