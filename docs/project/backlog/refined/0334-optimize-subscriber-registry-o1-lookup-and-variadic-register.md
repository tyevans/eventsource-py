---
id: TASK-0334
title: "Optimize SubscriberRegistry O(1) lookup by event type and variadic register()"
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

# TASK-0334: Optimize SubscriberRegistry O(1) lookup by event type and variadic register()

## Summary
Maintain an O(1) event-type index in `SubscriberRegistry` (`src/eventsource/application/projections/coordinator_subscriber_registry.py`), accept `subscribers=...` in `__init__`, and allow `register(*subscribers)` to chain fluently.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
