---
id: TASK-0316
title: "Compact 6 src modules at 252-254 lines below 230 lines"
status: Refined
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0316: Compact 6 src modules at 252-254 lines below 230 lines

## Summary
Compact `src/eventsource/adapters/memory/store.py` (254 lines), `src/eventsource/application/subscriptions/flow_control.py` (253 lines), `src/eventsource/application/subscriptions/config.py` (253 lines), `src/eventsource/application/subscriptions/manager_lifecycle.py` (252 lines), `src/eventsource/application/migration/exceptions_operations.py` (252 lines), and `src/eventsource/adapters/rabbitmq/consumer.py` (252 lines) strictly below 230 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
