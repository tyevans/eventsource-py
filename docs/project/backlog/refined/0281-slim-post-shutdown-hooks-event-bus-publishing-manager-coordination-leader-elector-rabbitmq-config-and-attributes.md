---
id: TASK-0281
title: "Slim post_shutdown_hooks, event_bus_publishing, manager_coordination, in_memory_leader_elector, rabbitmq_config, and test_attributes below 285 lines"
status: Refined
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0281: Slim post_shutdown_hooks, event_bus_publishing, manager_coordination, in_memory_leader_elector, rabbitmq_config, and test_attributes below 285 lines

## Summary
Compact `tests/unit/application/subscriptions/test_shutdown/test_post_shutdown_hooks.py`, `tests/unit/test_event_bus/test_event_bus_publishing.py`, `tests/unit/application/subscriptions/test_subscription_manager/test_manager_coordination.py`, `tests/unit/application/subscriptions/test_coordination/test_in_memory_leader_elector.py`, `tests/unit/test_rabbitmq_event_bus/test_rabbitmq_config.py`, and `tests/unit/observability/test_attributes.py` strictly below 285 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
