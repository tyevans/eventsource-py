---
id: TASK-0246
title: "Slim sqlite/store.py, subscriber.py, metrics_subscription.py, routing_mutation.py, and aggregate_root.py below 300 lines"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0246: Slim sqlite/store.py, subscriber.py, metrics_subscription.py, routing_mutation.py, and aggregate_root.py below 300 lines

## Summary
Compact `src/eventsource/adapters/sqlite/store.py`, `src/eventsource/application/subscriptions/subscriber.py`, `src/eventsource/application/subscriptions/metrics_subscription.py`, `src/eventsource/adapters/sql/migration/routing_mutation.py`, and `src/eventsource/domain/aggregate/aggregate_root.py` strictly below 300 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
