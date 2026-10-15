---
id: TASK-0308
title: "Compact 8 src/eventsource modules at 254-255 lines below 235 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0006
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0308: Compact 8 src/eventsource modules at 254-255 lines below 235 lines

## Summary
Compact `src/eventsource/domain/tenant_context.py` (255 lines), `src/eventsource/application/subscriptions/models.py` (255 lines), `src/eventsource/adapters/sqlite/store.py` (255 lines), `src/eventsource/adapters/rabbitmq/topology.py` (255 lines), `src/eventsource/adapters/rabbitmq/consumer_dispatch.py` (255 lines), `src/eventsource/ports/readmodels/model.py` (254 lines), `src/eventsource/application/migration/subscription_migrator_execution.py` (254 lines), and `src/eventsource/application/migration/circuit_breaker.py` (254 lines) strictly below 235 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 260` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
