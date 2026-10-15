---
id: TASK-0277
title: "Slim consumer_dispatch, postgresql/readmodels, kafka/publisher, consumer_message, coordinator_copy, and sqlite/readmodels_mutation below 265 lines"
status: Refined
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

# TASK-0277: Slim consumer_dispatch, postgresql/readmodels, kafka/publisher, consumer_message, coordinator_copy, and sqlite/readmodels_mutation below 265 lines

## Summary
Compact `src/eventsource/adapters/rabbitmq/consumer_dispatch.py`, `src/eventsource/adapters/postgresql/readmodels.py`, `src/eventsource/adapters/kafka/publisher.py`, `src/eventsource/adapters/kafka/consumer_message.py`, `src/eventsource/application/migration/coordinator_copy.py`, and `src/eventsource/adapters/sqlite/readmodels_mutation.py` strictly below 265 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 285` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
