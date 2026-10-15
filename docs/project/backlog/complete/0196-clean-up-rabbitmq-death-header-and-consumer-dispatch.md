---
id: TASK-0196
title: "Remove Legacy RabbitMQ Death Header Wrapper Docs and Slim Consumer Dispatch"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0102
dependencies: []
---

# TASK-0196: Remove Legacy RabbitMQ Death Header Wrapper Docs and Slim Consumer Dispatch

## Summary
Clean up legacy wrapper references in src/eventsource/adapters/rabbitmq/death_headers.py and slim src/eventsource/adapters/rabbitmq/consumer_dispatch.py below 350 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 400` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
