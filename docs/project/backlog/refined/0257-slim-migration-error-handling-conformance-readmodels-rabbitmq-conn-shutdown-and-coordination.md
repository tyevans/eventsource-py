---
id: TASK-0257
title: "Slim migration/error_handling.py, conformance_ports/readmodels.py, rabbitmq/connection.py, shutdown.py, and coordination_work.py below 295 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0257: Slim migration/error_handling.py, conformance_ports/readmodels.py, rabbitmq/connection.py, shutdown.py, and coordination_work.py below 295 lines

## Summary
Compact `src/eventsource/application/migration/error_handling.py`, `src/eventsource/testing/conformance_ports/readmodels.py`, `src/eventsource/adapters/rabbitmq/connection.py`, `src/eventsource/application/subscriptions/shutdown.py`, and `src/eventsource/application/subscriptions/coordination_work.py` strictly below 295 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 300` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
