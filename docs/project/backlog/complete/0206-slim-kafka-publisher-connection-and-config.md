---
id: TASK-0206
title: "Slim kafka/publisher.py, kafka/connection.py, and kafka/config.py below 340 lines"
status: Complete
priority: High
slice_type: domain
target_bc: adapters
governing_prd: PRD-0002
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0206: Slim kafka/publisher.py, kafka/connection.py, and kafka/config.py below 340 lines

## Summary
Compact `src/eventsource/adapters/kafka/publisher.py` (391 lines), `src/eventsource/adapters/kafka/connection.py` (367 lines), and `src/eventsource/adapters/kafka/config.py` (359 lines) so all three modules sit comfortably below 340 lines while preserving 100% of public contracts and behavior.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
