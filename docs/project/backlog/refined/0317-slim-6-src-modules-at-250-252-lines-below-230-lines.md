---
id: TASK-0317
title: "Compact 6 src modules at 250-252 lines below 230 lines"
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

# TASK-0317: Compact 6 src modules at 250-252 lines below 230 lines

## Summary
Compact `src/eventsource/adapters/rabbitmq/connection.py` (252 lines), `src/eventsource/adapters/kafka/publisher.py` (252 lines), `src/eventsource/__init__.py` (252 lines), `src/eventsource/testing/conformance_ports/feed.py` (250 lines), `src/eventsource/ports/migration/models/audit.py` (250 lines), and `src/eventsource/application/migration/position_mapper_recording.py` (250 lines) strictly below 230 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
