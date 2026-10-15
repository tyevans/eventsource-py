---
id: TASK-0321
title: "Compact 8 test and benchmark modules at 276-278 lines below 250 lines"
status: Complete
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

# TASK-0321: Compact 8 test and benchmark modules at 276-278 lines below 250 lines

## Summary
Compact `tests/unit/application/migration/test_phase2_integration/test_phase2_anchor.py` (278 lines), `tests/unit/adapters/_bus/test_registry.py` (278 lines), `tests/benchmarks/conftest.py` (278 lines), `tests/unit/test_redis_event_bus/test_redis_event_bus_pubsub.py` (277 lines), `tests/unit/application/subscriptions/test_sync_subscriber_dispatch.py` (277 lines), `tests/unit/application/subscriptions/test_error_handling/test_error_models.py` (277 lines), `tests/unit/application/subscriptions/test_subscriber/test_batch_subscriber.py` (276 lines), and `tests/unit/application/migration/test_dual_write/test_dual_write_failures.py` (276 lines) strictly below 250 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
