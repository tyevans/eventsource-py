---
id: TASK-0320
title: "Compact 8 test and example modules at 278-279 lines below 250 lines"
status: Refined
priority: High
slice_type: domain
target_bc: application
governing_prd: PRD-0001
governing_story: US-0002
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0320: Compact 8 test and example modules at 278-279 lines below 250 lines

## Summary
Compact `tests/unit/readmodels/test_base.py` (279 lines), `tests/unit/application/subscriptions/test_subscription_config/test_subscription_exceptions.py` (279 lines), `tests/unit/application/migration/test_phase3_integration/test_phase3_full_migration.py` (279 lines), `tests/unit/application/migration/test_migration_span_attributes.py` (279 lines), `examples/basic_usage.py` (279 lines), `tests/unit/readmodels/test_in_memory/test_in_memory_query.py` (278 lines), `tests/unit/domain/test_tenant_context.py` (278 lines), and `tests/unit/application/subscriptions/test_retry/test_circuit_breaker.py` (278 lines) strictly below 250 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly within architectural line ceilings (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
