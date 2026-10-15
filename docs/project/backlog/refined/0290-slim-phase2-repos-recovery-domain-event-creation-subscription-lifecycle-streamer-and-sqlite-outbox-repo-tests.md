---
id: TASK-0290
title: "Compact test modules at 298-299 lines below 275 lines"
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

# TASK-0290: Compact test modules at 298-299 lines below 275 lines

## Summary
Compact `tests/unit/application/migration/test_phase2_integration/_repositories.py` (299 lines), `tests/unit/application/migration/test_final_integration/test_recovery.py` (299 lines), `tests/unit/domain/test_domain_event/test_creation.py` (298 lines), `tests/unit/application/subscriptions/test_subscription_state/test_subscription_lifecycle.py` (298 lines), `tests/unit/application/migration/test_status_streamer/test_status_stream_manager.py` (298 lines), and `tests/unit/adapters/test_sqlite_outbox/test_sqlite_outbox_repository.py` (298 lines) strictly below 275 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 275` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
