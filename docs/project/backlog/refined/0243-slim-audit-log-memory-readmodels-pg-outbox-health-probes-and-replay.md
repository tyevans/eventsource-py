---
id: TASK-0243
title: "Slim sql/migration/audit_log.py, memory/readmodels.py, postgresql/outbox.py, health_provider_probes.py, and replay.py below 305 lines"
status: Refined
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

# TASK-0243: Slim sql/migration/audit_log.py, memory/readmodels.py, postgresql/outbox.py, health_provider_probes.py, and replay.py below 305 lines

## Summary
Compact `src/eventsource/adapters/sql/migration/audit_log.py` (323 lines), `src/eventsource/adapters/memory/readmodels.py` (323 lines), `src/eventsource/adapters/postgresql/outbox.py` (322 lines), `src/eventsource/application/subscriptions/health_provider_probes.py` (321 lines), and `src/eventsource/application/projections/replay.py` (321 lines) below 305 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 330` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
