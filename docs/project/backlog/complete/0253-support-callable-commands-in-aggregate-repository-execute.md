---
id: TASK-0253
title: "Support callable commands in AggregateRepository.execute and TenantAwareRepository.execute"
status: Complete
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

# TASK-0253: Support callable commands in AggregateRepository.execute and TenantAwareRepository.execute

## Summary
Support `Callable[[TAggregate], Any]` command callables in `AggregateRepository.execute(id, command)` and `TenantAwareRepository.execute(id, command)` so imperative and declarative aggregates can execute commands in a single call alongside DeciderAggregate command objects.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 325` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
