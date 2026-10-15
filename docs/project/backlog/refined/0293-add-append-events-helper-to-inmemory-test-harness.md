---
id: TASK-0293
title: "Add append_events(*events, expected_version=None) helper to InMemoryTestHarness"
status: Refined
priority: High
slice_type: domain
target_bc: testing
governing_prd: PRD-0001
governing_story: US-0015
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0293: Add append_events(*events, expected_version=None) helper to InMemoryTestHarness

## Summary
Add `await harness.append_events(*events, expected_version=None)` to `InMemoryTestHarness` in `src/eventsource/testing/harness.py` (and delegate `given_events` in `testing/bdd.py` to it) while compacting `harness.py` below 230 lines.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 275` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
