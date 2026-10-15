---
id: TASK-0205
title: "Remove testing.conformance shim, ReadModelRepositoryProtocol alias, and harness duplicate tracking"
status: Complete
priority: High
slice_type: domain
target_bc: ports
governing_prd: PRD-0001
governing_story: US-0001
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
dependencies: []
---

# TASK-0205: Remove testing.conformance shim, ReadModelRepositoryProtocol alias, and harness duplicate tracking

## Summary
Remove the remaining compatibility re-export shims in `eventsource.testing` and `eventsource.ports.readmodels`:
1. Delete `src/eventsource/testing/conformance.py` (which only re-exported `EventBusConformanceSuite` from `eventsource.testing.conformance_ports.bus` for backward compatibility) and update any references to import from `eventsource.testing.conformance_ports` or `eventsource.testing`.
2. Remove the `ReadModelRepositoryProtocol = ReadModelRepository` backward-compatibility alias in `src/eventsource/ports/readmodels/repository.py` and `src/eventsource/ports/readmodels/__init__.py`.
3. Simplify `src/eventsource/testing/harness.py` to delegate `published_events` and `get_events_of_type` directly to `InMemoryEventBus.published` and `InMemoryEventBus.published_of_type` instead of maintaining a redundant `_published` list and lock.

## Acceptance Criteria
1. All modified `.py` files remain strictly `< 340` lines (`uv run spec-ops health` reports 0 warnings and 0 violations).
2. All public frontdoor unit, property, and integration tests pass (`uv run pytest`).
