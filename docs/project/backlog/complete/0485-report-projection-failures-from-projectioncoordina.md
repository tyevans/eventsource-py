---
id: 0485
title: Report projection failures from ProjectionCoordinator.rebuild_all
status: Complete
governing_adrs:
- ADR-0109
governing_prds:
- PRD-0001
governing_stories:
- US-0008
target_bc: application
priority_rank: 25
---

# TASK-0485: Report projection failures from ProjectionCoordinator.rebuild_all

## Summary
`rebuild_all` returns the full event count when projections fail. The caller cannot know that a read model, for example a permission view, is incomplete.

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-754 (improper check for exceptional conditions)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The architecture doc states that the registry drops the exception and points to `replay()`. The projection guide (`docs/guides/projections.md:241`) does not warn. Projections also default to `unregistered_event_handling="ignore"` (documented, Info).

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/application/projections/coordinator_registry.py:108-122`
- `src/eventsource/application/projections/coordinator_projection.py:67-73`
- `src/eventsource/application/projections/base_declarative.py:35;` handlers.py:37

**Verification**: Proven: a projection that always fails returned `3 of 3` from `rebuild_all` with no error.

## Definition of Done (Blackbox Frontdoor TDD)
1. `rebuild_all` raises or returns a result that contains the failure count.
2. The projection guide warns about the failure behavior and the unregistered-event default.
3. A test with a failing projection fails on the current code.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
