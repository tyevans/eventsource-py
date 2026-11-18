---
id: 0498
title: Signal handler failures from ProjectionCoordinator.dispatch_events
status: Proposed
dependencies:
- TASK-0485
governing_adrs:
- ADR-0107
governing_prds:
- PRD-0001
governing_stories:
- US-0001
target_bc: application
---

# TASK-0498: Signal handler failures from ProjectionCoordinator.dispatch_events

## Summary
`ProjectionCoordinator.dispatch_events` does not tell the caller when a projection handler fails. TASK-0485 fixed the same defect for `rebuild_all`.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Found during TASK-0485. `ProjectionRegistry.dispatch_many` now returns the exceptions. `SubscriberRegistry.dispatch_many` and `dispatch_events` do not.

## Evidence
- `src/eventsource/application/projections/coordinator_projection.py:64` (`dispatch_events` returns a count only)
- `src/eventsource/application/projections/coordinator_subscriber_registry.py:124` (`SubscriberRegistry.dispatch_many` returns `None`)

## Definition of Done (Blackbox Frontdoor TDD)
1. `dispatch_events` raises or returns the failures. Document the choice.
2. A test drives a real coordinator with a failing projection and asserts the signal.
3. Tests use public entry points only. All files stay under the line ceilings.
