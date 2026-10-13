---
id: "0184"
title: Optimize Subscription EventFilter Fast-Paths, Decompose Filtering Modules, and Verify Lag Reconciliation
status: Refined
target_bc: application
governing_adrs:
  - ADR-0002
  - ADR-0007
  - ADR-0108
governing_prds:
  - PRD-0003
governing_stories:
  - US-0009
  - US-0010
  - US-0023
priority: P1
---

# TASK-0184: Optimize Subscription EventFilter Fast-Paths, Decompose Filtering Modules, and Verify Lag Reconciliation

## Summary
Optimize `EventFilter` matching performance via pre-compiled pattern regexes and exact-match fast paths, decompose `application/subscriptions/filtering.py` (479 lines) and oversized subscription unit test files below 400 lines, and verify lag reconciliation across stop and transition failure paths.

## Problem Statement & Context
- `src/eventsource/application/subscriptions/filtering.py` is 479 lines (triggering a proactive file-length warning) and evaluates wildcard patterns on every event via `fnmatch.fnmatch` without pre-compiled regex fast paths when no wildcards are configured.
- `tests/unit/application/subscriptions/test_health_api.py` (470 lines) and `tests/unit/application/subscriptions/test_tenant_filtering.py` (456 lines) exceed the 400-line warning threshold.
- Subscription stop and transition failure paths must guarantee `Subscription.lag == 0` when buffered wakes are cleared.

## Required Actions
1. Decompose `src/eventsource/application/subscriptions/filtering.py` (479 lines) into focused submodules (<400 lines each) and optimize `EventFilter.matches` / `matches_envelope` with pre-compiled regexes for wildcard patterns and O(1) set lookup fast paths when only exact event types are configured.
2. Decompose `tests/unit/application/subscriptions/test_health_api.py` (470 lines) and `tests/unit/application/subscriptions/test_tenant_filtering.py` (456 lines) into modular packages (<400 lines per file).
3. Add property-based and unit tests verifying `EventFilter` fast-path equivalence and `Subscription.lag == 0` reconciliation after `LiveRunner.stop()` and `TransitionCoordinator` failure/abort.

## Definition of Done (Blackbox Frontdoor TDD)
1. `filtering.py`, `test_health_api`, and `test_tenant_filtering` are all strictly under 400 lines per file.
2. `EventFilter` pre-compiles wildcard patterns at initialization and uses O(1) exact-match fast paths when no wildcards are present.
3. Stopping a subscription or aborting a transition reconciles `Subscription.lag` to 0.
