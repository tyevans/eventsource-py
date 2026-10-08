---
id: '0008'
title: Decompose Monolithic Modules SubscriptionManager and Shutdown
status: Refined
created: 2026-10-07
governing_adrs:
  - ADR-0002
  - ADR-0007
governing_prds:
  - PRD-0001
governing_stories:
  - US-0004
target_bc: application
---

# TASK-0008: Decompose Monolithic Modules SubscriptionManager and Shutdown

## Summary
`src/eventsource/application/subscriptions/manager.py` and `shutdown.py` have grown past 500 lines and are currently grandfathered. Decompose both files into cohesive submodules following AST seams to satisfy ADR-0002.

## Definition of Done
1. Split `manager.py` into lifecycle, dispatch, and registry submodules.
2. Split `shutdown.py` into signal handling and timeout coordinator modules.
3. Barrel exports preserve existing import paths for backwards compatibility.
4. All resulting files strictly under 500 lines.
