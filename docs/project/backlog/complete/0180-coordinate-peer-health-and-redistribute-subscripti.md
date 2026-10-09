---
id: "0180"
title: "Coordinate Peer Health and Redistribute Subscriptions on Instance Eviction"
status: Complete
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0108
- ADR-0111
governing_prds:
- PRD-0003
governing_stories:
- US-0026
target_bc: subscriptions
---

# TASK-0180: Coordinate Peer Health and Redistribute Subscriptions on Instance Eviction

## Summary
Baseline brownfield implementation and verification for governing story US-0026 under PRD-0003.

## Implementation Details
- Target Bounded Context: `subscriptions`
- Governing specification: `docs/project/user_stories/accepted/us-0026-*.md`
- Tested and verified through blackbox frontdoors in test suites (`tests/`).

## Definition of Done
1. Core capabilities implemented in `src/eventsource/` matching governing ADRs.
2. Verified with frontdoor unit, property (Hypothesis), and acceptance tests.
3. Zero mock backdoors; 100% test pass rate.
