---
id: "0170"
title: "Test Aggregate Invariants Fluently via Scenario Testing Harness"
status: Complete
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0006
- ADR-0103
governing_prds:
- PRD-0004
governing_stories:
- US-0016
target_bc: testing
---

# TASK-0170: Test Aggregate Invariants Fluently via Scenario Testing Harness

## Summary
Baseline brownfield implementation and verification for governing story US-0016 under PRD-0004.

## Implementation Details
- Target Bounded Context: `testing`
- Governing specification: `docs/project/user_stories/accepted/us-0016-*.md`
- Tested and verified through blackbox frontdoors in test suites (`tests/`).

## Definition of Done
1. Core capabilities implemented in `src/eventsource/` matching governing ADRs.
2. Verified with frontdoor unit, property (Hypothesis), and acceptance tests.
3. Zero mock backdoors; 100% test pass rate.
