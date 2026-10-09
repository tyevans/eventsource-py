---
id: "0163"
title: "Rebuild Projections Deterministically with Typed Read Models"
status: Complete
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0109
governing_prds:
- PRD-0001
governing_stories:
- US-0008
target_bc: projections
---

# TASK-0163: Rebuild Projections Deterministically with Typed Read Models

## Summary
Baseline brownfield implementation and verification for governing story US-0008 under PRD-0001.

## Implementation Details
- Target Bounded Context: `projections`
- Governing specification: `docs/project/user_stories/accepted/us-0008-*.md`
- Tested and verified through blackbox frontdoors in test suites (`tests/`).

## Definition of Done
1. Core capabilities implemented in `src/eventsource/` matching governing ADRs.
2. Verified with frontdoor unit, property (Hypothesis), and acceptance tests.
3. Zero mock backdoors; 100% test pass rate.
