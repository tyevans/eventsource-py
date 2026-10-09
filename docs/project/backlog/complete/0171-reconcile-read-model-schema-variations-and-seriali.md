---
id: "0171"
title: "Reconcile Read Model Schema Variations and Serialize Polymorphic Events"
status: Complete
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0104
- ADR-0109
governing_prds:
- PRD-0001
governing_stories:
- US-0017
target_bc: serialization
---

# TASK-0171: Reconcile Read Model Schema Variations and Serialize Polymorphic Events

## Summary
Baseline brownfield implementation and verification for governing story US-0017 under PRD-0001.

## Implementation Details
- Target Bounded Context: `serialization`
- Governing specification: `docs/project/user_stories/accepted/us-0017-*.md`
- Tested and verified through blackbox frontdoors in test suites (`tests/`).

## Definition of Done
1. Core capabilities implemented in `src/eventsource/` matching governing ADRs.
2. Verified with frontdoor unit, property (Hypothesis), and acceptance tests.
3. Zero mock backdoors; 100% test pass rate.
