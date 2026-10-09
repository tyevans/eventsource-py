---
id: "0172"
title: "Register Domain Events and Resolve Types via Thread-Safe Event Registry"
status: Complete
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0007
- ADR-0104
governing_prds:
- PRD-0001
governing_stories:
- US-0018
target_bc: domain
---

# TASK-0172: Register Domain Events and Resolve Types via Thread-Safe Event Registry

## Summary
Baseline brownfield implementation and verification for governing story US-0018 under PRD-0001.

## Implementation Details
- Target Bounded Context: `domain`
- Governing specification: `docs/project/user_stories/accepted/us-0018-*.md`
- Tested and verified through blackbox frontdoors in test suites (`tests/`).

## Definition of Done
1. Core capabilities implemented in `src/eventsource/` matching governing ADRs.
2. Verified with frontdoor unit, property (Hypothesis), and acceptance tests.
3. Zero mock backdoors; 100% test pass rate.
