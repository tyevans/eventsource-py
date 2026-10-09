---
id: "0169"
title: "Propagate Distributed Tracing Spans Across Message Buses and Aggregates"
status: Complete
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0112
governing_prds:
- PRD-0004
governing_stories:
- US-0015
target_bc: observability
---

# TASK-0169: Propagate Distributed Tracing Spans Across Message Buses and Aggregates

## Summary
Baseline brownfield implementation and verification for governing story US-0015 under PRD-0004.

## Implementation Details
- Target Bounded Context: `observability`
- Governing specification: `docs/project/user_stories/accepted/us-0015-*.md`
- Tested and verified through blackbox frontdoors in test suites (`tests/`).

## Definition of Done
1. Core capabilities implemented in `src/eventsource/` matching governing ADRs.
2. Verified with frontdoor unit, property (Hypothesis), and acceptance tests.
3. Zero mock backdoors; 100% test pass rate.
