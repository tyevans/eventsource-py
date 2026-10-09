---
id: "0178"
title: "Monitor Subscription Health via Composite Checks and Kubernetes Probes"
status: Complete
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0108
governing_prds:
- PRD-0003
governing_stories:
- US-0024
target_bc: subscriptions
---

# TASK-0178: Monitor Subscription Health via Composite Checks and Kubernetes Probes

## Summary
Baseline brownfield implementation and verification for governing story US-0024 under PRD-0003.

## Implementation Details
- Target Bounded Context: `subscriptions`
- Governing specification: `docs/project/user_stories/accepted/us-0024-*.md`
- Tested and verified through blackbox frontdoors in test suites (`tests/`).

## Definition of Done
1. Core capabilities implemented in `src/eventsource/` matching governing ADRs.
2. Verified with frontdoor unit, property (Hypothesis), and acceptance tests.
3. Zero mock backdoors; 100% test pass rate.
