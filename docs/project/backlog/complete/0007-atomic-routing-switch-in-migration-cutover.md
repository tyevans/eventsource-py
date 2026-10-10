---
id: '0007'
title: Atomic Routing Switch in Migration Cutover
status: Complete
governing_adrs:
- ADR-0007
- ADR-0114
- ADR-0128
- ADR-0134
governing_prds:
- PRD-0001
governing_stories:
- US-0006
target_bc: application
---

# TASK-0007: Atomic Routing Switch in Migration Cutover

## Summary
The migration coordinator's final routing switch currently relies on compensational fallback if the cutover step fails. Replace with an atomic transaction boundary to ensure cutover is strictly all-or-nothing.

## Definition of Done
1. Cutover routing switch executed inside an atomic transaction context.
2. Failure simulation tests verify no split-brain state occurs on abort.
3. 100% test pass rate across migration test suite.
