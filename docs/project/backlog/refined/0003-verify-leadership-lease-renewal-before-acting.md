---
id: '0003'
title: Verify Leadership Lease Renewal Before Acting
status: Refined
created: 2026-10-07
governing_adrs:
- ADR-0007
- ADR-0109
- ADR-0161
governing_prds:
- PRD-0001
governing_stories:
- US-0004
target_bc: application
---

# TASK-0003: Verify Leadership Lease Renewal Before Acting

## Summary
In distributed subscription coordination, instances hold leadership leases via advisory locks. Ensure leadership status is re-verified immediately prior to executing non-compensable cluster operations to eliminate split-brain windows.

## Definition of Done
1. Verification guard added before critical leadership execution paths.
2. Unit and integration tests verify lease expiration blocks unauthorized transitions.
3. Zero private backdoor mocks in test assertions.
