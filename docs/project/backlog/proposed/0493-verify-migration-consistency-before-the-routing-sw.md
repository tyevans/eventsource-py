---
id: 0493
title: Verify migration consistency before the routing switch
status: Proposed
dependencies:
- TASK-0471
governing_adrs:
- ADR-0110
governing_prds:
- PRD-0002
governing_stories:
- US-0002
target_bc: application
---

# TASK-0493: Verify migration consistency before the routing switch

## Summary
The cutover switches tenant routing first and checks consistency after. A failed check then leaves the tenant on the target store with the FAILED phase.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Found during TASK-0471 and TASK-0472. Those tasks made the failure visible (FAILED phase, `CutoverResult.consistency_verified`, metrics). They did not change the order of steps. A check during the write pause, before the switch, can refuse the cutover with no switch.

## Evidence
- `src/eventsource/application/migration/cutover.py`
- `src/eventsource/application/migration/coordinator_cutover_completion.py`

## Definition of Done (Blackbox Frontdoor TDD)
1. Decide if the consistency check runs during the write pause before the routing switch. Record the decision in an ADR or an amendment to ADR-0110.
2. If yes: a test drives a cutover with a failed check and shows that routing still points at the source store.
3. Tests use public entry points only. All files stay under the line ceilings.
