---
id: '0472'
title: Hold cutover completion when consistency verification fails
status: Proposed
governing_adrs:
- ADR-0111
governing_prds:
- PRD-0001
governing_stories:
- US-0006
target_bc: application
priority_rank: 12
---

# TASK-0472: Hold cutover completion when consistency verification fails

## Summary
Cutover sets the migration to COMPLETED after a consistency check finds violations, or after the check or subscription migration raises. The failure is only logged. Decide the policy and give the operator a visible state.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-636 (not failing securely)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

Routing already points to the target when the check runs. A target with missing events then serves reads, and the migration record says COMPLETED.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/application/migration/coordinator_cutover.py:83-90`, 92-102, 104
- `src/eventsource/application/migration/cutover_execution.py:111`
- `src/eventsource/ports/migration/models/config.py:51-52`

**Verification**: Control flow read: both exceptions are caught and logged, then `update_phase(COMPLETED)` runs.

## Definition of Done (Blackbox Frontdoor TDD)
1. A failed verification or subscription migration gives a visible result: a non-terminal phase, a failure status on the migration, or a raised error. The guide states which.
2. The returned cutover result and the audit entry contain the verification outcome.
3. Tests use the public coordinator API with an injected inconsistency. They fail on the current code.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
