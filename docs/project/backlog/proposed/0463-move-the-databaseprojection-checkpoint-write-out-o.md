---
id: '0463'
title: Move the DatabaseProjection checkpoint write out of the handler retry loop
status: Proposed
governing_adrs:
- ADR-0109
governing_prds:
- PRD-0001
governing_stories:
- US-0004
target_bc: adapters
priority_rank: 3
---

# TASK-0463: Move the DatabaseProjection checkpoint write out of the handler retry loop

## Summary
`DatabaseProjection` commits the read-model transaction and then writes the checkpoint inside the same retry block. If the checkpoint write fails, the retry runs the handler again. Non-idempotent read models (counters, balances) receive the event more than one time.

## Problem Statement & Context
- **Severity**: High
- **Weakness**: CWE-841 (improper enforcement of behavioral workflow)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The base `CheckpointTrackingProjection` had the same defect. ADR 0048 fixed it there (CHANGELOG.md:354), but the SQL sibling did not get the fix (recurring defect 1). A transient checkpoint failure (failover, deadlock, dropped connection) is enough to cause the double write. After the last retry, the event also goes to the DLQ, although the handler applied it.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/sql/projection.py:66-69` (commit, then checkpoint, inside the try)
- `src/eventsource/adapters/sql/projection.py:74-98` (retry and DLQ on the checkpoint error)
- `src/eventsource/adapters/sql/projection.py:110-117` (transaction commit)
- `src/eventsource/application/projections/base_checkpoint.py:31`, 36-55 (correct pattern)

**Verification**: Proven with in-memory SQLite, a checkpoint repository that always fails, and `max_retries=2`: the handler body ran 3 times and 1 DLQ row was written for an event that was applied.

## Definition of Done (Blackbox Frontdoor TDD)
1. A checkpoint failure after a successful commit does not run the handler again.
2. The checkpoint error goes to the caller, as in `CheckpointTrackingProjection`.
3. A test counts handler applications when the checkpoint repository fails. It expects exactly 1. The test fails on the current code.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
