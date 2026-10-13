---
id: 0181
title: Support non-numeric task IDs in spec-ops worktree and claimer
status: Refined
governing_adrs:
- ADR-0005
target_bc: core
---

# TASK-0181: Support non-numeric task IDs in spec-ops worktree and claimer

## Summary
When using `spec-ops worktree start` or `spec-ops queue claim --auto` on non-numeric task IDs (such as `TASK-REFACTOR-tests-...`), the command fails:
1. `_normalize_id` in `spec_ops.rescue.sandbox` attempts numeric normalization (`clean = raw_id.upper().replace("TASK-", "").lstrip("0")`, `tid_num = clean.zfill(4)`), causing the canonical ID lookup against backlog to fail.
2. `TaskClaimer.claim_auto` in `spec_ops.worker.claimer` scans `PRIORITY.md` with regex `re.search(r"TASK-0*(\d+)", line, re.IGNORECASE)`, ignoring alphanumeric task IDs.

## Root Cause & Remediation
- In `spec_ops.rescue.sandbox`: support arbitrary string IDs when not purely numeric (use sanitized slug for worktree folder, match exact canonical_id).
- In `spec_ops.worker.claimer`: adjust regex to match `TASK-[A-Za-z0-9_-]+` and resolve task accordingly.

## Definition of Done
1. Alphanumeric task IDs (e.g. `TASK-REFACTOR-...`) can be resolved by worktree start/finish and claimer.
2. Verified via automated tests in the spec-ops tool suite.
