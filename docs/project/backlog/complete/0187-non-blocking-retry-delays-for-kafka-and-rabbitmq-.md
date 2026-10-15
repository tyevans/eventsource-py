---
id: "0187"
title: Remove SQL & PostgreSQL Lock Legacy Shims and Decompose Lock & DLQ Modules
status: Complete
target_bc: adapters
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0105
  - ADR-0111
governing_prds:
  - PRD-0001
  - PRD-0003
governing_stories:
  - US-0011
  - US-0022
priority: P1
---

# TASK-0187: Remove SQL & PostgreSQL Lock Legacy Shims and Decompose Lock & DLQ Modules

## Summary
Remove the legacy `PostgreSQLLockManager` alias and SQL checkpoint/DLQ backward-compatible attribute shims, and decompose `src/eventsource/adapters/postgresql/locks.py` (474 lines), `src/eventsource/adapters/sql/dlq.py` (401 lines), `tests/integration/locks/test_postgresql_locks_integration.py` (447 lines), and `tests/integration/repositories/test_checkpoint.py` (400 lines) below 400 lines.

## Problem Statement & Context
- `src/eventsource/adapters/postgresql/locks.py` (474 lines) exceeds the 400-line warning threshold and retains a legacy `PostgreSQLLockManager = PostgreSQLAdvisoryLock` alias.
- `src/eventsource/adapters/sql/checkpoints.py` and `src/eventsource/adapters/sql/dlq.py` (401 lines) retain `# Kept for backwards-compatible attribute access` private attribute shims.
- `tests/integration/locks/test_postgresql_locks_integration.py` (447 lines) and `tests/integration/repositories/test_checkpoint.py` (400 lines) trigger proactive file-length warnings.

## Required Actions
1. Remove the legacy `PostgreSQLLockManager` alias from `src/eventsource/adapters/postgresql/locks.py` and `src/eventsource/adapters/postgresql/__init__.py` (updating any references to use `PostgreSQLAdvisoryLock`) and decompose `locks.py` below 400 lines.
2. Remove backward-compatible private attribute shims from `src/eventsource/adapters/sql/checkpoints.py` and `src/eventsource/adapters/sql/dlq.py`, bringing `sql/dlq.py` below 400 lines.
3. Decompose `tests/integration/locks/test_postgresql_locks_integration.py` and `tests/integration/repositories/test_checkpoint.py` below 400 lines.

## Definition of Done (Blackbox Frontdoor TDD)
1. Legacy `PostgreSQLLockManager` alias and SQL repository private attribute shims are removed.
2. `postgresql/locks.py`, `sql/dlq.py`, `test_postgresql_locks_integration`, and `test_checkpoint` are all strictly under 400 lines.
3. All unit and conformance tests pass cleanly.
