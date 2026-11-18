---
id: 0484
title: Align the append ExpectedVersion default across store adapters
status: Proposed
dependencies:
- TASK-0475
governing_adrs:
- ADR-0105
governing_prds:
- PRD-0001
governing_stories:
- US-0002
target_bc: adapters
priority_rank: 24
---

# TASK-0484: Align the append ExpectedVersion default across store adapters

## Summary
The in-memory store and the sync adapter default `expected` to `ExpectedVersion.any_()`, which disables the concurrency check. The port and the PostgreSQL and SQLite stores require the argument.

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-1188 (insecure default initialization)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

Tests on the in-memory store do not show a missing concurrency check. The difference between sibling adapters is recurring defect 1.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/memory/store.py:29`, 72
- `src/eventsource/adapters/sync/adapter.py:28`, 70, 76
- `src/eventsource/ports/store.py:35;` adapters/postgresql/store_append.py:72; adapters/sqlite/store.py:82

**Verification**: Signatures read on all adapters.

## Definition of Done (Blackbox Frontdoor TDD)
1. All store adapters have the same `append` signature as the port, or the ADR records the reason for a difference.
2. A conformance case pins the signature.
3. The sync guide matches the result.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
