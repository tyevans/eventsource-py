---
id: 0482
title: Create SQLite database files with owner-only permissions
status: Complete
governing_adrs:
- ADR-0105
governing_prds:
- PRD-0001
governing_stories:
- US-0002
target_bc: adapters
priority_rank: 22
---

# TASK-0482: Create SQLite database files with owner-only permissions

## Summary
`SQLiteEventStore` creates a new database file with the process umask, usually 0644. Other local users can read event payloads and DLQ tracebacks.

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-276 (incorrect default permissions)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The risk applies to shared hosts. No `chmod` or `umask` call exists in `src/eventsource`.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/sqlite/store.py:31-52`

**Verification**: Proven: a new store file had mode 0o644.

## Definition of Done (Blackbox Frontdoor TDD)
1. A new database file has mode 0600. An existing file keeps its mode.
2. A test creates a store through the public API and checks the file mode (POSIX only).
3. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
