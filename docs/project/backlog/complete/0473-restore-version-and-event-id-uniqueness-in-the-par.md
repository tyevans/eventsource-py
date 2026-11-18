---
id: '0473'
title: Restore version and event_id uniqueness in the partitioned events schema
status: Complete
governing_adrs:
- ADR-0105
governing_prds:
- PRD-0001
governing_stories:
- US-0002
target_bc: adapters
priority_rank: 13
---

# TASK-0473: Restore version and event_id uniqueness in the partitioned events schema

## Summary
The partitioned PostgreSQL schema puts `timestamp` into both UNIQUE keys. Two concurrent appends at the same version with different timestamps both commit, so the stream forks. The same event_id can be stored two times. The guide calls this schema a drop-in replacement.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-362 (race condition)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The default `events.sql` does not have this defect. Bulk copy also counts a DuplicateEventError as copied without a comparison of the stored event (low).

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/sql/schemas/templates/events_partitioned.sql:68`, 72-73
- `src/eventsource/adapters/postgresql/store_append.py:83-84` (MAX(version) without lock), 98-111
- `src/eventsource/domain/event.py:30-32` (occurred_at default)
- `src/eventsource/application/migration/bulk_copier_writer.py:112-117`
- `docs/guides/database-schema.md:87`, 480 ("drop-in replacement")

**Verification**: DDL and control flow read. Not run (needs PostgreSQL; CI integration job can run it).

## Definition of Done (Blackbox Frontdoor TDD)
1. Concurrent appends at the same version on the partitioned schema give one success and one OptimisticLockError (per-stream lock, or a side uniqueness table).
2. A second append of the same event_id with a different timestamp raises DuplicateEventError.
3. A PostgreSQL integration test runs both cases on the partitioned schema.
4. Bulk copy compares the stored event with the source on DuplicateEventError and fails on a mismatch.
5. The schema guide states any remaining difference from the default schema.
6. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
