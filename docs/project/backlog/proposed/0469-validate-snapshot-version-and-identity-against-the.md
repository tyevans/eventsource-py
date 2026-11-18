---
id: 0469
title: Validate snapshot version and identity against the stream on load
status: Proposed
governing_adrs:
- ADR-0106
governing_prds:
- PRD-0001
governing_stories:
- US-0007
target_bc: application
priority_rank: 9
---

# TASK-0469: Validate snapshot version and identity against the stream on load

## Summary
`AggregateRepository.load` accepts a snapshot with any version, an empty state, or no events in the stream. A snapshot that is newer than the events makes the aggregate permanently unable to save.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-345 (insufficient verification of data authenticity)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

`read_valid_snapshot` checks `schema_version` only. A realistic trigger is a restore of the events table from an older backup while the snapshot table is newer. An attacker with write access to the snapshot table can also set the loaded state and hide events from replay.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/application/aggregates/repository_query.py:50-70` (guard at :66 only when snapshot is None)
- `src/eventsource/application/aggregates/snapshotting.py:86-103`
- `src/eventsource/domain/aggregate/snapshot.py:43-86` (empty state sets the version without state)

**Verification**: Proven with in-memory stores: a stream with 2 events and a snapshot at version 999 loads as version 999. Every later save raises OptimisticLockError. A snapshot for an aggregate with zero events loads as a phantom aggregate.

## Definition of Done (Blackbox Frontdoor TDD)
1. Load treats a snapshot as a miss when its version is greater than the stream head, or when its aggregate id or type does not match the request. Load then replays from the start and logs a warning.
2. Load raises `AggregateNotFoundError` when the stream has no events, even if a snapshot exists.
3. Tests use the public repository and snapshot store. They fail on the current code.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
