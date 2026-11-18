---
id: 0486
title: Bound background snapshot tasks and status streamer polling
status: Proposed
governing_adrs:
- ADR-0106
governing_prds:
- PRD-0004
governing_stories:
- US-0007
target_bc: application
priority_rank: 26
---

# TASK-0486: Bound background snapshot tasks and status streamer polling

## Summary
The background snapshot scheduler has no task limit. The migration status streamer accepts any positive interval and any number of subscribers. `SubscriptionConfig.batch_size` has no upper limit.

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-770 (allocation without limits)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The snapshot limit is documented (`docs/guides/snapshotting.md:812-813`). The bus base class already falls back to inline work at capacity; the scheduler does not.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/application/aggregates/snapshotting.py:128`, 130-131; application/background_tasks.py:17, 23-25
- `src/eventsource/adapters/_bus/base.py:105-111` (pattern to copy)
- `src/eventsource/application/migration/status_streamer_core.py:56-57`, 71, 109
- `src/eventsource/application/subscriptions/config.py:52-54`

**Verification**: Code read.

## Definition of Done (Blackbox Frontdoor TDD)
1. `BackgroundScheduler` has a finite default `max_pending` and writes inline at capacity.
2. The status streamer rejects intervals below a floor and limits subscribers per migration.
3. `SubscriptionConfig` rejects a batch size above a documented maximum.
4. Tests use the public APIs.
5. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
