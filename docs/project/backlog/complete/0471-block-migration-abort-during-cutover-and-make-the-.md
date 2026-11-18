---
id: '0471'
title: Block migration abort during cutover and make the write pause shared
status: Complete
governing_adrs:
- ADR-0111
governing_prds:
- PRD-0001
governing_stories:
- US-0006
target_bc: application
priority_rank: 11
---

# TASK-0471: Block migration abort during cutover and make the write pause shared

## Summary
`abort_migration` is accepted while a cutover runs, and the cutover does not check the phase again before it switches routing. The write pause is in process memory only, and a dual-write interceptor is returned before the pause check. Writes can reach the old store after cutover.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-362 (race condition)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

`docs/guides/live-migration.md:175-176` says that abort is available from PENDING, BULK_COPY and DUAL_WRITE only. The code permits CUTOVER. A second process with its own router never sees the pause. A failed write to the target store is logged and not raised.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/application/migration/coordinator_control.py:91-111`
- `src/eventsource/application/migration/coordinator_cutover.py:71-77`
- `src/eventsource/application/migration/cutover_execution.py:85-112`
- `src/eventsource/application/migration/router_resolution.py:92-93`, 115-117
- `src/eventsource/application/migration/router_store.py:69;` write_pause_manager.py:37-46
- `src/eventsource/application/migration/dual_write_store.py:99-117`

**Verification**: `MigrationPhase.CUTOVER.can_transition_to(ABORTED)` returns True. Control flow read: abort clears routing, then the running cutover calls `switch_routing(MIGRATED)`.

## Definition of Done (Blackbox Frontdoor TDD)
1. `abort_migration` raises `MigrationError` in the CUTOVER phase, or it waits for the cutover lock.
2. The cutover checks the phase before `switch_routing` and stops if the migration was aborted.
3. The CUTOVER_PAUSED check runs before the dual-write interceptor branch.
4. An ADR or the guide states the multi-process limit, or the pause state moves to the routing table.
5. Tests use the public coordinator API and fail on the current code.
6. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
