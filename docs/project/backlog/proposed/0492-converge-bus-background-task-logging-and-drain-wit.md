---
id: 0492
title: Converge bus background-task logging and drain with BackgroundTaskManager
status: Proposed
governing_adrs:
- ADR-0107
governing_prds:
- PRD-0003
governing_stories:
- US-0003
target_bc: adapters
---

# TASK-0492: Converge bus background-task logging and drain with BackgroundTaskManager

## Summary
The event bus and `BackgroundTaskManager` each log "Background task failed" and each drain their tasks at shutdown. The two copies can drift.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Found during TASK-0486. `adapters/_bus/base.py` has `_on_background_task_done` and `_drain_background`. `application/background_tasks.py` has `_on_task_done` and `await_all`. A test pins the bus logger name, so a merge must keep that name.

## Evidence
- `src/eventsource/adapters/_bus/base.py`
- `src/eventsource/application/background_tasks.py`
- `tests/unit/adapters/_bus/test_base.py` (logger-name pin)

## Definition of Done (Blackbox Frontdoor TDD)
1. The bus uses `BackgroundTaskManager` for logging and drain, or a decision records why it does not.
2. The `eventsource.bus.*` logger name stays the same.
3. Existing bus shutdown tests pass without change.
4. Tests use public entry points only. All files stay under the line ceilings.
