---
id: 0497
title: Decide the default notify_on_severity for subscription error callbacks
status: Proposed
dependencies:
- TASK-0462
governing_adrs:
- ADR-0107
governing_prds:
- PRD-0003
governing_stories:
- US-0003
target_bc: application
---

# TASK-0497: Decide the default notify_on_severity for subscription error callbacks

## Summary
The subscription error handler has the default `notify_on_severity=HIGH`. A `ValueError` is MEDIUM, so it reaches the DLQ but not the `on_error` callbacks.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Found during TASK-0462. Users who register `on_error` can expect a call for each failed event. The current default gives no call for most handler errors.

## Evidence
- `src/eventsource/application/subscriptions/error_handling_handler.py`
- `src/eventsource/application/subscriptions/error_handling_types.py`

## Definition of Done (Blackbox Frontdoor TDD)
1. Decide the default. Record it in the docstring and in `docs/guides/error-handling.md`.
2. A test drives a real runner with a `ValueError` handler and asserts the callback behavior for the default.
3. Tests use public entry points only. All files stay under the line ceilings.
