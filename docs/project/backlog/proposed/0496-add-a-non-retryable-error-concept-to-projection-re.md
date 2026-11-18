---
id: 0496
title: Add a non-retryable error concept to projection retry policies
status: Proposed
dependencies:
- TASK-0467
governing_adrs:
- ADR-0107
governing_prds:
- PRD-0003
governing_stories:
- US-0003
target_bc: application
---

# TASK-0496: Add a non-retryable error concept to projection retry policies

## Summary
The projection retry policies retry all exceptions. They have no way to mark an error as not retryable.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Found during TASK-0467. `TenantContextNotSetError` is a configuration error, and retrying it only delays the DLQ entry. TASK-0467 excluded it with a special case in `base_checkpoint.py`. Other configuration and validation errors have the same problem.

## Evidence
- `src/eventsource/application/projections/retry.py`
- `src/eventsource/application/projections/base_checkpoint.py` (special case)

## Definition of Done (Blackbox Frontdoor TDD)
1. Retry policies accept a set of non-retryable exception types, or the exceptions carry a marker.
2. The `TenantContextNotSetError` special case uses the new concept.
3. A test drives a real projection and shows one attempt for a non-retryable error.
4. Tests use public entry points only. All files stay under the line ceilings.
