---
id: 0499
title: Run the PostgreSQL schema probes and upgrade scripts in integration tests
status: Proposed
dependencies:
- TASK-0464
governing_adrs:
- ADR-0113
governing_prds:
- PRD-0002
governing_stories:
- US-0002
target_bc: adapters
---

# TASK-0499: Run the PostgreSQL schema probes and upgrade scripts in integration tests

## Summary
The PostgreSQL column probes and the operator scripts `updates/005`, `006` and `007` have no integration test against a real database.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Found during TASK-0464, TASK-0473 and ADR-0114. The adapters probe `information_schema` and degrade with one warning when a column or table is missing. Unit tests use SQLite or fakes only. Role visibility in `information_schema` can also hide a column.

## Evidence
- `src/eventsource/adapters/postgresql/snapshot_tenant.py`
- `src/eventsource/adapters/sql/schemas/updates/`
- `tests/integration/` (CI postgres job)

## Definition of Done (Blackbox Frontdoor TDD)
1. An integration test creates the old schema, runs the adapter (degraded path), applies each update script, and runs the adapter again (full path).
2. The tests run in the CI postgres job.
3. The guide states the `information_schema` visibility condition.
4. Tests use public entry points only. All files stay under the line ceilings.
