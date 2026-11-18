---
id: '0474'
title: 'SPIKE: Define tenant scope for read models, DLQ and checkpoints'
status: Complete
dependencies:
- TASK-0464
governing_adrs:
- ADR-0110
governing_prds:
- PRD-0002
governing_stories:
- US-0010
target_bc: ports
priority_rank: 14
hypothesis: "A tenant column and an ambient-tenant predicate on the read-model, DLQ and checkpoint ports remove cross-tenant access without breaking single-tenant users."
timebox: 2 days
---

# TASK-0474: SPIKE: Define tenant scope for read models, DLQ and checkpoints

## Summary
Read-model rows, DLQ entries and checkpoints have no tenant column and no tenant predicate. Isolation depends on the application adding a filter to every call. Decide one policy for these tables.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-639 (authorization bypass through user-controlled key)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

Read models are the usual query surface of a SaaS application, so this is Medium. The DLQ uses sequential ids and has no tenant parameter (Low: operator surface). Checkpoints are keyed by projection name only; two per-tenant instances share one row and skip events (Low: correctness). `docs/guides/multi-tenant.md:524-525` refers to a section "Run one projection instance per tenant" that does not exist.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/ports/readmodels/repository.py:36`, 40, 83, 116, 125
- `src/eventsource/adapters/sql/readmodel_schema_column.py:41`
- `src/eventsource/ports/dlq.py:64-72;` adapters/sql/schemas/templates/dlq.sql:22
- `src/eventsource/ports/checkpoints.py:43;` adapters/sql/schemas/templates/checkpoints.sql:19

**Verification**: Code read of ports and schema templates.

## Definition of Done (Blackbox Frontdoor TDD)
1. An ADR records the tenant policy for read models, DLQ and checkpoints.
2. The spike gives follow-up tasks with blackbox conformance cases.
3. The multi-tenant guide has the missing section, or the reference is removed.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
