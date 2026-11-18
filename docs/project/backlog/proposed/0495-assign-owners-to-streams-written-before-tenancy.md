---
id: 0495
title: Assign owners to streams written before tenancy
status: Proposed
dependencies:
- TASK-0464
governing_adrs:
- ADR-0113
governing_prds:
- PRD-0002
governing_stories:
- US-0002
target_bc: application
---

# TASK-0495: Assign owners to streams written before tenancy

## Summary
Streams written before tenancy have events with no tenant. ADR-0113 ownership checks treat such a stream as not owned, so a tenant cannot load it.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Follow-up from TASK-0464. Operators need a supported way to give these streams an owner. The audit query from T9 finds them but does not change them.

## Evidence
- `src/eventsource/application/aggregates/tenant_ownership.py`
- `docs/guides/multi-tenant.md` (Auditing existing streams)

## Definition of Done (Blackbox Frontdoor TDD)
1. A documented procedure or tool assigns an owner to a pre-tenancy stream, with an audit record.
2. A test shows that after assignment the owning tenant can load and append, and other tenants cannot.
3. Tests use public entry points only. All files stay under the line ceilings.
