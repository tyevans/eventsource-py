---
id: 0468
title: Reject a command tenant that contradicts the ambient tenant
status: Proposed
governing_adrs:
- ADR-0110
governing_prds:
- PRD-0002
governing_stories:
- US-0005
target_bc: domain
priority_rank: 8
---

# TASK-0468: Reject a command tenant that contradicts the ambient tenant

## Summary
Event provenance uses `DomainCommand.tenant_id` before the ambient tenant. A command body from an untrusted request can stamp events with a different tenant. PRD-0002 says that such overrides are not accepted.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-639 (authorization bypass through user-controlled key)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

PRD-0002 (line 57) says that sealed aggregate operations do not accept caller overrides that contradict the ambient tenant. A plain `AggregateRepository` writes the events without a check. `TenantAwareRepository.save` catches the mismatch only when the application uses it.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/domain/aggregate/provenance.py:96-103`
- `src/eventsource/domain/decider.py:96`
- `src/eventsource/domain/command.py:41-51`

**Verification**: Proven: under `tenant_scope(A)`, `create_event(..., command=DomainCommand(tenant_id=B))` produced an event with tenant B.

## Definition of Done (Blackbox Frontdoor TDD)
1. When an ambient tenant is set and the command tenant is different, event creation raises a tenant mismatch error. Aggregate and decider styles behave the same.
2. When no ambient tenant is set, the command tenant is used as now.
3. Tests use the public aggregate and decider APIs. They fail on the current code.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
