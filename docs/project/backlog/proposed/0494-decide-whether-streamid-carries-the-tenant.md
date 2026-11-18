---
id: 0494
title: Decide whether StreamId carries the tenant
status: Proposed
dependencies:
- TASK-0464
governing_adrs:
- ADR-0113
governing_prds:
- PRD-0002
governing_stories:
- US-0002
target_bc: domain
---

# TASK-0494: Decide whether StreamId carries the tenant

## Summary
ADR-0113 keeps the tenant out of `StreamId` and checks ownership at the repository. Option A of the spike, a tenant field in `StreamId`, is still open.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Follow-up T12 from TASK-0464. Ownership checks now protect `TenantAwareRepository`. A caller that uses the store ports directly can still read or append to a stream of another tenant.

## Evidence
- `docs/adrs/0013-tenant-ownership-check-on-the-repository-read-and-write-path.md` (rejected and deferred options)
- `src/eventsource/domain/stream_id.py`

## Definition of Done (Blackbox Frontdoor TDD)
1. An ADR accepts or rejects a tenant field in `StreamId`, with the effect on stored stream identities.
2. If accepted, a migration plan for existing streams.
3. Tests use public entry points only. All files stay under the line ceilings.
