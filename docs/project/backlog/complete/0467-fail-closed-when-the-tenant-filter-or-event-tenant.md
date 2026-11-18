---
id: '0467'
title: Fail closed when the tenant filter or event tenant is absent
status: Complete
governing_adrs:
- ADR-0110
governing_prds:
- PRD-0002
governing_stories:
- US-0010
target_bc: application
priority_rank: 7
---

# TASK-0467: Fail closed when the tenant filter or event tenant is absent

## Summary
`DeclarativeProjection` processes every tenant's events when its callable tenant filter returns None. It also accepts events whose `tenant_id` is None. `TenantAwareRepository.save` accepts events whose `tenant_id` is None. Add a fail-closed behavior.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-636 (not failing securely)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The guide documents the open behavior (`docs/guides/multi-tenant.md:565-566`, `:693` "the filter is not a security boundary"). But the guide example at `:473` uses `get_current_tenant` as the filter, so a worker that runs without a tenant scope fills a tenant read model with all tenants' data. No error occurs.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/application/projections/base_declarative.py:76-86`
- `src/eventsource/application/projections/base_protocols.py:20`
- `src/eventsource/application/aggregates/tenant_repository.py:62`
- `src/eventsource/domain/aggregate/provenance.py:96`

**Verification**: Proven: a projection with `tenant_filter=get_current_tenant` and no tenant scope wrote rows for tenant A and tenant B. Expected: 0 rows or an error.

## Definition of Done (Blackbox Frontdoor TDD)
1. A projection that uses a callable tenant filter raises `TenantContextNotSetError` when the callable returns None. The fail-open behavior is available only through an explicit option.
2. When a tenant filter is set, an event with `tenant_id` None does not match.
3. `TenantAwareRepository.save` rejects events with `tenant_id` None when a tenant context is active.
4. Tests use the public projection and repository APIs. They fail on the current code.
5. The multi-tenant guide describes the new default.
6. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
