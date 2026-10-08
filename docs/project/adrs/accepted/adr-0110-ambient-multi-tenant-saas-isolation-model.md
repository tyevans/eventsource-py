---
id: '0110'
title: Ambient Multi-Tenant SaaS Isolation Model
status: Accepted
target_bc: tenant
governing_prds:
- PRD-0002
governing_stories:
- US-0005
- US-0010
---

# ADR-0110: Ambient Multi-Tenant SaaS Isolation Model

## Summary
ContextVar tenant propagation, hard reset safety, and storage query pushdown.

## Context
Multi-tenant SaaS architectures require absolute data isolation. Manual passing of tenant IDs is error-prone, while poorly isolated async contexts can leak tenant state across coroutines.

## Decision
1. Ambient tenant context is propagated across async tasks and threads via `tenant_scope(tenant_id)`.
2. Hard-clear reset semantics: `clear_tenant_context()` invalidates active context; attempting to restore a cleared token raises `TenantContextResetError` to prevent zombie context revival.
3. Precondition enforcement on aggregate load: repositories verify active tenant context before touching disk.
4. Storage-level tenant filtering: push `tenant_id` into adapter queries (`FeedReadOptions(tenant_id=...)`) preventing cross-tenant memory leakage.

## Consequences
- Bulletproof cross-tenant data leak defense in multi-tenant SaaS environments.
- Clean application code without polluting domain signatures with tenant parameters.
- Storage pushdown maximizes database index efficiency.
