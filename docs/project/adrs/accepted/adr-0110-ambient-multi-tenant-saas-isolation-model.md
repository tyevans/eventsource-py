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

Amended by [ADR-0113](adr-0113-tenant-ownership-check-on-the-repository-read-and-write-path.md) (Consequences).
Amended by [ADR-0114](adr-0114-tenant-scope-for-read-models-the-dead-letter-queue-and-checkpoints.md) (Consequences).

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
- Write-side tenant validation on `TenantAwareRepository`. Under an active scope, the repository also refuses to load, probe, or append to a stream that another tenant owns (ADR-0113). This is a repository-level control. It does not cover direct store access, unscoped repositories, or the global uniqueness of event ids. Under an active scope, read models through `TenantScopedReadModelRepository` and DLQ reads and state changes through the DLQ repository match only rows of the active tenant (ADR-0114). Checkpoints are not tenant-scoped. Each per-tenant projection instance uses its own name. This is an application-level control. It does not cover direct adapter access or the global feed.
- Clean application code without polluting domain signatures with tenant parameters.
- Storage pushdown maximizes database index efficiency.
