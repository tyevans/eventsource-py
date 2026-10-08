---
id: '0002'
title: Multi-Tenant SaaS Isolation Engine
status: Accepted
created: 2026-10-07
target_persona: Taylor
component: multitenancy
governing_adrs:
- ADR-0001
- ADR-0002
- ADR-0003
- ADR-0007
- ADR-0118
- ADR-0138
- ADR-0142
- ADR-0152
- ADR-0154
- ADR-0157
---

# PRD-0002 — Multi-Tenant SaaS Isolation Engine

## Who this is for

- **Taylor (The Multi-Tenant SaaS Architect)**: Cloud architects enforcing ambient tenant isolation, preventing cross-tenant leaks, and scoping repositories and feeds.
- **Jordan (The Streaming & Distributed Systems Platform Engineer)**: Platform operators streaming events across isolated customer boundaries.
- **Alex (The Event-Sourced Domain Architect)**: Domain modelers building aggregate state machines and projections with tenant-aware event contracts.
- **Morgan (The Autonomous Coding Agent)**: Coding agents verifying blackbox tenant isolation and context integrity across asynchronous coroutines.

## What the person cannot do today

- **Accidental Tenant Leaks**: Ad-hoc tenant filtering in application queries easily leaks private data across customer boundaries in multi-tenant SaaS deployments.
- **Manual Parameter Passing Friction**: Passing `tenant_id` manually through every layer, decider, repository method, and projection handler introduces human error and code clutter.
- **Async Context Bleed**: Background asynchronous tasks and coroutines can leak ambient tenant context if ContextVars are improperly cleared or if tokens are resurrected after request completion.
- **Cross-Tenant Projection Contamination**: Replaying global feeds or projecting events without storage-layer pushdown queries loads foreign tenant records into memory and threatens read-model isolation.

## What good looks like

1. **Ambient Context Propagation & Lifecycle**:
   - Safe, thread-and-coroutine-isolated ambient tenant context via `tenant_scope(tenant_id)`.
   - Strict `clear_tenant_context()` invalidation: once cleared, attempting to restore a prior token with `reset_tenant_context()` raises `TenantContextResetError`.

2. **Strict Domain Model Enforcement**:
   - `TenantDomainEvent` mandates non-null `tenant_id: UUID` at model definition.
   - Transparent provenance derivation stamps ambient tenant context onto uncommitted events automatically.

3. **Repository Preconditions & Save Guardrails**:
   - `TenantAwareRepository` intercepts aggregate saves, asserting that every uncommitted event strictly matches the active tenant context and raising `TenantMismatchError` before contacting storage.
   - `TenantAwareRepository.load()` enforces active tenant context preconditions when `require_tenant_context=True`, failing fast if context is absent.

4. **Storage-Level Query Pushdown**:
   - `FeedReadOptions(tenant_id=...)` pushes tenant criteria directly into SQL WHERE clauses or in-memory filters, preventing foreign tenant data from ever entering process memory.

5. **Isolated Projections & Replay**:
   - `DeclarativeProjection` supports callable `tenant_filter` to dynamically skip foreign tenant events.
   - `replay(feed, projections, tenant_id=...)` coordinates isolated foreground read model rebuilds strictly scoped to the specified tenant.

## What this does not do

- **Database-Level Physical Sharding**: Physical cluster partitioning and database instance sharding remain infrastructure provisioning responsibilities.
- **Cross-Tenant Data Blending**: No operations or query helpers permit cross-tenant data mingling without explicit unscoped bypass.
- **Arbitrary Parameter-Based Tenant Overrides**: Sealed domain aggregate operations do not accept ad-hoc caller overrides that contradict ambient tenant context.

## Checkable Outcomes

1. Spawning asynchronous coroutines or child tasks within `tenant_scope` propagates tenant context reliably without leaking across tasks.
2. Invoking `clear_tenant_context()` invalidates active tokens, and subsequent calls to `reset_tenant_context()` raise `TenantContextResetError`.
3. Creating a `TenantDomainEvent` without an explicit or ambient tenant ID raises a validation error.
4. Attempting to save an aggregate emitting an event with a discordant tenant ID through `TenantAwareRepository` raises `TenantMismatchError`.
5. Loading an aggregate through `TenantAwareRepository(require_tenant_context=True)` without an ambient tenant context raises `MissingTenantContextError`.
6. Reading from `GlobalEventFeed` or invoking `replay()` with `tenant_id` pushes the filter to the storage adapter and returns only matching envelopes.

## Linked User Stories

- [`US-0005`](../../user_stories/accepted/us-0005-scope-events-and-repositories-to-tenants.md): Scope Events and Repositories to Tenants
- [`US-0010`](../../user_stories/accepted/us-0010-isolate-cross-tenant-projections-and-feed-filtering.md): Isolate Cross-Tenant Projections and Feed Filtering
