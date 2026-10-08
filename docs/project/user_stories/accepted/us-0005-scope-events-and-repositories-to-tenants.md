---
id: '0005'
title: Scope Events and Repositories to Tenants
status: Accepted
created: 2026-10-07
persona: Taylor (The Multi-Tenant SaaS Architect)
target_bc: domain
feature: FEAT-MULTITENANCY
governing_prd: PRD-0002
scenarios:
- Ambient tenant context propagates across async coroutines and spawned tasks
- Hard clear invalidates tokens and prevents stale context resurrection
- Strict TenantDomainEvent enforces non-null tenant identity and context fallback
- TenantAwareRepository validates uncommitted events on save and rejects tenant mismatches
- TenantAwareRepository enforces active tenant context precondition on load
- Aggregate load without database RLS documents lack of stream event filtering
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0110
---

# US-0005 — Scope Events and Repositories to Tenants

## Governing PRD
- [`PRD-0002: Multi-Tenant SaaS Isolation Engine`](../../product/accepted/prd-0002-multi-tenant-saas-isolation-engine.md)

## User Story

**As a** multi-tenant SaaS architect (Taylor),
**I want** to isolate customer events using `tenant_scope` and `TenantAwareRepository`,
**So that** uncommitted events automatically inherit ambient tenant context, context resets cannot resurrect stale tokens, and cross-tenant write attempts raise immediate errors before reaching the database.

## Acceptance Criteria

```gherkin
Scenario: Ambient tenant context propagates across async coroutines and spawned tasks
  Given an ambient tenant context established via "tenant_scope(tenant_a)"
  When child tasks are spawned with "asyncio.create_task" or nested with "tenant_scope(tenant_b)"
  Then child tasks execute within "tenant_a" context without cross-task leakage
  And exiting the nested scope cleanly restores "tenant_a" as the ambient tenant.
```

```gherkin
Scenario: Hard clear invalidates tokens and prevents stale context resurrection
  Given an active tenant context with an issued "TenantContextToken"
  When "clear_tenant_context()" is invoked at a request or execution boundary
  Then "get_current_tenant()" immediately returns None
  And attempting to reset the prior token via "reset_tenant_context()" raises "TenantContextResetError".
```

```gherkin
Scenario: Strict TenantDomainEvent enforces non-null tenant identity and context fallback
  Given an ambient tenant context established for "tenant_a"
  When an event class deriving from "TenantDomainEvent" is created via "with_tenant_context"
  Then the event "tenant_id" matches "tenant_a"
  And instantiating a "TenantDomainEvent" without a "tenant_id" raises a model validation error.
```

```gherkin
Scenario: TenantAwareRepository validates uncommitted events on save and rejects tenant mismatches
  Given a "TenantAwareRepository" wrapping an aggregate repository under "tenant_a" scope
  When an aggregate with uncommitted events stamped with foreign "tenant_b" is saved
  Then a "TenantMismatchError" is raised detailing expected, actual, and offending event IDs
  And the append transaction is aborted before writing to storage.
```

```gherkin
Scenario: TenantAwareRepository enforces active tenant context precondition on load
  Given a "TenantAwareRepository" configured with "require_tenant_context=True"
  When the caller invokes "load()", "load_or_create()", or "exists()" with no active tenant context
  Then a "TenantContextNotSetError" is raised before querying the underlying store.
```

```gherkin
Scenario: Aggregate load without database RLS documents lack of stream event filtering
  Given an aggregate stream in storage populated with events belonging to "tenant_b"
  When the aggregate is loaded through "TenantAwareRepository" within an active "tenant_a" scope
  Then the aggregate is rehydrated from all stream events without partial filtering
  And read isolation is explicitly delegated to database row-level security or storage partitioning.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0118, ADR-0138, ADR-0142, ADR-0157
- **Verified Test Suites**:
  - `tests/unit/domain/test_tenant_context.py`: Verifies `tenant_scope` AsyncIO ContextVar propagation and task isolation.
  - `tests/unit/domain/test_tenant_events.py`: Verifies `TenantDomainEvent` strict non-null `tenant_id` and provenance fallback.
  - `tests/unit/repositories/test_tenant_aware.py`: Verifies `TenantAwareRepository` save-time validation (`TenantMismatchError`) and load-time precondition enforcement (`TenantContextNotSetError`).
  - `tests/unit/multitenancy/`: Verifies ambient scope lifecycle and reset security.
- **Architectural Invariants Verified**:
  - *Hard Clear Invalidation*: `clear_tenant_context()` invalidates active tokens, and `reset_tenant_context()` rejects stale tokens.
  - *Save-Time Isolation Guard*: Discordant tenant events are halted before database persistence.
  - *Explicit Load Precondition*: `require_tenant_context=True` prevents unauthenticated or context-free aggregate loads.
