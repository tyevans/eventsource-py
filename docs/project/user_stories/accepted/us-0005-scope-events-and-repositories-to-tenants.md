---
id: '0005'
title: Scope Events and Repositories to Tenants
status: Accepted
created: 2026-10-07
persona: Jordan (The Backend Platform Engineer)
target_bc: domain
feature: FEAT-MULTITENANCY
governing_prd: PRD-0001
scenarios:
  - Ambient tenant scope populates TenantDomainEvent
  - Saving foreign tenant event raises TenantMismatchError
---

# US-0005 — Scope Events and Repositories to Tenants

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As a** backend platform engineer (Jordan),
**I want** to isolate customer events using `tenant_scope` and `TenantAwareRepository`,
**So that** uncommitted events automatically inherit ambient tenant context and cross-tenant write attempts raise immediate errors before reaching the database.

## Acceptance Criteria

```gherkin
Scenario: Ambient tenant scope populates TenantDomainEvent
  Given an ambient context established via "tenant_scope(tenant_a)"
  When a "SubscriptionStarted" event is created via "create_event"
  Then the event "tenant_id" matches "tenant_a".
```

```gherkin
Scenario: Saving foreign tenant event raises TenantMismatchError
  Given an aggregate repository wrapped in "TenantAwareRepository" under "tenant_b" scope
  When the caller attempts to save an aggregate carrying an event stamped with "tenant_a"
  Then a "TenantMismatchError" is raised and the transaction is aborted.
```
