---
id: '0010'
title: Isolate Cross-Tenant Projections and Feed Filtering
status: Accepted
created: 2026-10-08
persona: Taylor (The Multi-Tenant SaaS Architect)
target_bc: projections
feature: FEAT-TENANT-ISOLATION
governing_prd: PRD-0002
scenarios:
- Storage-level tenant filtering pushes tenant_id into feed adapter queries
- Foreground replay isolates read models using storage-level tenant filters
- DeclarativeProjection filters events dynamically using callable tenant filter
- Subscription live and catchup runners isolate streams by tenant filter
governing_adrs:
- ADR-0007
- ADR-0118
- ADR-0152
- ADR-0154
---

# US-0010 — Isolate Cross-Tenant Projections and Feed Filtering

## Governing PRD
- [`PRD-0002: Multi-Tenant SaaS Isolation Engine`](../../product/accepted/prd-0002-multi-tenant-saas-isolation-engine.md)

## User Story

**As a** multi-tenant SaaS architect (Taylor),
**I want** to push `tenant_id` filters into `FeedReadOptions` queries and isolate projections with `TenantFilter`,
**So that** projection replays and subscription streams retrieve only tenant-authorized events at the storage layer without leaking cross-tenant records into memory.

## Acceptance Criteria

```gherkin
Scenario: Storage-level tenant filtering pushes tenant_id into feed adapter queries
  Given an event store containing interleaved event streams across multiple tenants
  When a global feed read is executed with "FeedReadOptions(tenant_id=tenant_a)"
  Then the storage adapter applies a SQL WHERE clause or in-memory filter on tenant ID
  And only envelopes matching "tenant_a" are returned without loading foreign tenant rows into memory.
```

```gherkin
Scenario: Foreground replay isolates read models using storage-level tenant filters
  Given a "GlobalEventFeed" spanning multiple tenant histories and registered projections
  When "replay(feed, projections, tenant_id=tenant_a)" is executed to rebuild read models
  Then the replay driver forwards "tenant_id" down to the storage feed query
  And projections fold only "tenant_a" events into their read model stores.
```

```gherkin
Scenario: DeclarativeProjection filters events dynamically using callable tenant filter
  Given a "DeclarativeProjection" configured with a callable "tenant_filter=get_current_tenant"
  When events with distinct tenant IDs are dispatched to the projection
  Then only events matching the active ambient tenant context are processed by handlers
  And events belonging to foreign tenants are skipped without raising errors.
```

```gherkin
Scenario: Subscription live and catchup runners isolate streams by tenant filter
  Given a subscription configured with "SubscriptionConfig(tenant_id=tenant_a)"
  When the subscription runner reads pages from the event store via "FeedReadOptions"
  Then the catchup runner receives only pages matching "tenant_a"
  And live event notifications for other tenants are filtered before handler dispatch.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0118, ADR-0152, ADR-0154
- **Verified Test Suites**:
  - `tests/unit/application/subscriptions/test_tenant_filtering.py`: Verifies tenant subscription filtering across live and catchup runners.
  - `tests/unit/domain/test_tenant_events.py`: Verifies tenant event isolation, schema validation, and ambient fallback.
  - `tests/unit/multitenancy/`: Verifies tenant scoping across async tasks and projection boundaries.
  - `tests/unit/application/projections/test_replay.py`: Verifies foreground replay with pushdown tenant query parameters.
- **Architectural Invariants Verified**:
  - *Storage Pushdown Filtering*: `FeedReadOptions(tenant_id=...)` ensures storage adapters filter tenant records before returning data.
  - *Dynamic Projection Filtering*: Callable `tenant_filter` skips discordant tenant events cleanly without throwing exceptions.
  - *Runner Stream Isolation*: Multi-tenant event streams partition cleanly across subscription runners.
