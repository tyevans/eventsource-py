---
id: '0464'
title: 'SPIKE: Make tenant part of stream and snapshot identity'
status: Complete
governing_adrs:
- ADR-0110
governing_prds:
- PRD-0002
governing_stories:
- US-0005
target_bc: domain
priority_rank: 4
hypothesis: "Adding tenant_id to StreamId, the events UNIQUE key, the version check and the snapshot key stops cross-tenant read and write on aggregate-id reuse, with a schema migration that existing users can apply."
timebox: 3 days
---

# TASK-0464: SPIKE: Make tenant part of stream and snapshot identity

## Summary
The stream key, the version check and the snapshot key contain no tenant. A tenant that uses another tenant's aggregate id loads that tenant's state and can append events to that tenant's stream. Decide the design in an ADR and specify the schema migration.

## Problem Statement & Context
- **Severity**: High
- **Weakness**: CWE-639 (authorization bypass through user-controlled key)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

`TenantAwareRepository(require_tenant_context=True)` checks that a tenant is set, not that the stream belongs to that tenant. `docs/guides/multi-tenant.md:91-93` says that load does not filter by tenant, so the read side is documented. The write side is not documented: line 5 of the same guide says that the repository rejects writes that mix tenants. It does not. ADR-0110 says "Bulletproof cross-tenant data leak defense". Exploitation needs the victim's aggregate UUID. UUIDs are not guessable, but they often appear in URLs and logs. Global `event_id` uniqueness also gives an existence oracle across tenants (DuplicateEventError).

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/application/aggregates/repository.py:100-101` (StreamId without tenant)
- `src/eventsource/domain/stream_id.py:51` (render)
- `src/eventsource/adapters/memory/store.py:78`, 83, 92-93, 116-117
- `src/eventsource/adapters/postgresql/store_read.py:51`, 72; adapters/postgresql/store_append.py:23-26 (MAX(version) without tenant)
- `src/eventsource/adapters/sqlite/queries.py:39`
- `src/eventsource/adapters/sql/schemas/templates/events.sql:24`, 57-58; templates/sqlite/events.sql:61
- `src/eventsource/adapters/memory/snapshots.py:26`, 50; adapters/postgresql/snapshots.py:33; templates/snapshots.sql:47
- `src/eventsource/application/aggregates/snapshotting.py:89`
- `src/eventsource/application/aggregates/tenant_repository.py:25`, 56-80, 102-113

**Verification**: Proven with the in-memory store: tenant A creates order O. Under tenant scope B, `load(O)` returns A's state. B saves a new event through `TenantAwareRepository`; A then sees B's value. A snapshot saved by B for the same key replaces A's snapshot. B gets DuplicateEventError when B reuses A's event_id.

## Definition of Done (Blackbox Frontdoor TDD)
1. An ADR records the decision: tenant in StreamId, or an ownership check on load and append. The ADR also states the snapshot and event_id scope.
2. The spike gives a schema migration plan for PostgreSQL and SQLite, with the new UNIQUE keys.
3. The spike gives follow-up implementation tasks, each with a blackbox conformance case that reproduces the verification above.
4. The decision says what `require_tenant_context` must guarantee for load, exists and get_version.
5. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
