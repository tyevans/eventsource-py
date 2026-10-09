---
id: '0019'
title: Query and Soft-Delete Read Models with Typed Query Builders
status: Accepted
created: 2026-10-09
persona: Alex (The Event-Sourced Domain Architect)
target_bc: projections
feature: FEAT-READMODEL-QUERYING
governing_prd: PRD-0001
scenarios:
- Read model repository soft-deletes and restores records
- Query builder filters and paginates read models across storage dialects
- Admin query includes soft-deleted records via include_deleted flag
- Bulk read model operations execute atomically
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0007
- ADR-0109
---

# US-0019: Query and Soft-Delete Read Models with Typed Query Builders

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** event-sourced domain architect (Alex),
**I want** read model repositories to support soft-deletion lifecycle methods and dialect-agnostic typed `Query` and `Filter` builders,
**So that** projection queries can filter, paginate, soft-delete, and restore view models across PostgreSQL, SQLite, and InMemory backends without writing raw SQL.

## Acceptance Criteria

```gherkin
Scenario: Read model repository soft-deletes and restores records
  Given a persisted "ReadModel" instance in the repository
  When the caller invokes "soft_delete(id)"
  Then the record's "deleted_at" timestamp is populated
  And standard queries "get(id)", "find()", and "exists(id)" exclude the record
  When the caller invokes "restore(id)"
  Then the "deleted_at" timestamp is cleared and the record is visible again.
```

```gherkin
Scenario: Query builder filters and paginates read models across storage dialects
  Given multiple read model records matching various statuses and numeric amounts
  When a Query is constructed with "Filter.eq('status', 'active')", "Filter.gt('amount', 100)", and pagination
  Then the repository compiles the query to dialect-specific SQL or in-memory predicates
  And only matching non-deleted records within the limit and offset are returned.
```

```gherkin
Scenario: Admin query includes soft-deleted records via include_deleted flag
  Given both active and soft-deleted read model records in the repository
  When "find(Query(include_deleted=True))" or "find_deleted()" is executed
  Then soft-deleted records are returned with their "deleted_at" timestamps populated.
```

```gherkin
Scenario: Bulk read model operations execute atomically
  Given a batch of read model instances to update or remove
  When "save_many()" or "get_many()" is executed
  Then the operation executes atomically in a single batch pass across the underlying storage engine.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified (100% test pass rate, 0 backdoor mocks)
- **Implementation Modules**:
  - `src/eventsource/ports/readmodels/query.py`: `Query`, `Filter`, filter operator enum.
  - `src/eventsource/ports/readmodels/repository.py`: `ReadModelRepository` interface.
  - `src/eventsource/ports/readmodels/model.py`: `ReadModel` base model with `deleted_at`.
  - `src/eventsource/adapters/postgresql/readmodels.py`, `sqlite/readmodels.py`, `memory/readmodels.py`.
- **Verified Test Suites**:
  - `tests/unit/readmodels/test_soft_delete.py`: Soft delete, restore, and filter exclusion.
  - `tests/unit/readmodels/test_query.py`: Query builder operator compilation and pagination.
  - `tests/unit/adapters/test_memory_readmodels_conformance.py`: In-memory conformance.
