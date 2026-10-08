---
id: '0002'
title: Append and Replay Events Across Storage Adapters
status: Accepted
created: 2026-10-07
persona: Jordan (The Streaming & Distributed Systems Platform Engineer)
target_bc: adapters
feature: FEAT-STORE-ADAPTERS
governing_prd: PRD-0001
scenarios:
- Append and load event stream in storage adapter
- Replay events from global position offset
- Optimistic concurrency conflict detection on concurrent stream append
- Filtered global feed query by aggregate type and tenant
- Engine lifecycle ownership and clean connection pool disposal
governing_adrs:
- ADR-0007
- ADR-0101
- ADR-0119
- ADR-0125
- ADR-0136
- ADR-0137
- ADR-0149
- ADR-0151
- ADR-0152
- ADR-0153
---

# US-0002 — Append and Replay Events Across Storage Adapters

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As a** streaming and distributed systems platform engineer (Jordan),
**I want** to persist events into PostgreSQL, SQLite, or InMemory storage backends through a uniform `EventStore` port and read global event feeds,
**So that** streams are stored with optimistic concurrency control, historical feeds can be replayed and filtered, and database connection lifecycles are managed without resource leaks.

## Acceptance Criteria

```gherkin
Scenario: Append and load event stream in storage adapter
  Given a configured "EventStore" adapter instance
  When the caller appends 3 domain events to aggregate stream "order-123"
  Then loading the stream "order-123" returns 3 events ordered by version
  And each event preserves its payload, metadata, and timestamp.
```

```gherkin
Scenario: Replay events from global position offset
  Given an event store with 10 total committed events across multiple streams
  When the caller reads all events through "GlobalEventFeed" from position 5 with batch limit 5
  Then exactly 5 events starting after position 5 are returned in strictly ascending position order.
```

```gherkin
Scenario: Optimistic concurrency conflict detection on concurrent stream append
  Given an existing aggregate stream at version 3
  When a caller attempts to append new events specifying expected version 2
  Then an "ExpectedVersionError" is raised
  And no uncommitted events are appended to the event store.
```

```gherkin
Scenario: Filtered global feed query by aggregate type and tenant
  Given an event store containing committed events across multiple aggregate types and tenants
  When the caller queries the global feed with FeedReadOptions specifying aggregate type "Order" and tenant "tenant-alpha"
  Then only events matching both aggregate type "Order" and tenant "tenant-alpha" are returned
  And events from other types and tenants are omitted from the result set.
```

```gherkin
Scenario: Engine lifecycle ownership and clean connection pool disposal
  Given an EventStore initialized with "owns_engine=True"
  When the caller invokes "close()" on the store adapter
  Then the underlying database engine and connection pool are cleanly disposed
  And subsequent query attempts raise an EventStoreConnectionError.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0101, ADR-0119, ADR-0125, ADR-0136, ADR-0137, ADR-0149, ADR-0151, ADR-0152, ADR-0153
- **Verified Test Suites**:
  - `tests/unit/adapters/event_store/test_memory_event_store.py`: Verifies memory event store appends, stream reads, and global position monotonicity.
  - `tests/unit/adapters/sql/test_sqlite_event_store.py`: Verifies SQLite event storage, schema migrations, and connection lifetime.
  - `tests/unit/adapters/event_store/test_store_concurrency.py`: Verifies optimistic concurrency conflict detection and expected version enforcement.
  - `tests/unit/test_engine.py`: Verifies engine autocommit, rollback, and busy timeout behavior.
  - `tests/unit/ports/test_lifecycle.py`: Verifies `SupportsClose` protocol and engine disposal semantics based on `owns_engine`.
- **Architectural Invariants Verified**:
  - *Clean Storage Ports*: Storage adapters implement uniform `EventStore` and `GlobalEventFeed` protocols without exposing backend-specific leaking types.
  - *Pushdown Feed Queries*: `FeedReadOptions` passes filtering criteria directly to database query execution.
  - *Connection Pool Protection*: External engines are preserved on close when `owns_engine=False`.
