---
id: '0011'
title: Coordinate Distributed Advisory Locks and Resilient Connection Lifecycle
status: Accepted
created: 2026-10-08
persona: Chris (The SRE / Resilience & Cutover Operator)
target_bc: locks
feature: FEAT-DISTRIBUTED-COORDINATION
governing_prd: PRD-0003
scenarios:
- PostgreSQL advisory lock manager provides mutually exclusive migration leases
- Session-level advisory lock auto-releases upon connection failure or worker crash
- SupportsClose protocol releases adapter-owned resources without disposing caller
  engine
- Store adapter with explicit engine ownership disposes connection pool upon close
- EventStoreConnectionError honestly wraps database driver failures with original
  cause
- EventSourceError universal base catches all domain and infrastructure library errors
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0105
- ADR-0111
---

# US-0011 — Coordinate Distributed Advisory Locks and Resilient Connection Lifecycle

## Governing PRD
- [`PRD-0003: Distributed Streaming and Subscription Coordination`](../../product/accepted/prd-0003-distributed-streaming-and-subscription-coordination.md)

## User Story

**As an** SRE and resilience operator (Chris),
**I want** to coordinate distributed operations via PostgreSQL session-level advisory locks, manage store connection lifecycles with explicit engine ownership, and receive honest connection exceptions rooted in `EventSourceError`,
**So that** concurrent migration operations maintain strict mutual exclusion without deadlock or orphaned leases, shared database connection pools are never prematurely disposed on close, and infrastructure failures report actionable diagnostic context.

## Acceptance Criteria

```gherkin
Scenario: PostgreSQL advisory lock manager provides mutually exclusive migration leases
  Given a PostgreSQLLockManager configured with an active PostgreSQL connection session
  When an operator process attempts to acquire an advisory lock for "cutover:<tenant-id>"
  Then the lock is granted via session-level advisory locking ("pg_try_advisory_lock")
  And any competing worker attempting to acquire the same lock key fails immediately or raises LockAcquisitionError
  And releasing the lock permits another worker to acquire it cleanly.
```

```gherkin
Scenario: Session-level advisory lock auto-releases upon connection failure or worker crash
  Given an operator process holding a PostgreSQL advisory lock through PostgreSQLLockManager
  When the process terminates unexpectedly or the underlying database connection drops
  Then PostgreSQL automatically frees all session-level advisory locks bound to that connection
  And surviving workers can acquire the lock immediately without stale lease timeouts or manual cleanup.
```

```gherkin
Scenario: SupportsClose protocol releases adapter-owned resources without disposing caller engine
  Given a PostgreSQLEventStore initialized with a caller-supplied AsyncEngine and default "owns_engine=False"
  When the store is closed via "SupportsClose.close()" during component teardown
  Then store-level statement caches and listeners are cleaned up
  And the caller-supplied AsyncEngine connection pool remains open and undamaged for other consumers.
```

```gherkin
Scenario: Store adapter with explicit engine ownership disposes connection pool upon close
  Given a PostgreSQLEventStore initialized with "owns_engine=True" or an adapter owning its connection
  When the application lifecycle invokes "SupportsClose.close()" on graceful shutdown
  Then the underlying connection pool or database connection is completely disposed
  And subsequent calls to "close()" execute idempotently without raising errors.
```

```gherkin
Scenario: EventStoreConnectionError honestly wraps database driver failures with original cause
  Given an event store adapter experiencing an underlying database disconnect or connection timeout
  When a store operation is executed by the application or migration coordinator
  Then EventStoreConnectionError is raised identifying the failing store adapter name
  And the underlying driver exception is attached as "__cause__"
  And the error inherits from EventStoreError rather than being misclassified under subscription errors.
```

```gherkin
Scenario: EventSourceError universal base catches all domain and infrastructure library errors
  Given operations executing across aggregates, event stores, advisory locks, and migration coordinators
  When an unexpected domain constraint, lock contention, or store connectivity error occurs
  Then the raised exception inherits from the universal base "EventSourceError"
  And operational perimeter error handlers catch all library failures with a single exception type.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0123, ADR-0129, ADR-0137, ADR-0144, ADR-0148, ADR-0153, ADR-0158
- **Verified Test Suites**:
  - `tests/unit/ports/test_locks.py`: Verifies advisory lock protocols, key generation, and timeout semantics.
  - `tests/unit/ports/test_lifecycle.py`: Verifies `SupportsClose` protocol and idempotency.
  - `tests/integration/locks/test_postgresql_locks_integration.py`: Verifies PostgreSQL session-level advisory locks and multi-worker contention.
  - `tests/unit/adapters/sqlite/snapshots.py`: Verifies SQLite connection ownership and cleanup.
- **Architectural Invariants Verified**:
  - *Automatic Session Unlock*: Session-level advisory locks auto-release on connection drop or crash.
  - *Engine Ownership Protection*: `owns_engine=False` leaves externally provided database engines intact.
  - *Honest Error Taxonomy*: Database connection exceptions wrap driver failures in `EventStoreConnectionError` rooted in universal `EventSourceError`.
