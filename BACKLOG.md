# Engineering Backlog

This document records active technical debt, design investigations, and planned improvements.
All items originated from codebase audits and retirement of the legacy issue tracker.

---

## Open Work Items

### Investigate Making SQLAlchemy an Optional Dependency (P3)

- **Problem**:
  SQLAlchemy is currently a required dependency in core packaging.
  Core domain and application modules do not require SQLAlchemy.

- **Technical Context**:
  - SQLAlchemy imports occur only in specific database adapters:
    - PostgreSQL adapters (`src/eventsource/adapters/postgresql/`): `locks.py`, `outbox.py`, `readmodels.py`, `snapshots.py`, and `store.py`.
    - Common SQL adapters (`src/eventsource/adapters/_sql/`): `connection.py`, `dialect.py`, and `engine.py`.
    - SQL adapters (`src/eventsource/adapters/sql/`): `checkpoints.py` and `dlq.py`.
    - SQL migration modules: `src/eventsource/adapters/sql/migration/`.
    - Alembic schema templates: `src/eventsource/adapters/sql/schemas/`.
  - Core interfaces do not import SQLAlchemy at the module level.
  - The Tier 0 import-linter contract and [`test_core_surface_purity.py`](file:///home/ty/workspace/eventsource-py/tests/unit/test_core_surface_purity.py) enforce interface purity.
  - Redis was removed from core dependencies in a prior task.

- **Required Actions**:
  - Move `sqlalchemy` from core dependencies to an optional package extra in packaging configuration.
  - Define functionality available in a default installation without optional extras.
  - Provide descriptive import error messages when an application accesses database adapters without SQLAlchemy.
  - Document installation instructions for database extras in project documentation.


### Deterministic or Scheduled Coverage for Bus Performance Assertions (P3)

- **Problem**:
  Message bus performance tests assert wall-clock execution limits.
  Shared CI runners cause non-deterministic timing measurements and false failures.
  These tests are excluded from blocking CI checks.
  Continuous integration currently does not monitor metric overhead.

- **Technical Context**:
  - Kafka and RabbitMQ duration and metric overhead tests use `@pytest.mark.benchmark`.
  - Wall-clock assertions fail intermittently on shared CI infrastructure.
  - For example, CI measured 55.79% overhead against a 20% limit on valid code paths.

- **Required Actions**:
  - Replace wall-clock duration assertions with deterministic proxies, such as counting instrumentation invocations.
  - Alternatively, create a scheduled non-blocking CI workflow that executes `-m benchmark` and records trend data.


### Small Ring-Consistency Cleanups (P3)

- **Problem**:
  Architectural transitions left minor structural inconsistencies across package boundaries and test assertions.

- **Technical Context**:
  - Hexagonal architecture rules govern ring boundaries (ADR-0002 and ADR-0102).
  - Impacted packages include `testing/conformance_ports/`, `observability/`, `protocols.py`, and `ports/readmodels/`.
  - Discovered during structure slice A and the engine relocation task on 2026-07-31.

- **Required Actions**:
  - Evaluate consolidating `testing/conformance_ports/` into `testing/conformance.py`, or document the namespace boundary.
  - Relocate the tracing decorator to a ports helper module so adapters do not import `observability/` directly.
  - Move remaining abstract base classes and protocols from `protocols.py` into their respective ring packages.
  - Provide temporary deprecation re-exports for moved protocols.
  - Update the AST-based port-purity test for `ports/readmodels/` to validate relative imports.


### Reconcile Dropped Live Events on Stop or Transition Failure (P3)

- **Problem**:
  When a subscription stops or fails during transition, the system drops buffered events without recording them as unseen.
  This behavior violates the lag reconciliation contract in `Subscription.lag` and leaves lag values permanently inflated.

- **Technical Context**:
  - The drop occurs in `_cleanup` within `src/eventsource/application/subscriptions/transition.py` (lines 392-402).
  - The drop also occurs in `stop()` within the subscription live runner.
  - Buffered events increment the seen counter upon receipt from the message bus.
  - Restarting an existing subscription object inherits inflated lag values.

- **Required Actions**:
  - Invoke `record_events_unseen` with the queue size (`qsize()`) when dropping transition or pause buffers.
  - Alternatively, reset both seen and processed counters at the start of transition execution.
  - Add tests to verify lag reconciliation when stopping and restarting subscriptions.


### Atomic Routing Switch for Migration Cutover (P2)

- **Problem**:
  Tenant cutover executes `set_routing()` and `set_migration_state(MIGRATED)` as two separate database writes.
  A process failure between these operations causes state inconsistency that requires manual operator recovery.

- **Technical Context**:
  - Tenant migration coordinator and `TenantRoutingRepository`.
  - ADR-0048 introduced compensating rollbacks (`_rollback`) to revert route changes on failure.
  - `TenantRoutingRepository` currently lacks multi-statement transaction support.

- **Required Actions**:
  - Extend the `TenantRoutingRepository` port to support multi-operation database transactions.
  - Update the migration coordinator to execute routing updates and migration state transitions in a single atomic transaction.


### Decompose Oversized Modules Exceeding Line Limits (P3)

- **Problem**:
  Multiple operational modules exceed project file length limits (governed by ADR-0002).

- **Technical Context**:
  - Affected modules include:
    - `SubscriptionManager` in `src/eventsource/application/subscriptions/`
    - `shutdown.py`
    - `migration/coordinator.py` in `src/eventsource/application/migration/`
  - ADR-0002 requires files under 500 lines and triggers proactive warnings at 400 lines.

- **Required Actions**:
  - Refactor `SubscriptionManager` into single-responsibility submodules under 400 lines.
  - Refactor `shutdown.py` into smaller lifecycle management components.
  - Decompose `migration/coordinator.py` into separate migration phase handlers.


### Non-Blocking Retry Delays for Kafka and RabbitMQ Consumers (P2)

- **Problem**:
  Retry backoff delays block consumers during message reprocessing.
  RabbitMQ consumers block inline during execution.
  Kafka consumers block the partition before `_process_message`.

- **Technical Context**:
  - Message bus consumers in `adapters/kafka/` and `adapters/rabbitmq/`.
  - Both consumers implement `RetryPolicy` per ADR-0048.
  - Synchronous backoff delays delay consumption of unrelated messages on the worker or partition.

- **Required Actions**:
  - Implement a dedicated retry topic and delay scheduler for Kafka consumers.
  - Implement dead-letter exchanges (DLX) and message time-to-live (TTL) delayed queues for RabbitMQ consumers.
  - Add integration tests verifying non-blocking throughput during retry backoff periods.


### Transactional Outbox Support for SQLite Adapter (P2)

- **Problem**:
  `adapters/sqlite/store.py` lacks transactional outbox support (`outbox_enabled`).
  `adapters/postgresql/store.py` provides transactional outbox support.
  Applications experience differing delivery guarantees when switching between backends.

- **Technical Context**:
  - `adapters/sqlite/store.py` and `adapters/postgresql/store.py`.
  - The gap is currently documented in the event bus guide per ADR-0048.
  - Resolving this gap requires schema changes and transactional boundary alignment.

- **Required Actions**:
  - Define outbox schema tables and migrations for the SQLite adapter.
  - Implement transactional outbox publishing in `adapters/sqlite/store.py`.
  - Add conformance tests to verify consistent outbox behavior across SQLite and PostgreSQL adapters.


### Diagnosability and Lifecycle Management for Connection-Owning Adapters (P2)

- **Problem**:
  Unclosed instances of `SQLiteSnapshotStore` leak worker threads managed by `aiosqlite`.
  When the event loop closes, `aiosqlite` worker threads raise unhandled `RuntimeError: Event loop is closed` errors.
  The error traceback does not identify the leaked store instance, file path, or allocation site.
  Pytest attributes these failures to unrelated subsequent tests as unhandled thread warnings.

- **Technical Context**:
  - `SQLiteSnapshotStore` in `src/eventsource/adapters/sqlite/snapshots.py` (governed by ADR-0053).
  - `aiosqlite` manages connections using non-daemon worker threads.
  - Store classes implement the `SupportsClose` protocol.
  - Upstream upgrade audits identified three leak sites: two in test fixtures and one in a consumer factory function.

- **Required Actions**:
  - Implement a `__del__` finalizer on `SQLiteSnapshotStore` that emits a `ResourceWarning` containing the database file path when unclosed.
  - Implement asynchronous context manager support (`__aenter__` and `__aexit__`) to support `async with` blocks.
  - Document explicit connection lifecycle requirements in the class docstring.
  - Audit test fixtures and factories to ensure callers close all store instances.


### Validate Aggregate Identity in Event Stamping (P2)

- **Problem**:
  `_stamp` does not verify that an event `aggregate_id` matches the aggregate instance `self.aggregate_id`.
  When a command specifies an explicit target identifier, mismatched events can append to the stream.
  This mismatch corrupts event stream integrity.

- **Technical Context**:
  - Command execution in `DeciderAggregate.execute` and event stamping in `_stamp`.
  - Consequence of ADR-0056 (relocating aggregate identifiers from `initial_state()` to command payloads).
  - `_stamp` already validates `aggregate_type` via `_reject_divergent_aggregate_type`, but does not validate `aggregate_id`.

- **Required Actions**:
  - Add an identity check in `_stamp` to compare explicit event `aggregate_id` against `self.aggregate_id`.
  - Reject mismatched identifiers by raising `CommandRejectedError`.
  - Add unit tests verifying that `DeciderAggregate` rejects commands that produce mismatched aggregate identifiers.

---

## Closed Work Items

### Closed in Sweep (2026-08-04)

| Item | Resolution | Verification |
|---|---|---|
| Read model aliasing in memory repository | Updated repository so read operations return copies and write operations store copies. | Verified in `ReadModelRepositoryConformance` across all backends (ADR-0050). |
| Duplicate `OptimisticLockError` naming | Renamed read-model error to `ReadModelVersionConflictError` without compatibility shims. | Verified in exception hierarchy and test suite (ADR-0050). |
| Duplicate `ExpectedVersion` dispatch | Consolidated four separate adapter implementations into `adapters/_common/`. | Verified in `src/eventsource/adapters/_common/` (ADR-0051). |
| Core surface purity verification | Added runtime check and expanded static contract driver list to six drivers. | [`test_core_surface_purity.py`](file:///home/ty/workspace/eventsource-py/tests/unit/test_core_surface_purity.py) and `docs/core-surface.md`. |
| Store identifier uniqueness documentation | Documented uniqueness requirements on `Position` and adapter defaults. Preserved defaults to protect checkpoints. | Verified in `Position` docstring and adapter documentation. |
| Ambiguous logging in `run_resync_pass` | Updated log messages to differentiate empty batch outcomes from full completion. | Verified in migration coordinator logging. |
| Stale Mutmut configuration selectors | Retargeted selectors away from deleted `repositories/` package. | Verified by [`test_mutmut_configure.py`](file:///home/ty/workspace/eventsource-py/tests/unit/test_mutmut_configure.py). |

### Closed Prior to Sweep (2026-08-04)

| Item | Resolution | Verification |
|---|---|---|
| Concurrent SQLite read isolation | Isolated read queries under the append lock to prevent dirty reads. | Verified in `tests/unit/adapters/test_sqlite_read_isolation.py`. |
| Dead letter queue cutoff semantics | Standardized both storage backends on rolling instant timestamps in `delete_resolved_events`. | Specified and verified in `ports/dlq.py`. |
| Catch-up premature termination | Added `envelopes_read` counter alongside `events_delivered` in `_BatchOutcome`. | Verified in catch-up batch processing tests. |
| Position mapper in copier coordinator | Replaced duplicate construction call sites with unified `_build_copier` factory. | Verified in migration coordinator. |
| Missing resync for dual-write mirror failures | Added `run_resync_pass` to reconcile primary and mirror stores. | Verified in dual-write replication tests. |
| Cutover lag tolerance threshold | Set default `cutover_max_lag_events` to 0 (strict zero lag). | Verified in cutover configuration and tests. |
| Relocate subscriptions package | Moved `subscriptions/` package from root namespace to application ring. | Completed under ADR-0032. |
| Remove bus facade compatibility shims | Migrated message bus components to ports and adapters. Removed legacy shims. | Completed under ADR-0031. |
| Remove locks and read models shims | Removed deprecated shim modules for locks and read models. | Completed under ADR-0030. |
| Ring placement for `engine.py` | Assigned `engine.py` to its permanent architectural ring. | Completed under ADR-0029. |
| Audit findings critical defects | Resolved three critical audit defects from 2026-08-02 review across adapters, application, and DX. | Completed in PR #112, PR #113, PR #114, and ADR-0048. |
| PostgreSQL catch-up horizon benchmark at scale | Verified index scans on `events_pkey` up to 1M events under concurrent appends (>40k events/sec throughput). | TASK-0004, `docs/reference/postgresql-catchup-benchmark.md`, `tests/benchmarks/test_postgresql_catchup.py`. |
| Delta compression for stored event payloads | Implemented `DeltaCodec` and `DeltaPayload` with RFC 1950 compression, CRC32 checksums, and SHA-256 verification (>35x compression). | TASK-0002, `docs/reference/delta-compression.md`, `tests/benchmarks/test_delta_compression.py`. |
