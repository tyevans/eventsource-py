# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2025-12-13

### Added

- **Tracer Protocol & Implementations** (`eventsource.observability.tracer`) - Composition-based tracing
  - `Tracer` protocol defining the contract for tracing implementations
  - `NullTracer` - No-op implementation for when tracing is disabled
  - `OpenTelemetryTracer` - Full OpenTelemetry integration when OTEL is available
  - `MockTracer` - Testing implementation for verifying trace calls
  - `create_tracer()` factory function for automatic tracer selection based on configuration
- **Serialization Module** (`eventsource.serialization`) - Centralized JSON utilities
  - `EventSourceJSONEncoder` for consistent JSON serialization across the library
  - `json_dumps()` and `json_loads()` helper functions
  - Proper handling of UUID, datetime, Enum, dataclass, and Pydantic model serialization
- **Handler Decorators** (`eventsource.handlers.decorators`) - Relocated and enhanced decorator
  - `@handles` decorator now in canonical location with full backward compatibility
  - `HandlerSignatureError` exception with detailed validation messages for invalid handler signatures
- **Repository Method Aliases** - Consistent naming conventions
  - `list_pending()` alias for `get_pending_events()` in OutboxRepository
  - `list_failed()` alias for `get_failed_events()` in DLQRepository
  - `get_by_id()` alias for `get_failed_event_by_id()` in DLQRepository
- **AsyncEventHandler ABC** - Consolidated to single definition in `eventsource.protocols`

### Changed

- **Tracing Architecture** - Migrated from inheritance to composition pattern
  - All 47+ traced classes now use `Tracer` composition instead of `TracingMixin` inheritance
  - Components accept optional `tracer` parameter for dependency injection
  - Enables easier testing with `MockTracer` and better separation of concerns
- **Handler Registry** - Improved validation and error messages
  - Better detection of invalid handler signatures
  - More descriptive error messages for common mistakes

### Tests

- Added comprehensive Tracer protocol tests (`tests/unit/observability/test_tracer.py`)
- Added handler decorator tests (`tests/unit/handlers/test_decorators.py`)
- Added handler registry tests (`tests/unit/handlers/test_registry.py`)
- Added serialization module tests (`tests/unit/serialization/test_json.py`)
- Added protocol consolidation tests (`tests/unit/test_protocols.py`)
- Added import compatibility tests (`tests/integration/test_imports.py`)
- Updated all existing tracing tests to use new composition pattern

## [0.3.1] - 2025-12-13

### Changed

- **Schema: `global_position` replaces `id` as primary key** - Events table now uses `global_position` as the primary key for strict sequential ordering, while `event_id` (UUID) remains as a unique constraint for deduplication and idempotency
  - PostgreSQL: `global_position BIGSERIAL PRIMARY KEY` with `event_id UUID NOT NULL UNIQUE`
  - SQLite: `global_position INTEGER PRIMARY KEY AUTOINCREMENT` with `event_id TEXT NOT NULL UNIQUE`
  - Updated all SQL templates, Alembic migration templates, and store implementations
  - Consistent naming across PostgreSQL and SQLite backends

### Fixed

- **SQLite store consistency** - SQLite event store now uses `global_position` column naming consistent with PostgreSQL, fixing column name mismatch between backends

## [0.3.0] - 2025-12-12

### Added

- **ReadModel Persistence Tooling** - Standardized read model persistence infrastructure (`eventsource.readmodels`)
  - **Phase 1 - Core Components**:
    - `ReadModel` base class with standard fields (id, timestamps, version, deleted_at)
    - `ReadModelRepository` protocol with 13 methods for CRUD, querying, and lifecycle management
    - `Query` and `Filter` classes for flexible, type-safe querying with operators (eq, ne, lt, gt, le, ge, in_, contains, startswith)
    - `InMemoryReadModelRepository` implementation for testing and development
  - **Phase 2 - SQL Backends**:
    - `PostgreSQLReadModelRepository` with full async support via asyncpg
    - `SQLiteReadModelRepository` with async support via aiosqlite
    - Schema generation utilities (`generate_postgresql_schema()`, `generate_sqlite_schema()`) for automatic table creation from ReadModel classes
  - **Phase 3 - Projection Integration**:
    - `ReadModelProjection` base class integrating with `DatabaseProjection`
    - `HandlerRegistry` integration with `@handles` decorator for event-driven updates
    - Automatic repository injection into event handlers
  - **Phase 4 - Enhanced Features**:
    - Soft delete support with `get_deleted()` and `find_deleted()` methods
    - Optimistic locking via `save_with_version_check()` for concurrent update safety
    - `OptimisticLockError` and `ReadModelNotFoundError` exceptions
  - Public exports from `eventsource.readmodels`: `ReadModel`, `ReadModelRepository`, `ReadModelProjection`, `Query`, `Filter`, `InMemoryReadModelRepository`, `PostgreSQLReadModelRepository`, `SQLiteReadModelRepository`
  - New observability attributes: `ATTR_READ_MODEL_TYPE`, `ATTR_READ_MODEL_ID`
- **Multi-Tenant Live Migration** - Zero-downtime tenant migration between event stores (`eventsource.migration`)
  - `MigrationCoordinator` orchestrating full migration lifecycle with pause/resume/abort controls
  - `BulkCopier` for streaming historical event migration with checkpointing and configurable batch sizes
  - `DualWriteInterceptor` for simultaneous writes to source and target stores during migration
  - `CutoverManager` for sub-100ms atomic tenant routing switch with rollback capability
  - `ConsistencyVerifier` for data integrity validation with COUNT, HASH, and FULL verification modes
  - `SubscriptionMigrator` for checkpoint position translation between stores
  - `TenantStoreRouter` for tenant-aware read/write routing during and after migration
  - `WritePauseManager` for coordinated write pausing during cutover
  - `SyncLagTracker` for monitoring replication lag between stores
  - Real-time status streaming via `StatusStreamer` for migration monitoring
  - Position mapping for checkpoint translation between source and target stores
  - Comprehensive error classification with retry policies and circuit breaker pattern
  - Audit logging for all migration operations
  - OpenTelemetry metrics integration (`eventsource.migration.metrics`)
- **PostgreSQL Advisory Locks** - Distributed locking for migration coordination (`eventsource.locks`)
  - `PostgreSQLAdvisoryLock` for session-level and transaction-level advisory locks
  - Lock context managers for safe acquisition and release
  - Lock timeout and retry configuration
- **Migration Exceptions** - Comprehensive exception hierarchy (`eventsource.migration.exceptions`)
  - `MigrationError`, `MigrationStateError`, `MigrationNotFoundError`
  - `BulkCopyError`, `DualWriteError`, `CutoverError`
  - `ConsistencyError`, `RoutingError`, `LockError`
  - Error classification with `ErrorCategory` and `ErrorSeverity` enums
- **Migration Documentation** - Comprehensive guides in `docs/migration/`:
  - Architecture overview and component documentation
  - Step-by-step migration guide
  - API reference for all migration components
  - Operational runbooks and troubleshooting guides
  - Monitoring and alerting setup

- **Subscription Tracing** - OpenTelemetry tracing for all subscription components
  - `SubscriptionManager` tracing for subscription lifecycle operations:
    - `subscribe`, `unsubscribe`, `start_subscription`, `stop`, `stop_subscription`
    - `pause_subscription`, `resume_subscription`
  - `TransitionCoordinator` tracing for catch-up to live transitions:
    - `execute` span with phase tracking (initial_catchup → live_subscribed → final_catchup → processing_buffer → live)
    - Watermark and buffer size attributes
  - `CatchUpRunner` tracing for historical event processing:
    - `run_until_position` span with batch progress
    - `deliver_event` span for individual event delivery
  - `LiveRunner` tracing for real-time event processing:
    - `start`, `stop`, `process_event` spans
    - `process_buffer`, `process_pause_buffer` for transition buffers
  - New subscription trace attributes in `eventsource.observability.attributes`:
    - `ATTR_SUBSCRIPTION_NAME`, `ATTR_SUBSCRIPTION_STATE`, `ATTR_SUBSCRIPTION_PHASE`
    - `ATTR_FROM_POSITION`, `ATTR_TO_POSITION`, `ATTR_BATCH_SIZE`
    - `ATTR_BUFFER_SIZE`, `ATTR_EVENTS_PROCESSED`, `ATTR_EVENTS_SKIPPED`, `ATTR_WATERMARK`
  - All components support `enable_tracing` parameter (default: `True`)
  - Graceful degradation when OpenTelemetry is not installed

- **Subscription Manager** - New `eventsource.subscriptions` module for building event-driven projections with catch-up subscriptions and live event streaming
  - `SubscriptionManager` class for coordinating subscriptions with unified lifecycle management
    - Automatic catch-up from event store historical data
    - Seamless transition to live event streaming via event bus
    - Multiple subscriber support with concurrent processing
    - Graceful shutdown with SIGTERM/SIGINT signal handling (`run_until_shutdown()`)
    - Pause/resume functionality for individual subscriptions
  - `Subscription` class with state machine for subscription lifecycle (idle → starting → catching_up → live → pausing → paused → resuming → stopping → stopped → failed)
  - `SubscriptionConfig` for configurable subscription behavior:
    - `batch_size`: Events per batch during catch-up (default: 100)
    - `checkpoint_interval`: Events between checkpoints (default: 100)
    - `checkpoint_strategy`: "batch" or "interval" checkpointing
    - `start_from`: Start position ("beginning", "end", or specific position)
    - `filter_event_types`: Optional event type filtering
  - Subscriber protocols and base classes:
    - `Subscriber` and `SyncSubscriber` protocols for event handlers
    - `BatchSubscriber` protocol for batch event processing
    - `BaseSubscriber`, `BatchAwareSubscriber`, and `FilteringSubscriber` base classes
  - Catch-up and live runner implementations:
    - `CatchupRunner` for reading historical events from event store with batching
    - `LiveRunner` for streaming real-time events from event bus
    - `TransitionCoordinator` for seamless handoff between modes
  - Comprehensive error handling (`eventsource.subscriptions.error_handling`):
    - `SubscriptionErrorHandler` with configurable retry policies
    - `ErrorSeverity` levels: low, medium, high, critical
    - `ErrorCategory` classification: event_processing, checkpoint, transition, infrastructure
    - Error callbacks: `on_error()` and `on_critical_error()` hooks
    - Circuit breaker pattern for failing subscriptions
  - Retry system (`eventsource.subscriptions.retry`):
    - Configurable retry with exponential backoff
    - Jitter support for distributed systems
    - Max retries and timeout limits
  - Health monitoring (`eventsource.subscriptions.health`):
    - `ManagerHealthChecker` for overall system health
    - `SubscriptionHealthChecker` for per-subscription health
    - Kubernetes-compatible liveness/readiness probes
    - `HealthStatus`, `LivenessStatus`, `ReadinessStatus` enums
    - Configurable health check thresholds
  - Metrics collection (`eventsource.subscriptions.metrics`):
    - Events processed, errors, lag, and processing duration metrics
    - Per-subscription and aggregate statistics
  - Flow control (`eventsource.subscriptions.flow_control`):
    - Backpressure handling for slow consumers
    - Rate limiting support
  - Graceful shutdown (`eventsource.subscriptions.shutdown`):
    - `ShutdownCoordinator` with phased shutdown sequence
    - Configurable shutdown timeout
    - In-flight event completion before shutdown
    - `FlowController.wait_for_drain()` for tracking in-flight events during shutdown
    - `ShutdownReason` enum for tracking shutdown triggers (SIGNAL_SIGTERM, SIGNAL_SIGINT, PROGRAMMATIC, HEALTH_CHECK, TIMEOUT, DOUBLE_SIGNAL)
    - Pre-shutdown hooks (`on_pre_shutdown()`) for cleanup before shutdown (e.g., load balancer deregistration)
    - Post-shutdown hooks (`on_post_shutdown()`) for actions after shutdown completes
    - Shutdown deadline support (`set_shutdown_deadline()`) for Kubernetes `terminationGracePeriodSeconds` compliance
    - Periodic checkpoint saves during drain phase (`checkpoint_interval` parameter)
    - Shutdown metrics with OpenTelemetry integration (`ShutdownMetricsSnapshot`)
  - Multi-instance coordination (`eventsource.subscriptions.coordination`):
    - `LeaderElector` protocol for distributed leadership election
    - `LeaderElectorWithLease` extended protocol for lease-based leadership
    - `InMemoryLeaderElector` implementation for single-instance and testing scenarios
    - `WorkRedistributionCoordinator` for coordinating work handoff during shutdown
    - `ShutdownNotification` and `HeartbeatMessage` for peer-to-peer coordination
    - Support for graceful work redistribution when instances shut down
  - Event filtering (`eventsource.subscriptions.filtering`):
    - Filter events by type, aggregate, or custom predicates
  - Global position support in event stores:
    - `PostgreSQLEventStore.subscribe_all_from_position()` for ordered event streaming
    - `SQLiteEventStore.subscribe_all_from_position()` for ordered event streaming
    - `global_position` field in stored events for total ordering
  - Database migrations for `checkpoints` table with position tracking
  - Comprehensive documentation: API reference, user guide, migration guide, and examples
  - Exception hierarchy: `SubscriptionError`, `SubscriptionConfigError`, `SubscriptionStateError`, `SubscriptionAlreadyExistsError`, `CheckpointNotFoundError`
- **TypeConverter Extraction** - Refactored serialization logic from event stores into a dedicated component
  - New `TypeConverter` protocol defining the contract for type conversion during event deserialization
  - `DefaultTypeConverter` implementation with configurable UUID and datetime field detection
  - `DEFAULT_UUID_FIELDS` and `DEFAULT_STRING_ID_FIELDS` constants for common field patterns
  - `DefaultTypeConverter.strict()` factory method for explicit-only UUID field configuration
  - SQLiteEventStore now has full configuration parity with PostgreSQLEventStore:
    - Added `uuid_fields`, `string_id_fields`, and `auto_detect_uuid` constructor parameters
    - Added `with_strict_uuid_detection()` factory method
  - Public exports from `eventsource.stores`: `TypeConverter`, `DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, `DEFAULT_STRING_ID_FIELDS`
  - 37 unit tests for comprehensive TypeConverter coverage

### Changed

- Removed ~120 lines of duplicate serialization code from PostgreSQL and SQLite event stores by extracting to shared `TypeConverter`

### Fixed

- **SQLiteOutboxRepository**: `get_pending_events()` now returns `list[OutboxEntry]` instead of `list[dict]`, matching the protocol specification
- **SQLiteOutboxRepository**: `add_event()` now properly stores and returns a UUID as the outbox ID, matching PostgreSQL behavior
- **SQLiteDLQRepository**: `get_failed_events()` now properly parses timestamp fields (`first_failed_at`, `last_failed_at`) from ISO 8601 strings to `datetime` objects
- **SQLiteDLQRepository**: `get_failed_event_by_id()` now properly parses timestamp fields (`first_failed_at`, `last_failed_at`, `resolved_at`) to `datetime` objects
- **SQLite schema**: Event outbox table now uses `TEXT PRIMARY KEY` for the `id` column (UUID as string) instead of `INTEGER PRIMARY KEY AUTOINCREMENT` to match PostgreSQL schema

### Breaking Changes (Internal)

- Internal methods `_is_uuid_field()` and `_convert_types()` on event stores have been removed
  - Users who were calling these internal methods directly should migrate to `store._type_converter.is_uuid_field()` and `store._type_converter.convert_types()`

### Documentation

- Added subscription manager user guide (`docs/guides/subscriptions.md`) covering:
  - Getting started with catch-up and live subscriptions
  - Basic usage patterns and configuration
  - Resilience patterns and error handling
  - Advanced patterns for production deployments
  - Troubleshooting guide
- Added subscription API reference (`docs/api/subscriptions.md`) with complete class and method documentation
- Added subscription migration guide (`docs/guides/subscription-migration.md`) for migrating from manual projection processing
- Added subscription examples (`examples/subscriptions/`) with:
  - Basic projection example
  - Multi-subscriber example
  - Resilient projection with error handling
- **Updated all projection examples to use SubscriptionManager pattern**:
  - `examples/projection_example.py` now demonstrates SubscriptionManager with catch-up, live subscriptions, and checkpoint tracking
  - `docs/getting-started.md` updated with SubscriptionManager as the recommended approach
  - `docs/examples/projections.md` now recommends SubscriptionManager over direct `event_bus.subscribe_all()`
  - `docs/examples/sqlite-usage.md` integration tests updated to use SubscriptionManager
- Added comparison table showing benefits of SubscriptionManager vs direct EventBus subscription
- Added Kubernetes deployment guide (`docs/guides/kubernetes-deployment.md`) covering:
  - Pod lifecycle integration and graceful shutdown
  - Health probe configuration (liveness, readiness, startup)
  - `terminationGracePeriodSeconds` configuration with shutdown deadline
  - Example Deployment, Service, and PodDisruptionBudget manifests
  - Spot instance and preemptible VM considerations (AWS, GCP, Azure)
  - Shutdown metrics and observability
  - Troubleshooting guide for common Kubernetes issues

### Tests

- Added comprehensive ReadModel persistence test suite:
  - Unit tests for all ReadModel components (`tests/unit/readmodels/`)
    - Base class and field validation tests
    - Query and Filter class tests
    - Repository protocol compliance tests
    - In-memory repository tests
    - PostgreSQL and SQLite repository tests
    - Schema generation tests
    - Projection integration tests
    - Handler registry integration tests
  - Integration tests (`tests/integration/readmodels/`)
    - Repository CRUD operations across all backends
    - Projection event handling flows
    - Enhanced features (soft delete, optimistic locking)
- Added comprehensive migration test suite (~950 new tests):
  - Unit tests for all migration components (`tests/unit/migration/`)
  - Integration tests for PostgreSQL locks and migration schema
  - Chaos tests for failure scenarios and recovery
  - Load testing benchmarks for performance validation
  - Phase integration tests for bulk copy, dual write, and cutover
- Added comprehensive subscription manager test suite:
  - Unit tests for all subscription components (`tests/unit/subscriptions/`)
  - Integration tests for catch-up, live, and transition flows (`tests/integration/subscriptions/`)
  - Resilience tests for error handling, retries, and recovery
  - Health check and metrics tests
  - Pause/resume functionality tests
  - Backpressure and flow control tests
  - Drain functionality tests (`test_drain.py`) for shutdown coordination
  - Coordination protocol tests (`test_coordination.py`) for leader election and work redistribution
  - Shutdown tests for pre/post hooks, deadline, metrics, and reason tracking

## [0.2.0] - 2025-12-08

### Added

#### Observability & Telemetry

- **Observability Module** (`eventsource.observability`) - Reusable OpenTelemetry utilities
  - `OTEL_AVAILABLE` constant for checking OpenTelemetry availability
  - `get_tracer()` and `should_trace()` helper functions
  - `@traced` decorator for method-level tracing
  - `TracingMixin` class for consistent tracing across components
- **Kafka Event Bus Metrics** - Comprehensive OpenTelemetry metrics
  - Counters: `messages.published`, `messages.consumed`, `handler.invocations`, `handler.errors`, `messages.dlq`, `connection.errors`, `reconnections`, `rebalances`, `publish.errors`
  - Histograms: `publish.duration`, `consume.duration`, `handler.duration`, `batch.size`
  - Gauges: `connections.active`, `consumer.lag` (per partition)
  - New `KafkaEventBusMetrics` class with `enable_metrics` config option
  - Less than 5% performance overhead
- **SQLiteEventStore Tracing** - `enable_tracing` parameter for `append_events` and `get_events` operations
- **InMemoryEventBus Tracing** - `enable_tracing` parameter for event dispatch and handler execution

#### Aggregate Snapshotting

- `Snapshot` dataclass for point-in-time aggregate state capture
- `SnapshotStore` interface with `InMemorySnapshotStore`, `PostgreSQLSnapshotStore`, and `SQLiteSnapshotStore` implementations
- `AggregateRepository` snapshot support: `snapshot_store`, `snapshot_threshold`, and `snapshot_mode` parameters
- `AggregateRoot.schema_version` for snapshot schema evolution with automatic invalidation
- `create_snapshot()` and `await_pending_snapshots()` methods
- Snapshot exceptions: `SnapshotError`, `SnapshotDeserializationError`, `SnapshotSchemaVersionError`, `SnapshotNotFoundError`
- Database migrations for `snapshots` table (PostgreSQL and SQLite)

#### Event Handling & Validation

- `EventVersionError` exception and configurable version validation via `AggregateRoot.validate_versions`
- `UnhandledEventError` exception with configurable handling via `unregistered_event_handling` attribute ("ignore", "warn", "error")
- `FlexibleEventHandler` and `FlexibleEventSubscriber` protocols for sync/async handler signatures
- Consolidated `eventsource.protocols` module as canonical location for protocol definitions

#### Database & Repository

- `DatabaseProjection` class for projections requiring raw database connection access
- `execute_with_connection` helper for consistent connection handling
- Configurable UUID field detection in `PostgreSQLEventStore` via `uuid_fields`, `string_id_fields`, and `auto_detect_uuid` parameters
- `PostgreSQLEventStore.with_strict_uuid_detection()` class method

#### Developer Experience

- Pre-commit hooks with ruff, mypy, and bandit
- GitHub Actions workflow for performance benchmarks with baseline tracking and PR comparison
- Shared test fixtures module (`tests/fixtures/`) with reusable components

### Changed

- Improved type annotations for better mypy compatibility
- Consolidated `@handles` decorator to `eventsource.projections.decorators` (old location deprecated)
- Consolidated protocol definitions to `eventsource.protocols` (old locations deprecated)
- Repository methods `get_pending_events`, `get_failed_events`, `get_failed_event_by_id` now return typed dataclasses (`OutboxEntry`, `DLQEntry`) instead of dicts
- Unified `get_events_by_type()` timestamp parameters to use `datetime` instead of `float`
- Refactored repositories to use `execute_with_connection` helper
- In-memory repositories now use `asyncio.Lock` for proper async concurrency

### Removed

- `SyncEventStore` abstract class (use `asyncio.run()` for sync access; see ADR-0007)

### Fixed

- Broken documentation links in ADRs and guides
- Mypy type errors in projections, repositories, and event bus modules
- `DeclarativeProjection` connection handling for proper transaction sharing

## [0.1.3] - 2025-12-07

### Documentation

- Added documentation badge linking to GitHub Pages
- Updated all documentation URLs to point to https://tyevans.github.io/eventsource-py
- Fixed mkdocs.yml site configuration with correct repository URLs
- Simplified README documentation section with links to hosted docs

## [0.1.2] - 2025-12-07

### Fixed

- Fixed release workflow version validation

## [0.1.1] - 2025-12-07

### Fixed

- Release infrastructure corrections

## [0.1.0] - 2025-12-07

### Added

- Initial release of eventsource-py library
- Event Store with PostgreSQL, SQLite, and In-Memory backends
- Domain Events with Pydantic validation
- Aggregate base class with optimistic concurrency control
- Projection system for building read models with checkpoint tracking
- Dead Letter Queue (DLQ) for failed event handling
- Snapshot support for aggregate state caching
- Multi-tenant support with tenant isolation
- Async-first API design throughout
- Comprehensive type hints and mypy compatibility
- Event registry for type-safe event deserialization
- Event Bus with In-Memory and Redis Streams backends
- Transactional Outbox pattern implementation
- `DatabaseProjection` class for projections requiring raw database connection access
- Pre-commit hooks configuration with ruff, mypy, and bandit

### Infrastructure

- PostgreSQL backend with connection pooling (asyncpg)
- SQLite backend for lightweight deployments, development, and testing
- Redis Streams backend for distributed event bus
- In-Memory backends for testing and development
- Automatic schema creation and migrations
- GitHub Actions CI/CD pipeline

[Unreleased]: https://github.com/tyevans/eventsource-py/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/tyevans/eventsource-py/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/tyevans/eventsource-py/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/tyevans/eventsource-py/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/tyevans/eventsource-py/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/tyevans/eventsource-py/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/tyevans/eventsource-py/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/tyevans/eventsource-py/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tyevans/eventsource-py/releases/tag/v0.1.0
