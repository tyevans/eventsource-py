# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Tests

- Added comprehensive subscription manager test suite:
  - Unit tests for all subscription components (`tests/unit/subscriptions/`)
  - Integration tests for catch-up, live, and transition flows (`tests/integration/subscriptions/`)
  - Resilience tests for error handling, retries, and recovery
  - Health check and metrics tests
  - Pause/resume functionality tests
  - Backpressure and flow control tests

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

[Unreleased]: https://github.com/tyevans/eventsource-py/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/tyevans/eventsource-py/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/tyevans/eventsource-py/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/tyevans/eventsource-py/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/tyevans/eventsource-py/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tyevans/eventsource-py/releases/tag/v0.1.0
