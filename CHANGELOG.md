# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
- **Kafka Event Bus OpenTelemetry Metrics** - Comprehensive metrics support for the Kafka Event Bus
  - Counter metrics: `messages.published`, `messages.consumed`, `handler.invocations`, `handler.errors`, `messages.dlq`, `connection.errors`, `reconnections`, `rebalances`, `publish.errors`
  - Histogram metrics: `publish.duration`, `consume.duration`, `handler.duration`, `batch.size`
  - Observable gauge metrics: `connections.active` (connection status), `consumer.lag` (per partition)
  - New `KafkaEventBusMetrics` class for metric instrument management
  - `enable_metrics` configuration option (default: True)
  - Full documentation with PromQL queries, alerting recommendations, and Grafana dashboard examples
  - Less than 5% performance overhead when enabled
- **Observability Module** - New `eventsource.observability` module providing reusable OpenTelemetry tracing utilities
  - `OTEL_AVAILABLE` constant as single source of truth for OpenTelemetry availability
  - `get_tracer()` helper function for safely obtaining tracers
  - `should_trace()` helper for combining component and global tracing settings
  - `@traced` decorator for method-level tracing with minimal boilerplate
  - `TracingMixin` class providing `_init_tracing()` and `_create_span_context()` methods
- **SQLiteEventStore OpenTelemetry Tracing** - Added `enable_tracing` parameter (default: True)
  - Traces `append_events` and `get_events` operations with span attributes
  - Consistent with PostgreSQLEventStore tracing behavior
- **InMemoryEventBus OpenTelemetry Tracing** - Added `enable_tracing` parameter (default: True)
  - Traces event dispatch and handler execution
  - Refactored to use `TracingMixin` for reduced code duplication
- **Aggregate Snapshotting** - Performance optimization for long-lived aggregates with many events
  - `Snapshot` dataclass for capturing point-in-time aggregate state
  - `SnapshotStore` abstract interface with three implementations:
    - `InMemorySnapshotStore` for testing and development
    - `PostgreSQLSnapshotStore` for production with PostgreSQL (includes OpenTelemetry tracing)
    - `SQLiteSnapshotStore` for embedded/lightweight deployments
  - `AggregateRepository` enhanced with snapshot support via new parameters:
    - `snapshot_store`: Optional snapshot store for state caching
    - `snapshot_threshold`: Number of events between automatic snapshots
    - `snapshot_mode`: "sync", "background", or "manual" snapshot creation
  - `AggregateRoot.schema_version` class attribute for snapshot schema evolution
  - Automatic snapshot invalidation when schema version changes
  - `create_snapshot()` method for manual snapshot creation
  - `await_pending_snapshots()` for testing background snapshot operations
  - Snapshot-specific exceptions: `SnapshotError`, `SnapshotDeserializationError`, `SnapshotSchemaVersionError`, `SnapshotNotFoundError`
  - Database schema migrations for `snapshots` table (PostgreSQL and SQLite)
  - Comprehensive documentation: API reference, user guide, migration guide, and examples
- Pre-commit hooks configuration with ruff, mypy, and bandit for automated code quality checks
- GitHub Actions workflow for performance benchmarks with automatic baseline tracking and PR comparison
- `DatabaseProjection` class for projections requiring raw database connection access
- `execute_with_connection` helper for consistent connection handling across repositories
- `EventVersionError` exception for event version validation failures
- Configurable event version validation in `AggregateRoot` via `validate_versions` class attribute
- `UnhandledEventError` exception for unregistered event handling
- Configurable unregistered event handling in `DeclarativeAggregate` and `DeclarativeProjection` via `unregistered_event_handling` class attribute (options: "ignore", "warn", "error")
- New `eventsource.protocols` module as the canonical location for protocol definitions
- `FlexibleEventHandler` and `FlexibleEventSubscriber` protocols for handlers supporting both sync and async signatures
- Configurable UUID field detection in `PostgreSQLEventStore` via `uuid_fields`, `string_id_fields`, and `auto_detect_uuid` parameters
- `PostgreSQLEventStore.with_strict_uuid_detection()` class method for explicit-only UUID field configuration

### Removed

- Removed unused `SyncEventStore` abstract class from the public API. Users needing synchronous access can wrap async calls with `asyncio.run()`. See ADR-0007 for details.

### Changed

- Improved type annotations for better mypy compatibility across all modules
- Applied consistent code formatting with ruff
- Refactored DLQ, outbox, and checkpoint repositories to use `execute_with_connection` helper
- In-memory repositories now use `asyncio.Lock` for proper async concurrency safety
- Consolidated `@handles` decorator to a single canonical location (`eventsource.projections.decorators`), with deprecation warning for the old location in `eventsource.aggregates.base`
- Consolidated protocol definitions (`EventHandler`, `SyncEventHandler`, `EventSubscriber`) to a single canonical location (`eventsource.protocols`), with deprecation warnings for imports from old locations (`eventsource.bus.interface` and `eventsource.projections.protocols`)
- Repository methods `get_pending_events`, `get_failed_events`, and `get_failed_event_by_id` now return typed dataclasses (`OutboxEntry`, `DLQEntry`) instead of raw dictionaries. Dict-style access is supported with deprecation warnings for backward compatibility.
- Unified timestamp parameter types across `get_events_by_type()` methods to use `datetime` instead of `float` (Unix timestamp). Float values are still accepted for backward compatibility but emit a deprecation warning.

### Fixed

- Fixed broken documentation links in ADRs and guides
- Resolved mypy type errors in projections, repositories, and event bus modules
- Fixed `DeclarativeProjection` connection handling to properly share transactions with checkpoint updates

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
- Added comprehensive observability guide (`docs/guides/observability.md`) with:
  - Overview of OpenTelemetry tracing support
  - Component-by-component span name reference
  - Standard attributes reference (all ATTR_* constants)
  - TracingMixin usage guide for custom components
  - Distributed tracing with RabbitMQ and Kafka
  - Example configurations for Jaeger, Zipkin, OTLP, and Grafana Tempo
  - Best practices and troubleshooting guide
- Added comprehensive installation guide (`docs/installation.md`) with detailed documentation of optional dependencies, troubleshooting, and version compatibility
- Enhanced README installation section with extras table and links to installation guide
- Updated getting-started guide to reference installation documentation
- Updated MkDocs configuration
- Fixed links in getting-started guide and ADR documents

### Tests

- Added comprehensive subscription manager test suite:
  - Unit tests for all subscription components (`tests/unit/subscriptions/`)
  - Integration tests for catch-up, live, and transition flows (`tests/integration/subscriptions/`)
  - Resilience tests for error handling, retries, and recovery
  - Health check and metrics tests
  - Pause/resume functionality tests
  - Backpressure and flow control tests
- Improved test fixtures and integration test configuration
- Enhanced unit test coverage and organization
- Added concurrency tests for `InMemoryCheckpointRepository`, `InMemoryOutboxRepository`, and `InMemoryDLQRepository`
- Created shared test fixtures module (`tests/fixtures/`) with reusable event types, aggregate implementations, and pytest fixtures to reduce duplication across test files
- Added comprehensive performance benchmark suite with pytest-benchmark covering event store operations, projections, repositories, and serialization

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

[Unreleased]: https://github.com/tyevans/eventsource-py/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/tyevans/eventsource-py/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/tyevans/eventsource-py/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/tyevans/eventsource-py/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tyevans/eventsource-py/releases/tag/v0.1.0
