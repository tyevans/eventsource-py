# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
