# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

- Added comprehensive installation guide (`docs/installation.md`) with detailed documentation of optional dependencies, troubleshooting, and version compatibility
- Enhanced README installation section with extras table and links to installation guide
- Updated getting-started guide to reference installation documentation
- Updated MkDocs configuration
- Fixed links in getting-started guide and ADR documents

### Tests

- Improved test fixtures and integration test configuration
- Enhanced unit test coverage and organization
- Added concurrency tests for `InMemoryCheckpointRepository`, `InMemoryOutboxRepository`, and `InMemoryDLQRepository`
- Created shared test fixtures module (`tests/fixtures/`) with reusable event types, aggregate implementations, and pytest fixtures to reduce duplication across test files
- Added comprehensive performance benchmark suite with pytest-benchmark covering event store operations, projections, repositories, and serialization

## [0.1.0] - 2025-XX-XX

### Added

- Initial release of eventsource library
- Event Store with PostgreSQL and In-Memory backends
- Domain Events with Pydantic validation
- Aggregate base class with optimistic concurrency control
- Projection system for building read models
- Snapshot support for aggregate state caching
- Multi-tenant support with tenant isolation
- Async-first API design throughout
- Comprehensive type hints and mypy compatibility
- Event registry for type-safe event deserialization

### Infrastructure

- PostgreSQL backend with connection pooling (asyncpg)
- In-memory backend for testing and development
- Automatic schema creation and migrations

[Unreleased]: https://github.com/[ORGANIZATION]/eventsource/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/[ORGANIZATION]/eventsource/releases/tag/v0.1.0
