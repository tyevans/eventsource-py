# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Pre-commit hooks configuration with ruff, mypy, and bandit for automated code quality checks
- `DatabaseProjection` class for projections requiring raw database connection access
- `execute_with_connection` helper for consistent connection handling across repositories
- `EventVersionError` exception for event version validation failures
- Configurable event version validation in `AggregateRoot` via `validate_versions` class attribute

### Changed

- Improved type annotations for better mypy compatibility across all modules
- Applied consistent code formatting with ruff
- Refactored DLQ, outbox, and checkpoint repositories to use `execute_with_connection` helper
- In-memory repositories now use `asyncio.Lock` for proper async concurrency safety

### Fixed

- Fixed broken documentation links in ADRs and guides
- Resolved mypy type errors in projections, repositories, and event bus modules
- Fixed `DeclarativeProjection` connection handling to properly share transactions with checkpoint updates

### Documentation

- Updated MkDocs configuration
- Fixed links in getting-started guide and ADR documents

### Tests

- Improved test fixtures and integration test configuration
- Enhanced unit test coverage and organization
- Added concurrency tests for `InMemoryCheckpointRepository`, `InMemoryOutboxRepository`, and `InMemoryDLQRepository`

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
