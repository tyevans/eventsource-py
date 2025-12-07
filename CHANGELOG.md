# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/tyevans/eventsource-py/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tyevans/eventsource-py/releases/tag/v0.1.0
