# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
