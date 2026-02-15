# Projections

Projection system for building read models from domain events. Projections consume events and maintain denormalized views optimized for specific query patterns (the query side of CQRS).

## Key Interfaces

- `Projection` -- Abstract base class for projections
- `DeclarativeProjection` -- Projection with `@handles` decorator and tenant filtering
- `DatabaseProjection` -- Projection with database connection support for handlers
- `CheckpointTrackingProjection` -- Adds checkpoint, retry, and DLQ support
- `ProjectionCoordinator` -- Coordinates event distribution to multiple projections
- `ProjectionRegistry` -- Registry for managing projection collections
- `TenantFilter` -- Type alias for tenant filter (UUID, callable, or None)

## Module Map

- `base.py` -- Core projection base classes
- `coordinator.py` -- `ProjectionCoordinator`, `ProjectionRegistry`, `SubscriberRegistry`
- `checkpoint_manager.py` -- `ProjectionCheckpointManager` for checkpoint persistence
- `dlq_manager.py` -- `ProjectionDLQManager` for dead letter queue
- `retry.py` -- `RetryPolicy`, `ExponentialBackoffRetryPolicy`
- `protocols.py` -- `AsyncEventHandler` protocol

## Invariants

- **Declarative routing**: `@handles(EventType)` decorator maps events to handler methods
- **Idempotent handlers**: Handlers must be idempotent (may receive same event multiple times)
- **Checkpoint per projection**: Each projection tracks its own position independently
- **Tenant filtering**: `DeclarativeProjection` with `tenant_filter` only processes matching tenant events
- **DLQ on repeated failure**: Events exceeding retry limit go to dead letter queue
- **Database projection transactions**: `DatabaseProjection` handlers receive a database connection; commit/rollback managed by base class
