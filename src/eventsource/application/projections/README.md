# Projections (application ring)

Projection use cases: build read models from domain events, track checkpoints, and route permanently-failed events to a dead letter queue. Projections consume events and maintain denormalized views optimized for specific query patterns (the query side of CQRS).

## Key Interfaces

- `Projection` -- Abstract base class for projections
- `DeclarativeProjection` -- Projection with `@handles` decorator and tenant filtering
- `CheckpointTrackingProjection` -- Adds checkpoint, retry, and DLQ support
- `ProjectionCoordinator` -- Coordinates event distribution to multiple projections
- `ProjectionRegistry` -- Registry for managing projection collections
- `TenantFilter` -- Type alias for tenant filter (UUID, callable, or None)

`DatabaseProjection` (the SQL-backed subclass, taking `async_sessionmaker`) is an
adapter, not a use case, and lives in `eventsource.adapters.sql.projection`.

## Module Map

- `base.py` -- Core projection base classes
- `coordinator.py` -- `ProjectionCoordinator`, `ProjectionRegistry`, `SubscriberRegistry`
- `checkpoints.py` -- `record_checkpoint`, `read_checkpoint`, `lag_metrics_dict`, `reset_checkpoint`
- `dlq.py` -- `send_to_dlq`, `read_failed_events`
- `retry.py` -- `RetryPolicy`, `ExponentialBackoffRetryPolicy`

`checkpoints.py` and `dlq.py` replace the former `ProjectionCheckpointManager` and
`ProjectionDLQManager` classes (ADR 0024). Those managers were stateless wrappers
around one repository plus a tracer, holding no invariant of their own; the
functions take the repository and tracer as arguments instead of owning them.

## Invariants

- **Declarative routing**: `@handles(EventType)` decorator maps events to handler methods
- **Idempotent handlers**: Handlers must be idempotent (may receive same event multiple times)
- **Checkpoint per projection**: Each projection tracks its own position independently
- **Tenant filtering**: `DeclarativeProjection` with `tenant_filter` only processes matching tenant events
- **DLQ on repeated failure**: Events exceeding retry limit go to dead letter queue
- **`None` disables the concern**: `checkpoint_repo=None` means no checkpoint is
  written and `get_checkpoint()` / `get_lag_metrics()` return `None`.
  `dlq_repo=None` means permanent failures are logged at `CRITICAL` and re-raised,
  with no DLQ write. This is a deliberate behavior change from the old managers,
  which silently constructed an in-memory repository when none was given.
