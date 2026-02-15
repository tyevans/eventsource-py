# Aggregates

Aggregate pattern implementations. Aggregates are the consistency boundaries in event sourcing, maintaining state by applying events and emitting new events when commands are executed.

## Key Interfaces

- `AggregateRoot[TState]` -- Base class for event-sourced aggregates
- `DeclarativeAggregate[TState]` -- Aggregate with `@handles` decorator support for event routing
- `AggregateRepository[TAggregate]` -- Repository for loading and saving aggregates
- `TState` -- Type variable for aggregate state (must be Pydantic BaseModel)

## Module Map

- `base.py` -- `AggregateRoot`, `DeclarativeAggregate`, core aggregate logic
- `repository.py` -- `AggregateRepository` for persistence operations
- `snapshot_manager.py` -- `SnapshotManager` for snapshot optimization
- `task_manager.py` -- `TaskManager` for async task coordination

## Invariants

- **Optimistic locking**: Aggregates use `expected_version` for concurrency control via `OptimisticLockError`
- **Event sourcing**: State is derived from events, never persisted directly (except snapshots)
- **Immutable state**: Use Pydantic's `model_copy(update=...)` to create new state instances
- **Uncommitted events**: `apply_event()` adds to `_uncommitted_events`; repository clears on save
- **Schema versioning**: Increment `schema_version` when state structure changes incompatibly
- **Declarative handlers**: `@handles(EventType)` decorator maps events to handler methods on `DeclarativeAggregate`
