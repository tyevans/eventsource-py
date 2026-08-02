# Aggregates (application ring)

Use-case layer for event-sourced aggregates: the repository that loads and
saves them, plus the collaborators that decide when and how snapshots are
taken. The aggregate types themselves (`AggregateRoot`, `DeclarativeAggregate`)
live one ring in, at `eventsource.domain.aggregate`.

## Key Interfaces

- `AggregateRepository[TAggregate]` -- Repository for loading and saving aggregates
- `SnapshotPolicy` (`EveryNEvents`, `Never`) -- decides *when* a snapshot is taken
- `SnapshotScheduler` (`ImmediateScheduler`, `BackgroundScheduler`) -- decides *how* the snapshot write executes
- `take_snapshot()`, `read_valid_snapshot()` -- the single spellings of snapshot construction and load-path validation

## Module Map

- `repository.py` -- `AggregateRepository` for persistence operations
- `snapshotting.py` -- `SnapshotPolicy`, `SnapshotScheduler`, and the `take_snapshot`/`read_valid_snapshot` functions

## Invariants

- **Optimistic locking**: Aggregates use `expected_version` for concurrency control via `OptimisticLockError`
- **Event sourcing**: State is derived from events, never persisted directly (except snapshots)
- **Immutable state**: Use Pydantic's `model_copy(update=...)` to create new state instances
- **Uncommitted events**: `apply_event()` adds to `_uncommitted_events`; repository clears on save
- **Schema versioning**: Increment `schema_version` when state structure changes incompatibly
- **Declarative handlers**: `@handles(EventType)` decorator maps events to handler methods on `DeclarativeAggregate`
- **Snapshots are disposable optimizations, never the source of truth** (ADR 0021): every automatic-path failure degrades to full event replay instead of raising
- **Policy vs. scheduler are independent knobs**: `snapshot_mode`/`snapshot_threshold` configure the built-in defaults; pass `snapshot_policy`/`snapshot_scheduler` directly for custom behavior (mutually exclusive with the mode/threshold knobs)
