# Aggregates (application ring)

Use-case layer for event-sourced aggregates: the repository that loads and
saves them, plus the collaborators that decide when and how snapshots are
taken. The aggregate types themselves (`AggregateRoot`, `DeclarativeAggregate`)
live one ring in, at `eventsource.domain.aggregate`.

## Key Interfaces

- `AggregateRepository[TAggregate]` -- Repository for loading and saving aggregates
- `TenantAwareRepository[TAggregate]` -- Wrapper that enforces tenant isolation on load, save, and existence checks
- `SnapshotPolicy` (`EveryNEvents`, `Never`) -- decides *when* a snapshot is taken
- `SnapshotScheduler` (`ImmediateScheduler`, `BackgroundScheduler`) -- decides *how* the snapshot write executes
- `take_snapshot()`, `read_valid_snapshot()` -- the single spellings of snapshot construction and load-path validation

## Module Map

- `repository.py` -- `AggregateRepository` class; composes the query, save, and snapshot mixins
- `repository_query.py` -- load, get, exists, and version reads; enforces the tenant scope on load
- `repository_save.py` -- append with optimistic locking, event dispatch, and snapshot scheduling after save
- `repository_snapshots.py` -- snapshot creation and pending-snapshot tracking for the repository
- `_load_guards.py` -- trust checks applied to a stream and its snapshot during load (ahead-of-stream, empty tail, restore failure, foreign events)
- `snapshotting.py` -- `SnapshotPolicy`, `SnapshotScheduler`, and the `take_snapshot`/`read_valid_snapshot` functions
- `snapshot_owner.py` -- `snapshot_owner()`: derives the owner tenant of a stream from its events
- `tenant_repository.py` -- `TenantAwareRepository` wrapper for write-path and read-path tenant isolation
- `tenant_ownership.py` -- owner probe (`owned_stream_version`) and append guard (`require_owned_append`) used by `TenantAwareRepository`

## Invariants

- **Optimistic locking**: Aggregates use `expected_version` for concurrency control via `OptimisticLockError`
- **Event sourcing**: State is derived from events, never persisted directly (except snapshots)
- **Immutable state**: Use Pydantic's `model_copy(update=...)` to create new state instances
- **Uncommitted events**: `apply_event()` adds to `_uncommitted_events`; repository clears on save
- **Schema versioning**: Increment `schema_version` when state structure changes incompatibly
- **Declarative handlers**: `@handles(EventType)` decorator maps events to handler methods on `DeclarativeAggregate`
- **Snapshots are disposable optimizations, never the source of truth** (ADR 0021): every automatic-path failure degrades to full event replay instead of raising
- **Policy vs. scheduler are independent knobs**: `snapshot_mode`/`snapshot_threshold` configure the built-in defaults; pass `snapshot_policy`/`snapshot_scheduler` directly for custom behavior (mutually exclusive with the mode/threshold knobs)
- **Stream owner tenant**: The owner of a stream is the tenant of its events. A stream whose events do not all share one tenant has no owner
- **Tenant-scoped load**: When a tenant scope is active, `load` raises `AggregateNotFoundError` for a stream with no events, a stream owned by another tenant, or a stream with untenanted events. This is the same error as for a missing stream. Without a scope, `load` does not check ownership
- **Tenant-scoped snapshots**: A snapshot records the owner tenant of its stream, or None when there is no single owner. Under a tenant scope, a snapshot with a different tenant is ignored and the stream is replayed
- **Snapshot identity and version checks**: A snapshot with a different aggregate ID or type is ignored. When the tail read after a snapshot returns no events, the snapshot is checked against the stream head. A snapshot whose version is past the head is discarded, and the stream is replayed from version 0
- **Foreign streams read as absent**: Under a tenant scope, `TenantAwareRepository.exists()` returns `False` and `get_version()` returns `0` for a stream that another tenant owns
- **Save ownership check**: Under a tenant scope, saving an existing aggregate checks that the active tenant owns its stream. The check raises `AggregateNotFoundError` for a foreign stream, and nothing is appended

## Limitations

- **Unscoped save of foreign events.** The ownership check runs only under a
  tenant scope. With no scope, `TenantAwareRepository.save()` does not check
  the stream owner. With the default `validate_on_save=True`, an unscoped save
  raises `TenantContextNotSetError`. With `validate_on_save=False`, it appends
  events of tenant B to a stream that tenant A owns. The events keep tenant B
  as their `tenant_id`, and the stream owner stays tenant A (the first event).
  `AggregateRepository` does no ownership check at all. Use
  `TenantAwareRepository` under a tenant scope on every write path.
- **`create_snapshot()` reads the full stream.** It reads every event of the
  stream to find the owner tenant, with no limit. For a long stream, this read
  is as large as a full replay.
