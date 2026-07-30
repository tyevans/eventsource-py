# 0017. Snapshot Strategy Pattern

Snapshots make loading a long-lived aggregate cheap: instead of replaying every
event from version 0, the repository restores serialized state at version N and
replays only what came after. This ADR explains why the *when* and *how* of
snapshot creation live behind a `SnapshotStrategy` protocol rather than inside
`AggregateRepository`, and why nearly every snapshot failure path degrades
silently instead of raising.

## Status

Accepted. Implemented in `src/eventsource/snapshots/strategies.py`
(`SnapshotStrategy`, `BaseSnapshotStrategy`, `ThresholdSnapshotStrategy`,
`BackgroundSnapshotStrategy`, `NoSnapshotStrategy`, `create_snapshot_strategy`)
and `src/eventsource/aggregates/snapshot_manager.py`
(`AggregateSnapshotManager`), wired up by `AggregateRepository.__init__` in
`src/eventsource/aggregates/repository.py`.

## Context

### Snapshot creation logic lived inside AggregateRepository

`AggregateRepository` already carries load, save, optimistic-locking, event
publication and tracing responsibilities. Snapshot creation added a further
concern that had nothing to do with the repository's core job: deciding whether
version 300 is a snapshot boundary, serializing state, writing to the snapshot
store, and swallowing the resulting errors.

### Adding a snapshot mode meant editing the repository (Open/Closed violation)

The public knob is a mode string — `snapshot_mode="sync" | "background" |
"manual"` on the repository constructor. When that string was interpreted by
branching inside the save path, a fourth mode (say, time-based or size-based
snapshotting) could not be added without editing repository code that was
already correct for the existing three.

### Three distinct timing behaviors were needed: sync, background, manual

The three modes differ only in *timing*, not in what a snapshot is:

- **sync** — write the snapshot before `save()` returns; predictable, but the
  snapshot write is on the caller's latency path.
- **background** — schedule the write and return immediately; keeps save latency
  flat under load.
- **manual** — never write automatically; the application decides when a
  snapshot is worth taking (a business milestone, an end-of-day close).

## Decision

### Extract a `SnapshotStrategy` Protocol (`snapshots/strategies.py`)

`SnapshotStrategy` is a `@runtime_checkable` `Protocol` with exactly two
members. Structural typing means an application can supply its own strategy
without importing or subclassing anything from the library.

#### `should_snapshot()` — the when

`should_snapshot(aggregate, events_since_snapshot) -> bool` is synchronous and
side-effect free. It is asked once per save, after the events are durably
written.

#### `execute_snapshot()` — the how

`execute_snapshot(aggregate, snapshot_store, aggregate_type) -> Snapshot | None`
is async and performs (or schedules) the write. Its `None` return is
overloaded on purpose: it means "skipped", "deferred to a background task", or
"attempted and failed" — the caller is not expected to distinguish.

### `BaseSnapshotStrategy` ABC holds the shared `_create_snapshot()` body

Every strategy that writes a snapshot does the same four things: read
`schema_version` off the aggregate class (defaulting to `1`), call
`aggregate._serialize_state()`, build a `Snapshot` stamped with
`datetime.now(UTC)`, and `await snapshot_store.save_snapshot(...)`.
`BaseSnapshotStrategy` implements that once as `_create_snapshot()` and
declares `execute_snapshot()` abstract. It also supplies the default
threshold-boundary `should_snapshot()` and a read-only `threshold` property.

### Three concrete strategies: Threshold (sync), Background, No-op (manual)

- `ThresholdSnapshotStrategy` awaits `_create_snapshot()` directly inside
  `execute_snapshot()`.
- `BackgroundSnapshotStrategy` wraps `_create_snapshot()` in
  `_create_snapshot_background()`, spawns it with `asyncio.create_task`, appends
  the task to `_pending_tasks`, prunes completed tasks, and returns `None`.
- `NoSnapshotStrategy` overrides `should_snapshot()` to return `False`
  unconditionally and `execute_snapshot()` to return `None`.

### Version-boundary predicate: `version > 0 and version % threshold == 0`

The default rule keys off the aggregate's *version*, not the
`events_since_snapshot` argument. With `threshold=100`, snapshots land at
versions 100, 200, 300. This makes snapshot points deterministic and
repository-independent: two processes saving the same aggregate agree on where
the boundaries are. A `threshold` of `None` disables the predicate entirely
(returns `False`). Note the consequence: a save that jumps the aggregate from
version 98 to 102 straddles the boundary without landing on it, and no snapshot
is taken until version 200 — acceptable because snapshots are an optimization,
not a guarantee.

### `create_snapshot_strategy(mode, threshold)` factory preserves the mode-string API

The factory maps `"sync"` → `ThresholdSnapshotStrategy`, `"background"` →
`BackgroundSnapshotStrategy`, `"manual"` → `NoSnapshotStrategy` (constructed
without a threshold), and raises `ValueError` for anything else. This is the
one place that understands mode strings; `AggregateRepository.__init__` calls it
once when a `snapshot_store` is supplied and keeps only the resulting object.
The ergonomic `snapshot_mode=` constructor argument survives, but the branching
does not.

### `AggregateSnapshotManager` owns load/validate; the strategy owns create

`AggregateSnapshotManager` sits between the repository and the strategy. It
holds the store, the aggregate type name, an optional strategy, and a tracer,
and exposes:

- `load_valid_snapshot()` — fetch and validate on the read path (no strategy
  involvement).
- `maybe_create_snapshot()` — short-circuit if there is no strategy or
  `should_snapshot()` says no, otherwise open a span and delegate to
  `execute_snapshot()`.
- `create_snapshot()` — unconditional, for manual mode.
- `pending_count` / `await_pending()` — forwarded to the strategy when it is a
  `BackgroundSnapshotStrategy`, otherwise `0`.

## Rationale

### Snapshots are disposable optimizations, not the source of truth

#### Events remain authoritative; snapshots are regenerable cache entries

The event stream is the system of record. A snapshot is a memoized fold over a
prefix of that stream. Deleting every snapshot in the database changes load
latency and nothing else.

#### Consequence: every snapshot failure path degrades rather than raises

Because the correct answer is always recoverable from events, a failed snapshot
write or an unreadable snapshot should never surface as an application error.
The worst outcome of any snapshot failure is a slower load.

### Failures are swallowed by design

#### Sync strategy catches and logs, returns `None`

`ThresholdSnapshotStrategy.execute_snapshot()` wraps `_create_snapshot()` in a
bare `except Exception`, emits a `logger.warning` with `exc_info=True`, and
returns `None`. A snapshot-store outage therefore cannot fail a `save()` whose
events were already committed.

#### Background strategy is fire-and-forget via `asyncio.create_task`

The task body `_create_snapshot_background()` has the same catch-log-continue
shape, so a failing background snapshot never becomes an unretrieved-exception
warning from the event loop.

#### `await_pending()` exists so tests can join the fire-and-forget tasks

Fire-and-forget is untestable without a join point. `await_pending()` gathers
`_pending_tasks` with `return_exceptions=True`, logs anything unexpected at
`error`, clears the list, and returns how many tasks it awaited. It is also
useful for graceful shutdown. `pending_count` prunes completed tasks before
reporting.

### Schema-version mismatch degrades to full replay, not an error

#### `load_valid_snapshot()` returns `None` on mismatch, store error, or missing snapshot

All three conditions collapse to the same signal. A store exception is caught
and logged at `warning`; a missing snapshot returns `None` silently; a
`snapshot.schema_version != aggregate_factory.schema_version` mismatch is logged
at `info` and returns `None`. The repository's fallback is identical in every
case: replay from version 0.

#### Why `SnapshotSchemaVersionError` exists but is not raised on the load path

`SnapshotSchemaVersionError` (and its siblings `SnapshotDeserializationError`,
`SnapshotNotFoundError`, under the `SnapshotError` base) is defined and
exported from `eventsource`, but nothing in the library's load path raises it —
`load_valid_snapshot()` logs and returns `None` instead. The type exists so
that a custom `SnapshotStore` or a caller with stricter requirements can signal
the condition precisely, and so applications can catch the whole family with a
single `except SnapshotError`.

#### Bulk invalidation via `delete_snapshots_by_type(schema_version_below=...)`

When an aggregate's state model changes and its `schema_version` is bumped,
existing snapshots are dead weight — they will be read, rejected, and ignored on
every load. `SnapshotStore.delete_snapshots_by_type(aggregate_type,
schema_version_below=N)` clears them in one operation, turning a slow-but-correct
state into a clean one.

## Consequences

### Positive: new modes are additive; strategies are unit-testable in isolation

A new timing behavior is a new `BaseSnapshotStrategy` subclass plus one entry in
the factory table; no existing strategy, the manager, or the repository changes.
Each strategy can be exercised with a fake store and a stub aggregate, with no
event store, session factory, or repository in play.

### Negative: silent failure means snapshot loss is only visible in logs/metrics

If the snapshot store is misconfigured, nothing breaks and nothing complains
loudly. Loads get progressively slower as streams grow, and the only in-band
evidence is warning-level log lines from `eventsource.snapshots.strategies`.

### Negative: background mode has no backpressure bound on `_pending_tasks`

`BackgroundSnapshotStrategy` appends to `_pending_tasks` without a cap. Pruning
only removes tasks that are already `done()`. Under a burst where snapshot
writes are slower than saves crossing thresholds, the list grows unbounded.

### Negative: two spellings of snapshot creation (strategy and manager) must stay in sync

`BaseSnapshotStrategy._create_snapshot()` and
`AggregateSnapshotManager.create_snapshot()` build the `Snapshot` object with
the same fields and the same `schema_version` lookup, in two places. The
manager's copy adds a tracing span and returns a non-optional `Snapshot`; the
strategy's copy is the one wrapped in failure handling. A change to snapshot
construction has to be applied to both.

### Operational guidance: monitor snapshot-failure log lines; snapshot absence is not an outage

Alert on the `Failed to create snapshot`, `Background snapshot creation failed`,
and `Error loading snapshot` warnings rather than on aggregate load errors —
the latter will never fire for snapshot reasons. Treat a sustained rise in
those lines as a latency problem to schedule, not a correctness incident to
page on. After bumping an aggregate's `schema_version`, expect the
`schema version mismatch` info lines until snapshots are regenerated, and use
`delete_snapshots_by_type(..., schema_version_below=...)` to cut the noise.

## Alternatives Considered

### Keep `snapshot_mode` branching in `AggregateRepository`

Simplest, and honest about there being only three modes. Rejected because the
repository was already the largest-responsibility class in `aggregates/`, and
each mode's error handling differs enough (synchronous try/except vs. task
lifecycle management) that inlining all three obscured the save path. The mode
string was kept as the public API precisely so this refactor cost callers
nothing.

### Raise on snapshot failure instead of degrading

Rejected: it inverts the value proposition. Events are already committed by the
time a snapshot is attempted, so raising would fail an operation that
*succeeded*, and would make an optional performance feature a new source of
availability risk. Callers who need strictness can use manual mode and call
`AggregateSnapshotManager.create_snapshot()`, which does propagate store errors.

### Auto-migrate snapshot state across schema versions instead of replaying

Rejected: an upcast function per schema transition is real code that must be
written, tested and kept forever, and a bug in it silently corrupts loaded
state. Replay is free, always correct, and self-healing — the next threshold
boundary writes a snapshot at the new schema version.

## References

- `src/eventsource/snapshots/strategies.py` — protocol, base class, three
  strategies, factory
- `src/eventsource/aggregates/snapshot_manager.py` — `AggregateSnapshotManager`
- `src/eventsource/aggregates/repository.py` — `snapshot_mode` /
  `snapshot_threshold` wiring
- `src/eventsource/snapshots/interface.py` — `Snapshot`, `SnapshotStore`,
  `delete_snapshots_by_type`
- `src/eventsource/snapshots/exceptions.py` — `SnapshotError` hierarchy
- `tests/unit/aggregates/test_repository_snapshot.py`,
  `tests/unit/snapshots/` — behavioral coverage
