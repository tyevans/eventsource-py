# Aggregates in the Application Ring: Snapshotting Without a Manager

**Date:** 2026-07-30
**Status:** Approved (design reviewed in session; pending implementation plan)
**Sub-project:** first slice of sub-project 2 (application layer) in the Clean
Architecture redesign (`2026-07-29-core-rings-design.md`)

## Context

Sub-project 1 (core rings) delivered `domain/`, `ports/`, and the
memory/sqlite/postgresql store+snapshot adapters (PR #77). This slice starts
sub-project 2 by moving the aggregates vertical into the rings and dissolving
`AggregateSnapshotManager`.

The manager is a DDD anti-pattern by name and by construction: it tangles four
responsibilities (read-path snapshot loading/validation, policy-gated creation,
manual creation, background-task introspection), duplicates snapshot
construction with `BaseSnapshotStrategy._create_snapshot()` (a documented
negative in ADR-0017), and reaches into strategy internals via
`isinstance(strategy, BackgroundSnapshotStrategy)` checks (an LSP violation).
The `SnapshotStrategy` protocol itself merges two questions — *when* to
snapshot and *how* (sync vs background) — that vary independently (an ISP
violation: `NoSnapshotStrategy` implements an `execute_snapshot` that can
never run).

The library is unreleased, so transition shims are cost without benefit: this
slice removes them outright rather than aliasing.

## ADR Impact

Per `.claude/rules/definition-of-done.md`:

| ADR | Disposition |
|-----|-------------|
| 0001 async-first design | **Stands.** All new collaborators keep the async write/read paths; policies are sync pure predicates. |
| 0012 event-type auto-derivation | **Stands.** Untouched. |
| 0015 optional-dependency extras | **Stands.** No dependency changes. |
| 0016 optional tracing no-op | **Stands.** Repository keeps its tracer; spans move with the code they wrap. |
| 0017 snapshot strategy pattern | **Superseded by ADR 0021.** The strategy protocol, three strategy classes, `create_snapshot_strategy`, and `AggregateSnapshotManager` are replaced by policy + scheduler + single construction path. ADR-0017 Status gains a "Superseded by 0021" pointer; its body is immutable. |
| 0019 clean-architecture store ports | **Stands.** This slice consumes it: `Snapshot`/`SnapshotStore` physically relocate to `ports/snapshots.py` as that ADR's layout intends. Repository-to-store-port migration remains future work. |
| 0020 broker backend decomposition | **Stands.** The 0.8.0-scheduled deprecation shim removals it anticipated happen here. |

New: **ADR 0021 — Snapshotting as policy + scheduler composition (no
manager)**, written as part of this work.

## Package Changes

```
src/eventsource/
  domain/
    aggregate.py           # AggregateRoot, DeclarativeAggregate (from aggregates/base.py)
  application/             # NEW: use-case ring
    __init__.py
    aggregates/
      __init__.py
      repository.py        # AggregateRepository (slimmed, composes snapshotting)
      snapshotting.py      # SnapshotPolicy, EveryNEvents, Never,
                           #   SnapshotScheduler, ImmediateScheduler, BackgroundScheduler,
                           #   take_snapshot(), read_valid_snapshot()
  ports/
    snapshots.py           # Snapshot VO + SnapshotStore protocol, physically here
                           #   (contents of snapshots/interface.py; alias ends)
  exceptions.py            # + SnapshotError hierarchy (from snapshots/exceptions.py)
```

Deleted (no aliases — unreleased software):

- `aggregates/` (base.py → domain, repository.py → application, snapshot_manager.py dissolved, README updated/moved)
- `snapshots/` entire package: `interface.py` → `ports/snapshots.py`;
  `exceptions.py` → top-level `exceptions.py`; `strategies.py` dissolved;
  `in_memory.py`/`postgresql.py`/`sqlite.py` alias modules → consumers import
  `eventsource.adapters.*` directly
- Deprecation shims already scheduled for 0.8.0: deprecated paths in
  `bus/memory.py` (2), `bus/kafka/bus.py` (2), `repositories/_json.py` (1)

All internal imports move to the new locations. The top-level public API is
unchanged in names: `AggregateRoot`, `DeclarativeAggregate`,
`AggregateRepository`, `Snapshot`, `SnapshotStore`, `InMemorySnapshotStore`,
and the `SnapshotError` hierarchy keep their `eventsource` re-exports (source
modules change). `eventsource.aggregates`, `eventsource.snapshots` import
paths cease to exist.

## Snapshotting Design

The manager's four jobs become four honest pieces in
`application/aggregates/snapshotting.py`:

### `SnapshotPolicy` — the *when*

```python
@runtime_checkable
class SnapshotPolicy(Protocol):
    def should_snapshot(self, aggregate: AggregateRoot[Any], events_since_snapshot: int) -> bool: ...
```

Sync, side-effect free. Implementations: `EveryNEvents(n)` keeps ADR-0017's
deterministic version-boundary predicate (`version > 0 and version % n == 0`);
`Never()` for manual mode. Custom policies (state-based, time-based) are
user-supplied implementations — the OCP win of ADR-0017 is preserved with a
smaller surface.

### `SnapshotScheduler` — the *sync vs background*

```python
@runtime_checkable
class SnapshotScheduler(Protocol):
    async def schedule(self, write: Coroutine[Any, Any, Snapshot]) -> Snapshot | None: ...
    @property
    def pending_count(self) -> int: ...
    async def await_pending(self) -> int: ...
```

`ImmediateScheduler` awaits the write, catch-log-degrade (ADR-0017 rationale
stands: snapshots are disposable optimizations; a failed write never fails a
save whose events committed). `BackgroundScheduler` submits to the existing
`_internal.background_tasks.BackgroundTaskManager`. **Every scheduler
implements the full surface** — `ImmediateScheduler.pending_count` is 0,
`await_pending()` returns 0 — so no caller ever isinstance-sniffs the
concrete type.

### `take_snapshot(aggregate, aggregate_type, store) -> Snapshot`

Module function; the single spelling of construction: read `schema_version`
off the aggregate class (default 1), take the memento via
`aggregate._serialize_state()`, build the `Snapshot` VO stamped
`datetime.now(UTC)`, `await store.save_snapshot(...)`. Used by the scheduled
path and by `AggregateRepository.create_snapshot()` (manual). Errors
propagate; degradation policy belongs to the scheduler, strictness to the
manual path — exactly ADR-0017's split, now structural.

### `read_valid_snapshot(store, aggregate_id, aggregate_type, factory) -> Snapshot | None`

Module function for the load path: fetch + schema-version validation. All
failure modes (store error, missing, schema mismatch) collapse to `None` →
full replay, with the same log levels as today.

### `AggregateRepository`

Composes `SnapshotStore` + `SnapshotPolicy` + `SnapshotScheduler` directly.
Constructor keeps the ergonomic knobs unchanged — `snapshot_store`,
`snapshot_threshold`, `snapshot_mode: "sync" | "background" | "manual"` — and
maps them internally (sync → `EveryNEvents` + `ImmediateScheduler`;
background → `EveryNEvents` + `BackgroundScheduler`; manual → `Never()`).
New optional `snapshot_policy=` / `snapshot_scheduler=` parameters accept
custom implementations (mutually exclusive with the mode string).
`create_snapshot()`, `await_pending_snapshots()`, `pending_snapshot_count`
survive, delegating to `take_snapshot`/scheduler. Behavior is otherwise
unchanged, including all failure-degradation semantics and tracing spans.

The repository continues to consume `stores.interface.EventStore` (the
transition port). Migrating it to `ports/store.py` — and retiring
`stores/legacy.py` — is the next sub-project-2 slice, out of scope here.

## Testing

- **Behavior preservation:** existing `tests/unit/aggregates/` and snapshot
  suites pass; tests importing deleted modules get import updates only, no
  assertion changes.
- **Property-based (hypothesis),** following the `test_*_properties.py`
  convention:
  - `EveryNEvents` predicate over generated (version, threshold): true iff
    `version > 0 and version % n == 0`; `Never` always false.
  - Memento round-trip: `_serialize_state()` → `_restore_from_snapshot()`
    preserves state and version for hypothesis-generated aggregate states.
  - `read_valid_snapshot` returns `None` exactly on missing/mismatch/error;
    returns the snapshot iff schema versions agree.
- **Mutation testing:** add `src/eventsource/application` to
  `[tool.mutmut] only_mutate` (domain/ already listed) and
  `tests/unit/application/` to the test-selection list.
- **Process discipline:** implementation agents run only tests targeted at
  their change; the orchestrator runs the full suite (`make check` parity)
  after each agent pass and dispatches fixes. Review agents may fix small
  findings in place.

## Documentation

Update: `docs/guides/snapshotting.md`, `docs/tutorials/14-snapshotting.md`,
`docs/api/snapshots.md`, `docs/development/code-structure.md`, per-directory
READMEs (aggregates → application/aggregates), `CLAUDE.md` project-structure
block, `.claude/rules/architecture.md` transition lists, import-linter
contracts (`application` may import domain/ports/transition-ports, never
adapters; Tier-0 contract entries follow the moved modules).

## Out of Scope

- Repository migration to `ports/store.py`; `stores/legacy.py` removal.
- Projections, subscriptions, migration verticals (later sub-project-2 slices).
- Snapshot store conformance changes (adapters untouched).
