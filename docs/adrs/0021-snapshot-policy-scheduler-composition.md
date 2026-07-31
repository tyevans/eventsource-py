# 0021. Snapshot Policy/Scheduler Composition

Snapshots make loading a long-lived aggregate cheap: instead of replaying
every event from version 0, the repository restores serialized state at
version N and replays only what came after. This ADR replaces the
`SnapshotStrategy` protocol and `AggregateSnapshotManager` class from ADR
0017 with four smaller collaborators — `SnapshotPolicy`, `SnapshotScheduler`,
`take_snapshot()`, and `read_valid_snapshot()` — composed directly by
`AggregateRepository`, and explains why the manager was removed rather than
carried forward into the application ring.

## Status

Accepted. Supersedes [ADR 0017](0017-snapshot-strategy-pattern.md).
Implemented in `src/eventsource/application/aggregates/snapshotting.py`
(`SnapshotPolicy`, `EveryNEvents`, `Never`, `SnapshotScheduler`,
`ImmediateScheduler`, `BackgroundScheduler`, `take_snapshot()`,
`read_valid_snapshot()`) and
`src/eventsource/application/aggregates/repository.py`
(`AggregateRepository.__init__` composition and wiring).

## Context

ADR 0017 split *when* to snapshot (`SnapshotStrategy.should_snapshot`) from
*how* to execute the write (`SnapshotStrategy.execute_snapshot`), but left
both questions on the same protocol and introduced a fourth object,
`AggregateSnapshotManager`, to hold the store, the strategy, and the
load-path validation logic. Moving that design into the new ring layout
surfaced four problems ADR 0017 itself had already named or was structurally
prone to:

### The manager mixed four responsibilities behind one interface

`AggregateSnapshotManager` owned: reading and validating a snapshot on load
(`load_valid_snapshot`), deciding whether to write one automatically
(`maybe_create_snapshot`, which just forwarded to the strategy), writing one
on demand (`create_snapshot`, which duplicated the strategy's serialization
logic rather than reusing it), and reporting on background work
(`pending_count` / `await_pending`, forwarded to the strategy when there was
one). None of those four are the same job, and three of them already lived
on the strategy or could be free functions — the manager's only irreducible
job was owning the store reference, which the repository already holds.

### Construction was duplicated between the manager and the strategy

ADR 0017's own Consequences section flagged this: "two spellings of snapshot
creation (strategy and manager) must stay in sync." `BaseSnapshotStrategy._create_snapshot()`
and `AggregateSnapshotManager.create_snapshot()` built the same `Snapshot`
object from the same fields, in two places, with only the manager's copy
carrying a tracing span. A field added to one and not the other was a latent
bug with no test able to catch it structurally.

### The manager isinstance-sniffed a concrete strategy (LSP)

`pending_count` and `await_pending()` on `AggregateSnapshotManager` did
`isinstance(self._strategy, BackgroundSnapshotStrategy)` and returned `0` /
a no-op otherwise. A collaborator that inspects the concrete type of another
collaborator to decide whether a method call is meaningful violates
Liskov substitution — code holding a `SnapshotStrategy` could not treat all
three implementations interchangeably without the manager's type check
standing in for the difference the Protocol was supposed to abstract away.

### `NoSnapshotStrategy` implemented an unrunnable method (ISP)

Every `SnapshotStrategy` had to declare `async execute_snapshot(...)`, but
`NoSnapshotStrategy` never needed to be called — its `should_snapshot()`
always returns `False`, so nothing in the manager ever reaches its
`execute_snapshot()`. The manual mode was forced to carry a method its
contract can never legitimately trigger, because *when* and *how* were
merged into one protocol. Splitting them lets manual mode be expressed by a
policy alone (`Never`), with no scheduler-shaped no-op required.

### The library is unreleased, so there is nothing to shim

ADR 0017 preserved the `snapshot_mode`/`snapshot_threshold` mode-string API
as a stable public surface across a factory-function indirection
(`create_snapshot_strategy`). `eventsource` has not yet shipped a release
with that surface as its own committed contract independent of this
refactor — the mode/threshold knobs are kept here because they are still a
good ergonomic default, not because a deprecation shim is owed to existing
callers. There is no 0.8.0-style compatibility layer for `SnapshotStrategy`,
`AggregateSnapshotManager`, or `create_snapshot_strategy`; they are deleted
outright.

## Decision

### `SnapshotPolicy` — pure predicate for *when*

`SnapshotPolicy` is a `@runtime_checkable` `Protocol` with one method:
`should_snapshot(aggregate, events_since_snapshot) -> bool`, synchronous and
side-effect free. Two implementations ship:

- `EveryNEvents(n)` — the ADR 0017 default rule, unchanged:
  `aggregate.version > 0 and aggregate.version % n == 0`. Keyed off the
  aggregate's absolute version, not `events_since_snapshot`, so two
  processes saving the same aggregate agree on where boundaries fall. The
  straddle caveat carries over verbatim: a save that jumps the version
  across a multiple of `n` without landing on it takes no snapshot until the
  next boundary — acceptable, because snapshots are an optimization, not a
  guarantee.
- `Never` — `should_snapshot` always returns `False`. This is manual mode,
  expressed as a policy with nothing to schedule, rather than as a strategy
  with an unrunnable execution method.

### `SnapshotScheduler` — uniform surface for *how*

`SnapshotScheduler` is a `@runtime_checkable` `Protocol` with `schedule(write, *, aggregate_type, aggregate_id) -> Snapshot | None`,
plus `pending_count` (property) and `await_pending()` (coroutine). Every
implementation carries the full surface — `pending_count` and
`await_pending()` return `0` for a scheduler with nothing ever in flight —
so no caller, including the repository, ever needs to sniff the concrete
type to decide whether asking about pending work is meaningful. This is the
fix for the ADR 0017 LSP violation: the isinstance check moves from a
consumer of the protocol into "every implementation answers the same
questions."

- `ImmediateScheduler` — awaits `write` inline; on failure, logs a
  `WARNING` with `exc_info=True` and returns `None`. Equivalent to ADR
  0017's `ThresholdSnapshotStrategy` execution half.
- `BackgroundScheduler` — hands `write` to a `BackgroundTaskManager`
  (`_internal/background_tasks.py`) and returns `None` immediately;
  `await_pending()` is the join point for tests and graceful shutdown.
  Equivalent to `BackgroundSnapshotStrategy`'s execution half, with the task
  bookkeeping delegated to the same collaborator the event bus already uses
  rather than reimplemented.

### `take_snapshot()` — the single construction path, strict

A free async function: `take_snapshot(aggregate, aggregate_type, store) -> Snapshot`.
It reads `schema_version` off the aggregate class (defaulting to `1`),
serializes state, builds the `Snapshot`, saves it, and returns it. There is
exactly one place a `Snapshot` object gets built from a live aggregate — the
two-spellings problem from ADR 0017 does not exist here because there is
only one spelling. `take_snapshot()` does not catch anything: errors from
`_serialize_state()` or `store.save_snapshot()` propagate to the caller.
Degradation is entirely the scheduler's job — `ImmediateScheduler` and
`BackgroundScheduler` both wrap their call to `take_snapshot()` in the
catch-log-swallow logic ADR 0017 established, so the automatic path still
degrades exactly as before. The manual path
(`AggregateRepository.create_snapshot()`) calls `take_snapshot()` directly,
with no scheduler in between, so it stays strict by construction rather than
by a separate manager method that happened to skip the try/except.

### `read_valid_snapshot()` — the single load-path validation function

A free async function: `read_valid_snapshot(store, aggregate_id, aggregate_type, aggregate_factory) -> Snapshot | None`.
Store error, missing snapshot, and `schema_version` mismatch all collapse to
`None`, exactly as `AggregateSnapshotManager.load_valid_snapshot()` did:
a store exception is caught and logged at `WARNING`; a missing snapshot
returns `None` silently; a `schema_version` mismatch is logged at `INFO`.
`AggregateRepository.load()` calls this directly.

### The repository composes all four

`AggregateRepository.__init__` builds a `SnapshotPolicy` and a
`SnapshotScheduler` and holds them as plain attributes — there is no
intervening manager object. `save()` asks `self._snapshot_policy.should_snapshot(...)`
and, if true, opens a span and calls
`self._snapshot_scheduler.schedule(take_snapshot(...), ...)`. `load()` calls
`read_valid_snapshot(...)` directly. `create_snapshot()` calls
`take_snapshot(...)` directly. Every one of the manager's four
responsibilities now lives with the collaborator that actually owns it:
policy, scheduler, or the repository's own load/save methods.

### Mode-string knobs preserved, mapped in `__init__`

The public constructor keeps `snapshot_mode: Literal["sync", "background", "manual"]`
and `snapshot_threshold: int | None`, mapped inside `__init__` rather than
through a factory function:

- `snapshot_mode != "manual"` and `snapshot_threshold is not None` →
  `EveryNEvents(snapshot_threshold)`; otherwise `Never()`.
- `snapshot_mode == "background"` → `BackgroundScheduler()`; otherwise
  `ImmediateScheduler()`.

### New escape hatches: `snapshot_policy=` / `snapshot_scheduler=`

Two new constructor parameters accept a `SnapshotPolicy` or
`SnapshotScheduler` directly, for custom policies (elapsed time, event type,
state size — ADR 0017's own suggested extension point) without subclassing
anything the library ships. They are mutually exclusive with
`snapshot_mode`/`snapshot_threshold`: passing either alongside a
non-default mode or an explicit threshold raises `ValueError` at
construction, so there is exactly one way to end up with a given
policy/scheduler pair, never a silent knob that was ignored.

## Carried-forward rationale

Snapshots remain disposable optimizations, never the source of truth: the
event stream is authoritative, and every *automatic* failure path degrades
to full replay rather than raising, for the same reasons ADR 0017 gives —
events are already durable by the time a snapshot is attempted, so failing
the operation that succeeded would invert the value proposition. The manual
path (`create_snapshot()` / `take_snapshot()`) stays strict for the same
reason ADR 0017's `AggregateSnapshotManager.create_snapshot()` was strict:
an explicit request should tell the caller whether it succeeded. See ADR
0017's Rationale section for the full argument; it is not restated here.

## Consequences

### Positive: one spelling of construction

`take_snapshot()` is the only place a `Snapshot` is built from a live
aggregate. The manual and automatic paths call the same function; there is
no second copy to keep in sync.

### Positive: no type-sniffing

Every `SnapshotScheduler` answers `pending_count` / `await_pending()`
uniformly, so `AggregateRepository.pending_snapshot_count` and
`await_pending_snapshots()` forward directly with no `isinstance` check
anywhere in the call chain.

### Positive: policies and schedulers are unit-testable in isolation

`EveryNEvents`, `Never`, `ImmediateScheduler`, and `BackgroundScheduler` are
each testable with a stub aggregate and a fake store, with no event store,
repository, or manager object in play — the same isolation ADR 0017 claimed
for strategies, now split across two smaller surfaces instead of one.

### Positive: custom policies and schedulers without library changes

A new *when* rule is a new `SnapshotPolicy` implementation; a new *how*
(bounded background queue, batched writes) is a new `SnapshotScheduler`
implementation. Neither requires touching `AggregateRepository` or any
existing collaborator, and each can be supplied independently — a custom
policy with the built-in `ImmediateScheduler`, or the built-in
`EveryNEvents` with a custom scheduler.

### Negative: `BackgroundScheduler` still has no backpressure bound

Carried forward from ADR 0017's `BackgroundSnapshotStrategy`: the
`BackgroundTaskManager` it delegates to tracks in-flight tasks but does not
cap them. Under a burst where snapshot writes are slower than saves crossing
policy boundaries, pending work grows unbounded. This ADR does not change
that; it is still a known gap.

### Observable change: tracing spans moved

`eventsource.snapshot_manager.*` spans (`load_valid_snapshot`,
`maybe_create_snapshot`, `create_snapshot`) no longer exist — there is no
manager object to open them. The repository's own spans remain:
`eventsource.repository.load`, `.save`, `.create_snapshot`, and a new
`eventsource.repository.snapshot` span opened around the scheduler call,
only when a snapshot is actually scheduled (i.e. only when
`should_snapshot()` returned `True`), mirroring the "no span for the common
non-boundary case" behavior ADR 0017's manager already had.

## Alternatives Considered

### Keep `AggregateSnapshotManager`, move it into `application/aggregates/`

Simplest migration: rename the module, update imports, done. Rejected
because it would carry the isinstance-sniffing and the duplicated
construction logic forward unchanged into a ring redesign that is
explicitly supposed to be an opportunity to fix known issues, not just
relocate them. The manager's four responsibilities were already candidates
for splitting under ADR 0017's own "one responsibility per module"
principle (see `docs/development/code-structure.md`); this ADR is that
split.

### Merge `SnapshotPolicy` and `SnapshotScheduler` back into one protocol

Rejected for the same ISP reason the original `SnapshotStrategy` was split
in spirit if not in code: manual mode has a *when* (never) with no *how* to
speak of, and a merged protocol forces every implementation to declare an
execution method even when it is unreachable. Two protocols let `Never`
exist with a single method.

### Bound `BackgroundScheduler`'s pending-task list in this ADR

Considered and rejected as out of scope: the backpressure gap is inherited
from ADR 0017 unchanged, and fixing it is an independent decision (a queue
depth, a rejection policy) that deserves its own ADR rather than being
folded into a decomposition refactor.

## References

- `docs/superpowers/specs/2026-07-30-aggregates-application-ring-design.md`
  — the design spec this decision implements.
- `src/eventsource/application/aggregates/snapshotting.py` —
  `SnapshotPolicy`, `EveryNEvents`, `Never`, `SnapshotScheduler`,
  `ImmediateScheduler`, `BackgroundScheduler`, `take_snapshot()`,
  `read_valid_snapshot()`.
- `src/eventsource/application/aggregates/repository.py` — composition and
  the `snapshot_mode`/`snapshot_threshold` → policy/scheduler mapping.
- `src/eventsource/ports/snapshots.py` — `Snapshot`, `SnapshotStore`.
- `src/eventsource/exceptions.py` — `SnapshotError` hierarchy (moved from
  `eventsource.snapshots.exceptions`).
- `tests/unit/application/aggregates/test_snapshotting_properties.py`,
  `tests/unit/domain/test_aggregate_memento_properties.py` — property-based
  coverage of the policy boundary predicate and the snapshot/replay
  equivalence.
- [ADR 0017](0017-snapshot-strategy-pattern.md) — the superseded design;
  read for the full rationale on graceful degradation, which this ADR
  carries forward without restating.
