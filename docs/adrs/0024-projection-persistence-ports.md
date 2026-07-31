# 0024. Projection Persistence Ports

Projections need two independent things from durable storage: a checkpoint
so restart doesn't reprocess the whole event stream, and a dead letter
queue so a permanently failing event doesn't silently disappear. Both used
to be served by one seven-method repository interface and a stateless
manager wrapped around it. This ADR splits the interface along its two real
consumer groups, dissolves the manager into module-level functions (the
same move ADR 0021 made for snapshots), and makes `None` mean "this concern
is disabled" instead of "fall back to an in-memory implementation."

## Status

Accepted. Amends [ADR 0015](0015-optional-dependency-extras.md). Sibling of
[ADR 0021](0021-snapshot-policy-scheduler-composition.md) — the same
manager-dissolution move, applied to projections. Implemented in
`src/eventsource/ports/checkpoints.py`, `src/eventsource/ports/dlq.py`,
`src/eventsource/application/projections/` (`base.py`, `checkpoints.py`,
`dlq.py`, `coordinator.py`, `retry.py`), `src/eventsource/adapters/sql/`
(`checkpoints.py`, `dlq.py`, `projection.py`), and
`src/eventsource/adapters/memory/` (`checkpoints.py`, `dlq.py`).

## Context

`docs/core-surface.md` carried a KNOWN VIOLATIONS block recording that
`src/eventsource/repositories/checkpoint.py` and `repositories/dlq.py` mixed
Protocol definitions with sqlalchemy-backed implementations in the same
module — the same defect ADR 0019 had already fixed for the event store and
ADR 0021 for snapshots, still outstanding here because projections hadn't
been through the ring redesign yet.

`CheckpointRepositoryProtocol` was a seven-method god-interface —
`get_checkpoint`, `update_checkpoint`, `reset_checkpoint`,
`get_lag_metrics`, `get_all_checkpoints`, plus the global-position pair
`get_position`/`save_position` — serving two disjoint consumer groups.
Subscription runners and `migration/coordinator.py` only ever call the
position pair: they track an opaque global offset and have no notion of a
named projection's lag. `application/projections/*` only ever calls the
checkpoint half: a projection has no notion of a raw stream position, only
"what event did I last process and how far behind am I." Only
`migration/subscription_migrator.py` uses both, because it drives a
subscription and reports projection-shaped progress at once. One interface
forced every implementer to satisfy methods it never called.

`ProjectionCheckpointManager` and `ProjectionDLQManager` were both
stateless wrappers holding exactly one repository reference and a tracer,
with no invariant of their own — every method was a direct forward to the
repository, wrapped in a tracing span and a log line. Nothing about them
required an object: they were the same shape ADR 0021 diagnosed in
`AggregateSnapshotManager`, applied to a different pair of repositories.

`eventsource` is unreleased, so there is no prior public contract to
preserve across the split and no deprecation shim owed to existing callers.

## Decision

### Decision 1 — ISP split: `ProjectionCheckpoints` + `SubscriptionPositions` + composed `CheckpointRepository`

`src/eventsource/ports/checkpoints.py` now declares three protocols:

- `ProjectionCheckpoints` — `get_checkpoint`, `update_checkpoint`,
  `reset_checkpoint`, `get_lag_metrics`, `get_all_checkpoints`. Everything
  `application/projections/*` calls.
- `SubscriptionPositions` — `get_position`, `save_position`. Everything
  subscription runners and `migration/coordinator.py` call.
- `CheckpointRepository(ProjectionCheckpoints, SubscriptionPositions,
  Protocol)` — the composed convenience protocol, kept because both
  capabilities land in the same checkpoints table in the SQL adapter and
  `migration/subscription_migrator.py` genuinely needs both at once.

This split is not speculative narrowing for its own sake: it follows the
call sites as they already existed. `get_all_checkpoints` stays on
`ProjectionCheckpoints` rather than becoming its own single-method protocol
— it's a checkpoint-table query with one consumer (administrative listing),
and a protocol with one method and one implementation buys nothing ISP
doesn't already grant it by living on the checkpoint side of the split.

### Decision 2 — manager dissolution

`ProjectionCheckpointManager` and `ProjectionDLQManager` are gone. In their
place, six module-level async functions:

- `application/projections/checkpoints.py` — `record_checkpoint()`,
  `read_checkpoint()`, `lag_metrics_dict()`, `reset_checkpoint()`.
- `application/projections/dlq.py` — `send_to_dlq()`, `read_failed_events()`.

Each takes the repository and a `Tracer` as explicit parameters rather than
holding them as instance state — the caller (`CheckpointTrackingProjection`
in `application/projections/base.py`) already owns both and passes them
through. Span names are kept exactly as they were —
`eventsource.checkpoint_manager.*` and `eventsource.dlq_manager.*` —
even though the classes those names describe no longer exist. Renaming
them would break users' dashboards and alerting for no functional gain;
if the names are ever revisited, that is its own change with its own
release note, not a side effect of a structural refactor.

### Decision 3 — `DatabaseProjection` is an adapter

`DatabaseProjection` lives in `src/eventsource/adapters/sql/projection.py`,
not the application ring, because its constructor takes an
`async_sessionmaker[AsyncSession]` — a concrete sqlalchemy type. Anything
whose signature names a specific persistence technology is an adapter by
the ring's own definition, regardless of how much orchestration logic it
otherwise contains. It continues to subclass `DeclarativeProjection` from
`application/projections/base.py`: an adapter depending inward, on the
ring it adapts for, is the dependency rule working as designed, not an
exception to it.

### Decision 4 — `None` means disabled

`CheckpointTrackingProjection.__init__` (and `DatabaseProjection` /
`ReadModelProjection` above it) accept `checkpoint_repo:
ProjectionCheckpoints | None = None` and `dlq_repo: DLQRepository | None =
None`. Previously, omitting either argument constructed a per-instance
in-memory repository as a default. Now it disables the concern: no
checkpoint is written and `get_checkpoint()` / `get_lag_metrics()` return
`None`; DLQ capture is skipped and a permanent failure is logged at
`critical` and re-raised, exactly as it would be with a DLQ repository that
itself failed to write.

Two independent reasons drove this, not one:

- **Mechanical.** The old in-memory default lived in the application ring
  and named a concrete adapter class by import. "The application ring must
  not import adapters" is a boundary this codebase enforces structurally
  (import-linter), and a default constructor argument is still an import.
- **By design.** An in-memory default is a production footgun disguised as
  a convenience. A projection constructed with no checkpoint repository
  *looks* durable from the outside — `get_checkpoint()` returns a value
  after events are processed, `get_lag_metrics()` returns real-looking
  numbers — while silently reprocessing the entire event stream from the
  beginning on every restart, because the in-memory store that made those
  calls succeed disappears with the process. The lag metric that would have
  revealed the problem is computed from the same amnesiac store, so it
  reports zero lag right up until the process restarts and the projection
  starts over.

This matches the precedent ADR 0021 set for `AggregateRepository
(snapshot_store=None)`: absence of a store means the concern is off, not
that a hidden default silently stands in for it.

## Consequences

### Positive

- The KNOWN VIOLATIONS block in `docs/core-surface.md` is deleted — the
  Protocol/implementation mixing it recorded no longer exists.
- `eventsource.application` is Tier-0-clean as a whole ring: nothing under
  `application/projections/` imports sqlalchemy, asyncpg, or aiosqlite.
- Consumers annotate the narrowest port they use — subscription runners and
  `migration/coordinator.py` take `SubscriptionPositions`;
  `application/projections/*` takes `ProjectionCheckpoints` or
  `DLQRepository`; only `migration/subscription_migrator.py` keeps the
  composed `CheckpointRepository` annotation, because it is the one caller
  that actually needs both halves.

### Negative / observable

- Eight test suites that previously relied on the implicit in-memory
  default now need explicit repository injection
  (`InMemoryCheckpointRepository()` / `InMemoryDLQRepository()`) to keep
  their old behavior.
- One extra `critical`-level log line appears when an event fails
  permanently with no DLQ repository configured, or when the DLQ write
  itself fails; its message states plainly that no DLQ entry was recorded,
  rather than claiming one was. The exception was already re-raised in that
  path before this change and still is — no caller's control flow changes,
  only the log volume and wording for a configuration that was already
  silently dropping failed events.
- The checkpoint and DLQ functions now trace under the projection's own
  tracer rather than a per-manager tracer. Span *names* are unchanged (see
  Decision 2), but the OpenTelemetry **instrumentation-scope name** for
  those spans changes from `eventsource.projections.checkpoint_manager` /
  `eventsource.projections.dlq_manager` to
  `eventsource.application.projections.base` — the module that now calls
  `tracer.span(...)` on the projection's own tracer. Anything that filters
  or groups spans by instrumentation scope rather than span name will see
  this shift.

## Alternatives Considered

### Keep the god-interface

Rejected on ISP grounds, and because the split was already latent in the
call sites: subscription runners never called a checkpoint method, and
`application/projections/*` never called a position method. Keeping one
interface would have kept forcing every implementer, including test
doubles, to satisfy five methods it never exercised.

### Keep the managers as thin classes, just relocate them

Simplest migration, same shape as the rejected alternative in ADR 0021:
rename the modules, update imports, done. Rejected for the same reason —
`ProjectionCheckpointManager` and `ProjectionDLQManager` held no state
beyond a repository reference and a tracer, decided nothing, and existed
only because something had to be a "Manager." Carrying that forward
unchanged into a ring redesign would relocate the smell instead of fixing
it.

### Rename the spans while dissolving the managers

Considered alongside Decision 2, since the manager classes the span names
describe are gone. Rejected: it is a user-visible change to anyone's
dashboards or alerting rules, and it is unrelated to the structural
decision this ADR is actually making. A span rename, if it happens, gets
its own change and its own release note.

### Make `None` construct a null-object repository

A `NullCheckpointRepository` / `NullDLQRepository` that silently accepts
writes and returns empty reads would keep the constructor signature
identical in spirit while making "disabled" an explicit type instead of
`None`. Rejected: it reintroduces exactly the "looks durable, isn't"
failure mode Decision 4 exists to eliminate, with extra machinery standing
in for the same footgun — a null object that accepts `update_checkpoint()`
calls without persisting them is indistinguishable, from the outside, from
the in-memory default this ADR removes.

## References

- `docs/superpowers/specs/2026-07-30-projections-ring-design.md` — the
  design spec this decision implements.
- [ADR 0019](0019-clean-architecture-store-ports.md) — the original
  Protocol/implementation split for the event store, the precedent this
  ADR follows for checkpoints and DLQ.
- [ADR 0021](0021-snapshot-policy-scheduler-composition.md) — the sibling
  manager-dissolution decision for snapshots; this ADR applies the same
  reasoning to projections.
- [ADR 0015](0015-optional-dependency-extras.md) — amended by this ADR; see
  its Consequences for the updated connection-helper split.
- `src/eventsource/ports/checkpoints.py` — `ProjectionCheckpoints`,
  `SubscriptionPositions`, `CheckpointRepository`, `CheckpointData`,
  `LagMetrics`.
- `src/eventsource/ports/dlq.py` — `DLQRepository`, `DLQEntry`, `DLQStats`,
  `ProjectionFailureCount`.
- `src/eventsource/application/projections/checkpoints.py`,
  `src/eventsource/application/projections/dlq.py` — the six dissolved
  functions.
- `src/eventsource/application/projections/base.py` —
  `CheckpointTrackingProjection`, the `None`-means-disabled constructor.
- `src/eventsource/adapters/sql/`, `src/eventsource/adapters/memory/` —
  the checkpoint, DLQ, and `DatabaseProjection` implementations.
- `src/eventsource/testing/conformance_ports/` — the conformance suites
  exercising both split protocols against every backend.
