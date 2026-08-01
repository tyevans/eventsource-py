# 0036. Snapshot Port as Composed Protocols

`ports/snapshots.py` hosted `SnapshotStore` as an `ABC` with a concrete
default body (`snapshot_exists`, implemented via `get_snapshot`) and a
`delete_snapshots_by_type` method that raised `NotImplementedError` by
default — both violate the settled ports rule that ports contain no
implementation code, ever, and that an optional capability is expressed by
a backend simply not implementing a port rather than by a stub that raises.
This ADR splits the port into a core `SnapshotStore` Protocol and a separate
optional `SnapshotTypeInvalidation` capability Protocol, and updates the
three snapshot adapters and the conformance suite to match.

## Status

**Accepted.** Implemented in `src/eventsource/ports/snapshots.py`
(`SnapshotStore`, `SnapshotTypeInvalidation`), `src/eventsource/adapters/{memory,sqlite,postgresql}/snapshots.py`
(structural implementations, no inheritance), and
`src/eventsource/testing/conformance_ports/snapshots.py`
(`SnapshotConformance`, `SnapshotTypeInvalidationConformance`,
`SnapshotStoreConformance`).

## Decision Table

| Before | After | Rationale |
|---|---|---|
| `SnapshotStore(ABC)` with `@abstractmethod` `save_snapshot`/`get_snapshot`/`delete_snapshot`, concrete `snapshot_exists`, and `delete_snapshots_by_type` raising `NotImplementedError` | `SnapshotStore(Protocol)`, `@runtime_checkable`, four bodyless methods including `snapshot_exists` | Every adapter implements `snapshot_exists` natively (none actually relies on the old default's `get_snapshot`-based implementation for correctness, only convenience), so it joins the core rather than becoming its own capability port. |
| (same class) `delete_snapshots_by_type` | New `SnapshotTypeInvalidation(Protocol)`, `@runtime_checkable`, single method | Optional capability, expressed the way the settled ports rule requires: a store that doesn't support bulk invalidation simply doesn't implement the Protocol. No default body, no `NotImplementedError`. |
| `InMemorySnapshotStore(SnapshotStore)`, `SQLiteSnapshotStore(SnapshotStore)`, `PostgreSQLSnapshotStore(SnapshotStore)` | Same three classes, no inheritance | Structural conformance, matching the precedent ADR 0034 set for the PostgreSQL migration repositories: adapters implement a Protocol's shape, they don't inherit its (now nonexistent) implementation. |
| `SnapshotConformance` (7 core tests + 2 bulk-delete tests, one combined class) | `SnapshotConformance` (core only, 7 tests + an `isinstance(store, SnapshotStore)` assertion), `SnapshotTypeInvalidationConformance` (2 bulk-delete tests + an `isinstance` assertion), `SnapshotStoreConformance(SnapshotConformance, SnapshotTypeInvalidationConformance)` (both) | Mirrors the `ProjectionCheckpointsConformance`/`SubscriptionPositionsConformance`/`CheckpointRepositoryConformance` mixin-combination pattern `checkpoints.py` already established, rather than skip-based logic. All three shipped adapters implement both Protocols today, so all three test classes subclass the combined `SnapshotStoreConformance`. |

## Context

No prior ADR is amended by this change. Grepping every existing ADR for
`SnapshotStore` finds only ADR 0021 (Snapshot Policy/Scheduler Composition)
citing `ports/snapshots.py` in its References section, as a module path,
not a decision about the port's shape — ADR 0021's actual decision was
about the *application*-ring collaborators (`SnapshotPolicy`,
`SnapshotScheduler`, `take_snapshot()`, `read_valid_snapshot()`) that
consume the port, not the port's own ABC-vs-Protocol design. No ADR ever
decided `SnapshotStore` should be an ABC; it arrived at that shape before
the ADR-per-slice convention existed and was carried forward unexamined
through several ring migrations. This ADR is the first to make the port's
shape itself a recorded decision.

The recon that planned this slice expected to find call sites needing an
`isinstance(store, SnapshotTypeInvalidation)` gate before calling
`delete_snapshots_by_type`, or `snapshot_exists`, somewhere in
`application/aggregates/`. There are none: `application/aggregates/repository.py`
and `application/aggregates/snapshotting.py` only ever call `save_snapshot`
and `get_snapshot` (via the `take_snapshot()`/`read_valid_snapshot()`
helpers ADR 0021 introduced). The optional-capability question this ADR
answers lives entirely at the port/adapter/conformance layer — there was no
application-ring code to change once the Protocols themselves were split
correctly.

## Consequences

### Positive

- Zero `NotImplementedError` remains anywhere in `ports/` — verified by an
  AST walk (not a string search, since the port's own docstrings correctly
  *talk about* `NotImplementedError` and `ABC` as things it no longer does)
  in `tests/unit/ports/test_snapshot_store_interface.py`.
- A future snapshot backend that cannot support bulk invalidation (a
  streaming or append-only store, say) can implement `SnapshotStore` alone
  and correctly *not* implement `SnapshotTypeInvalidation`, with no stub
  method and no `NotImplementedError` to maintain.
- The conformance suite gates the optional capability the same way
  `checkpoints.py` already gates `ProjectionCheckpoints` vs.
  `SubscriptionPositions` — a pattern contributors adding a fourth snapshot
  backend, or a similarly-shaped port elsewhere, can follow directly.

### Negative

- `SnapshotStore` can no longer be subclassed to get `TypeError` on missing
  abstract methods, or instantiated to get a default `snapshot_exists`
  implementation for free. `tests/unit/ports/test_snapshot_store_interface.py`
  was rewritten from ABC-subclassing tests to Protocol structural-satisfaction
  tests (`isinstance` checks against a bare, non-inheriting class).
- The combined conformance class that previously exercised both capabilities
  was named `SnapshotConformance`; it is now `SnapshotStoreConformance`, with
  `SnapshotConformance` narrowed to core-only. This is a breaking rename for
  anything outside this repository subclassing the old combined class — an
  internal-facing testing helper, and exactly the kind of API-shape change
  this ADR makes, so it was not shimmed.

## Alternatives Considered

**Keep `delete_snapshots_by_type` on `SnapshotStore` itself and document
that it may raise `NotImplementedError`.** Rejected: this is precisely the
shape the settled ports rule forbids — "optional capabilities are expressed
by a backend not implementing a port, never by raising `NotImplementedError`
from a method it claims to support." A caller checking `hasattr` or
try/except around a `NotImplementedError` cannot distinguish "not
implemented" from a genuine runtime failure the way `isinstance` against a
separate Protocol can.

**Add an `isinstance(store, SnapshotTypeInvalidation)` gate somewhere in
`application/` speculatively, in case a future caller wants to invoke bulk
invalidation.** Rejected: there is no such caller today, and adding
defensive gating for a call site that doesn't exist would be exactly the
kind of premature abstraction the codebase's ports rules discourage. If a
future use case needs bulk invalidation from application code, the gate
belongs at that call site when it is written, not speculatively here.

## References

- `src/eventsource/ports/snapshots.py`
- `src/eventsource/adapters/{memory,sqlite,postgresql}/snapshots.py`
- `src/eventsource/testing/conformance_ports/snapshots.py`
- `tests/unit/ports/test_snapshot_store_interface.py`
- [ADR 0021](0021-snapshot-policy-scheduler-composition.md) — the
  application-ring snapshotting collaborators this port serves; unaffected
  by this ADR
- [ADR 0034](0034-migration-ring-and-layers-contract.md) — the structural-
  conformance-over-inheritance precedent this ADR applies to the three
  snapshot adapters
- [ADR 0024](0024-projection-persistence-ports.md) — the
  `ProjectionCheckpoints`/`SubscriptionPositions`/`CheckpointRepositoryConformance`
  ISP-split-and-conformance-mixin pattern this ADR's conformance
  restructure follows

## Related

- BACKLOG.md's "Redesign SnapshotStore port as composed Protocols" entry
  (removed by this ADR's implementation; see CHANGELOG.md)
