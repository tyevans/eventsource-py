# 0041. Infrastructure Exceptions Move to `ports/exceptions.py`

Thirteen exception classes move out of `domain/exceptions.py` into a new
`ports/exceptions.py`: `CheckpointError`, `CheckpointNotFoundError`,
`EventBusConnectionError`, `EventStoreConnectionError`,
`LockAcquisitionError`, `LockNotHeldError`, `PositionDecodeError`,
`PositionForeignError`, `SubscriptionError`, `SubscriptionConfigError`,
`SubscriptionStateError`, `SubscriptionAlreadyExistsError`,
`TransitionError`. All thirteen stay rooted in `EventSourceError`; only
their module changes.

## Status

**Accepted.** Implemented in the domain-ring hardening wave (task 7).
`ports/exceptions.py` defines the thirteen classes;
`domain/exceptions.py` no longer does. No shim: `from
eventsource.domain.exceptions import LockAcquisitionError` (or any of the
other twelve) now raises `ImportError`.

**Amended by [0048 - Failure Paths Report Honestly and Retain What They Cannot
Handle](0048-failure-paths-report-and-retain.md)**, scoped to one class:
`EventStoreConnectionError` moves from `SubscriptionError` to
`EventStoreError`. The module it lives in and the other twelve classes are
unchanged.

**Amends [ADR 0030](0030-top-level-module-ring-consolidation.md).** ADR
0030's Decision is not retro-edited: `exceptions.py` still moved onto
`domain/` exactly as that ADR describes. What changes is that
`domain/exceptions.py`'s *contents* are no longer settled as one
undifferentiated hierarchy — this ADR carves thirteen of its members back
out into `ports/`. ADR 0030's Status section carries an "Amended by ADR
0041" pointer.

**Amends [ADR 0032](0032-subscriptions-ring-migration.md).** ADR 0032's
Decision §5 merged the subscription exception family into
`domain/exceptions.py` and rebased `SubscriptionError` onto
`EventSourceError`; neither of those choices is reversed here. What moves
is only the module: `SubscriptionError` and its seven subclasses relocate
from `domain/exceptions.py` to `ports/exceptions.py`, the same file the
lock and checkpoint exceptions land in by this same ADR. ADR 0032's Status
section carries an "Amended by ADR 0041" pointer.

## Context

Tier 0 — stdlib and pydantic only, no I/O, importable from every ring
without violating the Dependency Rule — became, over the course of the
ring-migration campaign, the universal error taxonomy: `domain/exceptions.py`
is the only module every ring may import, because `domain/` sits innermost
and every outer ring is permitted to depend on it. That made it the
path of least resistance every time a new subsystem needed an
`EventSourceError` subclass and a home for it, regardless of what the
subsystem actually did — ADR 0029 rebased the lock exceptions there, ADR
0032 rebased the subscription exceptions there, and `CheckpointError` and
the position-decoding errors accreted the same way. By the end of that
campaign, `domain/exceptions.py` held twenty-four classes, and roughly a
third of them described a *port*-contract failure — a lock could not be
acquired, a subscription's checkpoint could not be found, an event
store's connection dropped — with no bearing on aggregates, commands, or
event application. None of those thirteen carry domain meaning: an
`AggregateNotFoundError` or `OptimisticLockError` describes something a
`decide()` function or an aggregate's `_apply` could reasonably reason
about; `LockAcquisitionError` describes a `LockManager` adapter failing to
talk to Postgres advisory locks, which no domain code has any business
knowing about.

This is the same shape of problem ADR 0029 and ADR 0032 each *widened*
(rebasing a bare-`Exception` family onto `EventSourceError`) without
asking where the result should live — both moves defaulted to
`domain/exceptions.py` because that was the only ring-map location the
`EventSourceError` root already occupied. Task 7 of the domain-ring
hardening wave is the first pass that asks the placement question
directly, once `ports/` was mature enough (ADR 0019, 0024, 0029) to be an
obvious second home for exceptions describing port-contract failures
specifically.

## Decision

The thirteen classes named above move to a new module,
`ports/exceptions.py`, importing only `EventSourceError` from
`domain/exceptions.py`. Every class keeps its exact name, constructor
signature, attributes, and message format — this is a relocation, not a
redesign. `SubscriptionError`'s seven subclasses move as a unit, preserving
the existing single-level inheritance under `SubscriptionError` itself.

No shim. `import eventsource.domain.exceptions` still succeeds (the
module still exists and still defines its remaining eleven classes), but
`from eventsource.domain.exceptions import LockAcquisitionError` now
raises `ImportError` rather than resolving through a compatibility
re-export. This follows the same standing pre-1.0 rule ADR 0025, ADR
0026, and ADR 0030 already established: the library has no external
consumers, so a deprecation shim would buy nothing beyond
`__getattr__`/`__dir__` plumbing to maintain for a promise nobody is
relying on.

`domain/exceptions.py` keeps the broad domain-meaning categories —
`EventStoreError` and `EventBusError` remain there as the general-purpose
markers domain code itself raises or catches — along with everything
else domain code raises directly: `OptimisticLockError`,
`AggregateNotFoundError`, `EventVersionError`, `UnhandledEventError`,
`AggregateNotCreatedError`, the event-registry and handler exceptions,
`SnapshotError` and its subclasses, and the three tenant-context
exceptions ADR 0038 merged in. Nothing about those eleven classes' domain
membership is questioned by this ADR.

## Consequences

### Positive

- `domain/exceptions.py` now holds only exceptions with actual domain
  meaning — the entities ring's exception surface matches what the
  `.claude/rules/architecture.md` ring map has always claimed it should.
- `ports/exceptions.py` gives adapters and application code one place to
  import infrastructure-failure types from, symmetric with how
  `ports/locks.py`, `ports/subscribers.py`, and `ports/coordination.py`
  already hold the Protocol contracts those same failures relate to.
- `except EventSourceError` behavior is completely unchanged: every one
  of the thirteen classes is still `EventSourceError`-rooted, so no
  boundary handler that catches broadly needs to change.

### Negative

- **BREAKING:** any import of the form `from eventsource.domain.exceptions
  import LockAcquisitionError` (or the other twelve names) now raises
  `ImportError`. There is no transition window. Top-level
  `from eventsource import ...` re-exports are unaffected — none of the
  thirteen were re-exported from the root barrel, so no root-level import
  breaks.
- Two modules now define exception hierarchies with the same root
  (`domain/exceptions.py` and `ports/exceptions.py` both import
  `EventSourceError`), so a reader checking "where does this exception
  live" must know the split exists rather than assuming one file. This
  ADR and the exceptions reference doc (`docs/api/exceptions.md`) are
  where that split is now written down.

## Alternatives Considered

**Leave the thirteen classes where they were and document the
inconsistency instead of fixing it.** Rejected: `domain/exceptions.py`
having no I/O and no adapter awareness is exactly what makes it safely
importable from every ring; the moment it names `LockAcquisitionError`
with a `timeout` attribute describing a database wait, it is describing
adapter behavior from inside the one module every ring trusts to be free
of it. The inconsistency was cheap to fix and only got more expensive to
unwind the longer it sat.

**Split further, giving `SubscriptionError`'s subclasses their own module
separate from the lock and checkpoint exceptions.** Rejected: all
thirteen classes describe the same category of thing — a port contract
that could not be satisfied — and `ports/` already groups Protocols by
port, not by exception family; one `ports/exceptions.py` module mirrors
how `domain/exceptions.py` itself is one flat module for its own broader
category, rather than one file per exception origin.

## References

- `src/eventsource/ports/exceptions.py` — the new module
- `src/eventsource/domain/exceptions.py` — the eleven classes remaining
  after this move
- [ADR 0029](0029-locks-readmodels-and-engine-rings.md) — the lock
  exceptions' original rebase onto `EventSourceError`, whose module this
  ADR now changes
- [ADR 0030](0030-top-level-module-ring-consolidation.md) — settled
  `exceptions.py` onto `domain/` as a whole; amended above
- [ADR 0032](0032-subscriptions-ring-migration.md) — the subscription
  exceptions' original rebase and merge into `domain/exceptions.py`;
  amended above
- [ADR 0038](0038-multitenancy-dissolution.md) — the tenant exceptions
  that stay in `domain/exceptions.py`, unaffected by this ADR

## Related

- `docs/api/exceptions.md` — exception hierarchy reference, updated for
  the new module split
- `docs/guides/error-handling.md`, `docs/guides/distributed-locks.md`,
  `docs/api/locks.md` — updated import paths
- `.claude/rules/architecture.md` — entities-ring and ports-ring bullets
  updated to record the split
