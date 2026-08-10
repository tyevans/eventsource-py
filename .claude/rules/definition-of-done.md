---
paths:
  - "src/**/*.py"
  - "tests/**/*.py"
  - "docs/**/*.md"
---

# Definition of Done

## Architectural Decisions (applies to ALL work)

No work is complete if the decisions it made or changed are not properly
documented and amended:

1. **No brainstorming or design spec is complete** until it has been run
   against the existing ADRs in `docs/adrs/`. Every spec must include an
   **ADR Impact** section listing each related ADR and whether it
   **stands**, is **amended**, or is **superseded** by the proposed work.
2. If work amends or supersedes an ADR, that is part of the work, not a
   follow-up: write the new ADR (or amend the old one's Consequences), and
   update the old ADR's **Status** section with an "Amended by" /
   "Superseded by" pointer. ADR bodies are immutable records -- never
   rewrite a Decision retroactively; supersede it.
3. New architecturally significant decisions (delivery guarantees, layer
   boundaries, public contracts, rejected alternatives) get their own ADR
   in `docs/adrs/`, numbered after the current highest.
4. **ADR bodies carry no counts and no file tables.** "13 exceptions moved",
   "83 of 86 call sites", survivor tables — that goes in the commit message,
   which is immutable and scoped to a moment. An ADR states decision, forces,
   and consequences; those stay true, numbers decay. See
   `.claude/rules/recurring-defects.md` §5.
5. **ADR numbers are provisional until merge — but the filename is always
   `NNNN-slug.md`.** Parallel branches each grabbing "the next free number"
   has collided three times (`ce48fff`, `0750dfe`, `4f71b85`), so the number
   you take at drafting is a claim, not a reservation. Do **not** draft under
   a `DRAFT-` or branch-suffixed name: `.github/workflows/adr-check.yml`
   rejects any `docs/adrs/*.md` that is not `NNNN-slug.md`, so a provisional
   filename is a guaranteed red PR from its first push.

   The merge-time step is therefore *re-check and renumber*, not *allocate*:
   before merging, diff `docs/adrs/` against current `main`; if your number
   was claimed, `git mv` to the next free one and update every referrer
   (`grep -rn '<old-number>' docs/ src/ mkdocs.yml`), `index.md`, and the
   mkdocs nav — a strict build will not catch a nav omission.

## Recurring Defect Check (applies to ALL work)

`.claude/rules/recurring-defects.md` lists the six defect shapes this project
repeats. No work is complete without a pass against its quick checklist. The
two that gate most often:

- **Port method changed or added** — the semantics are pinned in
  `src/eventsource/testing/conformance*/`, exercised by every binding. A
  regression test in `tests/unit/adapters/test_<backend>_*.py` does not
  satisfy this: it cannot catch the next backend.
- **New counter, stat, or metric field** — a test asserts it non-zero under
  the condition it counts.

## New Feature

1. Implementation in `src/eventsource/` with type annotations (mypy strict passes)
2. Unit tests in `tests/unit/` covering happy path and edge cases
3. Integration tests if feature touches a backend (postgres, sqlite, redis, kafka, rabbitmq)
4. Public API re-exported from `src/eventsource/__init__.py` with `__all__` entry
5. `uv run ruff check` and `uv run ruff format` pass
6. `uv run mypy src/eventsource/` passes

## Bug Fix

1. Failing test that reproduces the bug, **proved red against the pre-fix
   source** via `git checkout HEAD~1 -- <paths>` (not `git stash`)
2. Fix implementation
3. All existing tests pass
4. Lint and type check pass
5. **If the bug was a divergence between two implementations of one port**,
   the regression test lives in the conformance suite and the per-backend
   duplicates it subsumes are deleted
6. **The assertion is written from the documented contract, not from observed
   output.** A test written from what the code currently prints encodes the
   bug as the spec — this has happened (`5d3692a`)

## Refactor

1. No behavior change -- existing tests pass without modification
2. Lint and type check pass
3. No public API changes unless explicitly intended

## New Backend Implementation

1. Implements the relevant port protocol (EventStore, EventBus, etc.) from
   `src/eventsource/ports/`
2. Lives under `src/eventsource/adapters/<backend>/` (`infrastructure/` was
   deleted; backends are colocated with their ring homes)
3. Optional dependency guard with try/except ImportError
4. **Runs the shared conformance suite for every port it binds.** There is no
   single "EventStore suite" — ports are small and composed, so store
   conformance is per-port. Bind every suite matching a port the backend
   implements:

   | Port bound | Suite | Module |
   |---|---|---|
   | `EventBus` | `EventBusConformanceSuite` | `testing/conformance.py` |
   | `EventAppender` | `AppenderConformance` | `testing/conformance_ports/` |
   | `StreamReader` | `StreamReaderConformance` | `testing/conformance_ports/` |
   | `GlobalEventFeed` | `GlobalFeedConformance` | `testing/conformance_ports/` |
   | `EventLookup` | `EventLookupConformance` | `testing/conformance_ports/` |
   | `CategoryQuery` | `CategoryQueryConformance` | `testing/conformance_ports/` |
   | `SnapshotStore` | `SnapshotStoreConformance` | `testing/conformance_ports/` |
   | checkpoints | `CheckpointRepositoryConformance` | `testing/conformance_ports/` |
   | read models | `ReadModelRepositoryConformance` | `testing/conformance_ports/` |
   | outbox | `OutboxRepositoryConformance` | `testing/conformance_ports/` |
   | DLQ | `DLQRepositoryConformance` | `testing/conformance_ports/` |
   | locks | `DistributedLockConformance` | `testing/conformance_ports/` |
   | `LeaderElector` | `LeaderElectorConformance` | `testing/conformance_ports/` |
   | `SupportsClose` | `SupportsCloseConformance` | `testing/conformance_ports/` |

   A `FullEventStore` binds the first five store suites. `StoreStateMachine`
   (hypothesis-driven) is available for stores and is not a substitute for the
   per-port suites. `SnapshotStoreConformance` composes `SnapshotConformance`
   and `SnapshotTypeInvalidationConformance`; `CheckpointRepositoryConformance`
   composes `ProjectionCheckpointsConformance` and
   `SubscriptionPositionsConformance` — bind the composed suite unless the
   backend implements only one half.

   If a port the backend binds has **no suite in this table**, that is a gap in
   the suite, not permission to skip: say so explicitly in the PR rather than
   substituting hand-written per-backend tests. A new backend that only has
   hand-written tests will diverge silently from its siblings; this has
   happened four times.
5. Integration tests with appropriate pytest marker
6. Docker service added to `docker-compose.test.yml` if needed
