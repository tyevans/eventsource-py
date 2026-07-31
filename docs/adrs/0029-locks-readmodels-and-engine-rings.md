# 0029. Locks, Read Models, and the Engine Factory: Completing the Ring Migration

The six remaining pre-ring modules -- `locks/`, `readmodels/`, and top-level
`engine.py` -- move onto the `ports`/`adapters` split ADR 0019, ADR 0024, and
ADR 0026 already applied to the store, checkpoint/DLQ, and outbox surfaces.
`locks/` splits into a Tier 0 Protocol pair plus a PostgreSQL adapter and an
in-memory test double; `readmodels/` splits into a Tier 0 subpackage plus
three backend adapters; `engine.py` relocates to `adapters/_sql/engine.py`
with no ring split, since it was never a Protocol/implementation pair to
begin with -- only a placement decision. Both `eventsource.locks` and
`eventsource.readmodels` remain importable as deprecated lazy shims, removed
in 0.8.0.

## Status

**Accepted.** Implemented in `src/eventsource/ports/locks.py`,
`src/eventsource/adapters/memory/locks.py`,
`src/eventsource/adapters/postgresql/locks.py`,
`src/eventsource/ports/readmodels/` (`model.py`, `query.py`, `repository.py`,
`exceptions.py`), `src/eventsource/adapters/memory/readmodels.py`,
`src/eventsource/adapters/postgresql/readmodels.py`,
`src/eventsource/adapters/sqlite/readmodels.py`,
`src/eventsource/adapters/sql/readmodel_schema.py`,
`src/eventsource/adapters/sql/readmodel_projection.py`, and
`src/eventsource/adapters/_sql/engine.py`. Conformance suites:
`DistributedLockConformance` and `ReadModelRepositoryConformance` in
`src/eventsource/testing/conformance_ports/`. Deprecation shims:
`src/eventsource/locks/__init__.py`, `src/eventsource/readmodels/__init__.py`.

**Amends [ADR 0023](0023-postgresql-advisory-locks.md).** ADR 0023's Decision
does not change and is not retro-edited: `PostgreSQLLockManager` remains the
only production distributed-lock implementation, and its "PostgreSQL-only"
scope stands exactly as ADR 0023 argued it. What this ADR adds is a
`ports/locks.py` Protocol pair (`DistributedLock`, `LockRegistry`) that ADR
0023 did not anticipate needing. The amendment is narrow and is worth stating
precisely, because it is easy to over-read: **the port describes the shape of
the dependency, not an equivalence of distributed semantics.** `DistributedLock`
says "something with `acquire`/`try_acquire`/`release`/`is_held`"; it says
nothing about cross-process exclusion, release on crash, or fairness. Those
guarantees are `PostgreSQLLockManager`'s alone, and ADR 0023's "When Not to
Use This" section -- and its single-primitive argument generally -- stands
untouched. `InMemoryLockManager`, the second conforming implementation this
ADR introduces, is a conforming implementation of the *narrower* Protocol
contract, restricted by its own docstring to single-process testing. ADR 0023
is **not** superseded; nothing about advisory locks as the production
mechanism changes.

Sibling of [ADR 0024](0024-projection-persistence-ports.md) and
[ADR 0026](0026-outbox-ring-migration.md) -- the same Protocol/implementation
split, applied to the last two pre-ring packages.

## Context

Four modules were left over from before the ring migration began: `locks/`
and `readmodels/` each mixed a Protocol (or a Protocol-shaped contract) with
its concrete implementations in the same package, the pattern ADR 0019, ADR
0024, and ADR 0026 already fixed for the store, checkpoint/DLQ, and outbox
surfaces; and top-level `engine.py`, which was never a Protocol/implementation
pair but nonetheless predated the ring structure and had no assigned ring.
`docs/core-surface.md` tracked all of these as open items, and `BACKLOG.md`
carried three "campaign residue" entries naming them directly: "Migrate
locks/ to ports/adapters," "Split readmodels/ into port + adapter," and
"Decide engine.py's ring placement."

`locks/` bundled `PostgreSQLLockManager`, `LockInfo`, `migration_lock_key`,
and the two lock exceptions in one package, with no Protocol anywhere --
`MigrationCoordinator` and `CutoverManager` annotated their `lock_manager`
parameters as the concrete `PostgreSQLLockManager` class because there was
nothing narrower to type against, and there was no way to write a fake lock
manager for a unit test short of hand-rolling one to the concrete class's
shape with no contract to check it against.

`readmodels/` already had its Tier 0 and non-Tier-0 halves in separate files
(`base.py`, `query.py`, `schema.py`, `repository.py`, `in_memory.py`,
`exceptions.py` on one side; `postgresql.py`, `sqlite.py`, `projection.py` on
the other) -- `docs/core-surface.md` finding 5 described the split as "already
made for it," needing only the package `__init__` to stop eagerly
re-exporting the backend half. `readmodels/postgresql.py` additionally
reached into `adapters/_sql/connection.py` for `sql_connection` across 16
call sites, a non-`adapters/` module importing from `adapters/` that ADR 0026
§4 recorded as accepted debt, resolved when the module itself moved under
`adapters/`.

`engine.py` held `create_async_engine` and `SQLITE_PRAGMAS`: a factory that
constructs a SQLAlchemy `AsyncEngine` and registers dialect-specific
`@event.listens_for` driver listeners for SQLite transaction control. It sat
at the top level of the package, imported eagerly by `eventsource/__init__.py`
as part of the sqlalchemy-on-import chain `docs/core-surface.md` finding 12
tracks. Unlike `locks/` and `readmodels/`, it was never a Protocol paired with
implementations -- there is exactly one `create_async_engine`, used by every
backend that needs a SQLAlchemy engine. The only question was where a
sqlalchemy-backed leaf module with no port of its own belongs.

## Decision

### 1. `locks/` splits along its two real consumer groups (ISP)

`ports/locks.py` defines two small Protocols rather than one wide interface,
because the library's actual callers split cleanly along a capability
boundary:

- **`DistributedLock`** -- `acquire`, `try_acquire`, `release`, `is_held`.
  What `migration/cutover.py` and `migration/coordinator.py` need: acquire
  and release individual locks.
- **`LockRegistry`** -- `release_all`, `held_lock_count`. Bulk lifecycle over
  everything one manager instance holds -- a shutdown-and-introspection
  concern for whoever owns the manager, not something a caller acquiring a
  single lock needs to see.

`PostgreSQLLockManager` (now `adapters/postgresql/locks.py`) and
`InMemoryLockManager` (`adapters/memory/locks.py`, new) both implement both
Protocols, so the split costs implementers nothing; it exists so a consumer
that only acquires locks can type against `DistributedLock` without pulling
in `release_all`/`held_lock_count` as part of its contract.

`acquire` is typed as returning `AbstractAsyncContextManager[LockInfo]`
rather than declared `async def acquire(...) -> AsyncIterator[LockInfo]`.
Both implementations decorate their `acquire` method with
`@asynccontextmanager`, and the *runtime* type `@asynccontextmanager`
produces is exactly `AbstractAsyncContextManager`, not an async generator --
declaring the Protocol method as `async def ... -> AsyncIterator[...]` would
describe the undecorated function body, not the object callers actually
receive, and would fail to type-check against either implementation.

`InMemoryLockManager` is a genuinely new implementation, not a relocation.
Its docstring leads with what it does not guarantee, and that ordering is
deliberate: **no cross-process exclusion** (a dict guarded by an
`asyncio.Condition`, not a database primitive); **no release on crash** (no
server session to drop when the holder disappears); **no fairness** (waiters
wake in whatever order `asyncio.Condition` happens to pick, no FIFO queue);
**single event loop, single process**. It exists so unit tests can exercise
code written against `DistributedLock` without a PostgreSQL instance, and for
nothing else -- reaching for it anywhere a real cross-process guarantee is
needed is a misuse the docstring is written to head off before the API
surface invites it.

### 2. Exception rebasing: `LockAcquisitionError` / `LockNotHeldError` onto `EventSourceError`

**Recorded exception (b).** `LockAcquisitionError` and `LockNotHeldError`
move to `eventsource.exceptions` and now subclass `EventSourceError`; they
previously derived directly from `Exception`, defined in
`eventsource/locks/postgresql.py`. This is **the one semantic change in the
whole slice**, and it is widening only: every existing
`except LockAcquisitionError` and `except Exception` still catches exactly as
before. The newly-catching clause is `except EventSourceError`, which caught
nothing lock-related prior to this change. The rebasing is motivated by
consistency, not by a defect -- there was no principled reason for the lock
exceptions to be the one library-raised error family outside the common base,
and application code that wants one boundary handler for "an eventsource
operation failed" can now write it without special-casing locks.

### 3. `readmodels/` becomes a `ports/readmodels/` subpackage, not a flat module

The rejected alternatives matter here as much as the decision, because both
were seriously considered:

**Rejected: flat `ports/readmodels.py`.** A single module mirroring
`ports/outbox.py` or `ports/dlq.py`'s shape. Rejected because `readmodels/`
is not one contract the way outbox or DLQ are -- it is four genuinely
distinct pure artifacts users reach for independently: a user-subclassable
pydantic base (`ReadModel`), a query specification language (`Query`,
`Filter`), a 15-method repository Protocol (`ReadModelRepository`), and an
exception family. Flattening them into one module would not reduce anything
real; it would just make a 400+-line file out of four files that are already
cleanly separated and already have independent reasons to change.

**Rejected: `domain/readmodel.py` for `ReadModel`.** Considered because
`ReadModel` is, in isolation, a plausible domain concept -- a pydantic base
class a user subclasses, much like `DomainEvent`. Rejected because
`ReadModel` is not a domain concept in the ring-architecture sense: it is the
*persistence contract's* payload shape, inseparable from `Query`/`Filter`
(which specify how it is queried) and `ReadModelRepository` (which specifies
how it is stored and retrieved). Splitting `ReadModel` into `domain/` while
its query and repository contract stayed in `ports/` would put one concept's
three faces in two rings for no boundary-clarifying reason -- the dependency
rule does not require it, since nothing about `ReadModel` reaches outward,
and colocating it with what it composes with reads more honestly than
colocating it with `DomainEvent`, which it is not.

**Decision: a subpackage.** `ports/readmodels/` holds `model.py` (`ReadModel`),
`query.py` (`Query`, `Filter`), `repository.py` (`ReadModelRepository`,
`ReadModelRepositoryProtocol`), and `exceptions.py` (`ReadModelError`,
`OptimisticLockError`, `ReadModelNotFoundError`). `eventsource.ports.readmodels`
is the one import path users see regardless of which of the four they want;
the submodule layout is an implementation detail, the same convention the
package docstring states explicitly.

The adapter half lands under `eventsource.adapters.{memory,postgresql,sqlite,
sql}`: `InMemoryReadModelRepository` in `adapters/memory/readmodels.py`,
`PostgreSQLReadModelRepository` in `adapters/postgresql/readmodels.py`,
`SQLiteReadModelRepository` in `adapters/sqlite/readmodels.py`.
`readmodels/postgresql.py`'s 16 `sql_connection` call sites now live inside
`adapters/`, resolving the accepted debt ADR 0026 §4 recorded against them --
there is no longer a non-`adapters/` module reaching into `adapters/` for a
connection helper on this path.

### 4. `schema.py` moves to `adapters/sql/readmodel_schema.py`, and loses its Tier-0 sqlalchemy guard by design

`generate_schema`, `generate_indexes`, `generate_full_schema`,
`POSTGRESQL_TYPE_MAP`, and `SQLITE_TYPE_MAP` move from `readmodels/schema.py`
to `adapters/sql/readmodel_schema.py`. Before this move, `docs/core-surface.md`
tracked the module as Tier 0 -- pure Python, no sqlalchemy import, generating
`CREATE TABLE` / `CREATE INDEX` text as plain strings. That was true and is
still true of the module's *imports*. It was the wrong thing to track,
though: the module hardcodes `POSTGRESQL_TYPE_MAP` and `SQLITE_TYPE_MAP` and
emits dialect-specific DDL text, which is dialect knowledge -- an adapter
concern -- regardless of whether the code happens to avoid importing the
`sqlalchemy` package to do it. The "must not import sqlalchemy" guard was
asserting a property (no sqlalchemy dependency) that happened to be true but
was never the property that actually mattered (does this module encode
backend-specific knowledge). Once the question was asked correctly, the
answer moved the module out of Tier 0's import-linter contract by design, not
by accident -- see the `pyproject.toml` comment beside the Tier 0 contract for
the same point stated at the enforcement layer.

`ReadModelProjection` (from `readmodels/projection.py`) moves to
`adapters/sql/readmodel_projection.py`, alongside `DatabaseProjection`, which
it already depended on module-level -- the same non-Tier-0 status it already
had, now correctly colocated with the SQL adapter it extends rather than
living in a package that also hosted Tier 0 contract code.

### 5. `engine.py` moves to `adapters/_sql/engine.py`

Two rejected alternatives, both narrower moves that were considered and set
aside:

**Rejected: dissolve into the existing connection helpers.** `engine.py`
could have been split across `adapters/_sql/connection.py` and
`adapters/_sql/dialect.py`, on the theory that a single-function module is a
seam not worth keeping. Rejected because `create_async_engine` is not a
connection-normalization helper or a dialect-parameterization helper -- it is
a distinct concern (constructing the `AsyncEngine` itself, plus SQLite
transaction-control and PRAGMA setup via `@event.listens_for` listeners) that
both of those modules currently need, not one they should absorb. Merging it
in would make either module do two unrelated jobs for no reduction in moving
parts.

**Rejected: `adapters/sql/engine.py`.** The narrower question was not
*whether* to move it into `adapters/`, but *which* `adapters/sql*` package.
`adapters/sql/__init__.py` eagerly imports `checkpoints`, `dlq`, and
`projection`, and `projection` (`adapters/sql/projection.py`) reaches into
`application/projections/`. Placing a leaf engine factory in `adapters/sql/`
would drag the application-ring projection machinery into whatever imports
`create_async_engine` -- exactly the front-door import-chain widening
`docs/core-surface.md` finding 12 exists to narrow, not grow.

**Decision: `adapters/_sql/engine.py`.** `adapters/_sql/__init__.py` is
import-free -- it already hosts `connection.py`, `dialect.py`, and
`positions.py` on that basis -- so placing `engine.py` there costs nothing
beyond what importing the module itself costs. `eventsource.create_async_engine`,
the public name, is unchanged in signature and behavior; only the module path
changed, and the module path was never the advertised import surface (nothing
in the docs told users to import from `eventsource.engine` directly). The
front-door import chain moves one module deeper --
`eventsource/__init__` -> `adapters/_sql/engine` -> `sqlalchemy` -- but does
not get cheaper or more expensive: `adapters/_sql/engine.py`'s own import
block is identical to `engine.py`'s, unchanged by the move.

### 6. Recorded exception (a): the read-model exception trio stays out of `exceptions.py`

`ReadModelError`, `OptimisticLockError` (the read-model one), and
`ReadModelNotFoundError` remain in `ports/readmodels/exceptions.py` rather
than joining the central `eventsource.exceptions` module the way
`LockAcquisitionError`/`LockNotHeldError` did. The reason is the
name collision this ADR does not resolve: `eventsource.exceptions` already
defines `OptimisticLockError` for aggregate/event-store version conflicts,
with a different constructor signature (`aggregate_id`, `expected_version`,
`actual_version` versus the read-model one's `model_id`, `expected_version`,
`actual_version=None`) and no inheritance relationship between the two.
Moving the read-model exceptions into `eventsource.exceptions` today would
either silently shadow the existing name or require resolving the collision
as a side effect of an unrelated ring-placement task -- neither is
acceptable. **This collision predates this slice**; it is not introduced by
the ring migration, and BACKLOG.md's "Resolve the duplicate
`OptimisticLockError` name (readmodels vs core)" entry (P2) is the follow-up
that names the fix (rename the read-model class to
`ReadModelVersionConflictError` with a deprecation alias) and tracks it
explicitly as pre-existing.

### 7. Deprecation shims: `eventsource.locks` and `eventsource.readmodels`

Both packages become lazy re-export shims. `eventsource/locks/__init__.py`
resolves `LockInfo`, `migration_lock_key` (from `ports.locks`),
`LockAcquisitionError`, `LockNotHeldError` (from `exceptions`), and
`PostgreSQLLockManager` (from `adapters.postgresql.locks`).
`eventsource/readmodels/__init__.py` resolves all sixteen names the old
package exported, split across `ports.readmodels` and the three adapter
modules plus `adapters.sql.readmodel_schema`/`readmodel_projection`. Each
`__getattr__` emits a `DeprecationWarning` naming the specific new import
path for the attribute being read, and each module defines `__dir__`
alongside `__getattr__` -- a lazy `__getattr__` without `__dir__` broke
introspection (`dir(module)`, tab completion, some `pytest` collection
paths) during the snapshots migration and had to be fixed after the fact;
both shims here ship with the fix already in place. Both packages are
removed in 0.8.0, tracked in `BACKLOG.md`.

## Consequences

**Positive.**

- `ports/locks.py` and `ports/readmodels/` are genuinely Tier 0: no
  sqlalchemy, importable in a bare interpreter. `docs/core-surface.md`'s
  Tier 0 boundary now covers two more subsystems that used to taint an
  entire package on import.
- Code that only needs to acquire a lock can type against `DistributedLock`
  and be tested against `InMemoryLockManager`, with no PostgreSQL dependency
  in the test suite for that code path.
- `ReadModelError`'s three exceptions, `ReadModel`, `Query`, and `Filter` are
  importable without pulling in any of the three backend adapters -- a
  consumer writing pure query-construction code no longer needs sqlalchemy
  or aiosqlite on the import path.
- The accepted debt ADR 0026 §4 recorded against `readmodels/postgresql.py`
  is resolved for that one module; `migration/repositories/*.py`'s four
  modules remain the open remainder, tracked separately.

**Negative / accepted.**

- The `OptimisticLockError` name collision is not fixed by this slice, only
  documented and tracked. Two unrelated classes with the same name across
  `eventsource.exceptions` and `eventsource.ports.readmodels.exceptions`
  remains a trap for anyone importing both into the same scope.
- `InMemoryLockManager` adds a second lock implementation to maintain, with
  no production use -- pure test-support surface area, justified only by
  what it buys for the Protocol split's usability.
- Front-door import time for `eventsource` is unchanged in cost, not reduced:
  the engine move relocates the sqlalchemy entry point, it does not remove
  it. `BACKLOG.md`'s "Lazy top-level eventsource/__init__" entry is updated
  to reflect the new one-module-deeper chain, but stays out of this slice's
  scope.
- Two deprecated packages now carry runtime warning overhead until 0.8.0.
  Existing code that imports from `eventsource.locks` or
  `eventsource.readmodels` continues to work but emits a `DeprecationWarning`
  per attribute access, which a `-W error::DeprecationWarning` test
  configuration will surface as failures until callers migrate.

## Alternatives Considered

**Leave `locks/` and `readmodels/` as-is, since only one implementation
exists for each.** Considered and rejected on the same grounds ADR 0024 and
ADR 0026 already rejected it for checkpoints/DLQ and outbox: a single
implementation today does not mean the Protocol/implementation split is
premature -- it means the split has been deferred, and every module in the
ring migration so far had exactly this shape (one production backend, a
contract worth naming anyway for the ports Tier 0 boundary and for testing).
The pattern established by five prior ADRs was reason enough to complete it
for the two remaining packages rather than leave `docs/core-surface.md`
recording two more open items indefinitely.

**Fold the engine-placement decision into a larger `adapters/_sql/`
reorganization.** Considered and rejected as scope creep: `engine.py`'s move
is a pure relocation with a two-sentence rationale, not a redesign, and
bundling it with unrelated `adapters/_sql/` restructuring would have made a
mechanical change harder to review for no benefit.
