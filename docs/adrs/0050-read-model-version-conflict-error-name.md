# 0050. The Read-Model Conflict Error Gets Its Own Name

Two unrelated exception classes were both called `OptimisticLockError`. Neither
caught the other, neither derived from the other, and their constructors did
not agree on a single argument. Only the import path distinguished them.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0029](0029-locks-readmodels-and-engine-rings.md) | Amended — its recorded exception, that `ports/readmodels/` would carry an `OptimisticLockError` colliding by name with the domain one, no longer applies. The subpackage split it decided is untouched. |
| [0041](0041-infrastructure-exceptions-to-ports.md) | Stands. The split it decided put infrastructure-meaning exceptions in `ports/exceptions.py` and domain ones in `domain/exceptions.py`; the read-model family was never part of either move and stays in `ports/readmodels/exceptions.py`. |

## Context

`eventsource.domain.exceptions.OptimisticLockError(EventSourceError)` is raised
by `append` when a stream's version does not match `expected_version`. It
carries `aggregate_id`, `expected_version`, `actual_version`.

`eventsource.ports.readmodels.exceptions.OptimisticLockError(ReadModelError)`
is raised by `save_with_version_check` when a read model's stored save-count
does not match the one the caller loaded. It carries `model_id`,
`expected_version`, `actual_version`.

The two guard different things. The first protects the consistency boundary of
an aggregate, and a caller handling it reloads the aggregate and re-runs a
command. The second protects a row against a lost update, and a caller handling
it re-reads the row and re-projects. A `version` on the read side counts saves
of that row and has no relationship to any position in the event stream.

Sharing a name across that seam costs in three ways. `except
OptimisticLockError` reads as though it covers both and covers exactly one,
with which one decided by an import the reader has to scroll up to find.
Prose has to disambiguate every mention — `ports/readmodels/__init__.py`
carried a paragraph doing nothing else, and `docs/architecture.md` carried
another. And an `__init__.py` that wants to re-export both cannot, which is
why the public API exports the domain one and leaves the read-model one
reachable only through `eventsource.ports.readmodels`.

## Decision

Rename the read-model class to **`ReadModelVersionConflictError`**. The domain
class keeps `OptimisticLockError` — it is the one the public API exports, the
one the tutorials and the event-store reference discuss, and the one whose name
matches the pattern's usual vocabulary.

The name is not `ReadModelOptimisticLockError`. The mechanism is optimistic
locking either way; what a reader needs from the name is *which thing
conflicted*, and `version conflict on a read model` is that. It also keeps the
class inside the `ReadModel*` prefix the rest of the family already uses
(`ReadModelError`, `ReadModelNotFoundError`).

**No deprecation alias.** The project's pre-1.0 no-shim policy (ADR 0030)
applies: the old name is gone, and `from eventsource.ports.readmodels import
OptimisticLockError` raises `ImportError` rather than resolving to something
that will be removed later. A silent alias would preserve exactly the ambiguity
this ADR exists to remove.

## Consequences

A caller that imported the read-model error by name must update the import;
one that caught it via `ReadModelError` is unaffected. The domain-side name and
import path do not change, so write-side code is untouched.

The disambiguating prose in `ports/readmodels/__init__.py` and
`docs/architecture.md` shrinks to a historical note — worth keeping, because a
reader who encounters the old name in a changelog or an older branch still
needs to know which class it was.

The public API remains free to export `ReadModelVersionConflictError` later
without a collision. It does not do so here only because it exports no
read-model exception today, and adding one alone would be arbitrary.
