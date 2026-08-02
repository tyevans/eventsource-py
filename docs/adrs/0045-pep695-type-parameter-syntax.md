# 0045. PEP 695 Type-Parameter Syntax

Every generic declaration in the library — classes, functions, and the one
type alias — moves from the pre-PEP-695 form (a module-level
`TypeVar` plus a `Generic[...]` base, or a `TypeAlias` annotation) to native
Python 3.13 type-parameter syntax. The rewrite is mechanical everywhere
except in one respect: a PEP 695 type parameter is scoped to the declaration
that introduces it, so the module-level `TypeVar` objects that used to back
these declarations cease to exist. Three of them were exported. Their
removal is a breaking change, and `TState` — a documented, top-level export
of `eventsource` — is the headline.

## Status

**Accepted.** Implemented across the five tasks of the PEP 695 wave
(commits `bcb8eb5`, `1e4213c`, `60473bc`, `808c179`, `ff8337d`).

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0043](0043-domain-model-guards-and-vocabulary.md) | Amended — 0043 raised the Python floor to 3.13 and staged three ruff `UP04x` rules as documented `pyproject.toml` ignores "pending a dedicated follow-up." This ADR is that follow-up; the ignores are gone. 0043's own Decision (the domain guards and the `domain/types.py` vocabulary) is unaffected except that `TState`, which 0043 left in place while deleting `Version`/`StreamPosition`/`GlobalPosition`, is now deleted too. |
| [0022](0022-command-objects-and-decider-style.md) | Stands — `DeciderAggregate` keeps both its one- and two-parameter subscript forms; only the declaration syntax changes. |

ADR 0043's Status section carries an "Amended by ADR 0045" pointer.

## Context

PR #104 (ADR 0043) bumped ruff's `target-version` to `py313`. That single
change activated three modernization rules repo-wide —

| Rule | What it flags |
| --- | --- |
| `UP046` | Generic class uses a `Generic` subclass instead of type parameters |
| `UP047` | Generic function should use type parameters |
| `UP040` | Type alias uses a `TypeAlias` annotation instead of the `type` keyword |

— which produced violations across files that wave had never touched: the
domain aggregates, the application repositories, all four read-model
adapters, the testing helpers, and the benchmark harness. Fixing them inside
the teaching-layer wave would have meant a structural rewrite of the
library's entire generic surface bolted onto an unrelated docs change, so
they were staged as three documented `ignore` entries in
`[tool.ruff.lint]`, carrying a comment that named the deferral and forbade
new violations.

That comment also named auto-variance as the risk that made the deferral
worth taking: PEP 695 removes the explicit `covariant=`/`contravariant=`
arguments and lets the type checker infer variance per declaration, which
can change what a checker accepts. **That risk turned out not to exist
here.** No `TypeVar` anywhere in this repository ever declared explicit
variance, so nothing was lost in translation and mypy's inferred variance
matched the previous invariant behavior at every site. Recording this
plainly matters: the deferral was justified by a hazard that a five-minute
audit would have shown to be absent.

The real cost was elsewhere, and it is the reason this needed its own wave:
type parameters are scoped, and exported `TypeVar`s are not.

## Decision

**Convert every generic declaration in `src/`, `tests/`, and `bench/` to
native type-parameter syntax, and delete the three staged ignores.** The
`[tool.ruff.lint] ignore` list now contains `E501` alone; a new pre-PEP-695
generic is a lint error, not a style preference.

### The scoping consequence, and three public-surface reductions

A pre-695 generic needs a module-level object:

```python
TState = TypeVar("TState", bound=BaseModel)

class AggregateRoot(Generic[TState], ABC): ...
```

That object is importable, which is why three of them had been exported. A
PEP 695 declaration has no such object:

```python
class AggregateRoot[TState: BaseModel](ABC): ...
```

`TState` here is a parameter of the class statement. There is nothing at
module scope to export, and under the pre-1.0 NO-SHIMS policy no aliased
fallback is introduced. Three exports therefore die:

| Name | Was exported from | Aggregated public API affected? |
| --- | --- | --- |
| `TState` | `domain/types.py`, `eventsource.domain.__all__`, `eventsource.__all__`, and the lazy-import map in `eventsource/__init__.py` | **Yes** — `from eventsource import TState` now raises `ImportError` |
| `TAggregate` | `application/aggregates/repository.py`'s `__all__` and `application/aggregates/__init__.py` | Yes, at that package path |
| `TEvent` | `testing/builder.py`'s `__all__` | No — never re-exported at the `eventsource.testing` level; a direct-import surface reduction only |

The migration for a consumer is one line. Code that wrote a generic helper
over aggregate state declares its own inline parameter instead of importing
the library's:

```python
# before
from eventsource import TState
def f(a: AggregateRoot[TState]) -> None: ...

# after
def f[T: BaseModel](a: AggregateRoot[T]) -> None: ...
```

The bound is `BaseModel`, exactly as the deleted `TypeVar` declared it, and
the parameter's name is now the caller's choice.

### Conversions beyond the planned inventory

The plan enumerated fifteen sites that ruff flagged. Five more were
converted because leaving them would have produced a module where a
function-scoped parameter shadows a same-named module-level `TypeVar` — a
shape that reads as a bug even when it is semantically identical:

- `DeclarativeAggregate` (`domain/aggregate.py`)
- `EventRegistry.register` and `EventRegistry._resolve_event_type`
  (`domain/event_registry.py`)
- `CircuitBreaker.execute` and `RetryableOperation.execute`
  (`application/subscriptions/retry.py`), which shared the module-level `T`
  with the flagged `retry_async`
- `when_command` (`testing/bdd.py`), which shared `TAggregate` with the
  flagged `then_event_published`

The governing principle: **never ship a module where a function-scoped type
parameter shadows a same-named module-level `TypeVar`.** Convert the
remaining users so the module-level name can die. All five are pure syntax;
none changed a signature.

### Bounds are evaluated eagerly and are not deferred annotations

The one genuine trap in this migration, which shaped three separate tasks:

> A PEP 695 **bound expression is evaluated eagerly, in its own scope, and
> is not covered by `from __future__ import annotations`.**

`from __future__ import annotations` postpones *annotations*. A bound is not
an annotation — it is part of the type-parameter declaration, evaluated when
the `class`/`def` statement executes. So a bound naming a symbol imported
only under `if TYPE_CHECKING:` raises `NameError` at import time unless it
is written as a string literal.

This cuts both ways in the codebase, and both spellings in it are correct:

- `domain/event_registry.py` genuinely needs its quoted form,
  `def register_event[TEvent: "DomainEvent"](...)`, because `DomainEvent` is
  `TYPE_CHECKING`-only there.
- Every module in the application, adapters, and testing rings imports its
  bound name at runtime — `AggregateRoot`, `ReadModel`, `DomainEvent` — even
  the ones that also carry `from __future__ import annotations` and have
  *other* `TYPE_CHECKING`-guarded imports. Those bounds are written bare,
  and quoting them would be noise.

Anyone adding a generic later needs to know this rule; the mixed spellings
in the tree are not inconsistency.

## Consequences

### Positive

- The library's generic surface reads as modern Python: the parameter, its
  bound, and the declaration it belongs to are one statement.
- The `ignore` list is empty of staged modernization. `E501` remains for
  line length; nothing else is deferred there.
- New pre-PEP-695 generics fail lint on arrival, so the migration cannot
  erode.
- `Generic`, `TypeVar`, and `TypeAlias` imports are gone from every
  converted module — `domain/types.py` now has zero third-party imports.

### Negative

- **Three breaking export removals**, listed in the table above. `TState`'s
  is the visible one: `from eventsource import TState` was documented public
  API. The fix is one line and mechanical, but it is a break, and there is
  no shim.
- Documentation that described `TState` as a module-level `TypeVar` had to
  be rewritten across `docs/api/index.md`, `docs/api/types.md`,
  `docs/api/aggregates.md`, `docs/core-surface.md`, and
  `docs/tutorials/03-first-aggregate.md`. Prose that uses `TState` merely as
  the parameter's *name* remains correct and was left alone.

### Incidental changes

Two changes belong to this wave without belonging to its Decision:

- **`.pre-commit-config.yaml` gained `default_language_version: python3.13`.**
  The `debug-statements` hook runs in pre-commit's own isolated environment,
  which resolved to Python 3.12 on the implementing machine. 3.12 supports
  PEP 695 type parameters but not PEP 696 defaults, so it could not parse
  `class DeciderAggregate[TState: BaseModel, TCommand = object]` and blocked
  every commit in the wave. The repo floor is already 3.13 per ADR 0043;
  this makes pre-commit agree with it.
- **`bench/adapters/base.py` gained three documented `# noqa: B027`.**
  Dropping `Generic[T]` as a second base surfaced three pre-existing
  findings that flake8-bugbear had been skipping — it does not check
  empty-ABC-method bodies on classes with two or more bases, so
  `class BenchAdapter(ABC, Generic[T])` was silently exempt while
  `class BenchAdapter[T](ABC)` is not. `setup`, `teardown`, and `destroy`
  are intentionally optional lifecycle hooks with no-op defaults; only
  `create()` is `@abstractmethod`. Suppression is the correct response —
  adding real `@abstractmethod` decorators would force no-op overrides onto
  every existing adapter, a behavior change this refactor does not license.

### Neutral

- **Variance:** no `TypeVar` in the repo declared `covariant=` or
  `contravariant=`, so mypy's per-declaration inference produced no
  behavioral difference. `uv run mypy src/eventsource/` reported no issues
  at every step of the wave.
- `DeciderAggregate[State]` and `DeciderAggregate[State, Command]` both
  remain valid — the PEP 696 default on `TCommand` survives the syntax
  change, pinned by a test in
  `tests/unit/domain/test_decider_aggregate.py`.

## Alternatives Considered

**Keep `TState` as a module-level `TypeVar` alongside the native
declarations, purely so the export survives.** Rejected: it would leave a
module-level object with the same name as, but no relationship to, the class
parameters — the exact shadowing shape this ADR converts five extra sites to
avoid — and an exported `TypeVar` that no declaration in the library uses is
a name with nothing behind it.

**Ship an aliased fallback (`TState = TypeVar("TState", bound=BaseModel)`
re-exported with a deprecation warning).** Rejected under the standing
pre-1.0 NO-SHIMS policy established during the rings campaign. The consumer
fix is a one-line inline declaration; a deprecation cycle costs more to
carry than the break costs to absorb.

**Leave the three ignores in place indefinitely.** Rejected: a permanent
ignore on a modernization rule is indistinguishable from a policy decision
not to modernize, and the comment's stated reason (auto-variance risk) was
false in this repository.

## References

- `src/eventsource/domain/aggregate.py`, `decider.py`, `event_registry.py`,
  `types.py` — the domain-ring conversions and the `TState` deletion
- `src/eventsource/application/aggregates/repository.py`,
  `tenant_repository.py` — `TAggregate`
- `src/eventsource/application/projections/base.py` — the sole `UP040` site
  (`TenantFilter` becomes a `type` alias)
- `src/eventsource/application/subscriptions/retry.py` — the shared-`T`
  cleanup
- `src/eventsource/adapters/{memory,postgresql,sql,sqlite}` read-model
  modules — `TModel`, never exported
- `src/eventsource/testing/builder.py`, `bdd.py` — `TEvent`, `TAggregate`
- `bench/adapters/base.py` — the `B027` suppressions
- `pyproject.toml` — `[tool.ruff.lint] ignore` reduced to `E501`
- [ADR 0043](0043-domain-model-guards-and-vocabulary.md) — raised the
  Python floor to 3.13 and staged the three ignores this ADR removes
- [ADR 0022](0022-command-objects-and-decider-style.md) — the decider surface
  whose two-arity subscript this migration preserves

## Related

- `CHANGELOG.md` — `[Unreleased]` **Breaking** entry for the `TState`
  removal
