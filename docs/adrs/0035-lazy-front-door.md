# 0035. PEP 562 Lazy Front Door

`import eventsource` eagerly imported sqlalchemy through the top-level
`__init__.py`'s module-level `from eventsource.adapters.postgresql import
...` and similar adapter imports, regardless of which names a caller
actually used. This ADR replaces those eager imports with a PEP 562
`__getattr__`/`__dir__` pair: every public name resolves and is cached on
first access, and a bare `import eventsource` no longer loads any adapter
or driver at all.

## Status

**Accepted.** Implemented in `src/eventsource/__init__.py`. `__all__` is
byte-identical to before this change — same 133 names (134 when
`aiosqlite` is installed, conditionally appending `SQLiteOutboxRepository`
as it always did), same order. `__version__` remains the only name computed
eagerly.

## Context

`docs/core-surface.md`'s BACKLOG note on this had tracked the eager-import
chain through two renames without it ever getting cheaper: `eventsource.engine`
(pre structure-slice-A) to `eventsource.adapters._sql.engine` (post ADR
0029) was one module deeper, but the front door still did exactly two
module-level adapter imports either way, at the same sqlalchemy cost. By the
time this ADR landed, `eventsource/__init__.py` imported
`eventsource.adapters.memory`, `eventsource.adapters.postgresql`, and
`eventsource.adapters.sql` at module level for the store/checkpoint/DLQ/outbox
exports, and those packages' own `__init__.py` files had accumulated further
eager imports of `readmodels`/`locks` submodules along the way — so the cost
was still there, just reached through more hops.

The fix is mechanical rather than judgment-heavy: a `_LAZY: dict[str, str]`
mapping every public name to the module that provides it, built by parsing
the *existing* import statements in `__init__.py` against `__all__` with a
small AST script rather than by hand — every one of the 133 names matched
its source module on the first pass, with zero manual guesses. A
`TYPE_CHECKING` block carries the exact same import statements verbatim, so
mypy and IDEs still resolve `eventsource.X` statically with no
`attr-defined` fallout anywhere in the codebase (`uv run mypy src` stayed
green across all 182 files touching this front door).

One name needed special handling. `__all__` conditionally appends
`"SQLiteOutboxRepository"` only when `aiosqlite` is installed — the
existing behavior, unchanged. Computing that condition by importing
`eventsource.adapters.sqlite` (to read its `SQLITE_AVAILABLE` flag) would
register `eventsource.adapters` in `sys.modules` as a side effect of
computing `__all__` itself, defeating the exact runtime check this ADR
exists to make possible. Instead, `__init__.py` probes `aiosqlite` directly
— a third-party package unrelated to `eventsource.adapters` — the same way
`eventsource.adapters.sqlite.store` itself probes it internally.

`__version__` is the only name that stays eager: it is a pure
`importlib.metadata.version()` lookup against stdlib, with no adapter or
driver cost, and callers reasonably expect it to be available the instant
the module is imported without triggering any lazy-resolution machinery.
`eventsource.domain.event_registry.default_registry` was checked for
import-time side effects (auto-registration, I/O) that might make it a
second eager candidate — it is not: `default_registry = EventRegistry()` is
a plain instantiation with no registration behavior at module load, so it
stays lazy along with everything else.

## Consequences

### Positive

- `import eventsource` no longer loads sqlalchemy, asyncpg, aiosqlite,
  redis, aiokafka, or aio-pika. Verified by a subprocess test (the pytest
  process itself has already imported half the world by the time any
  in-process test runs, so this cannot be checked without a fresh
  interpreter): `python -c "import eventsource, sys; assert 'sqlalchemy'
  not in sys.modules"`.
- Runtime Tier-0 purity checks become possible for the first time. The
  `ports/readmodels/` purity test
  (`tests/unit/ports/test_readmodels_port_surface.py`) had to be written as
  a static `ast` check over import statements rather than a runtime
  `sys.modules` assertion, specifically because importing `eventsource` at
  all — which `eventsource.ports` sits under — already loaded sqlalchemy
  through the eager front door, for a reason unrelated to `ports/readmodels/`
  itself. That constraint is gone; a runtime `import
  eventsource.ports.readmodels; assert "sqlalchemy" not in sys.modules`
  check is now a valid test, though the existing ast-based test is left in
  place since it still passes and rewriting it was out of this ADR's scope.
- Import time drops for any caller that only touches a subset of the public
  surface — a caller using only `DomainEvent`/`EventRegistry` never pays
  for sqlalchemy, asyncpg, or any broker client at all.

### Negative

- The one-time mechanical migration risk: if a future contributor adds a
  new name to `__all__` without a matching `_LAZY` entry, `getattr(eventsource,
  name)` raises `AttributeError` instead of `NameError` at definition time.
  Mitigated by `test_lazy_mapping_covers_every_dunder_all_name`, which
  asserts `set(_LAZY) >= set(__all__) - {"__version__"}` and fails loudly
  if the two ever drift.
- `dir(eventsource)` and `getattr` now do slightly more work per first
  access (a `importlib.import_module` call) than a plain attribute lookup
  would, though this is subsequently cached in the module's `globals()` and
  is not repeated on later access to the same name.

## Alternatives Considered

**Keep eager imports but reorder them to minimize the sqlalchemy-loading
modules touched.** Rejected: this was tried twice before (the
`eventsource.engine` renames the BACKLOG note tracked) and never reduced
the actual cost, only moved which two module-level imports paid it. The
problem is architectural (imports happen at all, regardless of use), not a
matter of import ordering.

**Only lazy-load the sqlalchemy-backed names (`PostgreSQLEventStore`,
`SQLCheckpointRepository`, etc.) and leave pure names
(`DomainEvent`, `EventRegistry`) eager.** Rejected: this would have required
maintaining two separate code paths (eager imports for pure names, lazy
`__getattr__` for the rest) for no real benefit — the pure names have no
import cost to save, so lazy-loading them too costs nothing extra and keeps
the mechanism uniform and mechanically verifiable against `__all__` in one
pass, rather than two overlapping ones.

## References

- `src/eventsource/__init__.py`
- `tests/unit/test_lazy_import.py`
- BACKLOG.md's "Lazy top-level eventsource/__init__" entry (removed by this
  ADR's implementation; see CHANGELOG.md)

## Related

- `docs/core-surface.md` — the eager-import-chain finding this ADR resolves;
  updated to record that bare `import eventsource` is now sqlalchemy-free
  and runtime-verifiable
- `tests/unit/ports/test_readmodels_port_surface.py` — the ast-based purity
  test whose docstring explained the pre-existing constraint this ADR lifts
