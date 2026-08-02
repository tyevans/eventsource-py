# PEP 695 Type-Parameter Syntax Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the library's 15 remaining pre-PEP-695 generic declarations to Python 3.13 native type-parameter syntax, then remove the three staged ruff ignores (`UP046`, `UP047`, `UP040`) that PR #104 left behind.

**Architecture:** PEP 695 scopes type parameters to the declaration that uses them, replacing module-level `TypeVar` objects. Each conversion is local: `class Foo(Generic[T])` → `class Foo[T]`, `def f(x: T) -> T` → `def f[T](x: T) -> T`, `X: TypeAlias = ...` → `type X = ...`. Bounds move inline (`TypeVar("T", bound=B)` → `[T: B]`); PEP 696 defaults move inline too (`TypeVar("T", default=object)` → `[T = object]`, native in 3.13).

**The one non-mechanical consequence:** `TState` is not a private helper — it is exported from `eventsource.__all__`, `eventsource.domain.__all__`, the lazy-import map, and documented on `docs/api/types.md`. Once `AggregateRoot` and `DeciderAggregate` scope their own parameters, no library code references it. This plan **deletes it** rather than leaving an exported TypeVar the library itself no longer uses. That is a breaking change, taken under the pre-1.0 NO-SHIMS policy, and it is the reason this wave carries an ADR.

**Tech Stack:** Python 3.13, pydantic v2, pytest, uv, ruff, import-linter, mypy strict.

## Global Constraints

- Branch `pep695-type-parameters` off `origin/main` (dde0236, includes PRs #103, #104, #105). PR targets `main`. **Never self-merge.**
- **Behavior must not change.** These are type-level rewrites. Existing tests pass **without modification** except where a test imports `TState` directly. If a test needs a behavioral edit, the conversion was wrong — stop and report.
- **No `TypeVar` may survive at module scope unless something still references it.** After converting a declaration, delete the now-orphaned `TypeVar` and drop the `Generic` / `TypeVar` / `TypeAlias` imports that become unused. `ruff check` (F401) is the gate.
- **Do not convert what the plan does not name.** `TypeVar`s that are genuinely shared across several declarations in one module, or bound into `Protocol` definitions, may legitimately remain — if a conversion forces a signature change beyond syntax, stop and report rather than inventing one.
- Every task: `uv run ruff check src tests bench && uv run mypy src/eventsource/ && uv run lint-imports` before commit.
- Commit style: `<type>: <lowercase description>`.
- Full gate at end: `make check`.
- Subagents run targeted tests; orchestrator owns the full suite.
- **Docstrings and comments stay with their declaration.** A comment that documents a deleted `TypeVar` (e.g. `# Type variable for event types`) goes with it; do not orphan it above an unrelated line.

## Conversion Inventory

All 15 sites, grouped by the task that owns them:

| Task | File | Site | Rule |
|---|---|---|---|
| 1 | `domain/aggregate.py:41` | `AggregateRoot(Generic[TState], ABC)` | UP046 |
| 1 | `domain/decider.py:22` | `DeciderAggregate(AggregateRoot[TState], Generic[TState, TCommand])` | UP046 |
| 1 | `domain/event_registry.py:272,284` | `register_event` overloads | UP047 |
| 1 | `domain/types.py:21` | `TState` — **deleted**, see Task 1 | — |
| 2 | `application/aggregates/repository.py:47` | `AggregateRepository` | UP046 |
| 2 | `application/aggregates/tenant_repository.py:28` | `TenantAwareRepository` | UP046 |
| 2 | `application/projections/base.py:61` | `TenantFilter` type alias | UP040 |
| 2 | `application/subscriptions/retry.py:256` | `retry_async` | UP047 |
| 3 | `adapters/memory/readmodels.py:33` | `InMemoryReadModelRepository` | UP046 |
| 3 | `adapters/postgresql/readmodels.py:38` | `PostgreSQLReadModelRepository` | UP046 |
| 3 | `adapters/sql/readmodel_projection.py:36` | `ReadModelProjection` | UP046 |
| 3 | `adapters/sqlite/readmodels.py:51` | `SQLiteReadModelRepository` | UP046 |
| 4 | `testing/builder.py:29` | `EventBuilder` | UP046 |
| 4 | `testing/bdd.py:158` | `then_event_published` | UP047 |
| 4 | `bench/adapters/base.py:16` | `BenchAdapter` | UP046 |

**Not in scope:** `ports/readmodels/repository.py:20` declares `TModel` for a `Protocol`; `domain/decorators.py:23` declares `F` bound to `Callable`; neither is flagged by ruff and neither is touched. Variance: **no `TypeVar` in this repo declares `covariant=` or `contravariant=`**, so there is no explicit variance to preserve. mypy infers variance for PEP 695 parameters; `mypy --strict` passing is the acceptance signal.

---

### Task 1: Convert the domain ring and retire `TState`

**Files:**
- Modify: `src/eventsource/domain/aggregate.py`, `src/eventsource/domain/decider.py`, `src/eventsource/domain/event_registry.py`, `src/eventsource/domain/types.py`
- Modify: `src/eventsource/domain/__init__.py`, `src/eventsource/__init__.py` (remove the `TState` export in all three places)
- Test: `tests/unit/domain/test_aggregate_snapshot_methods.py`, `tests/unit/domain/test_exceptions_home.py` (import updates only)

**Interfaces:**
- Produces: `AggregateRoot[TState: BaseModel]` and `DeciderAggregate[TState: BaseModel, TCommand = object]`. The two-parameter subscript `DeciderAggregate[OrderState, OrderCommand]` and the one-parameter `DeciderAggregate[OrderState]` must **both** keep working — the PEP 696 default is load-bearing and documented. Tasks 2-4 depend on `AggregateRoot`'s parameter staying bound to `BaseModel`.

- [ ] **Step 1: Convert `aggregate.py`**

`class AggregateRoot(Generic[TState], ABC)` → `class AggregateRoot[TState: BaseModel](ABC)`. Remove the `TState` import from `domain.types`. Leave `TEvent` at module scope — `create_event` uses it and is not in this task's inventory; convert it only if ruff still flags it after your change.

- [ ] **Step 2: Convert `decider.py`**

`class DeciderAggregate(AggregateRoot[TState], Generic[TState, TCommand])` → `class DeciderAggregate[TState: BaseModel, TCommand = object](AggregateRoot[TState])`. Delete the module-level `TCommand = TypeVar("TCommand", default=object)` and the now-unused `TState` re-import from `aggregate`.

**Verify the default survives** — add to `tests/unit/domain/test_decider.py` (or the nearest existing decider test module) a test that subscripts both arities and asserts construction works:

```python
def test_decider_aggregate_accepts_one_and_two_parameter_subscripts() -> None:
    """The PEP 696 default on TCommand keeps the single-parameter form valid."""
    assert DeciderAggregate[CounterState] is not None
    assert DeciderAggregate[CounterState, int] is not None
```

- [ ] **Step 3: Convert the `register_event` overloads in `event_registry.py`**

Both overloads at lines 272 and 284 take type parameters. The module-level `TEvent` (line 52) stays only if another declaration still uses it — check with `grep -n TEvent src/eventsource/domain/event_registry.py` after converting, and delete it if orphaned.

- [ ] **Step 4: Delete `TState` from `domain/types.py` and every export site**

Remove the definition, then remove it from:
- `src/eventsource/domain/__init__.py` (the import block ~line 51 and `__all__` ~line 89)
- `src/eventsource/__init__.py` (the import block ~line 173, `__all__` ~line 225, and the lazy-import map ~line 402)

If `domain/types.py` is left with no `TypeVar` usage, drop the `TypeVar` and `BaseModel` imports too.

- [ ] **Step 5: Sweep the repo for `TState` references**

This is the step that prior waves kept missing. Run it and act on every hit — do not defer to the final review:

```bash
grep -rn "TState" --include='*.py' --include='*.md' src tests bench docs
```

`docs/api/types.md` documents `TState` as public API; `docs/api/index.md`, `docs/core-surface.md`, `docs/architecture.md`, and several tutorials/guides reference it. Update prose to describe the inline `[TState: BaseModel]` form instead. **If `docs/api/types.md` autodocs the symbol via a mkdocstrings `:::` directive, that directive must be removed or the strict docs build fails** — verify with `uv run mkdocs build --strict` before committing. Do not update `docs/superpowers/plans/*` or `docs/superpowers/specs/*`: those are historical records of prior waves.

- [ ] **Step 6: Verify and commit**

```bash
uv run pytest tests/unit/domain/ -q
uv run ruff check src tests && uv run mypy src/eventsource/ && uv run lint-imports
uv run mkdocs build --strict
```

Commit: `refactor: convert domain ring to pep 695 type parameters`

---

### Task 2: Convert the application ring

**Files:**
- Modify: `src/eventsource/application/aggregates/repository.py`, `src/eventsource/application/aggregates/tenant_repository.py`, `src/eventsource/application/projections/base.py`, `src/eventsource/application/subscriptions/retry.py`

**Interfaces:**
- Consumes: `AggregateRoot[TState: BaseModel]` from Task 1.
- Produces: `AggregateRepository[TAggregate: AggregateRoot[Any]]`, `TenantAwareRepository[TAggregate: AggregateRoot[Any]]`, `type TenantFilter = ...`, `retry_async[T](...)`.

- [ ] **Step 1: Convert the two repositories**

Both declare an identical `TAggregate = TypeVar("TAggregate", bound="AggregateRoot[Any]")`. The bound is a **string forward reference**; inline it as `[TAggregate: AggregateRoot[Any]]`. PEP 695 bounds are lazily evaluated, so the quotes are no longer needed — but only drop them if `AggregateRoot` is actually imported at runtime in that module (check for a `TYPE_CHECKING` guard; if the import is guarded, keep the bound as a string).

- [ ] **Step 2: Convert `TenantFilter` in `projections/base.py`**

`TenantFilter: TypeAlias = ...` → `type TenantFilter = ...`. Note that `type` aliases are lazily evaluated and are **not** valid in runtime positions such as `isinstance` — grep for `TenantFilter` across `src` and `tests` and confirm every use is annotation-only before converting.

- [ ] **Step 3: Convert `retry_async` in `subscriptions/retry.py`**

`def retry_async[T](...)`. Delete the module-level `T = TypeVar("T")` if orphaned.

- [ ] **Step 4: Verify and commit**

```bash
uv run pytest tests/unit/application/ -q
uv run ruff check src tests && uv run mypy src/eventsource/ && uv run lint-imports
```

Commit: `refactor: convert application ring to pep 695 type parameters`

---

### Task 3: Convert the adapters ring

**Files:**
- Modify: `src/eventsource/adapters/memory/readmodels.py`, `src/eventsource/adapters/postgresql/readmodels.py`, `src/eventsource/adapters/sql/readmodel_projection.py`, `src/eventsource/adapters/sqlite/readmodels.py`

**Interfaces:**
- Consumes: `ReadModel` / `_BaseReadModel` bounds, unchanged. `ports/readmodels/repository.py`'s `TModel` is **not** touched — the adapters' own parameters become independent of it, which is correct: each class scopes its own.

- [ ] **Step 1: Convert all four classes**

Each declares a local `TModel` with a bound (`ReadModel` in three, `_BaseReadModel` in sqlite). Inline each as `[TModel: ReadModel]` / `[TModel: _BaseReadModel]` and delete the module-level declaration.

These classes implement the `ReadModelRepository` protocol from `ports`. Converting the concrete class's parameter does not change its protocol conformance — but mypy is the check, so run it before assuming.

- [ ] **Step 2: Verify and commit**

```bash
uv run pytest tests/unit/adapters/ -q
uv run ruff check src tests && uv run mypy src/eventsource/ && uv run lint-imports
```

Commit: `refactor: convert adapters ring to pep 695 type parameters`

---

### Task 4: Convert the testing helpers and the benchmark harness

**Files:**
- Modify: `src/eventsource/testing/builder.py`, `src/eventsource/testing/bdd.py`, `bench/adapters/base.py`

**Interfaces:**
- Produces: `EventBuilder[TEvent: DomainEvent]`, `then_event_published[TEvent: DomainEvent](...)`, `BenchAdapter[T]`.

- [ ] **Step 1: Convert `testing/builder.py` and `testing/bdd.py`**

`bdd.py` declares two TypeVars (`TAggregate`, `TEvent`) at module scope but only `then_event_published` is flagged. Convert that function, then check whether `TAggregate` and `TEvent` still have other users in the module — delete only what is orphaned.

`EventBuilder` is a plain generic class (not a pydantic model), so `class EventBuilder[TEvent: DomainEvent]:` is a direct swap.

- [ ] **Step 2: Convert `bench/adapters/base.py`**

`BenchAdapter(Generic[T])` → `BenchAdapter[T]`. `bench/` is linted (`known-first-party` includes it) but not covered by the test suite; verify with `uv run ruff check bench && uv run mypy bench` if the repo's mypy config includes `bench` — if it does not, ruff plus an import smoke check is sufficient.

- [ ] **Step 3: Verify and commit**

```bash
uv run pytest tests/unit/testing/ -q
uv run python -c "import bench.adapters.base"
uv run ruff check src tests bench && uv run mypy src/eventsource/
```

Commit: `refactor: convert testing helpers and bench harness to pep 695 type parameters`

---

### Task 5: Drop the staged ruff ignores and record the decision

**Files:**
- Modify: `pyproject.toml` (remove the three ignores and their staging comment)
- Create: `docs/adrs/0045-pep695-type-parameter-syntax.md`
- Modify: `docs/adrs/index.md`, `CHANGELOG.md`

**Interfaces:**
- Consumes: a clean `UP046`/`UP047`/`UP040` run from Tasks 1-4.

- [ ] **Step 1: Remove the ignores**

Delete the four-line staging comment and the `UP046` / `UP047` / `UP040` entries from `[tool.ruff.lint] ignore` in `pyproject.toml`. `E501` stays. Then:

```bash
uv run ruff check src tests bench
```

This must be clean. If any violation remains, it is a site Tasks 1-4 missed — fix it here rather than re-adding the ignore.

- [ ] **Step 2: Write ADR 0045**

Follow the format of `docs/adrs/0044-*.md`. It must record:
- **Context:** PR #104 bumped `target-version` to `py313`, which activated these rules repo-wide and produced violations in files that wave did not touch; they were staged as ignores with the migration deferred.
- **Decision:** convert all 15 sites to native syntax; parameters are scoped to their declaration.
- **The `TState` removal as a breaking change,** with the rationale: PEP 695 makes a separately-exported TypeVar redundant, and under the pre-1.0 NO-SHIMS policy there is no aliased fallback. Consumers who wrote generic helpers over aggregate state declare their own inline parameter (`def f[T: BaseModel](a: AggregateRoot[T]) -> None`).
- **Variance:** no TypeVar in the repo declared explicit variance, so nothing was lost; mypy now infers it per declaration.
- **Consequences:** the ignore list is empty of staged modernization; new pre-695 generics are now lint errors.

Add the pointer row to `docs/adrs/index.md`, matching the existing table's column layout exactly.

- [ ] **Step 3: Changelog**

Add to the Unreleased section. The `TState` removal goes under a **Breaking** heading (create it if the section lacks one), phrased for a consumer: what broke, and the one-line fix.

- [ ] **Step 4: Full gate**

```bash
uv run mkdocs build --strict
make check
```

Commit: `docs: adr 0045 and changelog for pep 695 migration`

---

## Definition of Done

- [ ] All 15 inventory sites use native type-parameter syntax
- [ ] `uv run ruff check src tests bench` clean with no `UP046`/`UP047`/`UP040` ignores in `pyproject.toml`
- [ ] `TState` gone from `domain/types.py`, both `__all__`s, the lazy-import map, and all non-historical docs
- [ ] `DeciderAggregate[State]` and `DeciderAggregate[State, Command]` both still valid, with a test asserting it
- [ ] No orphaned `TypeVar` / `Generic` / `TypeAlias` imports
- [ ] `uv run mkdocs build --strict` passes
- [ ] `make check` passes
- [ ] ADR 0045 written, indexed, and changelogged with the break called out
