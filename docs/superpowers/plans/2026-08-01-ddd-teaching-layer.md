# DDD Teaching Layer + Model Guards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the library practice the DDD it preaches — decider-first docs at the front doors, the event_type contradiction resolved, aggregate identity guarded at construction time, `domain/types.py` reconciled with real signatures, and typed decider commands — per `.fractal/ddd-dogfooding-review.md`.

**Architecture:** Code guards land first (Tasks 1-4) so the docs rewrite (Tasks 5-6) documents final behavior. All breaking changes are clean breaks (user mandate 2026-08-01: "we can break as desired right now"; no external users). Domain ring stays stdlib+pydantic.

**Tech Stack:** Python 3.13 (PEP 696 native TypeVar defaults), pydantic v2, pytest, uv, mkdocs, import-linter, mypy strict.

## Global Constraints

- Branch `ddd-teaching-wave` off `origin/main` (b7878b8, includes PR #103). PR targets `main`. **Never self-merge.**
- Breaking changes: no shims, changelog-flagged `**BREAKING:**`, recorded in ADR 0043 (current highest ADR: 0042).
- Every code task: TDD (failing test → RED evidence → implement → GREEN), then `uv run ruff check src tests && uv run mypy src/eventsource/` before commit. Docs tasks gate on `uv run mkdocs build --strict`.
- Commit style: `<type>: <lowercase description>`.
- Full gate at end: `make check`.
- Decider is the PRIMARY style (ADR-0022 §5): docs examples lead with `DeciderAggregate` + `DomainCommand`; legacy styles get one worked reference example each.
- `event_type` is auto-derived — docs/tests/docstrings never declare it by hand except for versioned wire names (which must say why).
- Money in teaching examples: `Decimal`, never `float`.
- Subagents run targeted tests; orchestrator owns full suite.
- **Python floor decision (Ty, 2026-08-01): `requires-python = ">=3.13"`.** Native `typing.TypeVar(default=...)` — no typing_extensions. Task 3 bumps pyproject (requires-python + classifiers) and the CI matrix; changelog flags it BREAKING.

---

### Task 1: Guard aggregate identity (required aggregate_type + pattern validation)

**Files:**
- Modify: `src/eventsource/domain/aggregate.py:130-133` (remove `"Unknown"` default), `__init__` (~line 144)
- Modify: `src/eventsource/domain/event.py` (aggregate_type field validator)
- Modify: `src/eventsource/domain/exceptions.py` (new `AggregateTypeNotSetError`)
- Modify: `src/eventsource/domain/__init__.py` (+ export)
- Test: `tests/unit/domain/test_aggregate_root.py`, `tests/unit/domain/test_domain_event.py`

**Interfaces:**
- Produces: `AggregateTypeNotSetError(class_name: str)` in `eventsource.domain.exceptions`; `AggregateRoot.aggregate_type: ClassVar[str]` (no default — construction of a subclass that doesn't set it raises).

- [ ] **Step 1: Setup — create the branch**

```bash
git fetch origin main
git checkout -b ddd-teaching-wave origin/main
```

- [ ] **Step 2: Write the failing tests**

In `tests/unit/domain/test_aggregate_root.py`:

```python
class TestAggregateTypeRequired:
    def test_subclass_without_aggregate_type_raises_on_construction(self) -> None:
        from eventsource.domain.exceptions import AggregateTypeNotSetError

        class ForgotType(AggregateRoot[dict]):
            def _apply(self, event: DomainEvent) -> None: ...

        with pytest.raises(AggregateTypeNotSetError, match="ForgotType"):
            ForgotType(uuid4())

    def test_subclass_with_aggregate_type_constructs(self) -> None:
        class HasType(AggregateRoot[dict]):
            aggregate_type = "HasType"

            def _apply(self, event: DomainEvent) -> None: ...

        assert HasType(uuid4()).aggregate_type == "HasType"
```

In `tests/unit/domain/test_domain_event.py`:

```python
class TestAggregateTypePattern:
    def test_invalid_category_characters_raise(self) -> None:
        class BadEvent(DomainEvent):
            aggregate_type: str = "Or:der"  # ':' is the StreamId separator

        with pytest.raises(ValidationError):
            BadEvent(aggregate_id=uuid4())

    def test_valid_category_passes(self) -> None:
        class GoodEvent(DomainEvent):
            aggregate_type: str = "Order_v2"

        assert GoodEvent(aggregate_id=uuid4()).aggregate_type == "Order_v2"
```

(Reuse each file's existing imports; add `ValidationError` from pydantic where missing. Note: declaring `aggregate_type: str = "..."` on an event class is fine and current practice — the rule against hand-declaration is about `event_type`.)

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_aggregate_root.py tests/unit/domain/test_domain_event.py -q -k "TestAggregateTypeRequired or TestAggregateTypePattern"`
Expected: FAIL — no `AggregateTypeNotSetError`; `"Or:der"` accepted.

- [ ] **Step 4: Implement**

(a) `src/eventsource/domain/exceptions.py`, next to `AggregateNotCreatedError`:

```python
class AggregateTypeNotSetError(EventSourceError):
    """
    Raised when a concrete aggregate class is constructed without declaring
    aggregate_type.

    Aggregate identity is not optional: aggregate_type becomes the stream
    category, so a missing value would silently create wrongly-typed
    streams (the old behavior was a silent "Unknown" default).
    """

    def __init__(self, class_name: str) -> None:
        self.class_name = class_name
        super().__init__(
            f"{class_name} does not declare 'aggregate_type'. Every concrete "
            f"aggregate class must set it to its stream category, e.g. "
            f'aggregate_type = "Order".'
        )
```

(b) `src/eventsource/domain/aggregate.py`: replace `aggregate_type: str = "Unknown"` (and its comment) with:

```python
    # Aggregate type identifier -- REQUIRED. Becomes the stream category;
    # construction raises AggregateTypeNotSetError if a concrete subclass
    # does not set it. (Annotated ClassVar, deliberately no default.)
    aggregate_type: ClassVar[str]
```

and add to `__init__`, before the existing assignments:

```python
        if not getattr(type(self), "aggregate_type", None):
            raise AggregateTypeNotSetError(type(self).__name__)
```

Import `AggregateTypeNotSetError` alongside the other exception imports; `ClassVar` is already imported.

(c) `src/eventsource/domain/event.py`: import at top (`import re`, `from eventsource.domain.stream_id import CATEGORY_PATTERN` — stream_id imports only stdlib, no cycle), module-level `_CATEGORY_REGEX = re.compile(CATEGORY_PATTERN)`, and add below `_ensure_event_type`:

```python
    @field_validator("aggregate_type")
    @classmethod
    def _validate_aggregate_type(cls, v: str) -> str:
        if not _CATEGORY_REGEX.match(v):
            raise ValueError(
                f"aggregate_type {v!r} is not a valid stream category "
                f"(must match {CATEGORY_PATTERN}); it is used as "
                f"StreamId.category on the event's stream"
            )
        return v
```

(`field_validator` joins the existing pydantic imports. If `CATEGORY_PATTERN` already carries `re` anchors incompatible with `.match`, mirror exactly how `stream_id.py` compiles/applies it — the two must accept identical values.)

(d) `src/eventsource/domain/__init__.py`: add `AggregateTypeNotSetError` to the import block and `__all__` (alphabetical). The Task-8-era completeness test in `tests/unit/domain/test_exceptions_home.py` will fail until this is done — that's your safety net, not an obstacle.

- [ ] **Step 5: Sweep the fallout**

Run: `uv run pytest tests/unit -q 2>&1 | tail -5`. Every test aggregate class that never set `aggregate_type` now fails construction. Fix each by adding an honest `aggregate_type = "<Something>"` — do NOT restore a default. Also `grep -rn '"Unknown"' src/eventsource docs` — remove/reword any mention of the old default (e.g. repository docstrings); `AggregateRepository._infer_aggregate_type` needs no code change (with no default, "Unknown" can no longer occur implicitly), but if its docstring mentions the default, update it. List every touched file in your report.

- [ ] **Step 6: Run full unit suite, lint, typecheck, commit**

```bash
uv run pytest tests/unit -q
uv run ruff check src tests && uv run mypy src/eventsource/
git add -A src tests
git commit -m "feat: require aggregate_type on aggregates and validate event aggregate_type as stream category"
```

---

### Task 2: Reconcile domain/types.py with reality

**Files:**
- Modify: `src/eventsource/domain/types.py` (full rewrite below)
- Modify: `src/eventsource/domain/event.py` (thread aliases through field annotations)
- Modify: `src/eventsource/domain/command.py` (same)
- Modify: `src/eventsource/domain/__init__.py`, `src/eventsource/__init__.py` (drop removed names)
- Test: `tests/unit/domain/test_domain_event.py`

**Interfaces:**
- Produces: `types.py` = `TState`, `AggregateId = UUID`, `EventId = UUID`, `TenantId = UUID`, `CorrelationId = UUID`, `CausationId = UUID`. **Deleted**: `Version`, `StreamPosition`, `GlobalPosition` (contradict the opaque `ports/positions.py` Position rule). **Shape change**: `TenantId`/`CausationId` are now plain `UUID` — optionality moves to the referencing field (`tenant_id: TenantId | None`).

- [ ] **Step 1: Write the failing test**

```python
class TestTypesVocabulary:
    def test_identity_aliases_are_plain_uuid(self) -> None:
        from uuid import UUID

        from eventsource.domain import types

        assert types.TenantId is UUID
        assert types.CausationId is UUID

    def test_position_aliases_are_gone(self) -> None:
        from eventsource.domain import types

        for name in ("Version", "StreamPosition", "GlobalPosition"):
            assert not hasattr(types, name)
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_domain_event.py -q -k TestTypesVocabulary`
Expected: FAIL — `TenantId` is `UUID | None`; position aliases exist.

- [ ] **Step 3: Implement**

Rewrite `src/eventsource/domain/types.py`:

```python
"""
Domain vocabulary type aliases.

These aliases name the identities that flow through DomainEvent and
DomainCommand, and are threaded through those signatures so the published
vocabulary and the real annotations agree. Optionality belongs to the
field that references an identity, never to the identity type itself: an
event's *reference* to a causing event is optional; a causation id is a
UUID.

Positions are deliberately absent: global feed positions are opaque
ordered tokens owned by the adapter -- see eventsource.ports.positions.
"""

from typing import TypeVar
from uuid import UUID

from pydantic import BaseModel

# Type variable for aggregate state
TState = TypeVar("TState", bound=BaseModel)

# Identity vocabulary
AggregateId = UUID
EventId = UUID
TenantId = UUID
CorrelationId = UUID
CausationId = UUID
```

Thread through `src/eventsource/domain/event.py` (import the aliases; annotation-only change, zero runtime effect since aliases erase to UUID): `event_id: EventId`, `aggregate_id: AggregateId`, `tenant_id: TenantId | None`, `correlation_id: CorrelationId`, `causation_id: CausationId | None`. Same in `command.py` for its `command_id`/`correlation_id`/`causation_id`/`tenant_id` fields (match names actually present — read the file first).

`src/eventsource/domain/__init__.py`: remove `Version`, `StreamPosition`, `GlobalPosition` from the import block and `__all__`. `src/eventsource/__init__.py`: `grep -n "StreamPosition\|GlobalPosition\|\"Version\"" src/eventsource/__init__.py` — remove any `_LAZY`/`__all__`/TYPE_CHECKING entries found (prior grep suggests none exist; verify). Then `grep -rn "StreamPosition\|GlobalPosition" src docs` and update stragglers (`ports`/`adapters` code that type-hints these must switch to plain `int` or `Position` as locally appropriate — expect few or none).

- [ ] **Step 4: Run tests, lint, typecheck, commit**

```bash
uv run pytest tests/unit -q
uv run ruff check src tests && uv run mypy src/eventsource/
git add -A src tests docs
git commit -m "refactor: reconcile domain vocabulary aliases with real signatures; drop position aliases"
```

---

### Task 3: Typed decider commands — `DeciderAggregate[TState, TCommand]`

**Files:**
- Modify: `src/eventsource/domain/decider.py`
- Modify: `pyproject.toml` (requires-python, classifiers)
- Modify: `.github/workflows/ci.yml` (python versions)
- Test: `tests/unit/domain/test_decider_aggregate.py`

**Interfaces:**
- Produces: `TCommand = TypeVar("TCommand", default=object)` (PEP 696, native on Python 3.13); `class DeciderAggregate(AggregateRoot[TState], Generic[TState, TCommand])`. **Parameter order is [TState, TCommand]** — the defaulted TypeVar must come last, so existing single-param subscripts (`DeciderAggregate[OrderState]`) keep working with `TCommand=object`. `decide(command: TCommand, state: TState)`, `execute(self, command: TCommand)`. ADR-0022's structural-typing decision is untouched (default stays `object`; no base class required).

- [ ] **Step 1: Write the failing test**

```python
class TestTypedDecider:
    def test_two_param_subscript_works_at_runtime(self) -> None:
        @dataclass(frozen=True)
        class Ping:
            target: UUID

        class PingState(BaseModel):
            id: UUID

        class PingDecider(DeciderAggregate[PingState, Ping]):
            aggregate_type = "Ping"

            @staticmethod
            def initial_state(aggregate_id: UUID) -> PingState:
                return PingState(id=aggregate_id)

            @staticmethod
            def decide(command: Ping, state: PingState) -> list[DomainEvent]:
                return []

            @staticmethod
            def evolve(state: PingState, event: DomainEvent) -> PingState:
                return state

        agg = PingDecider(uuid4())
        assert agg.execute(Ping(target=agg.aggregate_id)) == []

    def test_single_param_subscript_still_works(self) -> None:
        class LegacyState(BaseModel):
            id: UUID

        class LegacyDecider(DeciderAggregate[LegacyState]):
            aggregate_type = "Legacy"

            @staticmethod
            def initial_state(aggregate_id: UUID) -> LegacyState:
                return LegacyState(id=aggregate_id)

            @staticmethod
            def decide(command: object, state: LegacyState) -> list[DomainEvent]:
                return []

            @staticmethod
            def evolve(state: LegacyState, event: DomainEvent) -> LegacyState:
                return state

        assert LegacyDecider(uuid4()).execute(object()) == []
```

(Add `from dataclasses import dataclass` and `from pydantic import BaseModel` to the test file imports if absent.)

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_decider_aggregate.py -q -k TestTypedDecider`
Expected: FAIL — `DeciderAggregate[PingState, Ping]` raises TypeError (too many parameters).

- [ ] **Step 3: Implement**

In `decider.py`: add `Generic`, `TypeVar` to the typing import; define after the existing imports:

```python
TCommand = TypeVar("TCommand", default=object)
```

Change the class line to `class DeciderAggregate(AggregateRoot[TState], Generic[TState, TCommand]):`, and the signatures to `decide(command: TCommand, state: TState) -> list[DomainEvent]` and `execute(self, command: TCommand) -> list[DomainEvent]` (`_stamp` keeps `command: object`). Extend the class docstring: two-param subscript gives mypy exhaustiveness checking on a userland command union; single-param keeps `object` (structural typing per ADR-0022 unchanged).

- [ ] **Step 4: Bump the Python floor (decided: >=3.13)**

`pyproject.toml`: `requires-python = ">=3.13"`; delete the `3.11` and `3.12` classifier lines (keep `:: 3` and `:: 3.13`). `.github/workflows/ci.yml`: change every pinned `python-version: '3.11'` to `'3.13'`, the matrix `['3.11', '3.12']` to `['3.13']`, and any `if: matrix.python-version == '3.11'` guard to `'3.13'`. Also grep `ruff`/`mypy` config in pyproject for `target-version`/`python_version` pins (`grep -n "py311\|3.11\|target-version\|python_version" pyproject.toml`) and bump to py313/3.13 so the linters enforce the new floor.

- [ ] **Step 5: Run tests, lint, typecheck, commit**

```bash
uv run pytest tests/unit/domain -q && uv run pytest tests/unit/aggregates -q
uv run ruff check src tests && uv run mypy src/eventsource/
git add -A src tests pyproject.toml .github
git commit -m "feat: parameterize DeciderAggregate with defaulted TCommand; require python 3.13"
```

---

### Task 4: Strict-by-default DeclarativeAggregate + current-practice docstrings

**Files:**
- Modify: `src/eventsource/domain/aggregate.py` (`unregistered_event_handling` default ~line 689; class docstrings ~79-89 and the DeclarativeAggregate docstring; `create_event` docstring ~430-433)
- Modify: `src/eventsource/domain/event_registry.py:15-16,317,323` (docstrings)
- Test: `tests/unit/domain/test_aggregate_root.py`

**Interfaces:**
- Produces: `DeclarativeAggregate.unregistered_event_handling` default becomes `"error"` (was `"ignore"`). The write model no longer silently drops events it has no handler for; `"ignore"`/`"warn"` remain available as explicit opt-downs. Projections' own knob (`application/projections/handlers.py:110`) is untouched.

- [ ] **Step 1: Write the failing test**

```python
class TestStrictUnregisteredDefault:
    def test_unhandled_event_raises_by_default(self) -> None:
        from eventsource.domain.exceptions import UnhandledEventError

        class Surprise(DomainEvent):
            aggregate_type: str = "Strict"

        class StrictAggregate(DeclarativeAggregate[dict]):
            aggregate_type = "Strict"

        agg = StrictAggregate(uuid4())
        with pytest.raises(UnhandledEventError):
            agg.apply_event(Surprise(aggregate_id=agg.aggregate_id), is_new=True)

    def test_explicit_ignore_still_available(self) -> None:
        class Surprise2(DomainEvent):
            aggregate_type: str = "Lenient"

        class LenientAggregate(DeclarativeAggregate[dict]):
            aggregate_type = "Lenient"
            unregistered_event_handling = "ignore"

        agg = LenientAggregate(uuid4())
        agg.apply_event(Surprise2(aggregate_id=agg.aggregate_id), is_new=True)
        assert agg.version == 1
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_aggregate_root.py -q -k TestStrictUnregisteredDefault`
Expected: first test FAILS (no raise under the old "ignore" default).

- [ ] **Step 3: Implement**

(a) Flip the default: `unregistered_event_handling: ClassVar[UnregisteredEventHandling] = "error"`; rewrite the adjacent comment and the class docstring's strict-mode example (strictness is now the default; show `unregistered_event_handling = "ignore"` as the explicit opt-down for forward-compat replay scenarios). Rationale line for the docstring: an aggregate is the write model — a silently unapplied event means command handlers reason over divergent state.

(b) Sweep: `uv run pytest tests/unit -q` — Declarative test aggregates replaying events they don't handle now raise. Per failure, judge intent: if the test exercises lenient handling, set `unregistered_event_handling = "ignore"` explicitly; if the missing handler is an oversight, add the handler. List each in your report.

(c) Docstring refresh to current practice (no behavior change):
- `AggregateRoot` class docstring example (aggregate.py:79-89): rewrite to use `@register_event` on the event class, NO `event_type=` declaration, no manual `aggregate_type`/version stamping in the event constructor — use `create_event(...)` in the command method instead of hand-built events.
- `create_event` docstring: delete the sentence marketing the `aggregate_version=5` override (aggregate.py:431-433); replace with: "Overriding auto-stamped fields (e.g. `aggregate_version`) is an escape hatch for tests and migrations — in normal domain code, let the aggregate stamp them."
- `event_registry.py:15-16` module docstring and `:317,323` `register_event` docstring examples: drop the redundant `event_type: str = "OrderCreated"` lines (auto-derived); keep one explicit-name example ONLY in a spot that explains versioned wire names, labeled as such.

- [ ] **Step 4: Run full unit suite, lint, typecheck, commit**

```bash
uv run pytest tests/unit -q
uv run ruff check src tests && uv run mypy src/eventsource/
git add -A src tests
git commit -m "feat: default DeclarativeAggregate to error on unregistered events; refresh stale domain docstrings"
```

---

### Task 5: Decider-first front doors — getting-started.md + index.md

**Files:**
- Modify: `docs/getting-started.md` (rewrite the worked example + event_type guidance at :265-267)
- Modify: `docs/index.md` (concepts section :335-340, feature mentions)

**Interfaces:**
- Consumes: Task 3's `DeciderAggregate[TState, TCommand]` two-param subscript; Task 1's required `aggregate_type`; Task 4's strict default.

**Content requirements** (authorial task — these bind, wording is yours):

- [ ] **Step 1: Rewrite the getting-started worked example decider-first.** The quickstart's aggregate becomes a `DeciderAggregate` bank account: a frozen `AccountState` pydantic model; commands as `DomainCommand` subclasses (`OpenAccount`, `Deposit`, `Withdraw`); events with `@register_event`, **zero `event_type` declarations**, `Decimal` amounts (never float — import `from decimal import Decimal`); `decide()` raising `CommandRejectedError` for overdraft ("the domain said no"); `evolve()` total (`case _: return state`); two-param subscript `DeciderAggregate[AccountState, AccountCommand]` where `AccountCommand = OpenAccount | Deposit | Withdraw`; repository save/load round-trip kept from the current page. Every code block must be self-consistent (imports shown once, names match across blocks — a reader pasting top-to-bottom gets working code).
- [ ] **Step 2: Fix the event_type guidance.** Replace the section at `getting-started.md:265-267` ("Setting it explicitly ... is the clearest option") with the actual rule: `event_type` is auto-derived from the class name — never declare it by hand; declare it explicitly ONLY to pin a versioned wire name (e.g. `"order_created_v2"`), and show that one exception with a one-line why. Remove the redundant declarations at :212, :223, :233 (line numbers approximate — grep `event_type: str =` in the file).
- [ ] **Step 3: index.md** — add the decider to the concepts tour (currently `index.md:335-340` shows only AggregateRoot/DeclarativeAggregate): decider listed FIRST as the recommended style with a 5-10 line teaser example, the other two named with a pointer to `explanation/aggregate-styles.md`. Sweep the rest of index.md for aggregate examples that should mention the decider exists.
- [ ] **Step 4: Verify the code actually runs.** Extract the quickstart's final assembled example into a scratch script under `$CLAUDE_JOB_DIR/tmp` (or `/tmp`), run it with `uv run python`, and paste the output in your report. Fix the docs if it doesn't run — do not ship a broken quickstart.
- [ ] **Step 5: Gate and commit**

```bash
uv run mkdocs build --strict
git add docs
git commit -m "docs: rewrite getting-started and index decider-first"
```

---

### Task 6: Restructure the explanation layer — aggregate-styles.md + tutorial 08

**Files:**
- Modify: `docs/explanation/aggregate-styles.md` (retitle + reorder)
- Modify: `docs/tutorials/08-testing.md` (decider/DeciderScenario leads)
- Modify: `mkdocs.yml` (only if the nav shows the old page title)

**Content requirements:**

- [ ] **Step 1: aggregate-styles.md** — retitle to "Aggregate styles" (three-style comparison). Structure: (1) the decider first, full worked example, framed as the recommended style per ADR-0022; (2) a when-to-use comparison table (decider / declarative / imperative rows: state handling, invariant placement, testability, when it fits); (3) `DeclarativeAggregate` — ONE worked reference example; (4) `AggregateRoot` imperative — ONE worked reference example, framed for migration-from-legacy contexts. Delete surplus legacy examples (the page currently opens "two ways to write an aggregate" and introduces the decider as "a third style" at :33-38 — that framing inverts, decider is the default, the others are the alternatives).
- [ ] **Step 2: tutorial 08 (testing)** — restructure so the FIRST worked example tests a decider with `DeciderScenario` (currently buried at :676; the main example at :102 is Declarative). Given-When-Then via pure `decide`/`evolve` leads; the Declarative/harness material follows as the secondary path. Reuse the tutorial's existing domain rather than inventing a new one, unless its domain is float-money — then switch to `Decimal` while you're there.
- [ ] **Step 3: mkdocs nav** — `grep -n "aggregate-styles\|AggregateRoot vs" mkdocs.yml`; if the nav hardcodes the old title, update it. `uv run mkdocs build --strict` must pass.
- [ ] **Step 4: Commit**

```bash
git add docs mkdocs.yml
git commit -m "docs: aggregate-styles as three-style comparison, decider-first testing tutorial"
```

---

### Task 7: Migrate shared test fixtures to promoted patterns

**Files:**
- Modify: `tests/fixtures/events.py` (all 10 event classes)
- Test: existing suites are the net — no new test file.

**Interfaces:**
- Consumes: nothing new. Note: `tests/fixtures/aggregates.py:225` already provides a decider fixture — leave it.

- [ ] **Step 1:** In `tests/fixtures/events.py`: delete every `event_type: str = "..."` line (all 10 classes — the declared names equal the class names, so wire values are unchanged by auto-derivation) and decorate each class with `@register_event` (import from `eventsource.domain.event_registry`). Registration into the default registry is idempotent and matches what the docs teach.
- [ ] **Step 2:** Run the consumers: `uv run pytest tests/unit -q`. Any test asserting `event.event_type == "CounterIncremented"` still passes (auto-derived value is identical). If a test breaks on double-registration (same name, different class defined locally in a test), the LOCAL test class is the one to fix — rename it or use an isolated `EventRegistry()`; report each case.
- [ ] **Step 3: Lint and commit**

```bash
uv run ruff check tests && uv run pytest tests/unit -q
git add tests
git commit -m "test: migrate shared event fixtures to register_event and auto-derived event_type"
```

---

### Task 8: ADR 0043, changelog, architecture rules

**Files:**
- Create: `docs/adrs/0043-domain-model-guards-and-vocabulary.md`
- Modify: `docs/adrs/index.md`, `mkdocs.yml` (nav), `docs/adrs/0030-*.md` (Status pointer), `CHANGELOG.md`, `.claude/rules/architecture.md`

**Content requirements:**

- [ ] **Step 1: ADR 0043** (house format — read 0042 for structure). Decision sections: (1) `aggregate_type` required on aggregates (`AggregateTypeNotSetError`) and pattern-validated on `DomainEvent` against `CATEGORY_PATTERN` — closes two silent-corruption paths; (2) `DeclarativeAggregate.unregistered_event_handling` defaults to `"error"` — the write model must not silently drop events (projections keep `"ignore"`); (3) `domain/types.py` reshaped: identity aliases are plain `UUID` threaded through real signatures; `Version`/`StreamPosition`/`GlobalPosition` deleted (positions are opaque adapter tokens per `ports/positions.py`); (4) `DeciderAggregate[TState, TCommand]` with PEP-696 defaulted `TCommand` — typed decide without touching ADR-0022's structural typing; (5) teaching layer realigned decider-first (records that docs now comply with ADR-0022 §5, which STANDS); (6) Python floor raised to 3.13 (native PEP 696; one modern floor over compatibility imports — decided with no external users to migrate). ADR Impact: 0022 stands; 0030 (settled `types.py` contents) **amended** — add "Amended by ADR 0043" to its Status; 0042 stands.
- [ ] **Step 2: CHANGELOG** under `[Unreleased]`:

```markdown
### Changed
- **BREAKING: aggregates must declare `aggregate_type`.** The silent `"Unknown"` default on `AggregateRoot` is removed; constructing a concrete aggregate class that does not set the attribute raises the new `AggregateTypeNotSetError`. Aggregate identity is not optional — the old default silently created `"Unknown"`-typed streams.
- **BREAKING: `DomainEvent.aggregate_type` is validated as a stream category** (must match `CATEGORY_PATTERN`); values that would corrupt a `StreamId` (e.g. containing `:`) now fail at event construction instead of detonating at stream-render time.
- **BREAKING: `DeclarativeAggregate.unregistered_event_handling` now defaults to `"error"`** (was `"ignore"`). An aggregate replaying an event it has no `@handles` method for raises `UnhandledEventError` instead of silently skipping it — silent skips let command handlers reason over divergent state. Opt down explicitly with `unregistered_event_handling = "ignore"`. Projections are unaffected.
- **BREAKING: `eventsource.domain.types` reshaped.** `Version`, `StreamPosition`, and `GlobalPosition` are deleted (global positions are opaque adapter-owned tokens — use `eventsource.ports.positions.Position`); `TenantId` and `CausationId` are now plain `UUID` aliases (optionality belongs to the referencing field, not the identity type). The identity aliases are now threaded through `DomainEvent`/`DomainCommand` annotations, so the published vocabulary matches the real signatures.

- **BREAKING: Python 3.13+ is now required** (`requires-python = ">=3.13"`; was `>=3.11`). The typed decider uses native PEP 696 TypeVar defaults, and the project now targets one modern floor instead of carrying compatibility imports. CI tests 3.13.

### Added
- `DeciderAggregate[TState, TCommand]`: optional second type parameter (PEP 696 default `object`) so `decide` can be typed against a userland command union and mypy flags unhandled commands. Single-parameter subscripts keep working unchanged.
- `AggregateTypeNotSetError` exception.

### Docs
- The teaching layer is now decider-first per ADR-0022 §5: getting-started and index lead with `DeciderAggregate` + `DomainCommand` + `CommandRejectedError`; `explanation/aggregate-styles.md` is a three-style comparison with the decider first; the testing tutorial leads with `DeciderScenario`. The quickstart no longer recommends hand-declaring `event_type` (auto-derived; explicit only for versioned wire names) and uses `Decimal` money. Stale docstrings in `aggregate.py`/`event_registry.py` updated to current practice.
```

- [ ] **Step 3: architecture.md** — in the entities bullet: update the `domain/types.py` alias list to the new contents (drop the three deleted names; note identity aliases are plain UUID threaded through signatures, per ADR 0043); add a line that `aggregate_type` is required on aggregates and category-validated on events (ADR 0043). Check the Event Model Rules section for anything the strict default contradicts.
- [ ] **Step 4:** `docs/adrs/index.md` row + `mkdocs.yml` nav entry for ADR-0043 (mirror the 0042 entry pattern — nav omissions are not caught by strict build). Then `uv run mkdocs build --strict`.
- [ ] **Step 5: Commit**

```bash
git add docs CHANGELOG.md mkdocs.yml .claude/rules/architecture.md
git commit -m "docs: adr 0043, changelog, and architecture rules for ddd teaching wave"
```

---

### Task 9: Full gate and PR

- [ ] **Step 1:** `make check` — all green (fix nothing silently; failures route back through the controller).
- [ ] **Step 2:** Push and open the PR (never self-merge):

```bash
git push -u origin ddd-teaching-wave
gh pr create --base main --title "DDD teaching layer + model guards (ADR 0043)" --body "$(cat <<'EOF'
## Summary
- Teaching layer is now decider-first per ADR-0022 §5: getting-started/index rewritten around DeciderAggregate + DomainCommand + CommandRejectedError (Decimal money); aggregate-styles is a three-style comparison; testing tutorial leads with DeciderScenario; event_type hand-declaration contradiction resolved everywhere (docs, docstrings, shared fixtures).
- Model guards (ADR 0043, all breaking, pre-1.0 no-shims): aggregate_type required on aggregates (AggregateTypeNotSetError) and category-validated on events; DeclarativeAggregate defaults to error on unregistered events; domain/types.py reconciled (position aliases deleted, identity aliases plain UUID threaded through real signatures).
- DeciderAggregate[TState, TCommand] with PEP-696 defaulted TCommand: typed decide/execute, exhaustiveness under mypy, single-param subscripts unchanged.

Source: DDD + dogfooding review (.fractal/ddd-dogfooding-review.md), 2026-08-01.

## Test plan
- make check green; TDD per code task (identity guards, category validation, strict default, typed subscripts, vocabulary shapes).
- Quickstart example extracted and executed as a script (output in task report) — the front-door code is verified runnable.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Report** — PR link, breaking changes list, and follow-ups deliberately left out: the opportunistic 252-line `event_type` test sweep (fixture file done; per-test classes remain), guides' incidental legacy aggregates (repository-pattern.md, snapshotting.md, subscription examples), the `category` vs `aggregate_type` naming unification (docs-wording only, parked), and `application/migration/exceptions.py` decomposition (from the earlier audit, still open).
