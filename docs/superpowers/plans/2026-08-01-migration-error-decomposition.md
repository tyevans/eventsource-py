# Migration Error Module Decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `application/migration/exceptions.py` (1533 lines containing a circuit breaker, an error handler, retry config, and a classification vocabulary alongside the actual exception taxonomy) into four single-responsibility modules with a one-way dependency DAG.

**Architecture:** The current file has a genuine dependency cycle — `MigrationError._default_classification` needs `ErrorClassification`/`ErrorSeverity`/`RetryConfig`, while `classify_exception()` needs `MigrationError`. The split breaks it by layering: classification **vocabulary** (pure value objects, no exception imports) → exception **taxonomy** → **circuit breaker** → **error handling** (the runtime bridge, which is where `classify_exception` belongs). Every extraction is a pure move: `migration/__init__.py`'s `__all__` stays byte-identical, so this wave is **non-breaking**.

**Tech Stack:** Python 3.13, pydantic v2, pytest, uv, import-linter, mypy strict.

## Global Constraints

- Branch `migration-error-decomposition` off `origin/main` (98b011b, includes PRs #103 and #104). PR targets `main`. **Never self-merge.**
- **This wave is NON-BREAKING.** `src/eventsource/application/migration/__init__.py`'s import block may change source modules, but its `__all__` must stay byte-identical to the base commit. No public name moves, none deleted. If a task appears to require a public API change, STOP and report — that's a plan defect.
- Definition-of-done "Refactor" rules apply: no behavior change; existing tests must pass **without modification** except for import-path updates. If a test needs a behavioral edit, that's a signal the move wasn't pure — stop and report.
- Every task: `uv run ruff check src tests && uv run mypy src/eventsource/ && uv run lint-imports` before commit.
- Commit style: `<type>: <lowercase description>`.
- Full gate at end: `make check`.
- Subagents run targeted tests; orchestrator owns the full suite.
- **Docstrings and comments move verbatim with their code.** This is a move-refactor; rewriting prose mid-move hides real diffs from review. Only module-level docstrings are newly written.

## Target Module Layout

The dependency DAG (each layer may import only from layers above it):

| Layer | Module | Contents | May import |
|---|---|---|---|
| 1 | `error_classification.py` (new, ~360 lines) | `ErrorSeverity`, `ErrorRecoverability`, `ErrorClassification`, `RetryConfig`, `TRANSIENT_RETRY_CONFIG`, `CONNECTIVITY_RETRY_CONFIG`, `CUTOVER_RETRY_CONFIG` | stdlib only |
| 2 | `exceptions.py` (shrinks to ~680 lines) | `MigrationError` + 12 subclasses + `CircuitBreakerOpenError` | layer 1, `domain.exceptions` |
| 3 | `circuit_breaker.py` (new, ~230 lines) | `CircuitState`, `CircuitBreakerConfig`, `CircuitBreaker`, `CircuitBreakerContext` | layers 1-2 |
| 4 | `error_handling.py` (new, ~250 lines) | `ErrorHandler`, `classify_exception` | layers 1-3 |

`CircuitBreakerOpenError` stays in `exceptions.py` despite the circuit breaker moving out: it is a `MigrationError` subclass, and the project's settled convention (ADR 0041, `domain/exceptions.py`, `ports/exceptions.py`) is that an exception taxonomy lives in one file per ring-area. `circuit_breaker.py` imports it one-way.

`classify_exception` moves to `error_handling.py` rather than staying with the taxonomy: it depends on both the vocabulary and the taxonomy, and it is a runtime helper (the bridge from a caught exception to its classification), not a member of either.

---

### Task 1: Extract the classification vocabulary

**Files:**
- Create: `src/eventsource/application/migration/error_classification.py`
- Modify: `src/eventsource/application/migration/exceptions.py` (remove moved definitions, import them back)
- Modify: `src/eventsource/application/migration/__init__.py` (source the moved names from the new module)
- Test: `tests/unit/application/migration/test_error_classification.py` (import updates only)

**Interfaces:**
- Produces: `eventsource.application.migration.error_classification` exporting exactly `ErrorSeverity`, `ErrorRecoverability`, `ErrorClassification`, `RetryConfig`, `TRANSIENT_RETRY_CONFIG`, `CONNECTIVITY_RETRY_CONFIG`, `CUTOVER_RETRY_CONFIG`. This module must import nothing from `migration.exceptions` — that one-way rule is what breaks the cycle, and later tasks depend on it holding.

- [ ] **Step 1: Setup — create the branch**

```bash
git fetch origin main
git checkout -b migration-error-decomposition origin/main
```

- [ ] **Step 2: Write the failing test**

Create `tests/unit/application/migration/test_module_layering.py`:

```python
"""The migration error modules form a one-way dependency DAG (ADR 0044)."""

import ast
from pathlib import Path

import pytest

MIGRATION_DIR = Path(__file__).resolve().parents[4] / "src" / "eventsource" / "application" / "migration"

# module -> the migration-package modules it is allowed to import from
ALLOWED: dict[str, set[str]] = {
    "error_classification": set(),
    "exceptions": {"error_classification"},
    "circuit_breaker": {"error_classification", "exceptions"},
    "error_handling": {"error_classification", "exceptions", "circuit_breaker"},
}


def _migration_imports(module_name: str) -> set[str]:
    """Names of sibling migration modules imported by module_name."""
    source = (MIGRATION_DIR / f"{module_name}.py").read_text()
    tree = ast.parse(source)
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            prefix = "eventsource.application.migration."
            if node.module.startswith(prefix):
                found.add(node.module[len(prefix) :].split(".")[0])
            elif node.level and node.module in ALLOWED:
                found.add(node.module)
    return found


@pytest.mark.parametrize("module_name", sorted(ALLOWED))
def test_module_imports_stay_within_its_layer(module_name: str) -> None:
    violations = _migration_imports(module_name) - ALLOWED[module_name]
    assert not violations, f"{module_name}.py imports disallowed siblings: {sorted(violations)}"
```

- [ ] **Step 3: Run to verify it fails**

Run: `uv run pytest tests/unit/application/migration/test_module_layering.py -q`
Expected: FAIL — `FileNotFoundError` for `error_classification.py` (the other three modules don't exist yet either; this test goes green only after Task 3, and each task moves it closer. That is intended: it is the wave's structural invariant. Tasks 1-2 will still show failures for not-yet-created modules — that is expected and NOT a blocker for those tasks; each task's own gate is its targeted suite plus the parametrized cases for modules that exist.)

- [ ] **Step 4: Create the new module**

Create `src/eventsource/application/migration/error_classification.py` with this header, then **cut-paste verbatim** from `exceptions.py`: `ErrorSeverity` (lines 58-113), `ErrorRecoverability` (114-165), `ErrorClassification` (166-241), `RetryConfig` (242-337), and the three `*_RETRY_CONFIG` constants (338-362). Bring the imports those bodies need (`from __future__ import annotations`, `dataclass`/`field`, `Enum`, and any typing names actually referenced — do not copy imports the moved code doesn't use; ruff will flag unused ones).

```python
"""
Error classification vocabulary for migration operations.

The value objects that describe *what kind of error* occurred and *how to
respond to it*: severity, recoverability, the combined classification
record, and retry configuration.

This module is the base layer of the migration error stack and imports
nothing from its siblings — `exceptions.py` depends on this vocabulary to
declare each error's default classification, so a dependency in the other
direction would be a cycle (ADR 0044).
"""
```

Add `__all__` listing exactly the seven names.

- [ ] **Step 5: Re-point exceptions.py**

Delete the moved definitions from `exceptions.py` and add near its other imports:

```python
from eventsource.application.migration.error_classification import (
    CONNECTIVITY_RETRY_CONFIG,
    CUTOVER_RETRY_CONFIG,
    TRANSIENT_RETRY_CONFIG,
    ErrorClassification,
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
```

Keep only the names `exceptions.py` still uses — if ruff reports some as unused, delete those from the import (the retry-config constants in particular may be referenced only by subclass `retry_config` overrides; check before trimming).

- [ ] **Step 6: Re-point the package facade**

In `migration/__init__.py`, move the seven names out of the `from ...exceptions import (...)` block into a new `from ...error_classification import (...)` block, keeping alphabetical order within each block. **`__all__` must not change** — verify with `git diff src/eventsource/application/migration/__init__.py` that no `__all__` line is touched.

- [ ] **Step 7: Update test imports**

`grep -rln "migration.exceptions import" tests/ | xargs grep -l "ErrorSeverity\|ErrorRecoverability\|ErrorClassification\|RetryConfig\|RETRY_CONFIG"` — for each hit, split the import so moved names come from `eventsource.application.migration.error_classification`. Change import lines only; no test body edits.

- [ ] **Step 8: Verify and commit**

```bash
uv run pytest tests/unit/application/migration -q
uv run pytest tests/unit/application/migration/test_module_layering.py -q -k "error_classification or exceptions"
uv run ruff check src tests && uv run mypy src/eventsource/ && uv run lint-imports
git add -A src tests
git commit -m "refactor: extract migration error classification vocabulary"
```

---

### Task 2: Extract the circuit breaker

**Files:**
- Create: `src/eventsource/application/migration/circuit_breaker.py`
- Modify: `src/eventsource/application/migration/exceptions.py` (remove moved definitions)
- Modify: `src/eventsource/application/migration/__init__.py`
- Test: existing migration tests (import updates only)

**Interfaces:**
- Consumes: Task 1's `error_classification` module.
- Produces: `eventsource.application.migration.circuit_breaker` exporting `CircuitState`, `CircuitBreakerConfig`, `CircuitBreaker`, `CircuitBreakerContext`. `CircuitBreakerOpenError` deliberately does NOT move — it stays in `exceptions.py` and `circuit_breaker.py` imports it.

- [ ] **Step 1: Create the module**

Create `src/eventsource/application/migration/circuit_breaker.py` with this header, then cut-paste verbatim: `CircuitState` (lines ~993-1013 pre-Task-1 numbering — match on class name, line numbers shifted), `CircuitBreakerConfig`, `CircuitBreaker`, `CircuitBreakerContext`. Note `CircuitBreakerOpenError` sits between them in the original file — leave it where it is.

```python
"""
Circuit breaker for migration operations.

Trips after a configured number of consecutive failures and rejects calls
while open, preventing a struggling backend from being hammered during a
migration. `CircuitBreakerOpenError` lives with the rest of the migration
exception taxonomy in `exceptions.py` (ADR 0044); this module imports it.
"""
```

Its imports: `asyncio`, `logging`, `time`, `Enum`, `dataclass`, whatever typing names the bodies use, plus `from eventsource.application.migration.exceptions import CircuitBreakerOpenError`. Give the module its own `logger = logging.getLogger(__name__)` if the moved code logs. Add `__all__` with the four names.

- [ ] **Step 2: Remove from exceptions.py and re-point the facade**

Delete the four moved definitions from `exceptions.py`. In `migration/__init__.py`, move `CircuitBreaker`, `CircuitBreakerConfig`, `CircuitState` (and `CircuitBreakerContext` if currently exported) into a new `from ...circuit_breaker import (...)` block. `CircuitBreakerOpenError` stays sourced from `exceptions`. **`__all__` unchanged.**

- [ ] **Step 3: Update importers**

```bash
grep -rn "CircuitBreaker\b\|CircuitBreakerConfig\|CircuitState\|CircuitBreakerContext" src tests --include="*.py" | grep -v "circuit_breaker.py"
```

For each file importing these from `...migration.exceptions`, re-point to `...migration.circuit_breaker`. Files importing from the package facade (`from eventsource.application.migration import ...`) need no change — that's the facade earning its keep.

- [ ] **Step 4: Verify and commit**

```bash
uv run pytest tests/unit/application/migration -q
uv run ruff check src tests && uv run mypy src/eventsource/ && uv run lint-imports
git add -A src tests
git commit -m "refactor: extract migration circuit breaker into its own module"
```

---

### Task 3: Extract the error handler, leaving a pure taxonomy

**Files:**
- Create: `src/eventsource/application/migration/error_handling.py`
- Modify: `src/eventsource/application/migration/exceptions.py` (now pure taxonomy)
- Modify: `src/eventsource/application/migration/__init__.py`
- Test: existing migration tests (import updates only); `tests/unit/application/migration/test_module_layering.py` must now fully pass

**Interfaces:**
- Consumes: Tasks 1-2.
- Produces: `eventsource.application.migration.error_handling` exporting `ErrorHandler` and `classify_exception`.

- [ ] **Step 1: Create the module**

Create `src/eventsource/application/migration/error_handling.py` with this header, then cut-paste verbatim `ErrorHandler` and `classify_exception`:

```python
"""
Runtime error handling for migration operations.

`ErrorHandler` composes the other three modules — it classifies a failure,
consults its retry configuration, and optionally guards the call with a
circuit breaker. `classify_exception` is the bridge from an arbitrary
caught exception to an `ErrorClassification`, and lives here rather than
with the taxonomy because it depends on both the taxonomy and the
vocabulary (ADR 0044).
"""
```

Its imports: `asyncio`, `functools`, `logging`, `Callable`/`Coroutine`, `Any`/`TypeVar`, `UUID`, plus the three sibling modules. Move the `T = TypeVar("T")` declaration here if `exceptions.py` no longer uses it (check with grep before deleting it there). Add `__all__` with the two names.

- [ ] **Step 2: Reduce exceptions.py to the taxonomy**

Delete `ErrorHandler` and `classify_exception` from `exceptions.py`. Then update its module docstring (this is the one place prose is rewritten) to describe what the file now is:

```python
"""
Migration exception taxonomy.

`MigrationError` and its subclasses, rooted in `EventSourceError`. Each
declares its default classification (severity, recoverability, retry
policy) using the vocabulary from `error_classification.py`.

Sibling modules own the runtime machinery that used to live here:
`circuit_breaker.py` (failure gating) and `error_handling.py`
(`ErrorHandler`, `classify_exception`). See ADR 0044.
"""
```

Then prune now-unused imports (`asyncio`, `functools`, `time`, `Coroutine`, `TypeVar` are all likely dead here) — ruff will name them.

- [ ] **Step 3: Re-point the facade and importers**

`migration/__init__.py`: `ErrorHandler` and `classify_exception` move to a `from ...error_handling import (...)` block. **`__all__` unchanged.** Then:

```bash
grep -rn "ErrorHandler\|classify_exception" src tests --include="*.py" | grep -v "error_handling.py"
```

Re-point any direct `...migration.exceptions` imports of these two names.

- [ ] **Step 4: The layering test must now pass in full**

Run: `uv run pytest tests/unit/application/migration/test_module_layering.py -q`
Expected: PASS, all four parametrized cases. If `error_classification` shows a violation, the cycle was reintroduced — stop and report rather than adding a `TYPE_CHECKING` escape hatch.

- [ ] **Step 5: Verify sizes and commit**

```bash
wc -l src/eventsource/application/migration/{exceptions,error_classification,circuit_breaker,error_handling}.py
uv run pytest tests/unit/application/migration -q
uv run ruff check src tests && uv run mypy src/eventsource/ && uv run lint-imports
git add -A src tests
git commit -m "refactor: extract migration error handler; exceptions.py is now a pure taxonomy"
```

Report the four line counts — no module should exceed ~700 lines.

---

### Task 4: Split the test module to match

**Files:**
- Modify: `tests/unit/application/migration/test_exceptions.py` (483 lines)
- Create: `tests/unit/application/migration/test_circuit_breaker.py`, `tests/unit/application/migration/test_error_handling.py` (only if the existing file actually contains tests for those units — see Step 1)

**Interfaces:** consumes Tasks 1-3; no production code changes.

- [ ] **Step 1: Survey before splitting**

```bash
grep -n "^class Test\|^def test_" tests/unit/application/migration/test_exceptions.py
grep -n "^class Test\|^def test_" tests/unit/application/migration/test_error_classification.py
```

Classify each test class by which of the four modules it exercises. If `test_exceptions.py` is already taxonomy-only, this task is a no-op — report that and skip to Step 4. Do NOT split for symmetry's sake; only move tests whose subject moved.

- [ ] **Step 2: Move the test classes**

For each test class whose subject now lives in `circuit_breaker.py` or `error_handling.py`, cut-paste it verbatim into the matching new test file (with the file's needed imports and a one-line module docstring naming the unit under test). Test bodies are moved unchanged — a body edit here would mean the production move wasn't pure.

- [ ] **Step 3: Verify no tests were lost**

```bash
uv run pytest tests/unit/application/migration -q --collect-only | tail -3
```

Compare the collected count against the pre-split count (get it from `git stash` + collect, or from the prior task's run). It must be identical — report both numbers.

- [ ] **Step 4: Commit**

```bash
uv run pytest tests/unit/application/migration -q
uv run ruff check tests
git add -A tests
git commit -m "test: split migration error tests to match module decomposition"
```

(If Step 1 found nothing to move: skip the commit and report the task as a verified no-op.)

---

### Task 5: ADR 0044, changelog, architecture rules

**Files:**
- Create: `docs/adrs/0044-migration-error-module-decomposition.md`
- Modify: `docs/adrs/index.md`, `mkdocs.yml` (nav), `CHANGELOG.md`, `.claude/rules/architecture.md`
- Modify: `docs/adrs/0034-*.md` (Status pointer — it settled the migration package's fourteen modules)

**Interfaces:** consumes Tasks 1-4; no code changes.

- [ ] **Step 1: Write ADR 0044** (read `docs/adrs/0043-*.md` first for house format). Content requirements:
  - **Context**: `exceptions.py` had grown to 1533 lines and was an exceptions module in name only — it held a circuit breaker, an error handler, a retry-config system, and a classification vocabulary. Flagged by the 2026-08-01 DDD/dogfooding review as the largest remaining structural defect after the domain-ring work.
  - **Decision**: the four-module DAG, with the table from this plan's "Target Module Layout" reproduced. State the two placement calls explicitly and why: `CircuitBreakerOpenError` stays with the taxonomy (one taxonomy per area, matching ADR 0041); `classify_exception` goes to `error_handling.py` (depends on both layers; it is a runtime bridge).
  - **The cycle**: document it — `MigrationError` needs the vocabulary for its default classification, `classify_exception` needs `MigrationError`. Record that the layering is enforced by `tests/unit/application/migration/test_module_layering.py`, and that a `TYPE_CHECKING` guard was explicitly rejected as a way to paper over a cycle.
  - **Consequences**: non-breaking (facade `__all__` unchanged); import sites inside the package now name a specific module; the layering test fails loudly if a future change reintroduces a cycle.
  - **ADR Impact**: 0034 (settled the migration package's module list) **amended** — add "Amended by ADR 0044" to its Status section, body untouched. 0041 **stands** (this follows its one-taxonomy-per-area convention).

- [ ] **Step 2: CHANGELOG** under `## [Unreleased]`:

```markdown
### Changed

- **`eventsource.application.migration.exceptions` is decomposed into four single-responsibility modules** (ADR 0044): the classification vocabulary (`ErrorSeverity`, `ErrorRecoverability`, `ErrorClassification`, `RetryConfig` and the three retry-config constants) moves to `error_classification.py`; the circuit breaker (`CircuitBreaker`, `CircuitBreakerConfig`, `CircuitState`, `CircuitBreakerContext`) to `circuit_breaker.py`; `ErrorHandler` and `classify_exception` to `error_handling.py`. `exceptions.py` is now the `MigrationError` taxonomy alone, down from 1533 lines. **This is not a breaking change** — every name is still exported from `eventsource.application.migration` and the package's `__all__` is unchanged; only direct submodule imports (`from eventsource.application.migration.exceptions import CircuitBreaker`) need updating to the new module.
```

- [ ] **Step 3: architecture.md** — in the application-ring bullet, the migration entry currently lists fourteen modules including `exceptions.py`. Update that list to name the three new modules alongside it, and add a sentence: the four error modules form a one-way DAG (vocabulary → taxonomy → circuit breaker → handling) enforced by a layering test, per ADR 0044.

- [ ] **Step 4: Nav and gate**

Add the ADR-0044 row to `docs/adrs/index.md` and the nav entry to `mkdocs.yml` (mirror the 0043 entries — strict build does NOT catch nav omissions). Then `uv run mkdocs build --strict` must pass.

- [ ] **Step 5: Commit**

```bash
git add docs CHANGELOG.md mkdocs.yml .claude/rules/architecture.md
git commit -m "docs: adr 0044 and changelog for migration error decomposition"
```

---

### Task 6: Full gate and PR

- [ ] **Step 1:** `make check` — all green. Failures route back through the controller; never push red.
- [ ] **Step 2:** Push and open the PR (never self-merge):

```bash
git push -u origin migration-error-decomposition
gh pr create --base main --title "Decompose migration error module (ADR 0044)" --body "$(cat <<'EOF'
## Summary
- Splits `application/migration/exceptions.py` (1533 lines) into four single-responsibility modules: `error_classification.py` (vocabulary), `exceptions.py` (taxonomy only), `circuit_breaker.py`, `error_handling.py`.
- Breaks a real dependency cycle — `MigrationError` needs the classification vocabulary for its defaults, `classify_exception` needs `MigrationError` — by layering the vocabulary underneath the taxonomy and moving the bridge function downstream. A layering test enforces the DAG so it cannot silently regress.
- **Non-breaking**: every name is still exported from `eventsource.application.migration`; the package `__all__` is byte-identical.

Source: DDD + dogfooding review (`.fractal/ddd-dogfooding-review.md`) — the largest remaining structural finding after PRs #103 and #104.

## Test plan
- `make check` green; existing migration tests pass without behavioral modification (import-path updates only, per the refactor definition-of-done).
- New `test_module_layering.py` asserts the four-module DAG by AST-parsing each module's sibling imports.
- Collected-test count verified identical before and after the test-file split.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Report** — PR link, the four final line counts, and confirmation that `__all__` is unchanged. Note remaining queue items deliberately out of scope: PEP 695 syntax migration, the per-test `event_type` sweep, the fuller quickstart walkthrough, fixtures float→Decimal, tutorial 03 two-param consistency.
