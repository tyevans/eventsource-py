# Merge report: origin/main → worktree-aggregates-application-ring

Merge-base: `0f52d22411dcd08b71f4edb3f86d7ff39938e8c4`. Merge completed 2026-07-30.

## Per-conflict resolution

### 1. `src/eventsource/aggregates/base.py` → `src/eventsource/domain/aggregate.py`
No manual porting was needed: git's **file-level** rename detection succeeded even
though directory rename detection split, and the merge staged their full +20/−3
`create_event(command=)` hunk set onto `src/eventsource/domain/aggregate.py`
automatically (verified against `git diff MB..origin/main -- aggregates/base.py` —
identical hunks: `DomainCommand` import, keyword-only `command` parameter, provenance
stamping block, docstring updates). The new
`from eventsource.commands.base import DomainCommand` runtime import is kept —
`commands/` is pure (pydantic-only) entities-ring material.

### 2. `src/eventsource/aggregates/decider.py` → `src/eventsource/domain/decider.py`
`git mv` to `domain/decider.py`; single import re-pointed
(`eventsource.aggregates.base` → `eventsource.domain.aggregate`); ruff re-sorted the
import block. Other imports (`commands.base`, `events.base`) unchanged. Verified pure:
stdlib + pydantic + commands + events only — no outward imports, so it sits in the
entities ring and was added to the Tier-0 import-linter contract (passes).

### 3. `src/eventsource/aggregates/__init__.py` (DU)
Their version exported `DeciderAggregate`; that export was replicated in
`src/eventsource/domain/__init__.py` (import + `__all__`, alongside
AggregateRoot/DeclarativeAggregate), then the path was `git rm`'d so the
`aggregates/` package stays deleted.

### 4. `src/eventsource/__init__.py` (UU)
Union of both sides. Kept our re-pointed imports (`adapters.memory.snapshots`,
`application.aggregates.repository`, `domain.aggregate`) and added
`from eventsource.domain.decider import DeciderAggregate`. Their auto-merged parts
(`DomainCommand` from `eventsource.commands`, `CommandRejectedError` in the
exceptions import, and the three `__all__` additions) survived untouched.

### 5. `pyproject.toml` (UU)
Conflict was only in the Tier-0 forbidden contract's `source_modules`. Resolution:
kept ours (`eventsource.domain.aggregate`), kept theirs (`eventsource.commands`),
dropped their `eventsource.aggregates.base` (module no longer exists), and **added
`eventsource.domain.decider`** since it is equally Tier-0. Their pytest-xdist
additions (dependency `pytest-xdist>=3.5`, `-n auto --dist worksteal` in Makefile and
CI) auto-merged cleanly. Our mutmut/import-linter application-ring entries intact.

### 6. `docs/adrs/index.md` (UU)
Kept both: our 0017 entry (with "**Superseded by ADR 0021.**" tag) + our 0021 entry +
their 0022 entry, in numeric order under "Aggregates and snapshots". Their side's
only change to the file was the 0022 entry — no numbering prose was touched.

### 7. `tests/unit/aggregates/`
Their side's only change under this path was the **new**
`test_decider_aggregate.py` — `git diff MB..origin/main` confirmed none of the
previously-moved test files (test_create_event, test_deferred_state, etc.) were
modified, so no hunks needed porting. `test_decider_aggregate.py` moved to
`tests/unit/domain/test_decider_aggregate.py` with imports re-pointed
(`aggregates.decider` → `domain.decider`, `aggregates.base` → `domain.aggregate`
at 3 sites). `tests/unit/aggregates/` removed entirely (only `__pycache__` remained).

### 8. Cleanly-merged additions left alone
`src/eventsource/commands/` (own package, parallel to `events/`),
`tests/unit/commands/`, `exceptions.py` (`CommandRejectedError` added after
`AggregateNotFoundError`; `SnapshotError` hierarchy and comments intact),
`examples/basic_usage.py` rewrite + `examples/imperative_example.py`,
`.github/workflows/adr-check.yml`, `.claude/rules/user-trust.md`, their
specs/plans docs, `mkdocs.yml` ADR-0022 nav entry, `uv.lock`.

### 9. Stale-path audit
`grep -rn "eventsource.aggregates|aggregates.base|aggregates.decider"` over
docs/examples/src/tests/bench: **src, tests, examples, bench are clean.** Remaining
hits are only in ADR bodies (0013, 0017 — immutable records) and
`docs/superpowers/` plans/specs (history). ADR-0022's body itself has no stale path
references. `docs/explanation/decider-pattern.md` (referenced by the decider
docstring) exists.

### 10. CHANGELOG.md
Neither side touched CHANGELOG.md relative to the merge-base — nothing to reconcile.

## Verification (all green)

- `uv run pytest tests/unit/domain/ tests/unit/commands/ tests/unit/application/ tests/unit/test_public_api.py -q` → **373 passed**, 4 pre-existing collection warnings
- `uv run mypy src/eventsource/ --config-file=pyproject.toml` → Success: no issues found in 179 source files
- `uv run lint-imports` → **3 kept, 0 broken** (Tier-0 contract now covers `eventsource.domain.decider` and `eventsource.commands`)
- `python -c "from eventsource import DeciderAggregate, DomainCommand, CommandRejectedError, AggregateRoot, AggregateRepository, Snapshot"` → ok
- `uv run ruff check` / `ruff format --check` on all touched files → clean
