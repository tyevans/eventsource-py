---
name: migrating-modules-to-rings
description: Use when moving a top-level module or package under src/eventsource/ into the ring architecture (domain/application/ports/adapters), retiring a legacy import path, or planning such a migration. Also use when a sweep, import-linter contract, or public-API question comes up mid-migration.
---

# Migrating Modules to Rings

## Overview

Dissolve a top-level `src/eventsource/<pkg>/` into the ring map with clean
breaks — no deprecation shims, ever (ADR 0025/0026/0030 standing rule: the
library has no external users). History is preserved with `git mv`; the
public API surface of `eventsource/__init__.py` stays byte-identical unless
an ADR says otherwise.

## Classification rules

Classify each module by what it imports and who imports it:

| Signal | Destination |
|---|---|
| Interface only (Protocol/ABC, zero implementation) | `ports/` |
| Pure stdlib + pydantic entities/exceptions/types | `domain/` |
| Touches a driver, wire format, or storage format | `adapters/` (shared internals → `adapters/_sql`-style underscore pkg) |
| Use-case orchestration | `application/` |

Exceptions merge into `domain/exceptions.py` (rebase roots onto
`EventSourceError`; verify every `except` site first). If application code
needs 1-2 methods of a wider infra interface, cut a small Protocol in
`ports/` instead of importing across rings or weakening a contract.

## Sibling campaigns — coordinate before writing anything

Parallel ring migrations collide at shared seams, and the collisions cost
more than any slice. Before the plan:

- `gh pr list` and inspect open ring PRs for (a) ADR numbers they claim
  (grep `docs/adrs/` on their branches) — take max + 1; (b) shared seam
  files they touch (`ports/bus.py`, `ports/__init__.py`,
  `domain/exceptions.py`). If a symbol could land in two homes, agree the
  canonical home now, not at merge time.
- When a sibling PR merges, merge origin/main into your branch immediately —
  drift compounds. Resolve conflicted files hunk-by-hunk; never
  `git checkout --ours` a partially-conflicted file (it silently discards
  main's auto-merged changes in the non-conflicted hunks).

## Workflow

1. **Plan first, as an artifact.** Ring-assignment table, move list, slice
   list — foundation slice first (the one whose outputs other slices import).
   Dispatch one slice at a time; targeted tests per slice, full gate at the end.
2. **Foundation slice**: extract ports/domain/adapters pieces while the old
   package stays put and re-exports them. Add identity tests
   (`new.X is old.X`) now; retarget them when the package moves.
3. **Move slice**: `git mv` whole files (verify `git status` shows `R`
   renames, not delete+add); mirror the unit-test tree; integration tests
   stay in place with imports repointed. Add a
   `pytest.raises(ModuleNotFoundError)` test for the old path.
4. **Docs/meta slice**: ADR (next number in `docs/adrs/`), "Amended by"
   Status-line pointers on affected prior ADRs (bodies immutable),
   `docs/adrs/index.md`, mkdocs nav, live docs pages, `CLAUDE.md` structure
   block, `.claude/rules/architecture.md` transitional list, CHANGELOG
   `**BREAKING: ...**` entry naming old path → `ModuleNotFoundError` and all
   replacement paths.

## Sweep — whole repo, denylist not allowlist

Run `.claude/skills/migrating-modules-to-rings/sweep.sh <pkg>` from the
repo root. Fatal findings (nonzero exit): import-shaped references
(`from`/`import eventsource.<pkg>`) outside by-design locations
(CHANGELOG, ADR bodies, plan artifacts, `test_public_api.py` guard tests),
and leftover dirs at `src/eventsource/<pkg>` or `tests/unit/<pkg>` — even
`__pycache__`-only debris resurrects the old path.
`tests/integration/<pkg>` stays in place by design. Bare path mentions
(logger names, prose) print as a non-fatal triage list — read it; some
mentions are intentional, some are stale docs.

Directory allowlists have failed four times (`bench/` twice, `examples/`
once — CI's validate-examples job executes top-level examples, so a stale
import there fails the build — and `README.md` once, found only when this
script's whole-repo grep first ran). Sweep everything. In pyproject.toml
check three spots: import-linter contract module lists,
`[tool.mutmut] only_mutate`, pytest test-selection args.

## Gates

- Per slice: targeted pytest + `uv run lint-imports` + ruff + mypy.
- Orchestrator, before PR: `make check` (CI parity) + Docker integration
  suite + `uv run python scripts/validate_examples.py` +
  `uv run mkdocs build --strict`. The last two are seconds-cheap and are
  the only local checks covering examples/ and docs — `make check` runs
  neither. Subagents never run the full suite.
- Re-run the full gate only at stable points (slice complete, post-merge,
  post-fix), not after every mechanical edit.

## Common mistakes

| Mistake | Reality |
|---|---|
| Sweep a directory allowlist | Allowlists rot (missed bench/ twice, examples/ once); sweep the whole repo via `sweep.sh` |
| Pick "next ADR number" from local docs/adrs/ | Sibling branches claim numbers concurrently; check open PRs first |
| First commit fails → assume error | pre-commit's ruff-format hook reformats and fails the commit; re-add and re-commit |
| Poll `gh pr view --json mergeable` right after push | Eventually-consistent; reads UNKNOWN/stale for a minute or two |
| `git rm`/`mv` then move on | Leftover dirs/`__pycache__` = silent namespace packages; `test -d` for the old dirs |
| "Strict docs build will catch nav" | It won't; add new pages to mkdocs nav by hand |
| Tree-wide `ruff format` | Collides with parallel campaigns; format only touched files. Shared files (`__init__.py`, pyproject.toml): re-read immediately before each surgical single-line edit |
| "Keep a shim for safety" | No shims. Clean break + BREAKING changelog entry |
| Edit an old ADR body | Amendments are Status-line pointers only |
