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

## Sweep scope — always all five

```
grep -rn "eventsource\.<pkg>" src/ tests/ bench/ docs/ pyproject.toml
```

`bench/` has been missed twice; docstrings and comments count. In
pyproject.toml check three spots: import-linter contract module lists,
`[tool.mutmut] only_mutate`, pytest test-selection args.

## Gates

- Per slice: targeted pytest + `uv run lint-imports` + ruff + mypy.
- Orchestrator, before PR: `make check` (CI parity) + Docker integration
  suite. Subagents never run the full suite.

## Common mistakes

| Mistake | Reality |
|---|---|
| Sweep only src/ and tests/ | bench/, docs/, pyproject carry paths too |
| `git rm`/`mv` then move on | Leftover dirs/`__pycache__` = silent namespace packages; `test -d` for the old dirs |
| "Strict docs build will catch nav" | It won't; add new pages to mkdocs nav by hand |
| Tree-wide `ruff format` | Collides with parallel campaigns; format only touched files. Shared files (`__init__.py`, pyproject.toml): re-read immediately before each surgical single-line edit |
| "Keep a shim for safety" | No shims. Clean break + BREAKING changelog entry |
| Edit an old ADR body | Amendments are Status-line pointers only |
