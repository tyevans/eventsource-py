# Task 2e: Project Tooling — Report

Worktree: `/home/ty/workspace/eventsource-py/.claude/worktrees/docs-sync`

Commits:
- `260c662` chore: add import-linter architecture contracts
- `19edf8d` chore: add pytest-timeout with a 60s default
- `3a28c4f` chore: add dependabot config for pip and github-actions
- `4e8ef68` chore: enforce coverage floor at 86%

(pytest-randomly and pip-audit were added as dev dependencies in `260c662`
alongside import-linter; both activate automatically once installed and
needed no further config changes, so they have no separate commit.)

## 1. import-linter

Added `[tool.importlinter]` in `pyproject.toml` with two contracts, both
passing today, wired into `.pre-commit-config.yaml` (local `lint-imports`
hook) and a new `import-linter` CI job in `.github/workflows/ci.yml`:

- **"Infrastructure backends must not import each other"** (independence):
  `stores.postgresql`/`sqlite`/`in_memory`, `bus.redis`/`kafka`/`rabbitmq`/`memory`.
  No violations found.
- **"Tier 0 modules must not import sqlalchemy"** (forbidden): the full
  Tier 0 module list from `docs/core-surface.md`, minus two modules excluded
  for the reason below.

### Architecture violations found (not fixed, not hidden)

1. `docs/core-surface.md` / `.claude/rules/architecture.md` claim
   `projections/` is core domain with no infrastructure imports. In
   practice `projections/base.py`, `projections/checkpoint_manager.py`,
   and `projections/dlq_manager.py` import
   `eventsource.repositories.checkpoint` / `eventsource.repositories.dlq`,
   which mix Protocol definitions with sqlalchemy implementations in the
   same module — there's no way to import the protocol without pulling in
   sqlalchemy.
2. `docs/core-surface.md` lists `aggregates/repository.py` (and
   transitively `multitenancy/repository.py`) as Tier 0, reasoning that
   snapshot types are imported lazily/under `TYPE_CHECKING`. That's true
   of the module itself, but `eventsource/snapshots/__init__.py`
   unconditionally does
   `from eventsource.snapshots.postgresql import PostgreSQLSnapshotStore`
   at module level (unguarded, unlike the sqlite import a few lines
   below), so `import eventsource.snapshots` always pulls in sqlalchemy.
   Both repository modules are excluded from the contract's
   `source_modules` for this reason, with a comment in `pyproject.toml`
   pointing at the exact line.

## 2. pytest-randomly

Added as a dev dependency. Ran the full `tests/unit/` suite multiple
times (random seeds, plus one deterministic `-p no:randomly` run for
comparison, plus a targeted rerun pinned to a reproducing seed).

**One of the three initially-observed "failures" turned out to NOT be
order-dependent** — `tests/unit/testing/test_module_structure.py::TestAllExports::test_all_contains_expected_exports`
fails in every run, randomized or not: `eventsource.testing.__all__` now
includes `EventStoreConformanceSuite` and `EventBusConformanceSuite`
(real exports) that the test's hardcoded `expected` set was never updated
for. This is a stale test, not an order-dependence bug. Not fixed, per
instructions.

**Two genuine order-dependent failures**, both reproduced with
`--randomly-seed=3041903891`:

- `tests/unit/migration/test_error_classification.py::TestErrorHandler::test_with_retry_decorator`
  — calls `asyncio.get_event_loop().run_until_complete(...)` directly
  instead of using `@pytest.mark.asyncio`. Under `asyncio_mode = "auto"`
  with a **session-scoped** event loop fixture, whether
  `asyncio.get_event_loop()` returns a usable loop depends on whether an
  async test has already bound/closed a loop on this thread earlier in
  the run — i.e. it depends on what ran before it. Shared state: the
  process-wide asyncio event loop policy, made session-scoped by
  `asyncio_default_fixture_loop_scope = "session"` /
  `asyncio_default_test_loop_scope = "session"` in `pyproject.toml`.

- `tests/unit/multitenancy/test_repository.py::TestTenantAwareRepositoryProperties::test_repr`
  — asserts `"MagicMock" in repr(tenant_repo)`, but the `mock_repo`
  fixture's `repr()` sometimes renders as `TestMock` instead of
  `MagicMock` (`type(mock_repo).__name__` resolves to `TestMock`). This
  points at cross-test leakage of `unittest.mock` state — some other test
  module defines/uses a class literally named `TestMock` and, depending
  on run order, that name ends up attached to (or specced onto) the mock
  used here. Root cause not further isolated within this task's scope;
  flagged for investigation.

Reproduction: `uv run pytest tests/unit/ --no-cov -p randomly --randomly-seed=3041903891`

Neither test was fixed and the seed was not pinned to hide this — both
are reported as-is.

## 3. pytest-timeout

Added `--timeout=60` to `addopts` in `[tool.pytest.ini_options]`. 60s is
generous for the unit suite (which normally completes in under 4 minutes
total) while still catching a genuine hang. The integration job shares
the same default; docker-backed services should respond well inside 60s
per test, but individual slow integration tests can override with
`@pytest.mark.timeout(N)` if this proves too tight — none currently
exceed it (all unit tests observed passing well under the limit).

## 4. Python 3.11 / 3.12 verification

Installed both via `uv python install 3.11 3.12`. Ran
`tests/unit/test_engine.py`, `tests/unit/serialization/`, and
`tests/unit/repositories/` under each:

- **3.11.13**: 152 passed, 2.98s.
- **3.12.11**: 152 passed, 3.08s.

**No behavioral differences found** between 3.11 and 3.12 for
`engine.py`'s SQLite transaction control (explicit `BEGIN` emission,
AUTOCOMMIT detection). This worktree's 3.13 environment was not
re-verified as part of this comparison; it was implicitly exercised via
the other tasks above.

`requires-python = ">=3.11"` in `pyproject.toml` matches CI's matrix
(3.11, 3.12) — no discrepancy.

## 5. Dependency vulnerability scanning

Added `pip-audit` as a dev dependency, a `pip-audit` CI job, and
`.github/dependabot.yml` (pip + github-actions, weekly). First
`pip-audit` run found **28 known vulnerabilities across 11 packages**
(all transitive dev/doc/test dependencies, not `eventsource`'s runtime
deps): click, filelock, idna, pygments, pyjwt, pymdown-extensions,
pytest, python-dotenv, requests, urllib3, virtualenv. Full list with
advisory IDs and fix versions is in the raw command output; not
reproduced here in full and not fixed in this task.

## 6. Coverage threshold

Measured unit-suite coverage (deterministic run, `tests/unit/`,
`--no-cov` excluded from the measurement run itself): **87%**
(15279 statements, 1919 missed). Set
`[tool.coverage.report] fail_under = 86` — a 1-point margin below the
measured figure, intended as a regression ratchet rather than a stretch
target.

Note: this same coverage run surfaced the
`test_all_contains_expected_exports` failure described in section 2
(deterministically, confirming it is not order-dependent).
