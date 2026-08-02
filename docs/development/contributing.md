# Contributing

This guide shows you how to set up a development environment for `eventsource-py`, run the
same checks CI runs, and get a pull request merged.

## Before you start

You need Python 3.13 or newer (the package declares `requires-python = ">=3.13"`), [uv](https://docs.astral.sh/uv/)
for dependency management, and Docker if you plan to run the integration tests.

### Set up your environment

Clone the repository and sync the lockfile with every extra installed:

```bash
git clone https://github.com/tyevans/eventsource-py.git
cd eventsource-py
uv sync --all-extras
```

`uv sync --all-extras` is the recommended default — it installs the core dependencies
(`pydantic>=2`, `sqlalchemy>=2`) plus every optional group, so no test or example is skipped for a
missing import. If you want a narrower install, pick from the extras defined in `pyproject.toml`:

| Extra | Installs | Enables |
| --- | --- | --- |
| `postgresql` | `asyncpg` | PostgreSQL event store, snapshot store, advisory locks, and repositories |
| `sqlite` | `aiosqlite` | SQLite event store, snapshot store, and repositories |
| `redis` | `redis` | Redis event bus backend |
| `rabbitmq` | `aio-pika` | RabbitMQ event bus backend |
| `kafka` | `aiokafka` | Kafka event bus backend |
| `kafka-schema-registry` | `aiokafka`, `confluent-kafka` | Kafka with Confluent Schema Registry support |
| `telemetry` | `opentelemetry-api`, `opentelemetry-sdk` | The `eventsource.observability` OpenTelemetry tracing integration |
| `all-backends` | `postgresql` + `sqlite` | Both persistent event store backends, no messaging |
| `all` | `postgresql`, `sqlite`, `redis`, `rabbitmq`, `kafka`, `telemetry` | Everything a user might import at runtime |
| `benchmark` | `pytest-benchmark` | The `tests/benchmarks/` suite |
| `dev` | pytest, pytest-asyncio, pytest-cov, pytest-benchmark, mypy, ruff, testcontainers, pre-commit | The full local check suite |
| `docs` | mkdocs, mkdocs-material, mkdocstrings[python], pymdown-extensions | Building this documentation site |

The extra for OpenTelemetry is named `telemetry`, not `observability` — `observability` is the
name of the *module* it enables (`src/eventsource/observability/`).

## Install the pre-commit hooks

`pre-commit` ships with the `dev` extra, so `uv sync --all-extras` already installed it. Install the
git hook once per clone so the checks run on every commit:

```bash
uv run pre-commit install
```

This writes `.git/hooks/pre-commit`. From then on, `git commit` runs the hooks against your
**staged files only**. Hooks that rewrite files (`ruff --fix`, `ruff-format`, `trailing-whitespace`,
`end-of-file-fixer`) abort the commit after modifying your working tree — re-stage with `git add`
and commit again. To bypass the hooks for a work-in-progress commit, use `git commit --no-verify`;
CI still runs the same checks, so fix them before you push for review.

### What the hooks enforce

`.pre-commit-config.yaml` has two kinds of hook.

**Hygiene hooks** (`pre-commit-hooks` v5.0.0) run in pre-commit's own environment:
`trailing-whitespace`, `end-of-file-fixer`, `check-yaml`, `check-added-large-files`,
`check-merge-conflict`, and `debug-statements` (which fails the commit if you left a
`breakpoint()` or `pdb` import behind). These have no project dependencies, so there is
nothing for them to drift from.

**Python linting hooks** are `language: system` and shell out through `uv run`, so they
use the versions in `uv.lock` — the same ones the Makefile and CI use:

- **ruff**: `ruff check --fix` followed by `ruff format`. Both read `[tool.ruff]` from
  `pyproject.toml` — line length 100, target `py313`, rule sets `E`, `F`, `I`, `N`, `W`, `UP`,
  `B`, `C4`, `SIM`, with `E501` ignored (the formatter owns line wrapping) and `eventsource`
  treated as first-party for import sorting.
- **mypy** with `--config-file=pyproject.toml`, restricted to `files: ^src/`. The config sets
  `strict = true`, `python_version = "3.13"`, `warn_return_any`, and `warn_unused_ignores`.
  Tests and examples are never type-checked by the hook.
- **bandit** with `-c pyproject.toml`, which applies `exclude_dirs = ["tests", "examples"]`
  and `skips = ["B101"]` so plain `assert` statements are allowed. Scoped to `^src/` to match
  the `audit` job in CI.
- **import-linter**, which checks the `[tool.importlinter]` contracts.

!!! note "Why these are `language: system`"

    Letting pre-commit manage isolated environments means a third set of tool versions
    alongside `uv.lock` and CI, plus a hand-maintained copy of the project's runtime
    dependencies for mypy to type against. That copy had already drifted — it was missing
    `orjson`, a core dependency — so mypy failed in the hook while passing in CI and
    locally. Routing through `uv run` removes the whole class of problem.

    The trade-off is that `pre-commit autoupdate` no longer bumps these; upgrade them with
    `uv lock --upgrade-package ruff` (or mypy, bandit, import-linter) instead.

### Running the hooks manually

To check the whole tree without committing:

```bash
uv run pre-commit run --all-files
```

## Run the checks locally

### The short version: `make`

Every gate below has a `make` target, so you rarely need to remember the raw
commands. `make help` lists them all.

```bash
make install   # uv sync --all-extras, once
make check     # lint + types + arch + security + unit tests, all of it
make fix       # auto-fix whatever ruff can fix
```

Individual gates are available separately when you want a fast loop --
`make lint`, `make types`, `make arch`, `make sec`, `make test`. The slower
or service-dependent suites are deliberately *not* part of `make check`:

```bash
make integration      # starts Postgres + Redis via docker compose first
make mutation         # mutmut over the curated set
make docs             # mkdocs --strict + runnable-example validation
```

`make check` is CI parity by construction: `.github/workflows/ci.yml` installs
with `uv sync --all-extras --locked` and invokes each tool through `uv run`,
exactly as the Makefile does. Both therefore execute the versions pinned in
`uv.lock`. If you add a gate to one, add it to the other.

!!! note "Why CI installs with uv rather than pip"

    CI previously used `pip install -e ".[dev,all]"`. That ignores `uv.lock`
    — it floats tool versions above their pins, and it skips
    `[dependency-groups] dev` entirely, because pip does not install PEP 735
    groups without `--group`. `pip-audit`, `import-linter`, `pytest-timeout`
    and `bandit` live only in that group, so those jobs failed with "command
    not found" while passing locally. Please do not switch it back.

    `--locked` additionally makes a stale `uv.lock` a hard CI failure instead
    of silent resolution drift. If it trips, run `uv lock` and commit the
    result.

The raw commands behind each target follow.

### Lint and format

```bash
uv run ruff check src/ tests/ --fix
uv run ruff format src/ tests/
```

CI runs `ruff check .` and `ruff format --check .` across the whole repository, so widen the paths
(or just run the pre-commit hooks) before pushing if you touched `examples/` or `scripts/`.

### Type check

```bash
uv run mypy src/eventsource/ --config-file=pyproject.toml
```

Third-party modules without stubs are exempted per-module in `[[tool.mypy.overrides]]`
(`opentelemetry.*`, `redis.*`, `aio_pika.*`, `aiormq.*`, `aiosqlite.*`, `aiokafka.*`). Everything
under `src/eventsource/` must pass `strict`.

### Unit tests (no Docker required)

```bash
uv run pytest tests/unit/ -v
```

`[tool.pytest.ini_options]` sets `asyncio_mode = "auto"`, so `async def` tests need no decorator,
and `addopts` already includes `--cov=src/eventsource --cov-report=term-missing`.

### Integration tests

Bring up the local services first:

```bash
docker compose -f docker-compose.test.yml up -d
uv run pytest tests/integration/ -v
docker compose -f docker-compose.test.yml down -v
```

`docker-compose.test.yml` starts PostgreSQL 15 on host port `5433` (override with `POSTGRES_PORT`)
and Redis 7 on host port `6380` (override with `REDIS_PORT`), with database/user/password all set
to `eventsource_test` / `test` / `test`.

### Backend-specific markers

The markers registered in `pyproject.toml` let you slice the suite:

```bash
uv run pytest tests/ -m postgres
uv run pytest tests/ -m sqlite
uv run pytest tests/ -m redis
uv run pytest tests/ -m kafka
uv run pytest tests/ -m rabbitmq
uv run pytest tests/ -m e2e
```

`integration` marks anything that may need Docker; `kafka` and `rabbitmq` are excluded from default
runs and must be requested explicitly; `slow` and `benchmark` are also available.

## Validate examples and docs

Everything in `examples/` is executed in CI, so it must actually run:

```bash
python scripts/validate_examples.py
```

Useful flags:

- `--syntax` — parse each example with `ast.parse` only, skipping execution.
- `--docs` — additionally syntax-check Python code blocks inside Markdown files.
- `--timeout N` — per-example execution timeout in seconds (default `60`); raise it for slow examples.

### Building the docs locally

```bash
uv run mkdocs build --strict
```

`--strict` is what CI uses, so warnings — broken internal links, pages missing from `nav` — fail
the build.

## Understand what CI will run on your PR

Every job in `.github/workflows/ci.yml` installs with `uv sync --all-extras --locked` and runs its
tools through `uv run`, so CI executes exactly the versions pinned in `uv.lock` — the same ones
`make check` uses locally. The jobs:

- **`lint`** — `ruff check .` plus `ruff format --check .` on the whole repo.
- **`type-check`** — `mypy src/`.
- **`import-linter`** — the ring-architecture contracts.
- **`audit`** — `bandit` and `pip-audit`.
- **`test`** — a matrix over Python 3.13 running
  `pytest -m "not integration and not postgres and not redis and not e2e"` with XML coverage; the
  3.13 leg uploads `coverage.xml` as an artifact (7-day retention).
- **`redis`** — the Redis-backed suite against a service container.
- **`integration`** — spins up `postgres:16` and `redis:7` service containers,
  sets `DATABASE_URL=postgresql://test:test@localhost:5432/eventsource_test` and
  `REDIS_URL=redis://localhost:6379`, and runs `pytest -m "integration or postgres or redis"`. It is
  gated on `github.event_name == 'push' && github.ref == 'refs/heads/main'`, so **it never runs on
  your PR** — run the integration suite locally before you ask for a merge.
- **`broker-tests`** — the Kafka and RabbitMQ suites; runs on PRs and on pushes to `main`.

`.github/workflows/docs.yml` runs three jobs in parallel — `build` (`mkdocs build --strict`, plus a
`docs-preview` artifact on PRs), `link-check` (lychee over `docs/**/*.md` and `README.md`, with
`site/`, `tyevans.github.io`, `localhost`, and `127.0.0.1` excluded), and `validate-examples`
(`--syntax` then a full run) — and only deploys to GitHub Pages on a push to `main`.

The docs workflow triggers on changes to `docs/**`, `mkdocs.yml`, `src/**/*.py`, `examples/**`,
`scripts/validate_examples.py`, or `.github/workflows/docs.yml`.

## Open a pull request

### Filling out the pull request template

`.github/PULL_REQUEST_TEMPLATE.md` asks for a summary, a **Type of Change** selection (bug fix,
new feature, breaking change, documentation update, refactoring, tests only), a checklist, related
issues (`Closes #123`), and any notes for reviewers.

Each checklist item maps to something you can actually run:

| Checklist item | How to satisfy it |
| --- | --- |
| Code follows the project's code style | `uv run pre-commit run --all-files` |
| Added tests that prove the fix/feature works | New tests under `tests/unit/` (and `tests/integration/` if a backend is involved) |
| All new and existing tests pass | `uv run pytest tests/unit/ -v`, plus the integration suite with Docker up |
| Updated the documentation | Edit under `docs/`, then `uv run mkdocs build --strict` |
| Updated the CHANGELOG.md | Add a bullet under `## [Unreleased]` |
| Added type hints to new code | `uv run mypy src/eventsource/ --config-file=pyproject.toml` |

### Updating CHANGELOG.md

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and semantic
versioning. Add your entry under the `## [Unreleased]` heading in an `### Added` / `### Changed` /
`### Fixed` / `### Removed` subsection. Existing entries name the affected module and list the new
public exports — match that level of detail for anything user-visible.

## Filing issues

Blank issues are disabled; pick one of the two templates in `.github/ISSUE_TEMPLATE/`:

- **Bug report** (labelled `bug`) — description, steps to reproduce, expected vs. actual behavior,
  environment (Python version, package version, OS), and additional context such as stack traces.
- **Feature request** (labelled `enhancement`) — problem statement, proposed solution, alternatives
  considered, and additional context.

For anything that is not a defect or a proposal, `config.yml` points elsewhere: the
[documentation site](https://tyevans.github.io/eventsource-py) for questions the docs already
answer, and open-ended questions and ideas can go in a GitHub issue tagged `question`.

## Project conventions to follow

### Commit message format

Use `<type>: <short description in lowercase>`, with no trailing period. Common types: `feat:`,
`fix:`, `refactor:`, `chore:`, `test:`, `docs:`. Release commits are `release X.Y.Z`; merge commits
use GitHub's default PR format.

### Definition of done

- **Feature** — implementation in `src/eventsource/` with full type annotations; unit tests covering
  happy path and edge cases; integration tests if it touches a backend; public API re-exported from
  `src/eventsource/__init__.py` with an `__all__` entry; ruff and mypy clean.
- **Bug fix** — a failing test that reproduces the bug first, then the fix, then the full suite and
  the lint/type checks.
- **Refactor** — no behavior change, so existing tests must pass *unmodified*; no public API changes
  unless that is the explicit intent.
- **New backend** — implements the relevant interface, guards its optional import with
  `try/except ImportError` and an `*_AVAILABLE` flag, ships integration tests behind the right
  marker, and adds a service to `docker-compose.test.yml` if one is needed.

### Files that must not be modified

- `py.typed` — the marker that makes the package's inline types visible to consumers.
- `src/eventsource/adapters/sql/schemas/` SQL files — append-only by design; add a new file rather than
  editing an existing one.
- Public exports in `src/eventsource/__init__.py` — do not remove or rename without an explicit
  backward-compatibility decision.

## Troubleshooting

**A hook fails in pre-commit but passes locally.** The mypy hook runs in its own virtualenv with
pinned stub versions (notably `redis==5.3.1`) that may differ from what `uv sync --all-extras`
resolved. Reproduce the hook's view with `uv run pre-commit run mypy --all-files` rather than
trusting a bare `mypy` run, and if the two genuinely disagree, update the
`additional_dependencies` list in `.pre-commit-config.yaml` alongside your change.

**Integration tests fail locally.** Confirm the services are up and healthy
(`docker compose -f docker-compose.test.yml ps`). Remember that the local compose file uses ports
`5433` and `6380` while CI uses the defaults `5432` and `6379` — set `DATABASE_URL` and `REDIS_URL`
to match your local ports, e.g.
`DATABASE_URL=postgresql://test:test@localhost:5433/eventsource_test` and
`REDIS_URL=redis://localhost:6380`. If a run left bad state behind, reset the volumes with
`docker compose -f docker-compose.test.yml down -v`.

**`mkdocs build --strict` fails.** Strict mode promotes warnings to errors. The usual causes are a
relative link to a page that moved or does not exist, and a new page under `docs/` that was never
added to the `nav` tree in `mkdocs.yml`. Read the warning text — it names the offending file — fix
the link or add the nav entry, and rebuild.
