# Development

This section covers working *on* eventsource-py rather than *with* it: setting up a
local checkout, running the test suite, passing the same lint/type/security gates CI
runs, and cutting a release.

Each page below is task-oriented — jump to the one that matches what you are doing.
If you only need one command, here are the ones you will use most:

```bash
uv sync --all-extras                     # install everything, including dev tools
uv run pytest tests/unit/ -v             # fast feedback, no external services
pre-commit run --all-files               # the full lint/format/type/security gate
```

What to expect from the rest of this section:

- **Prerequisites** — Python 3.13 (the version CI tests), plus Docker and
  Docker Compose if you intend to run integration tests.
- **Set Up a Local Environment** — cloning, `uv sync --all-extras`, the optional
  dependency extras, and installing the pre-commit hooks.
- **Project Layout at a Glance** — how `src/eventsource/`, the three test trees, and
  the append-only `migrations/` directory fit together.
- **Run the Test Suite** — unit vs. integration tests, starting backing services from
  `docker-compose.test.yml`, marker selection, async conventions, and coverage.
- **Lint, Format, and Type Check** — `ruff`, `mypy` in strict mode, and `bandit`.
- **Definition of Done** — the checklists a feature, bug fix, refactor, or new backend
  implementation must satisfy before it is considered finished.
- **Commit and Branch Conventions** — conventional commit types and message style.
- **Continuous Integration** — the jobs in `.github/workflows/ci.yml` and how to
  reproduce a failure locally.
- **Build the Documentation** — serving MkDocs locally and adding a page to the nav.
- **Release Process** — version bump, changelog, tagging, and the automated publish.

The project is async-first and backend-agnostic: most interfaces (the event store ports,
`EventBus`, the checkpoint/DLQ/outbox repositories) have several implementations behind
one contract. That shapes development in two practical ways — new code is written
`async def` by default, and any change to an interface needs to be carried through
every backend that implements it, with conformance tests to prove it. Keep both in mind
as you work through the pages that follow.

## Prerequisites

Before you start, make sure you have:

- **Python 3.13** on your `PATH`.
- **[uv](https://docs.astral.sh/uv/)** — every command in this section is written as
  `uv run ...`. If you prefer plain `pip` and a virtualenv, `pip install -e ".[dev,all]"`
  installs the same set of packages (that is what CI does).
- **Git**, for the checkout and for the `pre-commit` hooks.
- **Docker and Docker Compose** — only if you plan to run the integration tests.

Nothing else is required for the unit test suite: the runtime dependencies are just
`pydantic` and `sqlalchemy`, and everything else is an optional extra.

### Python version support (3.13)

`pyproject.toml` declares `requires-python = ">=3.13"`, and the CI test matrix in
`.github/workflows/ci.yml` runs on **3.13** only. The `lint` and `type-check` jobs
also pin 3.13, and the coverage upload step runs on that same matrix leg, so 3.13 is
the version to reach for if you want your local results to match CI exactly.

Older interpreters are not supported: `requires-python` blocks installation on 3.12 and
below.

### Docker and Docker Compose (integration tests only)

The unit suite under `tests/unit/` runs entirely in-process — in-memory stores, buses,
and repositories — so it needs no containers. The integration suite under
`tests/integration/` needs a Docker daemon.

You do **not** normally start the services yourself. `tests/integration/conftest.py`
uses [testcontainers](https://testcontainers-python.readthedocs.io/) to start
`postgres:15` and `redis:7` on demand, session-scoped, and stop them when the run ends.
Two things must be true for that to work:

- `testcontainers>=4.0` is installed — it is in the `dev` extra, so `uv sync --all-extras`
  covers it.
- `docker info` succeeds. The conftest shells out to exactly that command (with a 5s
  timeout) to decide whether Docker is available.

If either check fails, the affected tests **skip rather than fail**, with a reason of
`"testcontainers not installed"` or `"Docker not available or not running"`. An
integration run that finishes suspiciously fast usually means Docker was not reachable —
read the skip reasons in the summary before believing a green result.

```bash
docker info                                  # must exit 0
uv run pytest tests/integration/ -v          # containers start automatically
```

#### When you want long-lived services instead

`docker-compose.test.yml` at the repo root exists for manual work — poking at a database
between runs, or running examples against real backends. It is not what the integration
fixtures connect to.

| Service | Image | Host port | Credentials / database |
| --- | --- | --- | --- |
| PostgreSQL | `postgres:15` | `5433` (override with `POSTGRES_PORT`) | user `test`, password `test`, db `eventsource_test` |
| Redis | `redis:7` | `6380` (override with `REDIS_PORT`) | — |

The non-default host ports are deliberate: they keep these containers from colliding
with a PostgreSQL or Redis you may already have running on 5432/6379. Both services
declare health checks, so `docker compose -f docker-compose.test.yml ps` tells you when
they are actually ready rather than merely started.

```bash
docker compose -f docker-compose.test.yml up -d
docker compose -f docker-compose.test.yml down -v   # -v also drops the data volumes
```

#### What does not need Docker

SQLite-backed tests need only the `aiosqlite` extra. Kafka and RabbitMQ appear in
neither the compose file nor the testcontainers fixtures; their tests carry the `kafka`
and `rabbitmq` markers, which no CI marker expression selects, so ignore that
infrastructure unless you are working on those buses — in which case run `-m kafka` /
`-m rabbitmq` against your own broker.

See [Run the Test Suite](#run-the-test-suite) for the marker expressions and coverage
flags.

## Set Up a Local Environment

One command gets you a working checkout with every backend, every dev tool, and the
docs toolchain:

```bash
git clone https://github.com/tyevans/eventsource-py.git
cd eventsource-py
uv sync --all-extras
uv run pre-commit install
```

From there, `uv run pytest tests/unit/ -v` should pass without any services running.

### Clone and install with `uv sync --all-extras`

`uv sync --all-extras` creates `.venv/` in the repo root and installs:

- the runtime dependencies (`pydantic>=2.8`, `sqlalchemy[asyncio]>=2.0.43`, `orjson>=3.10.7`);
- **every** optional extra declared in `pyproject.toml` — including `dev`, `docs`,
  `benchmark`, and each backend extra;
- the `dev` **dependency group** (`[dependency-groups]` in `pyproject.toml`), which uv
  installs by default and which is where `bandit>=1.9.2` and `mypy>=1.19.0` live.

That last point matters. CI installs with `uv sync --all-extras --locked` on every
job and runs each tool through `uv run`, so it executes exactly the versions
pinned in `uv.lock`; `make check` uses the same flags for the same reason. A
`pip install -e ".[dev,all]"` environment is **not** equivalent: pip honours the
`[project.optional-dependencies]` extras but not the PEP 735 dependency group, so
it has no `bandit`, `pip-audit`, `import-linter`, or `pytest-timeout` on the path,
and it floats tool versions above their pins instead of respecting the lock. If
you install with pip rather than uv and want the full local gate to match, add the
group's tools explicitly:

```bash
pip install -e ".[dev,all]" bandit pip-audit import-linter pytest-timeout
```

Either way, install the `dev` **extra** — it is opt-in. Without it there is no
`ruff` or `pytest` in the environment, and `uv run ruff` will quietly use whatever
`ruff` is on your `PATH` instead of failing.

Prefix commands with `uv run` and you never have to activate the virtualenv:

```bash
uv run pytest tests/unit/ -q
uv run mypy src/eventsource/ --config-file=pyproject.toml
```

To work against a specific interpreter, pass `--python`:

```bash
uv sync --all-extras --python 3.13
```

### Optional dependency extras (postgresql, sqlite, redis, rabbitmq, kafka, telemetry, all)

`--all-extras` is the right default for development, but knowing what each extra pulls
in helps when you are diagnosing an `ImportError` or deciding what a user of the library
actually needs:

| Extra | Installs | Enables |
| --- | --- | --- |
| `postgresql` | `asyncpg>=0.30,<1.0` | PostgreSQL event store, snapshot store, advisory locks, repositories |
| `sqlite` | `aiosqlite>=0.19,<1.0` | SQLite event store, snapshot store, repositories |
| `redis` | `redis>=8.0,<9.0` | Redis event bus |
| `rabbitmq` | `aio-pika>=9.0.5` | RabbitMQ event bus |
| `kafka` | `aiokafka>=0.12,<1.0` | Kafka event bus |
| `kafka-schema-registry` | `aiokafka` + `confluent-kafka>=2.6,<3.0` | Kafka bus with Schema Registry integration |
| `telemetry` | `opentelemetry-api`/`-sdk` `>=1.16.0,<2.0` | OpenTelemetry tracing in `observability/` |
| `benchmark` | `pytest-benchmark>=4.0` | `tests/benchmarks/` |
| `dev` | pytest, pytest-asyncio, pytest-cov, pytest-benchmark, `mypy>=1.8`, `ruff>=0.14.8`, `testcontainers>=4.0`, pre-commit | the test and lint toolchain |
| `docs` | mkdocs, mkdocs-material, mkdocstrings[python], pymdown-extensions | building this site |

Two aggregate extras exist as conveniences:

- **`all`** — `postgresql`, `sqlite`, `redis`, `rabbitmq`, `kafka`, `telemetry`. Note it
  does *not* include `dev`, `docs`, `benchmark`, or `kafka-schema-registry`.
- **`all-backends`** — just `postgresql` + `sqlite`, for when you want the persistence
  backends without any message broker clients.

Note that `bandit` is in neither table: it lives in the PEP 735 `dev` dependency *group*
alongside `mypy>=1.19.0`, not in an extra. See the previous page for why that matters.

#### How a missing extra shows up

Optional imports are guarded in the source by `try/except ImportError` with a
`*_AVAILABLE` flag — `AIOSQLITE_AVAILABLE` in the top-level `__init__.py`, `REDIS_AVAILABLE`,
`RABBITMQ_AVAILABLE`, and `KAFKA_AVAILABLE` in the corresponding `bus/` modules. Nothing
fails at import time; the failure is deferred to the point of use, and it names the extra:

```
RedisNotAvailableError: Redis package is not installed. Install it with: pip install eventsource[redis]
```

`KafkaNotAvailableError` and `RabbitMQNotAvailableError` behave the same way. (The
distribution is published as `eventsource-py`, so the literal command in those messages
is `pip install "eventsource-py[redis]"`.)

The `telemetry` extra is the exception: OpenTelemetry is *optional at runtime by design*.
When it is absent, the metrics helpers in `application/subscriptions/metrics.py`,
`application/subscriptions/shutdown.py`, and `migration/metrics.py` degrade to no-ops rather than
raising, so tracing silently does nothing instead of breaking your process.

If a test skips with a message about an unavailable backend, an extra is missing from
your environment — re-run `uv sync --all-extras`.

### Install pre-commit hooks

The hooks are a superset of the checks CI runs, so installing them once per clone is the
cheapest way to avoid a red pipeline:

```bash
uv run pre-commit install
```

`.pre-commit-config.yaml` configures four repos:

| Repo (rev) | Hooks |
| --- | --- |
| `pre-commit/pre-commit-hooks` (v5.0.0) | `trailing-whitespace`, `end-of-file-fixer`, `check-yaml`, `check-added-large-files`, `check-merge-conflict`, `debug-statements` |
| `astral-sh/ruff-pre-commit` (v0.14.8) | `ruff` with `--fix`, then `ruff-format` |
| `pre-commit/mirrors-mypy` (v1.19.0) | `mypy --config-file=pyproject.toml`, restricted to `files: ^src/` |
| `PyCQA/bandit` (1.8.3) | `bandit -c pyproject.toml` (installed as `bandit[toml]`) |

Three details are worth knowing before you trust — or debug — a hook result.

**The Python hooks run in your `.venv`, not their own.** `ruff`, `ruff-format`, `mypy`,
`bandit` and `import-linter` are `repo: local` hooks with `language: system`, invoked
through `uv run`, so they see exactly the versions in `uv.lock` — the same ones CI and
`make check` use. They previously ran in pre-commit-managed environments with a
hand-maintained `additional_dependencies` list restating the project's runtime
dependencies for mypy to type against; that list drifted (it was missing `orjson`,
a core dependency) and mypy failed in the hook while passing everywhere else, which is
why the copy was deleted rather than corrected. Do not reintroduce it: it is a second
declaration site for facts `pyproject.toml` already owns.

**Two hooks are stricter than CI, two are looser.** CI runs no `pre-commit` job at all —
it runs `ruff check .` and `ruff format --check .` across the whole repo and `mypy src/`.
So:

- `ruff` and `mypy` in the hook only see *staged* files, while CI sees everything. A
  `pre-commit run --all-files` closes that gap.
- `bandit` and the whitespace/YAML hooks have **no CI equivalent**. They gate your
  commits locally and nowhere else.

**`bandit` is configured in `pyproject.toml`.** `[tool.bandit]` excludes `tests` and
`examples` and skips `B101`, so `assert` in production code is allowed. Everything else
it flags is a real finding to address rather than silence.

Run the whole gate manually at any time — do this before opening a pull request:

```bash
uv run pre-commit run --all-files
```

The first invocation builds each hook's environment and takes a minute or two;
afterwards they are cached. To skip the hooks for a single commit (a work-in-progress
checkpoint, say), use `git commit --no-verify` — but run the gate before you push.
