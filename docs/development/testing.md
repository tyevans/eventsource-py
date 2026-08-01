# Testing

This page is a working guide to the `eventsource-py` test suite: how to install what each
tier needs, how to run it, and how to read the skips when something is missing.

The suite is split by what a test needs to run. Most of it — everything under
`tests/unit/`, plus the SQLite-backed suites — needs nothing but Python and the `dev`
extra. A smaller set under `tests/integration/` needs Docker (via testcontainers or
`docker-compose.test.yml`), and `tests/benchmarks/` needs `pytest-benchmark`. Markers
registered in `pyproject.toml` let you select or deselect each group.

A few conventions apply everywhere and are worth knowing up front:

- **Async tests need no decorator.** `asyncio_mode = "auto"` is set in
  `[tool.pytest.ini_options]`, so plain `async def test_*` functions run as-is. Async
  fixtures still use `@pytest_asyncio.fixture`.
- **Coverage is on by default.** The configured `addopts` are
  `-v --cov=src/eventsource --cov-report=term-missing`, so every run prints a coverage
  table. Pass `--no-cov` when you want a quick, quiet run.
- **Shared fixtures live in `tests/conftest.py`**, and the domain models they build on
  (counter and order events, states, and aggregates) live in `tests/fixtures/`. Reuse
  them rather than defining new sample events.
- **Optional backends degrade to skips.** Modules guard imports such as `aiosqlite`, the
  OpenTelemetry SDK, and `testcontainers` behind `*_AVAILABLE` flags and matching
  `skip_if_no_*` markers, so a partial install skips those tests instead of erroring.

If you only want the fast feedback loop, `uv run pytest tests/unit --no-cov` is the
command to remember; the sections below cover everything else.

## Prerequisites

You need a checkout of the repository, Python 3.13 or newer, and either
[uv](https://docs.astral.sh/uv/) or pip. Docker is optional — it is only required for
`tests/integration/`.

### Install the dev and backend extras

The `dev` extra carries everything the test runner itself needs: `pytest`,
`pytest-asyncio`, `pytest-cov`, `pytest-benchmark`, `testcontainers`, plus `mypy`,
`ruff`, and `pre-commit`. It installs **no** backend drivers, so on a `dev`-only
environment the SQLite, PostgreSQL, Redis, Kafka, RabbitMQ, and OpenTelemetry tests
report as skips rather than failures — `tests/conftest.py` sets `AIOSQLITE_AVAILABLE`
and `OTEL_METRICS_AVAILABLE` from guarded imports and exposes `skip_if_no_aiosqlite` /
`skip_if_no_otel_metrics` markers built from them.

Install everything the suite can exercise in one command:

```bash
uv sync --all-extras
```

With pip, the equivalent is the `dev` and `all` extras together:

```bash
pip install -e ".[dev,all]"
```

`all` is an aggregate of `postgresql`, `sqlite`, `redis`, `rabbitmq`, `kafka`, and
`telemetry`, so this combination leaves nothing skipped for dependency reasons. Use it
unless you have a reason to keep the environment lean. (`--all-extras` also pulls in
`docs`, the mkdocs tooling, which the test run does not need.)

If you do want a narrower install, the extras compose. `all-backends` is the useful
middle ground — `asyncpg` (PostgreSQL) plus `aiosqlite` (SQLite), which unskips
`tests/stores/`, `tests/repositories/`, and `tests/locks/` without pulling in message
brokers:

```bash
uv sync --extra dev --extra all-backends
```

What each extra unlocks:

| Extra | Adds | Unskips |
| --- | --- | --- |
| `dev` | pytest stack, testcontainers, lint/type tooling | the runner itself; `tests/unit/` |
| `sqlite` (also via `all-backends`) | `aiosqlite` | the SQLite stores, snapshot store, and repositories |
| `postgresql` (also via `all-backends`) | `asyncpg` | PostgreSQL stores, repositories, and advisory locks |
| `redis` | `redis` | the Redis bus and Redis-backed integration tests |
| `rabbitmq` / `kafka` | `aio-pika` / `aiokafka` | the `rabbitmq` and `kafka` marked tests |
| `telemetry` | OpenTelemetry API + SDK | the OpenTelemetry tracing and metrics tests |
| `benchmark` | `pytest-benchmark` | `tests/benchmarks/` (already included in `dev`) |
| `all` | aggregate of the six backend/telemetry extras | every optional-dependency test |
| `all-backends` | aggregate of `postgresql` + `sqlite` | the non-Docker backend suites |

### Docker requirement

`tests/integration/` provisions PostgreSQL and Redis through `testcontainers`, which
needs a running Docker daemon. You do not have to tell the suite whether Docker is
there — `tests/integration/conftest.py` works it out at import time and skips whatever
it cannot run.

It checks two things independently:

- **`TESTCONTAINERS_AVAILABLE`** — set by attempting `from testcontainers.postgres import
  PostgresContainer` and `from testcontainers.redis import RedisContainer`. False means
  the `dev` extra was not installed (or `testcontainers` was removed from the env).
- **`DOCKER_AVAILABLE`** — set by running `docker info` in a subprocess with a five-second
  timeout and checking for exit code 0. A missing `docker` binary, a daemon that is not
  running, and a daemon that is too slow to answer all resolve to False.

Two skip markers combine them, and both require *both* flags:

```python
skip_if_no_postgres_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE),
    reason="PostgreSQL test infrastructure not available",
)
```

`skip_if_no_redis_infra` is the same check with a Redis-flavoured reason string. There
are also narrower `skip_if_no_testcontainers` and `skip_if_no_docker` markers whose
reason strings ("testcontainers not installed (pip install testcontainers)" and "Docker
not available or not running") tell you which half is missing. The session-scoped
`postgres_container` and `redis_container` fixtures re-check the same pair and call
`pytest.skip(...)` rather than raising, so anything that reaches them by fixture request
instead of by marker still skips cleanly.

The broker suites layer their own requirements on top: `skip_if_no_kafka_infra` also
needs `KAFKA_AVAILABLE` (`aiokafka`) and an importable `testcontainers.kafka`, and
`skip_if_no_rabbitmq_infra` also needs `RABBITMQ_AVAILABLE` (`aio-pika`). Both modules
additionally call `pytest.skip(..., allow_module_level=True)` when their client library
is missing, so the whole file drops out before any container work is attempted.

The practical consequence: on a machine without Docker, the integration suite skips
instead of failing and you still get a green run — just a smaller one. Nothing needs to
be configured or disabled by hand. The trade-off is that a silent skip looks a lot like
a pass, so if you are relying on the integration tier, confirm Docker first:

```bash
docker info > /dev/null && echo "docker ok"
```

and run the suite with `-rs` to print the skip reasons (see
[Troubleshooting](#tests-silently-skipping-missing-optional-dependency-vs-missing-docker)).

Because `DOCKER_AVAILABLE` is evaluated once at collection time, starting Docker
*after* pytest has begun will not un-skip anything — start the daemon, then start the
run.

Verify the install with the fastest tier:

```bash
uv run pytest tests/unit --no-cov
```

Coverage is enabled through `addopts`, so `--no-cov` is what keeps this check quick.

## Run the unit suite (no Docker)

`tests/unit/` is the tier you will run most: no Docker, no network, no external
services. It is also the bulk of the suite — roughly 5,200 tests, about three minutes on
a warm checkout.

The everyday command:

```bash
uv run pytest tests/unit --no-cov
```

Drop `--no-cov` when you want the coverage table (`addopts` supplies
`-v --cov=src/eventsource --cov-report=term-missing`); keep it when you want speed. With
pip instead of uv, drop the `uv run` prefix — `pytest tests/unit --no-cov`.

Narrow it while you are working on one area. Any of these work:

```bash
# one module
uv run pytest tests/unit/test_aggregate_root.py --no-cov

# one package of the tier
uv run pytest tests/unit/subscriptions --no-cov

# one test, by node id
uv run pytest tests/unit/test_domain_event.py::TestDomainEventImmutability::test_event_is_frozen --no-cov

# everything whose name matches
uv run pytest tests/unit -k "outbox" --no-cov
```

Useful flags on top of those: `-x` to stop at the first failure, `--lf` to re-run only
what failed last time, `-q` to trade the default `-v` listing for a progress line, and
`-rs` to print the reason behind every skip.

A few things to know about this tier:

- **It does not need any backend driver.** The PostgreSQL, Kafka, RabbitMQ, and Redis
  modules under `tests/unit/` (`test_postgresql_event_store.py`,
  `test_kafka_event_bus.py`, `test_rabbitmq_event_bus.py`, `test_redis_event_bus.py`, and
  friends) drive their clients through mocks rather than live services. On a full
  `--all-extras` install the tier reports essentially no skips.
- **Async tests need no marker.** `asyncio_mode = "auto"` handles them; the
  `@pytest.mark.asyncio` decorators you will see scattered through the files are
  redundant belt-and-braces, not a requirement for new tests.
- **`-m` selection is not needed here.** Nothing in `tests/unit/` is marked
  `integration`, `postgres`, `redis`, or `e2e`, so the CI deselection expression
  (`-m "not integration and not postgres and not redis and not e2e"`) is a no-op against
  this path. Run the directory, not a marker expression.

If you want the full no-Docker run rather than just this directory, add the other tiers
that need no services — SQLite and mocked-PostgreSQL suites live outside `tests/unit/`:

```bash
uv run pytest tests/unit tests/stores tests/repositories tests/locks --no-cov
```

That is the closest local equivalent to what CI's unit job covers, and it still touches
nothing but your filesystem.

## Run the integration suite

`tests/integration/` is the tier that talks to real services: PostgreSQL, Redis, and
(for the broker suites) Kafka and RabbitMQ. Everything in it is marked `integration`,
plus a backend marker — `postgres`, `redis`, `kafka`, `rabbitmq`, or `e2e` — and guarded
by a `skip_if_no_*_infra` marker, so the whole tier drops to skips when Docker is not
reachable.

The default command:

```bash
uv run pytest tests/integration -v
```

Or by marker, which is what CI's integration job runs:

```bash
uv run pytest -m "integration or postgres or redis"
```

Kafka and RabbitMQ are *not* in that expression. They are opt-in through `-m kafka` and
`-m rabbitmq`; see [Kafka and RabbitMQ suites](#kafka-and-rabbitmq-suites).

You have two ways to supply the services:

- **Option A — let testcontainers do it.** This is the default and needs no setup beyond
  a running Docker daemon. `tests/integration/conftest.py` starts a `postgres:15` and a
  `redis:7` container per session and hands each test a connection URL from the running
  container.
- **Option B — run `docker-compose.test.yml`.** A persistent local stack on fixed ports,
  useful when you want to keep data between runs or `psql` into the database while
  debugging.

One thing to know before you pick: **the session fixtures always use testcontainers.**
`postgres_container` and `redis_container` construct `PostgresContainer("postgres:15")`
and `RedisContainer("redis:7")` unconditionally, and `postgres_connection_url` /
`redis_connection_url` derive from the container's mapped host and port. They do not read
`DATABASE_URL` or `REDIS_URL`. So starting the compose stack does not redirect the suite
at it — the two coexist, and the compose stack is there for manual inspection, for the
handful of tests that *do* read environment variables (the distributed-tracing tests fall
back to `REDIS_URL`, `RABBITMQ_URL`, and `KAFKA_BOOTSTRAP_SERVERS`), and for standing up
a stack you drive yourself. CI takes the same shape from the other direction: it provides
GitHub Actions service containers on the standard ports and sets `DATABASE_URL` /
`REDIS_URL`, while the testcontainer fixtures still start their own containers alongside.

A first run pulls the `postgres:15` and `redis:7` images, so budget a minute or two;
after that the containers start in seconds and are reused for the whole session. If you
see the tier finish suspiciously fast, it skipped — re-run with `-rs` to see why.

### Option A: let testcontainers manage services

This is the default, and it needs nothing from you but a running Docker daemon:

```bash
uv run pytest tests/integration -v
```

`tests/integration/conftest.py` owns the lifecycle. Two session-scoped fixtures start one
container each and stop it when the session ends:

```python
@pytest.fixture(scope="session")
def postgres_container() -> Generator[Any, None, None]:
    if not TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("PostgreSQL testcontainer not available")

    container = PostgresContainer("postgres:15")
    container.start()
    yield container
    container.stop()
```

`redis_container` is the same shape around `RedisContainer("redis:7")`. Session scope
means one Postgres and one Redis for the entire run, not one per test — the images are
pulled on first use and the containers start in a few seconds after that.

**Connection URLs come from the container, not from your environment.** Each container
publishes on an ephemeral host port, and the URL fixtures read it back:

- `postgres_connection_url` takes `postgres_container.get_connection_url()` and rewrites
  the driver — `postgresql://` becomes `postgresql+asyncpg://`, and any `psycopg2` in the
  URL becomes `asyncpg` — because the suite drives SQLAlchemy's async engine.
- `redis_connection_url` builds `redis://{host}:{port}` from
  `get_container_host_ip()` and `get_exposed_port(6379)`.

Neither reads `DATABASE_URL` or `REDIS_URL`. That is the practical difference from
[Option B](#option-b-use-docker-composetestyml-for-a-persistent-local-stack): with
testcontainers you never pick a port, and you also cannot point the suite at a database
you started yourself.

#### What you get on top of the bare containers

The fixture chain layers schema and isolation over the raw containers, so tests receive
something ready to use:

| Fixture | Scope | Gives you |
| --- | --- | --- |
| `postgres_engine` | session | An `AsyncEngine` (pool size 5, max overflow 10) with the test schema already created |
| `postgres_session_factory` | function | An `async_sessionmaker` with `expire_on_commit=False` |
| `clean_postgres_tables` | function | `TRUNCATE ... CASCADE` on all four tables before *and* after the test |
| `postgres_event_store` / `..._with_outbox` | function | A `PostgreSQLEventStore` on a clean database, with `outbox_enabled` off / on |
| `postgres_checkpoint_repo`, `postgres_dlq_repo`, `postgres_outbox_repo` | function | Repositories bound to the engine, on clean tables |
| `redis_client` | function | An async Redis client, `flushall()`-ed before and after |
| `clean_redis` | function | The flush without the client, for tests that make their own |
| `redis_event_bus_factory` / `redis_event_bus` | function | An unconnected `RedisEventBus` builder / a connected bus on a clean Redis |

`postgres_engine` creates the schema itself rather than running the files in
`src/eventsource/adapters/sql/schemas/`: `events`, `projection_checkpoints`, `dead_letter_queue`,
and `event_outbox`, each as a list of single statements (`EVENTS_SCHEMA_STATEMENTS` and
friends) because asyncpg will not accept multi-statement strings. It runs once per
session; per-test isolation comes from `clean_postgres_tables` truncating, not from
recreating the database.

The Redis fixtures build their clients with `single_connection_client=True`, and
`redis_event_bus` is deliberately split into a factory plus a thin wrapper so the bus is
constructed in the fixture but `connect()`ed inside the test's event loop. Both are
workarounds for connection pools binding to the wrong loop; if you add a Redis fixture,
follow the same pattern and mark it `@pytest_asyncio.fixture(loop_scope="session")`.

`conftest.py` also defines the domain used by this tier — `TestItemCreated`,
`TestItemUpdated`, `TestItemDeleted`, `TestOrderCreated`, `TestOrderItemAdded`,
`TestOrderCompleted`, and the `TestOrderAggregate` that folds them into `TestOrderState`.
The store fixtures register those six event types into a fresh `EventRegistry` per test
rather than relying on the global one. Reuse them instead of declaring new sample events.

#### When it skips

Both container fixtures re-check `TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE` and call
`pytest.skip(...)`, so a test that requests them without carrying
`skip_if_no_postgres_infra` / `skip_if_no_redis_infra` still skips cleanly instead of
erroring on a connection refusal. `redis_client` and `clean_redis` add one more:
`import redis.asyncio` in a `try`, skipping with "redis package not installed" when the
`redis` extra is absent, and `redis_event_bus_factory` checks the library's
`REDIS_AVAILABLE` flag for the same reason.

Because these are silent by default, run with `-rs` when you expect the tier to do work:

```bash
uv run pytest tests/integration -rs
```

#### Leftover containers

testcontainers stops what it started at the end of the session, but a hard interrupt
(`SIGKILL`, a crashed runner) can leave one behind. They are named by testcontainers
rather than by this repo, so find them by image:

```bash
docker ps --filter ancestor=postgres:15 --filter ancestor=redis:7
```

The compose stack is unaffected by this — its containers are named
`eventsource-test-postgres` and `eventsource-test-redis`, and are cleaned up with
`docker compose -f docker-compose.test.yml down -v` instead.

## Test layout

`testpaths = ["tests"]`, and everything under it is grouped by *what a test needs to
run*, not by which module it covers:

```
tests/
  conftest.py          # shared fixtures for the whole suite
  fixtures/            # counter/order events, states, aggregates (importable models)
  unit/                # no external services
  stores/              # SQLite event store + in-memory read_all filtering
  repositories/        # SQLite checkpoint/outbox/DLQ repos
  locks/               # PostgreSQL advisory-lock logic, mocked
  integration/         # docker/testcontainers-backed
  benchmarks/          # pytest-benchmark suite, own conftest.py
```

### `tests/unit/` — flat `test_*.py` modules, no external services

This is the largest tier and the one you will spend the most time in. It has two shapes
side by side: **flat `test_*.py` modules** directly under `tests/unit/`, and **package
subdirectories** that mirror `src/eventsource/`.

The flat modules — 37 of them — cover cross-cutting concerns or a single public class
whose source lives in a package you would otherwise have to pick a subdirectory for:

| Module | Covers |
| --- | --- |
| `test_domain_event.py`, `test_event_registry.py`, `test_event_type_auto.py`, `test_timestamp_types.py` | `DomainEvent` semantics, auto-registration, the global `EventRegistry` |
| `test_aggregate_root.py`, `test_aggregate_repository.py`, `test_transition.py` | `AggregateRoot`, `AggregateRepository`, state transitions |
| `test_connection_helper.py` | the shared SQLAlchemy connection-helper used by the SQL-backed adapters |
| `test_event_bus.py`, `test_redis_event_bus.py`, `test_kafka_event_bus.py`, `test_rabbitmq_event_bus.py`, `test_rabbitmq_exports.py`, `test_redis_event_bus_tracing.py` | bus interface and each broker client |
| `test_checkpoint_repository.py`, `test_checkpoint_position.py`, `test_outbox_repository.py`, `test_dlq_repository.py` | checkpoint, outbox, and DLQ repositories |
| `test_projection_base.py`, `test_projection_decorators.py`, `test_projection_protocols.py`, `test_projection_coordinator.py` | projection base classes and wiring |
| `test_subscription_config.py`, `test_subscription_manager.py`, `test_catchup_runner.py`, `test_live_runner.py` | subscription lifecycle and runners |
| `test_protocols.py`, `test_exceptions.py`, `test_json_encoder.py`, `test_conformance.py`, `test_fixtures.py` | `protocols.py`, `exceptions.py`, serialization, the shipped conformance suites, and the shared fixtures themselves |
| `test_additional_coverage.py`, `test_edge_cases.py` | grab-bag coverage and boundary cases across modules |

The package subdirectories mirror source packages one-for-one: `aggregates/`, `bus/`,
`handlers/`, `migration/`, `migrations/`, `multitenancy/`, `observability/`,
`projections/`, `readmodels/`, `repositories/`, `serialization/`,
`adapters/`, `ports/`, `application/subscriptions/`, `sync/`, and `testing/`. These hold the deeper,
feature-specific coverage — `migration/` alone has 25 modules covering dual-write,
cutover, position mapping, and chaos scenarios, and `application/subscriptions/` has 15 covering
backpressure, drain, retry, health, and pause/resume.

The rule of thumb when adding a module: if it exercises one source package in depth, put
it in the matching subdirectory; if it exercises a public contract that spans packages,
a flat module is fine.

#### No external services

Nothing in this tier opens a socket. Modules named after a server-backed backend belong
here precisely because they drive the client against mocks:
`test_postgresql_event_store.py` states in its own docstring that "all database
interactions are mocked using `unittest.mock`", and `readmodels/test_postgresql.py` and
`readmodels/test_sqlite.py` do the same to assert on generated SQL. Roughly 48 modules in
the tier import from `unittest.mock`. That is why these tests run on a `dev`-only install
with no drivers present.

The one deliberate exception is the SQLite modules that want a real (in-process)
database — `stores/test_sqlite_tracing.py` and `snapshots/test_sqlite_snapshot_store.py`.
They pull the guard from the shared conftest and apply it module-wide:

```python
from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]
```

That combination both tags them for `-m sqlite` selection and skips them cleanly when
`aiosqlite` is absent. A handful of other modules apply the same idea per-test with
`@pytest.mark.skipif(not KAFKA_AVAILABLE, ...)`, `not AIOSQLITE_AVAILABLE`, or
`not OTEL_METRICS_AVAILABLE`.

#### Conventions inside the tier

- **Async tests need no decorator.** `asyncio_mode = "auto"` means a plain
  `async def test_*` runs. You will still see `@pytest.mark.asyncio` on a large number of
  existing tests; it is redundant but harmless, and new tests can omit it.
- **Import shared models, don't define new ones.** Prefer `from tests.fixtures import ...`
  or the fixtures re-exported by `tests/conftest.py`. Because `DomainEvent` subclasses
  auto-register into the global `EventRegistry`, every ad-hoc event class declared at
  module scope is a process-wide side effect. Some existing modules do declare local
  `SampleEvent`-style classes where they need an event shape the fixtures do not provide;
  treat that as the exception, not the default.
- **No markers needed by default.** An ordinary unit test carries no marker at all —
  it is neither `integration` nor backend-tagged, so it survives every deselection filter
  in the sections below.

Run just this tier with:

```bash
uv run pytest tests/unit --no-cov
```

### `tests/repositories/`, `tests/locks/` — backend-specific suites

Two small top-level suites sit beside `unit/`. They exist as their own directories
because they are organised around a *backend* rather than a source package, and because
each one is the non-Docker half of a pair whose other half lives in
`tests/integration/`. The equivalent real-`SQLiteEventStore` coverage now lives under
`tests/unit/adapters/` (`test_sqlite_conformance.py`) alongside the other adapter suites
rather than in its own top-level directory.

| Directory | Modules | Needs |
| --- | --- | --- |
| `tests/repositories/` | `test_sqlite_repos.py` | `aiosqlite` |
| `tests/locks/` | `test_postgresql_locks.py` | nothing |

Each directory has an `__init__.py` and no `conftest.py` of its own — every fixture they
use comes from `tests/conftest.py`.

#### `tests/unit/adapters/test_sqlite_conformance.py`

This is the one place a real `SQLiteEventStore` (imported from
`eventsource.adapters.sqlite`) is driven end-to-end against the store port
conformance suites in `eventsource.testing.conformance_ports`, alongside
`SQLiteSnapshotStore` and the SQL-backed checkpoint/DLQ repositories. It
constructs stores directly rather than through a fixture, covers WAL mode and
busy-timeout configuration, schema initialization, append and optimistic
locking, idempotency, retrieval, stream reading, and version queries, and is
guarded module-wide:

```python
from tests.conftest import skip_if_no_aiosqlite

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]
```

Tenant-filtering coverage for `read_all()` and `read_category()` now lives
alongside the other port-level and application-level tenant tests (for example
`tests/unit/application/projections/test_tenant_filter.py`) rather than in a
dedicated store-tenant-filter module.

#### `tests/repositories/`

`test_sqlite_repos.py` covers `SQLiteCheckpointRepository`, `SQLiteOutboxRepository`, and
`SQLiteDLQRepository` against a real SQLite database. Unlike the store suite, it works
entirely through the shared fixtures — `sqlite_checkpoint_repo`, `sqlite_outbox_repo`, and
`sqlite_dlq_repo`, each of which layers a `CREATE TABLE` on the `sqlite_connection`
fixture and hands back a wired repository. Every class begins with a
`test_implements_protocol` case asserting `isinstance(repo, CheckpointRepository)` (and
likewise for the outbox and DLQ protocols), then walks the rest of the interface:
checkpoint get/update/reset and lag metrics, outbox add/pending/publish/fail/retry/cleanup
and stats, DLQ add/query/resolve/retry, failure statistics, and event-data serialization.

Same guard as the store suite — `pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]`
plus an `if AIOSQLITE_AVAILABLE:` import block.

#### `tests/locks/`

Despite the name, nothing here touches PostgreSQL. `test_postgresql_locks.py` covers the
parts of the advisory-lock module that are pure logic: the `LockInfo` dataclass, the
`LockAcquisitionError` and `LockNotHeldError` messages, `PostgreSQLLockManager._key_to_lock_id`
(determinism, collision behaviour, that the result fits PostgreSQL's signed 63-bit
`bigint`, and that Unicode keys hash cleanly), and the `migration_lock_key` helper's
`"<operation>:<tenant_id>"` format. No marker, no guard, no driver — it runs anywhere.

The half that needs a live server is
`tests/integration/locks/test_postgresql_locks_integration.py`; the unit module ends with
a comment pointing at it.

#### Running them

They are ordinary paths, so name them directly:

```bash
uv run pytest tests/stores tests/repositories tests/locks --no-cov
```

On a `dev`-only install that run is mostly skips. Add `--extra all-backends` (or
`--extra sqlite`) to the sync and the SQLite modules light up; `-m sqlite` selects just
those, and `-m "not sqlite"` leaves you with the tenant-filter and lock-logic modules.

### `tests/integration/` — docker/testcontainers-backed tests

This is the tier that talks to real services. Nineteen test modules across nine
subdirectories, all sharing one `tests/integration/conftest.py` that owns container
lifecycle, schema creation, cleanup, and the skip logic:

| Directory | Modules | Backend |
| --- | --- | --- |
| `bus/` | `test_redis.py`, `test_kafka.py`, `test_rabbitmq.py` | Redis, Kafka, RabbitMQ |
| `stores/` | `test_postgresql.py` | PostgreSQL |
| `repositories/` | `test_checkpoint.py`, `test_dlq.py`, `test_outbox.py` | PostgreSQL |
| `locks/` | `test_postgresql_locks_integration.py` | PostgreSQL (advisory locks) |
| `migrations/` | `test_migration_schema_postgresql.py` | PostgreSQL |
| `projections/` | `test_database_projection.py` | PostgreSQL |
| `readmodels/` | `test_repositories.py`, `test_projection.py`, `test_enhanced_features.py` | parametrized: in-memory, SQLite, PostgreSQL |
| `application/subscriptions/` | `test_catchup.py`, `test_live.py`, `test_transition.py`, `test_full_flow.py`, `test_resilience.py`, `test_advanced_features.py` | none — in-memory |
| `observability/` | `test_tracing_integration.py`, `test_distributed_tracing.py` | none / externally provided brokers |
| _(root)_ | `test_imports.py` | none |

`observability/`, `application/subscriptions/`, and `readmodels/` layer their own `conftest.py` on top
of the shared one.

#### How containers are provisioned

The shared conftest starts containers with [testcontainers], at **session scope**, so one
PostgreSQL and one Redis serve the whole run:

- `postgres_container` → `PostgresContainer("postgres:15")`
- `redis_container` → `RedisContainer("redis:7")`

`postgres_connection_url` rewrites the psycopg2 URL testcontainers hands back into an
`postgresql+asyncpg://` one, and `postgres_engine` (also session-scoped) creates the
SQLAlchemy async engine *and* the schema — `events`, `projection_checkpoints`,
`dead_letter_queue`, and `event_outbox`, each as a list of individual statements because
asyncpg will not accept a multi-statement string.

Isolation is per-test, not per-container. `clean_postgres_tables` truncates all four
tables before and after each test, and `clean_redis` / `redis_client` call `flushall()` on
both sides of the yield. Redis fixtures use `single_connection_client=True` and
`@pytest_asyncio.fixture(loop_scope="session")` to sidestep connection-pool/event-loop
mismatches — worth copying if you add a Redis fixture of your own.

Kafka and RabbitMQ containers are *not* in the shared conftest; `bus/test_kafka.py` and
`bus/test_rabbitmq.py` each define their own session-scoped container fixture, since those
brokers are excluded from the default run anyway.

Ready-made store and repository fixtures sit on top: `postgres_event_store`,
`postgres_event_store_with_outbox`, `postgres_checkpoint_repo`, `postgres_dlq_repo`,
`postgres_outbox_repo`, and `redis_event_bus` (plus `redis_event_bus_factory` when a test
needs to control `connect()` / `shutdown()` timing itself). Each store fixture builds a
*fresh* `EventRegistry` rather than using the global one.

[testcontainers]: https://testcontainers-python.readthedocs.io/

#### Skips instead of failures

The conftest probes for both halves of the requirement — the library and the daemon:

```python
try:
    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer
    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    ...

DOCKER_AVAILABLE = is_docker_available()   # `docker info`, 5s timeout
```

`is_docker_available()` shells out to `docker info` with a five-second timeout and treats
any failure — missing binary, timeout, daemon down — as "no Docker". From those two flags
it builds `skip_if_no_testcontainers`, `skip_if_no_docker`, `skip_if_no_postgres_infra`,
and `skip_if_no_redis_infra`.

Modules apply the right one module-wide alongside their markers:

```python
pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]
```

The broker suites add the client library to the condition —
`skip_if_no_kafka_infra` also requires `KAFKA_AVAILABLE` and `testcontainers.kafka`,
`skip_if_no_rabbitmq_infra` also requires `RABBITMQ_AVAILABLE` — and both call
`pytest.skip(..., allow_module_level=True)` before importing `KafkaEventBus` /
`RabbitMQEventBus`, so collection succeeds even with the driver absent.

The net effect: on a machine with no Docker, `pytest tests/integration` is all skips and
no errors. Three groups still run there — `application/subscriptions/` (in-memory store and bus
throughout), `observability/test_tracing_integration.py` (in-memory store and bus with an
OpenTelemetry span exporter), and `test_imports.py` (a circular-import guard that just
imports the public surface). `readmodels/` parametrizes its `repo` fixture over
`["inmemory", "sqlite", "postgresql"]` and skips only the PostgreSQL parameter.

#### Markers on this tier

Every module here carries `pytest.mark.integration`, plus the backend marker it needs, and
`e2e/test_full_flow.py` adds `pytest.mark.e2e`. `application/subscriptions/test_resilience.py` is also
`pytest.mark.slow`. The `application/subscriptions/` and `readmodels/` modules carry no backend marker
because they do not require one.

Note that `tests/integration/conftest.py` re-registers `integration`, `postgres`, `redis`,
`rabbitmq`, `kafka`, and `e2e` through its own `pytest_configure` even though
`pyproject.toml` already declares them — harmless duplication that keeps the directory
runnable in isolation.

#### Running the tier

With Docker running and the extras installed:

```bash
uv run pytest tests/integration
```

That covers PostgreSQL and Redis. Kafka and RabbitMQ are excluded from the default run by
convention (their markers are documented as "excluded by default"), so select them
explicitly:

```bash
uv run pytest tests/integration -m kafka
uv run pytest tests/integration -m rabbitmq
```

To use the long-lived `docker-compose.test.yml` services instead of per-run containers,
bring them up first:

```bash
docker compose -f docker-compose.test.yml up -d
```

That gives you `postgres:15` on host port 5433 (`eventsource_test` / `test` / `test`) and
`redis:7` on 6380, overridable with `POSTGRES_PORT` and `REDIS_PORT`. Be aware that the
fixtures in the shared conftest always go through testcontainers — the compose services
are for the modules that read a URL from the environment, currently
`observability/test_distributed_tracing.py`, which honours `RABBITMQ_URL`,
`KAFKA_BOOTSTRAP_SERVERS`, and `REDIS_URL`.

#### Adding a module here

1. Put it in the subdirectory for the area, or add one with an `__init__.py`.
2. Set `pytestmark` to `[pytest.mark.integration, pytest.mark.<backend>, skip_if_no_<backend>_infra]`,
   importing the guard from `tests.integration.conftest` (or `..conftest` from a
   subdirectory).
3. Reuse the shared fixtures — `postgres_event_store`, `postgres_*_repo`,
   `redis_event_bus` — and depend on `clean_postgres_tables` or `clean_redis` for
   isolation rather than cleaning up by hand.
4. Reuse the conftest's `TestItemCreated` / `TestOrderCreated` events and
   `TestOrderAggregate` instead of declaring new event types; they are already registered
   in the fixtures' registries.
5. If your test needs a table the schema constants do not create, add the statement to the
   relevant `*_SCHEMA_STATEMENTS` list — one statement per entry.

### `tests/benchmarks/` — pytest-benchmark suite with its own `conftest.py`

Sixty-six timed tests across four modules, plus a dedicated
`tests/benchmarks/conftest.py`. Everything here runs against in-memory implementations —
no driver, no Docker, no marker — so the numbers measure library overhead rather than a
backend:

| Module | Measures |
| --- | --- |
| `test_event_store.py` | `InMemoryEventStore` append (single, batch of 10/100, sequential), reads (100 and 1000 events, `read_all`, stream iterator, `get_stream_version`), concurrency (parallel appends/reads, mixed workload), and idempotency (`event_exists`) |
| `test_projections.py` | `DeclarativeProjection` throughput for 1/100/1000 events, multi-handler dispatch, checkpoint-tracking overhead, an order lifecycle, a deliberately compute-heavy handler, and handler-routing lookup |
| `test_repositories.py` | The in-memory checkpoint, DLQ, and outbox repositories — update/get/reset/lag metrics, add and query failed events, the full outbox add → pending → publish workflow, and concurrent updates |
| `test_serialization.py` | `to_dict`/`from_dict`, pydantic `model_dump`/`model_validate`, JSON encode/decode, round trips at 1 and 100 events, and the `with_*` copy helpers |

Each module's docstring records the target baseline it was written against — for example
`test_event_store.py` aims for `< 0.1ms` on a single append and `< 10ms` to read 1000
events. Treat those as intent, not as assertions: nothing in the suite fails on a
regression, so a slowdown shows up only when you compare runs yourself (see
[Comparing runs and saving baselines](#comparing-runs-and-saving-baselines)).

#### The `conftest.py`

It exists because benchmarks need setup done *outside* the timed region, and because
pytest-benchmark's `benchmark` fixture calls a **synchronous** callable. It provides:

- **`run_async(coro)`** — a one-line `asyncio.run()` wrapper. Import it directly
  (`from tests.benchmarks.conftest import run_async`); it is a module-level function, not
  a fixture. Every timed async call in the suite goes through it.
- **Event batches** — `sample_events_10`, `sample_events_100`, `sample_events_1000`
  (built from `SampleEvent`), `counter_events_100`, and `order_events_100`, all keyed off
  a single `benchmark_aggregate_id` so repeated iterations hit the same stream.
  `event_generator` hands back the shared `create_event` factory.
- **Stores** — `benchmark_store` (a fresh `InMemoryEventStore`) and the pre-populated
  `populated_benchmark_store_100` / `populated_benchmark_store_1000`, which append their
  batch during setup and yield the loaded store.
- **Repositories** — `benchmark_checkpoint_repo`, `benchmark_dlq_repo`,
  `benchmark_outbox_repo`, and `populated_checkpoint_repo`, which pre-records 100
  checkpoints spread across ten projection names.

Note that these are separate from the identically-purposed fixtures in
`tests/conftest.py` — the benchmark versions exist so the population step is paid once
per fixture rather than inside the measured function.

#### Running them

The suite needs `pytest-benchmark`, which comes with `dev` (or the standalone `benchmark`
extra). Point pytest at the path:

```bash
uv run pytest tests/benchmarks --no-cov
```

Two things to know before you rely on a marker or a coverage-enabled run:

- **`-m benchmark` selects nothing.** The marker is registered in `pyproject.toml`, but no
  module in `tests/benchmarks/` applies it, and pytest-benchmark does not add it
  automatically — `pytest tests/benchmarks -m benchmark` deselects all 66 tests. Select by
  path, or add `--benchmark-only`, which restricts the run to tests using the `benchmark`
  fixture. If you add a module here, applying
  `pytestmark = pytest.mark.benchmark` is a good habit; it would make the marker useful.
- **`--no-cov` matters.** Coverage instrumentation is on by default through `addopts`, and
  it inflates every measurement.

To collect the suite as ordinary correctness tests — useful in CI, where timings are
noise — disable the timing loop so each function runs exactly once:

```bash
uv run pytest tests/benchmarks --no-cov --benchmark-disable
```

### `tests/fixtures/` — shared event, state, and aggregate models

Not a test directory — an importable package of domain models the rest of the suite
builds on. `events.py` defines `CounterIncremented`, `CounterDecremented`,
`CounterNamed`, `CounterReset`, `OrderCreated`, `OrderItemAdded`, `OrderShipped`,
`OrderCancelled`, `SampleEvent`, `UserRegistered`, and the `create_event` factory;
`aggregates.py` defines `CounterState`, `OrderState`, `CounterAggregate`,
`DeclarativeCounterAggregate`, and `OrderAggregate`. All of it is re-exported from
`tests.fixtures`.

Import these instead of writing new sample events. Because `DomainEvent` subclasses
auto-register into the global `EventRegistry`, a one-off event type defined inside a test
module is a global side effect; reusing the shared models avoids that.

### Where to put a new test

Work down this list and stop at the first match:

1. Does it need Docker (PostgreSQL, Redis, Kafka, RabbitMQ over the wire)? →
   `tests/integration/<area>/`.
2. Is it a performance measurement? → `tests/benchmarks/`.
3. Does it need a real SQLite database, or exercise PostgreSQL lock logic with mocks? →
   `tests/stores/`, `tests/repositories/`, or `tests/locks/`.
4. Otherwise → `tests/unit/`. Use the subdirectory that mirrors the source package if one
   exists; add a flat `test_*.py` only for cross-cutting concerns.

A new model that more than one test needs belongs in `tests/fixtures/`, and a new fixture
that more than one directory needs belongs in `tests/conftest.py`.
