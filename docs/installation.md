# Installation

Reference for installing `eventsource-py` and selecting the right dependency set for your stack.

The distribution is published to PyPI as **`eventsource-py`** and imported as **`eventsource`**:

```bash
pip install eventsource-py
```

```python
import eventsource
```

The core package is deliberately thin. It depends only on `pydantic` and `sqlalchemy`; every storage backend, message bus, and observability integration is an opt-in extra. Installing the core package alone gives you the domain model (`DomainEvent`, `AggregateRoot`, projections, subscriptions) plus the in-memory implementations of `EventStore`, `EventBus`, and the checkpoint/DLQ/outbox repositories -- enough to build and test an entire event-sourced application without any external service running.

The package ships a `py.typed` marker, so type checkers resolve its annotations directly from the installed distribution with no separate stub package.

This page covers:

- The Python and platform requirements.
- Installing the core package and what it pulls in.
- Every optional extra declared in `pyproject.toml`, and which backend each one enables.
- Setting up a development checkout with `uv`, pre-commit, tests, linting, type checking, and security scanning.
- Verifying an installation and resolving common failures.

If you want a guided first application rather than an installation reference, start with the getting-started guide instead.

## Requirements

`eventsource-py` requires **Python 3.13 or newer** (`requires-python = ">=3.13"` in `pyproject.toml`). CI runs the full test suite on 3.13, and the lint, type-check, docs, and release jobs run on 3.13 as well. Both `mypy` (`python_version = "3.13"`, `strict = true`) and `ruff` (`target-version = "py313"`) are configured against the 3.13 baseline, so 3.13 is the language level the source is written and checked at.

There is no minimum version below 3.13: the codebase uses modern typing syntax (including PEP 696 TypeVar defaults) and `asyncio` behavior that earlier interpreters do not provide, and `pip` will refuse to install the distribution on Python 3.12 or older.

### Platform

The library is pure Python with no compiled extensions of its own, so it installs on Linux, macOS, and Windows alike. The practical platform constraints come from the optional dependencies rather than from `eventsource-py`: `asyncpg`, `confluent-kafka`, and `pydantic`'s Rust core all ship binary wheels for the common platform/interpreter combinations, and a platform without a prebuilt wheel will require the corresponding build toolchain.

### Runtime services

Nothing external is required to import or use the package. The in-memory `EventStore`, `EventBus`, and checkpoint/DLQ/outbox repositories run entirely in-process, and the unit test suite (`uv run pytest tests/unit/`) needs no services.

External services only become a requirement when you install and use the matching extra:

| Backend | Extra | Service requirement |
| --- | --- | --- |
| PostgreSQL event store, snapshots, outbox, advisory locks | `postgresql` | PostgreSQL 12 or newer. The bundled schema (`src/eventsource/adapters/sql/schemas/schemas/all.sql`) documents 12+ as the target, uses `JSONB` columns, and creates the `uuid-ossp` extension, which requires privileges to run `CREATE EXTENSION`. |
| SQLite event store and snapshots | `sqlite` | No server. A file (or `:memory:`) accessed through `aiosqlite`; JSON payloads are stored as `TEXT` since SQLite has no native `JSONB` type. |
| Redis event bus | `redis` | A reachable Redis server. The integration environment uses Redis 7. |
| RabbitMQ event bus | `rabbitmq` | A RabbitMQ broker reachable over AMQP (`aio-pika`). |
| Kafka event bus | `kafka` / `kafka-schema-registry` | A Kafka cluster; the schema-registry variant additionally expects a Confluent Schema Registry. |
| Tracing | `telemetry` | No service required to import; an OTLP collector only if you export spans. |

For local development, `docker-compose.test.yml` provisions PostgreSQL 15 and Redis 7 with the ports the integration tests expect (`POSTGRES_PORT`, default `5433`; `REDIS_PORT`, default `6380`). Running the integration suite therefore requires **Docker** (or a compatible container runtime), and the `dev` extra pulls in `testcontainers` for tests that spin up their own containers.

### Tooling

The core install works with any PEP 517-capable installer -- `pip`, `uv`, `poetry`, or `pdm`. The repository itself is managed with [`uv`](https://docs.astral.sh/uv/) and is built with `hatchling`; the development workflow documented below (`uv sync --all-extras`, `uv run ...`) assumes `uv` is on your PATH. `pre-commit` is included in the `dev` extra rather than expected as a global install.

## Install the Core Package

Install the distribution by its PyPI name, `eventsource-py`:

```bash
pip install eventsource-py
```

Equivalent commands for the other common installers:

```bash
uv pip install eventsource-py       # uv, into the active environment
uv add eventsource-py               # uv, recorded in pyproject.toml
poetry add eventsource-py
pdm add eventsource-py
```

The import name differs from the distribution name. Install `eventsource-py`, then import `eventsource`:

```python
from eventsource import AggregateRoot, DomainEvent, InMemoryEventStore
```

### What the core install pulls in

The `dependencies` list in `pyproject.toml` contains exactly two entries:

| Dependency | Constraint | Why it is required |
| --- | --- | --- |
| `pydantic` | `>=2.0,<3.0` | `DomainEvent` is a Pydantic v2 `BaseModel`; event payloads are validated and serialized through it. Pydantic v1 is not supported. |
| `sqlalchemy` | `>=2.0,<3.0` | The SQL-backed stores and the checkpoint/DLQ/outbox repositories are written against the SQLAlchemy 2.0 async API. |

Note that SQLAlchemy is a *core* dependency even though no database is required at runtime -- it supplies the shared SQL layer that the `postgresql` and `sqlite` extras attach a driver to. Installing the core package does **not** install a driver, so no database connection is possible until you add one of those extras.

No backend client library (`asyncpg`, `aiosqlite`, `redis`, `aio-pika`, `aiokafka`, `confluent-kafka`) and no OpenTelemetry package is installed by default. Every module that needs one guards the import and exposes an availability flag, so `import eventsource` succeeds regardless:

```python
from eventsource import REDIS_AVAILABLE, KAFKA_AVAILABLE, RABBITMQ_AVAILABLE

REDIS_AVAILABLE  # False on a core-only install
```

Constructing a backend whose dependency is missing raises the corresponding `*NotAvailableError` (for example `RedisNotAvailableError`) rather than failing at import time.

### What you can build with core only

A core-only install is fully functional for development and testing. The in-process implementations exported from the top-level package cover every interface the library defines:

- `InMemoryEventStore` -- append and read streams, with optimistic locking.
- `InMemoryEventBus` -- publish and subscribe.
- `InMemorySnapshotStore` -- snapshot save/load.
- `InMemoryCheckpointRepository`, `InMemoryDLQRepository`, `InMemoryOutboxRepository` -- subscription checkpoints, dead-lettering, and the transactional outbox.

Together with `AggregateRoot`, `DeclarativeAggregate`, `AggregateRepository`, the projection and subscription machinery, and the helpers in `eventsource.testing`, this is enough to write and exercise a complete event-sourced application before choosing any infrastructure. Swapping to PostgreSQL or Redis later is a change of construction site, not of domain code -- the interfaces are identical.

### Version and typing

The installed version is available at runtime, resolved from package metadata:

```python
import eventsource

eventsource.__version__  # e.g. "0.5.0"
```

When running from a source checkout that has not been installed, this falls back to `"0.0.0.dev0"` rather than raising.

The wheel includes a `py.typed` marker (`src/eventsource/py.typed`), so `mypy`, `pyright`, and other checkers read the inline annotations from the installed package. There is no `types-eventsource` stub package to install.

### Pinning

The project follows semantic versioning and is currently at `0.5.0`, classified as `Development Status :: 4 - Beta`. Because the public API can still change in minor releases before 1.0, pin at least the minor version in applications:

```
eventsource-py>=0.5,<0.6
```

Libraries that depend on `eventsource-py` should prefer a compatible-release constraint (`~=0.5`) and let the application pick the exact version.

## Core Dependencies

Two runtime dependencies, both declared in the `dependencies` array of `pyproject.toml`:

```toml
dependencies = [
    "pydantic>=2.8.0,<3.0",
    "sqlalchemy[asyncio]>=2.0.43,<3.0",
]
```

Their transitive closure adds only the packages those two require themselves (`pydantic-core`, `annotated-types`, `typing-extensions` for Pydantic; `greenlet` and `typing-extensions` for SQLAlchemy on platforms where SQLAlchemy requests it). Nothing else is installed.

### pydantic (`>=2.0,<3.0`)

Pydantic is the event model. `DomainEvent` in `src/eventsource/events/base.py` subclasses `pydantic.BaseModel` and declares its metadata fields with `Field(...)`:

- `event_id: UUID` (default `uuid4()`), `event_type: str` (derived from the class name when left blank), `event_version: int` (`ge=1`), `occurred_at: datetime` (default `datetime.now(UTC)`).
- `aggregate_id: UUID` and `aggregate_type: str` (both required), `aggregate_version: int` (`ge=1`).
- `tenant_id: UUID | None`, defaulting to `None`.

Three Pydantic behaviors the library depends on directly:

| Behavior | Where it is used |
| --- | --- |
| `model_config = ConfigDict(frozen=True)` | Makes every event immutable after construction -- the invariant the whole store relies on. |
| `model_dump(mode="json")` / `model_validate(...)` | `DomainEvent.to_dict()` and `DomainEvent.from_dict()`, the boundary between event objects and stored JSON payloads. |
| `__init_subclass__` + `model_validator` | Auto-registration into the global `EventRegistry` and validation of the `event_type` / class-name relationship. |

The upper bound `<3.0` is a deliberate major-version guard, and the lower bound is a hard floor: **Pydantic v1 is not supported**. `ConfigDict`, `model_validator`, `model_dump`, and `model_validate` are all v2-only APIs, so a v1 environment fails at import. If your application is still on Pydantic v1, migrate it before adopting `eventsource-py`; there is no compatibility shim.

### sqlalchemy (`>=2.0,<3.0`)

SQLAlchemy is the shared SQL layer, not a database driver. The library uses its 2.0 **async** API -- `AsyncEngine`, `AsyncConnection`, `AsyncSession`, `async_sessionmaker`, and `text()` -- as the common abstraction that every SQL-backed component is written against:

- `adapters/postgresql/store.py` and `adapters/postgresql/snapshots.py` -- the PostgreSQL event and snapshot stores.
- `adapters/sql/checkpoints.py`, `adapters/sql/dlq.py`, `adapters/postgresql/outbox.py`, and the shared `adapters/_sql/connection.py` helper. (`adapters/sqlite/outbox.py` is not on this list -- it is written against `aiosqlite`, not sqlalchemy.)
- `adapters/sql/projection.py` and `readmodels/` -- database-backed projections and read models, which accept an engine or sessionmaker. `application/projections/base.py`, by contrast, is sqlalchemy-free: it defines the checkpoint/DLQ/retry orchestration against pure ports and only the `adapters/sql/` implementations pull in the driver.
- `locks/postgresql.py` -- advisory locks.
- `migration/repositories/` -- the live-migration bookkeeping tables.

Because the async API is used throughout, SQLAlchemy 1.4 and the 2.0 legacy synchronous patterns are out of scope; the `<3.0` bound again guards against a future major release.

Two consequences are worth stating plainly:

1. **SQLAlchemy is installed even if you never touch a database.** It is a core dependency because it is imported by modules reachable from the top-level package (the SQL-backed checkpoint/DLQ/outbox adapters, the PostgreSQL/SQLite projection and read model bases). An in-memory-only application still carries it.
2. **SQLAlchemy alone cannot connect to anything.** A DBAPI driver is supplied by the extras: `asyncpg` via `postgresql`, `aiosqlite` via `sqlite`. Without one, creating an engine against `postgresql+asyncpg://...` raises a driver-not-found error from SQLAlchemy, not from `eventsource-py`.

### What is deliberately not a core dependency

Redis was a core dependency in earlier versions and was moved to the `redis` extra; the same treatment applies to every other backend client. `asyncpg`, `aiosqlite`, `redis`, `aio-pika`, `aiokafka`, `confluent-kafka`, and the OpenTelemetry packages are all optional, and each integration guards its import:

```python
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
```

`REDIS_AVAILABLE`, `RABBITMQ_AVAILABLE`, `KAFKA_AVAILABLE`, and (when the module loads) `SQLITE_AVAILABLE` are exported from the top-level package so you can branch on them, and the corresponding `*NotAvailableError` is raised only when you try to construct the backend. This is what keeps `import eventsource` working -- and the unit test suite runnable -- on a two-dependency install.

## Optional Dependencies (Extras)

Every backend integration is an extra declared under `[project.optional-dependencies]` in `pyproject.toml`. Extras are installed with the bracket syntax:

```bash
pip install "eventsource-py[postgresql,redis]"
uv add "eventsource-py[postgresql,redis]"
```

Quote the argument -- most shells treat `[` and `]` as glob characters.

The full set of declared extras:

| Extra | Packages pulled in | Enables |
| --- | --- | --- |
| `postgresql` | `asyncpg>=0.30.0,<1.0` | PostgreSQL event store, snapshot store, checkpoint/DLQ/outbox repositories, advisory locks |
| `sqlite` | `aiosqlite>=0.19.0,<1.0` | SQLite event store, snapshot store, and repositories |
| `redis` | `redis>=8.0,<9.0` | `RedisEventBus` |
| `rabbitmq` | `aio-pika>=9.0.5` | `RabbitMQEventBus` |
| `kafka` | `aiokafka>=0.12.0,<1.0.0` | `KafkaEventBus` |
| `kafka-schema-registry` | `aiokafka` plus `confluent-kafka>=2.6.0,<3.0.0` | Kafka bus plus the Confluent client for a custom schema-registry serializer |
| `telemetry` | `opentelemetry-api>=1.16.0,<2.0`, `opentelemetry-sdk>=1.16.0,<2.0` | Tracing and metrics instrumentation |
| `all` | `postgresql,sqlite,redis,rabbitmq,kafka,telemetry` | Every runtime backend |
| `all-backends` | `postgresql,sqlite` | Both storage backends only |
| `dev` | pytest stack, mypy, ruff, testcontainers, pre-commit | Contributor toolchain |
| `docs` | mkdocs, mkdocs-material, mkdocstrings, pymdown-extensions | Building the documentation site |
| `benchmark` | `pytest-benchmark>=4.0.0` | Running the benchmark suite |

Extras are additive and independent: installing `postgresql` does not imply `sqlite`, and none of them change the behavior of code you have already written. They only make previously-unavailable constructors work.

### Storage Backends: `[postgresql]`, `[sqlite]`

Both storage extras add a **driver** to the SQLAlchemy core dependency; neither adds SQL code of its own.

`postgresql` installs `asyncpg`. Note that no module in `src/eventsource/` imports `asyncpg` directly -- `stores/postgresql.py`, `snapshots/postgresql.py`, `locks/postgresql.py`, and the SQL repositories are written entirely against SQLAlchemy's async API. `asyncpg` is loaded by SQLAlchemy when you create the engine:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from eventsource import PostgreSQLEventStore

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/app")
store = PostgreSQLEventStore(engine)
```

Because the driver is resolved by the dialect name in the URL, a missing `asyncpg` surfaces as a SQLAlchemy `ModuleNotFoundError` at `create_async_engine` time, not as an `eventsource` error. Everything importable from `eventsource` for PostgreSQL is available on a core install; only the connection fails.

`sqlite` installs `aiosqlite`, and here the import guard is visible in the package surface. `src/eventsource/adapters/sqlite/store.py` imports `aiosqlite` at module top level, so `eventsource.adapters.sqlite` (and the top-level `__init__.py`, which re-exports from it) wraps the SQLite exports:

```python
try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False
    aiosqlite = None
```

`SQLCheckpointRepository` and `SQLDLQRepository` (`eventsource.adapters.sql`) are not behind this guard: they are dialect-parameterized over the same SQLAlchemy async API PostgreSQL and SQLite both already require through the core `sqlalchemy` dependency, so they're unconditional exports regardless of which optional driver extra is installed.

The practical consequence: without the `sqlite` extra, `from eventsource import SQLiteEventStore` raises `ImportError` -- the name is simply not bound. Check `SQLITE_AVAILABLE` (or `AIOSQLITE_AVAILABLE`) if you need to branch. `SQLiteSnapshotStore` is the exception; it guards `aiosqlite` internally and raises `SQLiteNotAvailableError` (a subclass of `ImportError`) from its constructor instead.

The two backends are not interchangeable in storage representation. PostgreSQL uses `JSONB` columns and the `uuid-ossp` extension; SQLite has no `JSONB` type, so payloads are stored as `TEXT`. Choose PostgreSQL for production, SQLite for embedded deployments and fast local integration tests without Docker.

Installing both is normal and supported -- the `all-backends` extra exists for exactly that:

```bash
pip install "eventsource-py[all-backends]"   # postgresql + sqlite
```

### Message Bus Backends: `[redis]`, `[rabbitmq]`, `[kafka]`, `[kafka-schema-registry]`

All four bus extras follow the same pattern: the module imports its client inside a `try`/`except ImportError`, sets an availability flag, and the bus constructor raises a dedicated `ImportError` subclass when the client is missing. Unlike the SQLite store, the **class is always importable** -- only construction fails.

| Extra | Module | Flag | Constructor error |
| --- | --- | --- | --- |
| `redis` | `bus/redis.py` | `REDIS_AVAILABLE` | `RedisNotAvailableError` |
| `rabbitmq` | `bus/rabbitmq.py` | `RABBITMQ_AVAILABLE` | `RabbitMQNotAvailableError` |
| `kafka` | `bus/kafka.py` | `KAFKA_AVAILABLE` | `KafkaNotAvailableError` |

So this is always safe:

```python
from eventsource import KafkaEventBus, KAFKA_AVAILABLE

if KAFKA_AVAILABLE:
    bus = KafkaEventBus(bootstrap_servers="localhost:9092")
```

and this is what you get otherwise:

```python
>>> KafkaEventBus(bootstrap_servers="localhost:9092")
KafkaNotAvailableError: ...
```

**`redis`** pins `redis>=8.0,<9.0` and uses `redis.asyncio`. The floor moved up from 5.x when redis-py's typing improved enough for the bus to type-check against it (ADR 0015); 4.x was never supported. Only `bus/redis.py` imports it -- there is no Redis-backed store, snapshot, or repository in the library.

**`rabbitmq`** installs `aio-pika>=9.0.5` (no upper bound). The floor is 9.0.5 rather than 9.0.0 because releases below it import `pkg_resources`, which no longer exists on Python 3.13. `RabbitMQEventBus` connects over AMQP through `aio-pika` and adds a second, independent guard for OpenTelemetry context propagation: `PROPAGATION_AVAILABLE` is true only when both `aio-pika` and the OTel propagation API are importable, so trace headers are injected into published messages only if you also installed `telemetry`.

**`kafka`** installs `aiokafka>=0.12.0,<1.0.0`, from which `bus/kafka.py` imports `AIOKafkaProducer`, `AIOKafkaConsumer`, `TopicPartition`, the rebalance-listener ABC, and the Kafka error types. When the import fails these names are bound to `None` (and the listener base to `object`) so the module body still evaluates. Like RabbitMQ, the Kafka bus has a separate `PROPAGATION_AVAILABLE` guard for OTel propagation and metrics.

**`kafka-schema-registry`** adds `confluent-kafka>=2.6.0,<3.0.0` on top of the same `aiokafka` pin. This extra is a convenience for *your* code, not a switch inside the library: no module under `src/eventsource/` imports `confluent_kafka`. The library's serialization boundary is the `EventSerializer` base class in `bus/kafka.py`, whose default implementation is JSON via Pydantic; its docstring points at subclassing it for Avro or Protobuf against a registry client. Install this extra when you intend to write such a serializer and pass it to the bus -- installing it alone changes nothing about the default behavior. It is also the only extra that pulls a package with meaningful native-build implications, since `confluent-kafka` wraps `librdkafka`.

Note the test markers: `kafka` and `rabbitmq` tests are excluded from the default `pytest` run and must be selected explicitly with `-m kafka` or `-m rabbitmq`, whereas `redis` tests run as part of the normal integration suite.

### Observability: `[telemetry]`

```bash
pip install "eventsource-py[telemetry]"
```

This extra installs `opentelemetry-api` and `opentelemetry-sdk`, both `>=1.16.0,<2.0`. Instrumentation is spread across the codebase but funnels through a single guard in `src/eventsource/observability/tracing.py`:

```python
try:
    from opentelemetry import trace

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    trace = None
```

`get_tracer(name)` returns `None` when OTel is absent, `should_trace(enable_tracing)` is `enable_tracing and OTEL_AVAILABLE`, and `create_tracer(...)` / the `@traced(...)` decorator degrade to no-ops. Instrumented code paths -- event stores, buses, subscriptions, the migration tooling -- therefore run identically with or without the extra. The event store adapters (`InMemoryEventStore`, `SQLiteEventStore`, `PostgreSQLEventStore`) no longer take a `tracer`/`enable_tracing` constructor argument; check the current signature in `src/eventsource/adapters/*/store.py` before assuming tracing is component-configurable at construction time.

Metrics use a parallel flag, `OTEL_METRICS_AVAILABLE`, guarding `from opentelemetry import metrics` independently in `application/subscriptions/metrics.py`, `application/subscriptions/shutdown.py`, and `migration/metrics.py`; the Kafka and RabbitMQ buses add `PROPAGATION_AVAILABLE` for injecting and extracting trace context in message headers.

Two things this extra does *not* do. It does not configure a provider: without an SDK `TracerProvider` and `MeterProvider` set up in your application, the API records nothing. And it does not install an exporter -- `opentelemetry-exporter-otlp` (or whichever backend you use) is a separate dependency of your application, not of `eventsource-py`. `mypy` is configured with `ignore_missing_imports = true` for `opentelemetry.*`, so type checking a project without the extra installed still passes.

### Bundle Extras: `[all]`, `[all-backends]`

Two aggregate extras exist, both defined by self-reference:

```toml
all = ["eventsource-py[postgresql,sqlite,redis,rabbitmq,kafka,telemetry]"]
all-backends = ["eventsource-py[postgresql,sqlite]"]
```

`all` installs every runtime backend plus telemetry: `asyncpg`, `aiosqlite`, `redis`, `aio-pika`, `aiokafka`, and the two OpenTelemetry packages. It deliberately **excludes** `kafka-schema-registry`, so `confluent-kafka` is not installed by `all` -- add it explicitly if you need it. It also excludes the tooling extras (`dev`, `docs`, `benchmark`).

`all-backends` is narrower than the name suggests: it means both *storage* backends, PostgreSQL and SQLite, and no message bus. Reach for it when you want to run the storage test suites or keep the option of switching stores open, without pulling in broker clients.

Use `all` for exploration, CI matrices, and local development where you want everything importable. For production deployments, prefer naming the extras you actually use -- a service that runs on PostgreSQL and Redis has no reason to ship `aiokafka` and `aio-pika` in its image.

### Tooling Extras: `[dev]`, `[docs]`, `[benchmark]`

These three are for working *on* the library rather than with it, and are not needed by applications that depend on it.

**`dev`** is the contributor toolchain: `pytest>=8.0`, `pytest-asyncio>=0.23`, `pytest-cov>=4.0`, `pytest-benchmark>=4.0.0`, `mypy>=1.19.0`, `ruff>=0.14.8`, `testcontainers>=4.0`, and `pre-commit>=4.0`. Three notes. `pre-commit` is included here rather than assumed as a global install, so `uv run pre-commit install` works straight after a sync. `bandit` is *not* in this extra -- it lives in the `[dependency-groups] dev` group, and the pre-commit hook installs `bandit[toml]` itself; `uv sync` picks up dependency groups automatically, `pip install -e ".[dev]"` does not. And because this toolchain is an **extra**, it is opt-in: sync with `--all-extras` (what `make install` and CI both do), or `uv run` will not find `ruff`/`pytest` in the environment and will silently fall through to whatever happens to be on your `PATH`.

**`docs`** installs the documentation stack: `mkdocs>=1.5`, `mkdocs-material>=9.5`, `mkdocstrings[python]>=0.24`, and `pymdown-extensions>=10.0`. Because `mkdocstrings` renders API pages from live docstrings, building the full site requires the package itself to be importable -- sync it alongside the library rather than into a bare environment.

**`benchmark`** adds only `pytest-benchmark>=4.0.0`, for tests marked `@pytest.mark.benchmark`. It is a subset of `dev`, which already includes the same package; install it standalone only when you want to run benchmarks without the rest of the toolchain.

The repository workflow installs everything at once:

```bash
uv sync --all-extras
```
