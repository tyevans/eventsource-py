# 15. Optional Dependency Extras and the Core/Backend Split

Installing `eventsource-py` pulls in exactly two third-party packages: `pydantic>=2.0,<3.0` and `sqlalchemy>=2.0,<3.0`. Everything that talks to a real piece of infrastructure -- asyncpg for PostgreSQL, aiosqlite for SQLite, redis for the Redis Streams bus, aio-pika for RabbitMQ, aiokafka for Kafka, the OpenTelemetry API and SDK for tracing and metrics -- sits behind a named extra in `pyproject.toml` and is absent unless you ask for it.

This ADR records why the dependency surface is cut that way, and what the cut costs. The library's contracts are backend-neutral by design: `protocols.py`, `events/`, `serialization/`, and the `interface.py` modules under `stores/`, `bus/`, and `snapshots/` describe event sourcing without naming a driver (see [`docs/core-surface.md`](../core-surface.md) for the Tier 0 boundary this shares a motivation with). A single mandatory install list would have made every consumer of those contracts carry every driver the project has ever shipped a backend for. Making each backend opt-in keeps the base install small and lets a deployment's dependency tree reflect the infrastructure it actually runs against.

The price is paid inside the source tree rather than by the installer. Because the drivers may genuinely be missing at import time, every backend module opens with a `try: import X / except ImportError` guard that sets an `X_AVAILABLE` flag and rebinds the module symbols to `None`, and each backend then has to choose what "missing" means for it. `RedisEventBus.__init__` raises `RedisNotAvailableError` when `REDIS_AVAILABLE` is false, so the failure is loud and lands at construction. `stores/__init__.py` and the root `__init__.py` simply leave the SQLite exports out of `__all__` when aiosqlite is absent, so the symbol quietly does not exist. `observability/tracing.py` and `subscriptions/metrics.py` degrade to no-ops and keep running. Those three behaviours, the `*_AVAILABLE` flags they hang off (which are public API, re-exported from `eventsource`, `eventsource.bus`, `eventsource.subscriptions`, and `eventsource.observability`), the `# type: ignore` comments the rebinding demands, the pytest markers that mirror the extras, and one acknowledged inconsistency around asyncpg are the consequences worked through below.

## Status

Accepted, and in force as of 0.5.0. `[project.dependencies]` in `pyproject.toml` lists `pydantic>=2.0,<3.0` and `sqlalchemy>=2.0,<3.0` and nothing else; `[project.optional-dependencies]` carries `postgresql`, `sqlite`, `redis`, `telemetry`, `rabbitmq`, `kafka`, `kafka-schema-registry`, the two aggregates `all` and `all-backends`, and the non-runtime `benchmark`, `dev`, and `docs`.

Amended after 0.5.0: the `redis` extra moved from `redis>=5.0,<6.0` to `redis>=8.0,<9.0`. The decision itself stands -- Redis remains an extra, pinned with an upper bound -- but the supported range no longer includes redis-py 5.x, 6.x, or 7.x, so this is a breaking change for anyone installing `eventsource-py[redis]` against a pin below 8.0. Two consequences are worth recording. First, the old `<6.0` ceiling was doing more than bounding risk: redis-py 5.x shipped effectively untyped, so the `module = "redis.*"` / `ignore_missing_imports` override in `pyproject.toml` hid the fact that `bus/redis.py` reads `xreadgroup`, `xpending_range`, `xclaim`, and `xrange` results as `str` while the library annotates them `bytes | str | int`. That assumption is sound at runtime because the client connects with `decode_responses=True`, but it is invisible to the type checker; raising the floor surfaced 24 mypy errors, now resolved by casting at each command boundary rather than by suppressing them. Second, the range had never been verified past 5.x, because the redis-marked tests only ran in the `integration` job, which is gated to pushes on `main`. A PR-scoped `redis` job was added alongside this change so the extra's declared range stays checked against a real server.

The preceding change under this decision was demoting `redis` from a core dependency to an extra, which landed in the 0.5.0 cycle. That is a breaking change for anyone who installed plain `eventsource-py` and imported `RedisEventBus` -- those deployments now need `eventsource-py[redis]`. It is the reason `RedisEventBus` fails loudly at construction rather than quietly at import.

One part of the decision is knowingly unimplemented: asyncpg has no `*_AVAILABLE` guard, so the PostgreSQL paths import their driver unconditionally and surface a missing `postgresql` extra later, at driver-URL resolution, instead of at the guard. That inconsistency is documented under Consequences rather than treated as a bug to be silently fixed, because closing it changes the failure mode users currently see.

Amended by [ADR 0024](0024-projection-persistence-ports.md).

## Context

### The dependency surface before the split

The extras mechanism is not new; the shape of what it covered is. From the initial commit, `pyproject.toml` already carried `[project.optional-dependencies]` with `postgresql` (asyncpg) and `telemetry` (the OpenTelemetry API and SDK), and later cycles added `sqlite`, `rabbitmq`, `kafka`, and `kafka-schema-registry` as backends landed. What was inconsistent was the treatment of Redis. `redis>=5.0` sat in `[project.dependencies]` alongside pydantic and sqlalchemy *and* was declared as a `redis` extra (`redis>=5.0,<6.0`) at the same time, which made the extra all but a no-op: every install already had the driver, so `pip install eventsource[redis]` differed from a plain install only by tightening an upper bound. The `all` aggregate of that era -- `postgresql,redis,telemetry` -- likewise listed a package the base install could not avoid.

That left three tiers coexisting under one nominal policy. Pydantic and sqlalchemy were core because the contracts genuinely need them -- `DomainEvent` is a Pydantic `BaseModel`, and the PostgreSQL and SQLite stores are written against the SQLAlchemy async engine rather than against a raw driver. Redis was core by accident of history. Everything else was already opt-in. Nothing in the source distinguished the second case from the first, so the only way to tell that Redis was core-by-accident was to notice the duplicate declaration.

### Pressure: every backend was a mandatory install

The pressure the split responds to is what happens when the Redis precedent is generalized. A message bus is not a universal requirement of an event-sourced application -- a service that persists to PostgreSQL and projects in-process needs `InMemoryEventBus` (`bus/memory.py`, stdlib-only: `asyncio`, `logging`, `threading`, `collections`) and nothing more -- yet every such deployment was resolving and installing the Redis client, plus its transitive tree, because one of several interchangeable bus implementations happened to be listed as core. The library ships four bus backends behind a single `EventBus` ABC: `bus/memory.py`, `bus/redis.py`, `bus/rabbitmq.py`, `bus/kafka.py`. Had each been promoted on the reasoning that kept Redis core, the base install would have carried `redis`, `aio-pika`, and `aiokafka` to serve users who select exactly one -- and the `kafka-schema-registry` extra would have dragged `confluent-kafka` in behind them.

The same argument applies across the store and snapshot backends. `stores/` and `snapshots/` each ship in-memory, PostgreSQL, and SQLite implementations, and a deployment picks one. The asymmetry is worth naming: the PostgreSQL implementations are written against the SQLAlchemy async engine, so their driver requirement is a URL-scheme concern (`postgresql+asyncpg://...`) rather than a module import, while `stores/sqlite.py` and `snapshots/sqlite.py` `import aiosqlite` outright. Mandatory-install pressure therefore lands unevenly -- which is also why the guards described under Consequences are not uniform across backends.

The cost of a mandatory driver is not only download size: it is a version constraint the consumer must satisfy, a package that appears in their audit and vulnerability surface, and one more thing that can conflict with something they actually use. Several of these are heavy in their own right -- `aiokafka`, `aio-pika`, and the OpenTelemetry API and SDK each bring their own transitive trees, and `confluent-kafka` bundles a compiled `librdkafka`. For a library whose whole design premise is that backends are swappable behind `EventStore`, `EventBus`, and `SnapshotStore`, making any particular backend non-optional contradicts the interface it sits behind.

### Constraint: `protocols.py`, `events/`, `serialization/` are backend-neutral by design

The split is only coherent because the contract layer never names a driver, and that property is visible in the import blocks rather than merely asserted. `serialization/json.py` imports `json`, `datetime`, `typing`, and `uuid` -- standard library only, with no reach back into the rest of the package, and `serialization/__init__.py` re-exports from it and nothing else. `protocols.py` imports `abc`, `collections.abc`, `typing`, and `DomainEvent`, so its heaviest dependency is pydantic by transitivity. `events/base.py` imports pydantic directly (`BaseModel`, `ConfigDict`, `Field`, `model_validator`) and nothing else third-party; `events/registry.py` adds only `logging`, `threading`, `collections.abc`, and `typing`, taking `DomainEvent` under `TYPE_CHECKING`.

The interface modules that the backends implement are equally clean. `stores/interface.py` -- `EventStore`, `StoredEvent`, `EventStream`, `ReadOptions`, `AppendResult`, `EventPublisher` -- reaches no further than stdlib (`abc`, `dataclasses`, `datetime`, `enum`, `typing`, `uuid`) plus `DomainEvent`. `bus/interface.py` adds only `eventsource.protocols` on top of stdlib and `DomainEvent`. `snapshots/interface.py` is stricter still: `abc`, `dataclasses`, `datetime`, `typing`, `uuid`, and no internal imports at all. Nothing in that set knows that asyncpg, aiosqlite, redis, aio-pika, or aiokafka exist.

The practical consequence is that the base install is sufficient to *write against* the library even when it is insufficient to *run* any particular backend. A shared domain package can define its events, declare its aggregates, and type its handlers against `EventStore` and `EventBus` with only pydantic and sqlalchemy resolved, then let the deploying service choose which extra supplies the implementation.

This is the same boundary catalogued as Tier 0 in [`docs/core-surface.md`](../core-surface.md), approached from the packaging side. That document asks which modules could be extracted into a standalone contracts package; this ADR asks which distributions a user must install to get them. The answers coincide because they follow from the same fact -- the contracts are expressible without any infrastructure library -- and the constraint that fact imposes on this decision is a hard one. If a driver import ever appeared in `protocols.py`, `events/`, `serialization/`, or an `interface.py`, extras would stop being an honest description of the dependency surface: the core install would silently require whatever that module reached for, and the guards described under Consequences would be papering over a layering violation rather than expressing a genuine choice.

## Decision

### Core is `pydantic>=2,<3` + `sqlalchemy>=2,<3` and nothing else

`[project.dependencies]` contains exactly two entries, `pydantic>=2.0,<3.0` and `sqlalchemy>=2.0,<3.0`. Neither is there for convenience, but they earn their place for different reasons, and the difference is worth stating plainly because it is what makes the boundary defensible.

Pydantic is core because it is inseparable from the contract layer itself. `events/base.py` imports `BaseModel`, `ConfigDict`, `Field`, and `model_validator` directly, and `DomainEvent` is declared with `model_config = ConfigDict(frozen=True)`. Every event a user defines is a Pydantic model subclass, and `protocols.py` types its handler signatures against `DomainEvent`, so pydantic arrives transitively the moment anyone imports a contract. There is no version of this library in which pydantic is optional.

SQLAlchemy is core for a weaker but still binding reason: it is the *shared* persistence abstraction, not a backend. No module named under Context imports it -- `protocols.py`, `events/`, `serialization/`, and the `interface.py` files are sqlalchemy-free -- but every SQL-backed implementation that ships in this distribution is written against the async engine rather than a driver API. `stores/postgresql.py` imports `text`, `IntegrityError`, `AsyncSession`, and `async_sessionmaker`; `repositories/_connection.py` normalizes `AsyncConnection | AsyncEngine` for every repository; the SQLite store, the snapshot stores, the outbox, checkpoint, and DLQ repositories, the read-model projections, the advisory locks, and the migration repositories all go through the same layer. That is what reduces "PostgreSQL" and "SQLite" to a URL-scheme choice (`postgresql+asyncpg://…` versus `sqlite+aiosqlite://…`) instead of two disjoint codebases. Demoting sqlalchemy to an extra would buy little -- it has no compiled component and no driver of its own -- while splitting the one abstraction that the persistence backends share.

The rule the pair expresses is narrow and checkable: a package belongs in core only if it is either unavoidable in the backend-neutral modules named under Context, or the common abstraction that more than one optional backend is implemented on top of. A driver, a broker client, or a telemetry SDK satisfies neither test -- each serves exactly one backend and is reachable only through code the user opted into. Nothing beyond these two qualifies today, and admitting a third would mean the Tier 0 boundary in [`docs/core-surface.md`](../core-surface.md) had moved.

### Every backend driver is an extra

Each piece of infrastructure gets its own named extra in `[project.optional-dependencies]`, pinned with the same upper-bound discipline as core:

| Extra | Pulls in | Backs |
| --- | --- | --- |
| `postgresql` | `asyncpg>=0.27.0,<1.0` | PostgreSQL store, snapshots, repositories, advisory locks |
| `sqlite` | `aiosqlite>=0.19.0,<1.0` | SQLite store, snapshots, repositories |
| `redis` | `redis>=8.0,<9.0` | `RedisEventBus` (Redis Streams) |
| `rabbitmq` | `aio-pika>=9.0.0` | `RabbitMQEventBus` |
| `kafka` | `aiokafka>=0.9.0,<1.0.0` | `KafkaEventBus` |
| `kafka-schema-registry` | `aiokafka` + `confluent-kafka>=2.0.0,<3.0.0` | Kafka bus with Schema Registry serialization |
| `telemetry` | `opentelemetry-api>=1.0,<2.0`, `opentelemetry-sdk>=1.0,<2.0` | `observability/` tracing and subscription metrics |

The extra name tracks the infrastructure, not the driver package, so `postgresql` rather than `asyncpg` and `rabbitmq` rather than `aio-pika`. Users select by the thing they operate. `kafka-schema-registry` is a superset of `kafka` rather than a sibling, because Schema Registry serialization is an additional capability layered on the same bus, not a different bus.

### Redis demoted from core to extra

`redis>=5.0` was removed from `[project.dependencies]` and now exists only as the `redis` extra. This is the one change that makes the policy uniform: before it, the extra was decorative, since every install already had the driver and `pip install eventsource-py[redis]` differed from a plain install by nothing but a tightened upper bound.

The demotion is deliberately breaking. Code that imported `RedisEventBus` from a plain install used to work and now does not, and there is no compatibility shim -- the fix is to declare `eventsource-py[redis]`. Accepting that break was preferred to the alternative of leaving one bus backend permanently privileged over three interchangeable peers, which would have made the `EventBus` abstraction a claim the packaging contradicted.

### Two aggregate extras: `all` vs `all-backends`

Two convenience aggregates exist because two audiences want very different definitions of "everything":

- `all` resolves to `eventsource-py[postgresql,sqlite,redis,rabbitmq,kafka,telemetry]` -- every runtime backend, including all three external brokers and the OpenTelemetry API and SDK. It is the development and CI install: the environment where the conformance suites run against every implementation. Note that it deliberately omits `kafka-schema-registry`; `confluent-kafka` bundles a compiled `librdkafka`, and requiring it of everyone who typed `[all]` would put a build/wheel-availability constraint on an install whose selling point is convenience.
- `all-backends` resolves to `eventsource-py[postgresql,sqlite]` -- persistence only, no brokers, no telemetry. It serves the common production shape: durable event storage plus the in-process `InMemoryEventBus`, with no message broker in the deployment at all.

Naming them this way puts the smaller, more conservative set behind the more specific name, so `all` keeps its ordinary meaning and no one gets a broker client by asking for a database.

### Non-runtime extras kept separate: `dev`, `docs`, `benchmark`

`dev` (pytest and its asyncio/cov plugins, mypy, ruff, testcontainers, pre-commit), `docs` (mkdocs, mkdocs-material, mkdocstrings, pymdown-extensions), and `benchmark` (pytest-benchmark) are extras of the same package but are never referenced by an aggregate and never imported by anything under `src/eventsource/`. They exist so contributors can provision a working environment from `pyproject.toml`, not because any library code path can reach them.

Keeping them out of `all` is the point. An aggregate that mixed toolchain packages into a runtime install would put ruff, mypy, and a container runtime helper into production dependency trees and audit reports, which is precisely the outcome the core/backend split exists to prevent.

## Consequences

[ADR 0024](0024-projection-persistence-ports.md) split the checkpoint and DLQ repositories out of `repositories/` and into `ports/` + `adapters/sql/`; they now go through `adapters/_sql/connection.py`, while the outbox, read-model, and migration repositories keep going through `repositories/_connection.py`. The rationale above named `repositories/_connection.py` as the single shared connection-normalization layer for every SQL-backed implementation -- that is no longer true by file name, since two such layers now exist. The shared-abstraction argument the rationale actually rests on (every SQL-backed implementation is written against sqlalchemy's async engine rather than a driver API, which is what reduces "PostgreSQL" and "SQLite" to a URL-scheme choice) is unaffected by which module does the normalizing, so the conclusion -- sqlalchemy stays a core dependency -- is unchanged.
