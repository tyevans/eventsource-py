---
paths:
  - "src/eventsource/**/*.py"
---

# Architecture Rules

**The library follows Clean Architecture, hard DDD, and SOLID.**
Target layout is being migrated to in phases (see `docs/superpowers/specs/` for the
redesign specs); these rules govern all new and modified code now.

## The Dependency Rule

Source-code dependencies point only inward, toward higher-level policy. Nothing in an
inner ring may know anything about an outer ring — no names, no types, no imports.
The rings, innermost first:

1. **Entities** (`domain/`): Enterprise business
   rules — domain events, the event registry, domain value objects, domain
   exceptions. Pure: stdlib + pydantic only. No I/O. `domain/aggregate.py`
   (`AggregateRoot`, `DeclarativeAggregate`) lives here now; `aggregates/` is no
   longer a transitional location for it. `aggregate_type` is a required
   `ClassVar[str]` on `AggregateRoot` — a concrete subclass that does not set
   it raises `AggregateTypeNotSetError` at construction — and
   `DomainEvent.aggregate_type` is validated against `CATEGORY_PATTERN` at
   construction (ADR 0043). `domain/types.py` (type aliases:
   `AggregateId`, `EventId`, `TenantId`, `CorrelationId`, `CausationId` — plain
   `UUID` aliases threaded through `DomainEvent`/`DomainCommand` annotations —
   the module's entire contents; `TState` is not among them — it is a PEP 695
   inline type parameter on `AggregateRoot[TState: BaseModel]` and
   `DeciderAggregate`, declared at each class rather than as a shared `TypeVar`
   (ADR 0045); `Version`, `StreamPosition`, and `GlobalPosition` are deleted,
   positions being opaque adapter-owned tokens per `ports/positions.py`, ADR
   0043), `domain/exceptions.py` (the
   domain exception hierarchy — `EventSourceError` root, aggregate/event/
   snapshot/tenant errors; infrastructure error types live in
   `ports/exceptions.py`, ADR 0041), and `domain/command.py` (`DomainCommand`) are settled, not
   transitional; `types.py`, `exceptions.py`, and `commands/` at the top level are
   no longer transitional locations for any of it (ADR 0030). `domain/event.py`
   (`DomainEvent`), `domain/event_registry.py` (`EventRegistry`, `register_event`,
   `default_registry`, and the lookup functions), and `domain/decorators.py`
   (`@handles`, `get_handled_event_type`, `is_event_handler` — domain-ring because
   `DeclarativeAggregate` is their only consumer) are settled, not transitional;
   top-level `events/` and `handlers/` are no longer transitional locations for
   any of it (ADR 0033).
2. **Use cases** (`application/`):
   Application business rules —
   aggregate repositories, projection engines, subscription lifecycle, migration
   orchestration. Depends on entities and on the boundary ports it owns. Never on a
   concrete adapter, driver, or framework. `application/aggregates/`
   (`AggregateRepository` plus the `SnapshotPolicy`/`SnapshotScheduler`
   collaborators, and `tenant_repository.py`'s `TenantAwareRepository`,
   settled there since ADR 0038) is settled, not transitional; so is `application/projections/`
   (`Projection`/`CheckpointTrackingProjection`/`DeclarativeProjection`, the
   `ProjectionCoordinator`/`ProjectionRegistry`/`SubscriberRegistry` collaborators,
   the checkpoint and DLQ functions, and the retry policies) — `projections/` is no
   longer a transitional location for any of it. `application/subscriptions/`
   (`SubscriptionManager` and its lifecycle/registry/pause-resume/health
   collaborators, the catch-up and live runners, retry and circuit-breaking,
   flow control, filtering, and the coordination message types plus
   `WorkRedistributionCoordinator`) is settled, not transitional; top-level
   `subscriptions/` no longer exists (ADR 0032). `Subscriber`/`SyncSubscriber`/
   `BatchSubscriber` (in `ports/subscribers.py`) and `LeaderElector`/
   `LeaderElectorWithLease` (in `ports/coordination.py`) are Protocols, not
   part of this ring — see the ports entry below. `application/projections/handlers.py`
   (`HandlerRegistry`, `HandlerInfo` — the ADR-0013 collaborator extracted out of
   `DeclarativeProjection`) and `application/background_tasks.py`
   (`BackgroundTaskManager`, shared by `application/aggregates/`'s background
   snapshot scheduling and `adapters/_bus/`'s shutdown drain — owned here because
   the Dependency Rule lets an outer ring depend inward but never the reverse, so
   a utility used by both application and adapters is owned by the innermost of
   the two) are settled, not transitional; top-level `handlers/` and `_internal/`
   are gone (ADR 0033). `application/migration/` (seventeen orchestration modules
   — `bulk_copier.py`, `circuit_breaker.py`, `consistency.py`, `coordinator.py`,
   `cutover.py`, `dual_write.py`, `error_classification.py`, `error_handling.py`,
   `exceptions.py`, `metrics.py`, `position_mapper.py`, `router.py`,
   `status_streamer.py`, `subscription_migrator.py`, `sync_lag_tracker.py`,
   `write_pause.py`, plus `__init__.py`) is settled, not transitional; top-level
   `migration/` no longer exists (ADR 0034) — it was the last top-level package
   outside the ring map. The four error-handling modules
   (`error_classification.py`, `exceptions.py`, `circuit_breaker.py`,
   `error_handling.py`) form a one-way DAG — vocabulary → taxonomy → circuit
   breaker → handling — enforced by
   `tests/unit/application/migration/test_module_layering.py`, per ADR 0044.
   `domain/tenant_context.py`
   (`tenant_context`, `TenantContextToken`, `get_current_tenant`,
   `get_required_tenant`, `set_current_tenant`, `reset_tenant_context`,
   `clear_tenant_context`, `tenant_scope`, `tenant_scope_sync`) and
   `domain/tenant_events.py` (`TenantDomainEvent`) are settled, not
   transitional; top-level `multitenancy/` no longer exists (ADR 0038). The
   three tenant exceptions (`TenantContextNotSetError`,
   `TenantContextResetError`, `TenantMismatchError`) merged into
   `domain/exceptions.py` as part of the same move.
3. **Interface adapters** (`adapters/`):
   Gateways that
   implement the ports for a specific technology, converting between the use-case
   format (value objects, domain events) and the storage/wire format (rows, JSON,
   frames). Event bus backends (`InMemoryEventBus`, `RedisEventBus`, `KafkaEventBus`,
   `RabbitMQEventBus`) live under `adapters/memory/bus.py`, `adapters/redis/`,
   `adapters/kafka/`, `adapters/rabbitmq/`; shared bus collaborators
   (`BaseEventBus`, `SubscriptionRegistry`) live under adapters-internal
   `adapters/_bus/`, the same pattern as `adapters/_sql/`; top-level `bus/` no
   longer exists (ADR 0031). `adapters/_common/` is the third adapters-internal
   package, and the one to reach for when the shared code is neither
   dialect- nor transport-specific: port semantics every store adapter needs
   and none of them should re-derive (`check_expected`, `describe_expected` —
   ADR 0051). Snapshot store backends (`InMemorySnapshotStore`,
   `PostgreSQLSnapshotStore`, `SQLiteSnapshotStore`) live under `adapters/memory/`,
   `adapters/postgresql/`, `adapters/sqlite/`; `snapshots/` is no longer a
   transitional adapter location. Checkpoint and DLQ adapters (dialect-parameterized
   for PostgreSQL and SQLite, plus `DatabaseProjection`) live under `adapters/sql/`;
   the in-memory checkpoint and DLQ adapters live under `adapters/memory/`.
   Outbox adapters are per-technology rather than dialect-parameterized — one
   module each under `adapters/memory/`, `adapters/postgresql/`, and
   `adapters/sqlite/`, since the SQLite implementation takes a raw
   `aiosqlite.Connection` rather than a sqlalchemy engine or session. The
   `repositories/` package no longer exists — it is not a transitional
   location for anything. `adapters/sync/` (`SyncEventStoreAdapter`) and
   `adapters/serialization/` (`EventSourceJSONEncoder`, `json_dumps`,
   `json_loads`) are settled, not transitional; top-level `sync/` and
   `serialization/` no longer exist (ADR 0030). Distributed lock adapters
   (`PostgreSQLLockManager`, `InMemoryLockManager`) live under
   `adapters/postgresql/` and `adapters/memory/`; read-model adapters live
   under `adapters/{memory,postgresql,sqlite}/`; top-level `locks/` and
   `readmodels/` no longer exist — there is no shim at either path (ADR 0030).
   `adapters/memory/coordination.py` (`InMemoryLeaderElector`, `SharedLeaderState`)
   is settled, not transitional — the only concrete implementation of the
   `ports/coordination.py` Protocol pair (ADR 0032). `adapters/_bus/handler_adapter.py`
   (`HandlerAdapter`, `get_handler_name`) is settled, not transitional — every
   importer is a bus adapter, so it joins `adapters/_bus/base.py` and
   `adapters/_bus/registry.py` as adapters-internal shared code; top-level
   `handlers/` is no longer a transitional location for it (ADR 0033).
   `adapters/sql/migration/` (`PostgreSQLMigrationRepository`,
   `PostgreSQLTenantRoutingRepository`, `PostgreSQLPositionMappingRepository`,
   `PostgreSQLMigrationAuditLogRepository`, `VALID_TRANSITIONS`) is settled, not
   transitional — the sqlalchemy-backed implementations of the four Protocols
   in `ports/migration/repositories.py`; top-level `migration/repositories/` no
   longer exists (ADR 0034). `adapters/sql/schemas/` (the SQL schema DDL, Alembic
   templates, and append-only update scripts consumed by the PostgreSQL and
   SQLite store adapters' `get_schema()` calls) is settled, not transitional —
   the storage format is an adapters-ring concern by definition; top-level
   `migrations/` (plural — distinct from the singular `migration/` above) no
   longer exists (ADR 0039).
4. **Frameworks & drivers**: sqlalchemy, asyncpg, aiosqlite, redis, aiokafka,
   aio-pika. Imported only inside the adapter that needs them, always guarded
   (see below). Driver types never appear in port signatures.

The **public API** (`__init__.py`) is the delivery mechanism's front door: it
re-exports from all rings and is the only module users import from.

## Ports (Boundary Interfaces)

Ports are owned by the inner rings and implemented by adapters — dependencies are
inverted at every boundary crossing (the D in SOLID).

- Ports live in `ports/`, depend only on
  entities, and contain no implementation code, ever. `ports/handlers.py`
  (`EventHandler`, `SyncEventHandler`, `FlexibleEventHandler`, `EventSubscriber`,
  `AsyncEventHandler`, `FlexibleEventSubscriber`) is settled, not transitional;
  top-level `protocols.py` is no longer a transitional location for it (ADR 0030).
  `ports/bus.py` (`EventBus`, `EventPublisher`, `EventHandlerFunc`,
  `SubscribableEventBus`) is settled, not transitional; top-level
  `bus/interface.py` is no longer a transitional location for it (ADR 0031).
  `SubscribableEventBus` — a two-method Protocol (`subscribe`/`unsubscribe`)
  `EventBus` satisfies structurally — is what `application/subscriptions/`
  type-hints for its bus dependency (narrowest-port rule). `ports/subscribers.py`
  (`Subscriber`, `SyncSubscriber`, `BatchSubscriber`, `supports_batch_handling()`,
  `get_subscribed_event_types()`) and `ports/coordination.py` (`LeaderElector`,
  `LeaderElectorWithLease`, `LeaderChangeCallback`) are settled, not transitional;
  top-level `subscriptions/` is no longer a transitional location for any of
  these (ADR 0032). `ports/migration/` (`models.py` — `Migration`,
  `MigrationConfig`, `MigrationPhase`, `MigrationStatus`, `MigrationResult`,
  `TenantRouting`, `TenantMigrationState`, `PositionMapping`, `SyncLag`,
  `CutoverResult`, `MigrationAuditEntry`, `AuditEventType`; `repositories.py` —
  `MigrationRepository`, `TenantRoutingRepository`, `PositionMappingRepository`,
  `MigrationAuditLogRepository` Protocols) is settled, not transitional — a
  subpackage rather than a flat module, following the `ports/readmodels/`
  precedent; top-level `migration/` and `migration/models.py` are no longer
  transitional locations for any of it (ADR 0034). `ports/snapshots.py`
  (`Snapshot`, `SnapshotStore` — a `Protocol`, not an `ABC` — and
  `SnapshotTypeInvalidation`, the optional bulk-invalidation capability port)
  is settled (ADR 0036). `ports/lifecycle.py` (`SupportsClose`, the optional
  resource-release capability port) is settled (ADR 0037). `ports/exceptions.py`
  (the thirteen infrastructure-meaning exceptions — `CheckpointError`,
  `CheckpointNotFoundError`, `EventBusConnectionError`,
  `EventStoreConnectionError`, `LockAcquisitionError`, `LockNotHeldError`,
  `PositionDecodeError`, `PositionForeignError`, `SubscriptionError` and its
  seven subclasses — rooted in `EventSourceError`) is settled, not transitional;
  `domain/exceptions.py` is no longer their location (ADR 0041).
- Our store/repository/bus ports are **output ports** (gateways) in Clean
  Architecture terms: the use-case ring calls them; adapters implement them.
- Ports are small, composed `Protocol` classes — one capability per port
  (e.g., appending, stream reading, global feed reading, type querying). Never grow a
  god-interface; add a new port instead (the I in SOLID).
- Consumers type-hint the narrowest port union they need, never a concrete adapter
  and never a wider port than they use.
- Optional capabilities are expressed by a backend *not implementing* a port — never
  by raising `NotImplementedError` from a method it claims to support.
- Mark ports `@runtime_checkable` only when a consumer genuinely needs isinstance
  checks.

## Value Objects (Boundary Crossing)

Data crosses ring boundaries as immutable value objects (frozen pydantic models or
frozen dataclasses), never as primitives, dicts, or driver types: positions, stream
identities, expected versions, event envelopes.

- Positions in the global feed are **opaque ordered tokens** defined by the adapter —
  consumers may compare and persist them, never do arithmetic on them. Per-stream
  versions are integers (required for optimistic concurrency).
- Value objects live with the ring that owns the concept: domain concepts in
  entities, port payloads in `ports/`.

## Adapters

- One directory (or module) per technology; implements whichever ports it honestly
  can.
- Never import from another adapter or another backend.
- Backend-specific dependencies imported inside the adapter only, guarded:

```python
try:
    from some_optional import lib
    BACKEND_AVAILABLE = True
except ImportError:
    BACKEND_AVAILABLE = False
```

Export an `*_AVAILABLE` flag so users can check at runtime.

- Every adapter must pass the conformance suite for each port it implements
  (`testing/conformance.py`).

## Event Model Rules

- All events subclass `DomainEvent` (pydantic BaseModel), frozen.
- `__init_subclass__` auto-derives `event_type` from the class name — do not declare it by
  hand. The one exception is a wire name pinned by already-stored events or an external
  contract, where it must differ from the class name; set
  `suppress_event_type_warning = True` there to silence the mismatch check. A declaration
  whose value *equals* the class name is always noise. Registry membership is explicit:
  decorate with `@register_event` (deserialization needs it); there is no auto-registration.
- Event types are immutable after creation — never modify event schema, add new event
  types instead.

## Out-of-Ring Packages

Two top-level packages are settled *outside* the four rings, not transitional
and not future ring candidates (ADR 0040): `observability/` (`attributes.py`,
`tracer.py`, `tracing.py`) is the cross-cutting, guarded-optional telemetry
toolkit consumed by `application/` and `adapters/` wherever a span or metric
is recorded — `domain/` and `ports/` must never import it. `testing/`
(`assertions.py`, `bdd.py`, `builder.py`, `conformance.py`,
`conformance_ports/`, `harness.py`, `partitioned_memory.py`, `recording.py`,
`sync_facade.py`) is the public test toolkit; it imports adapters by design,
so no ring — `domain`, `ports`, `application`, or `adapters` — may import it
back. Both boundaries are `import-linter` forbidden contracts. With these two
exceptions recorded, every top-level package under `src/eventsource/` is
either one of the four rings or one of these two settled packages — the
ring-migration campaign's completion criterion.

Logger names, meter names, and OTel attribute-string constants
(`"eventsource.bus.*"`, `"eventsource.migration.*"`) are a stable public
telemetry schema, deliberately decoupled from Python import paths — never
rename one of these strings just because the module emitting it moves.

## Enforcement

- Ring boundaries are enforced by import-linter contracts (`pyproject.toml`); when you
  add or move a module, update the contracts to match the ring map above — never relax
  them to make an import work.
- If a change requires an outward dependency (entities → ports, ports → adapter,
  use case → driver), the design is wrong: introduce or extend a port instead.
