# User Personas

Archetypes representing the core users, operators, and automated workers interacting with `eventsource-py`.

---

## 1. Alex — The Event-Sourced Domain Architect

- **Archetype & Title**: Principal Enterprise Architect / Domain-Driven Design Specialist
- **Background & Technical Stack**: Python 3.13+, Domain-Driven Design (DDD), Clean & Hexagonal Architecture, CQRS, Event Sourcing, Pydantic v2.
- **Core Responsibilities & Jobs-to-be-Done (JTBD)**:
  - Model complex, mission-critical business domains as pure state machines without leaking I/O, database concerns, or transport details into domain logic.
  - Implement command handling and state evolution using functional deciders (`DeciderAggregate[TState, TCommand]`), declarative aggregates (`DeclarativeAggregate` with `@handles`), and classic `AggregateRoot[TState]`.
  - Guarantee deterministic state folding from zero: nullary `initial_state()` receiving commands that carry identity ([ADR-0103](../adrs/accepted/adr-0103-pure-functional-decider-pattern-and-immutable-state-folding.md)).
  - Enforce optimistic concurrency control on stream appends using version checks (`expected_version`, `ExpectedVersionError`, `OptimisticLockError`).
  - Build projections and read models using `DeclarativeProjection` and `StoreProjection[TStore]` with deterministic replay capability ([ADR-0109](../adrs/accepted/adr-0109-projection-engine-deterministic-replay-and-additive-read-model-reconciliation.md)).
- **Major Pain Points & Hazards**:
  - State corruption from mutable event payloads or side-effects during aggregate rehydration.
  - Silent aggregate ID crosstalk where an aggregate accidentally emits an event naming another stream key.
  - Stream miscategorization where repository settings or event class defaults disagree with aggregate class declarations.
  - Unregistered event types silently dropped or unhandled during projection replays.
  - Concurrency anomalies under heavy write contention causing race conditions or lost updates.
- **Critical Invariants in eventsource-py**:
  - *Immutable Domain Events*: `DomainEvent` is a frozen Pydantic v2 model with `extra="forbid"`, immutable payloads, and explicit version derivation (`with_metadata`, `with_causation`, `with_aggregate_version`) ([ADR-0104](../adrs/accepted/adr-0104-declarative-aggregate-root-and-domain-event-strictness.md)).
  - *Stream Identity Discipline*: An event cannot name an aggregate other than the one emitting it (`AggregateIdMismatchError`, [ADR-0104](../adrs/accepted/adr-0104-declarative-aggregate-root-and-domain-event-strictness.md)).
  - *Single Source Aggregate Type*: `aggregate_type` is exclusively defined by the aggregate class `ClassVar[str]`, preventing miscategorization ([ADR-0104](../adrs/accepted/adr-0104-declarative-aggregate-root-and-domain-event-strictness.md)).
  - *Strict Event Handling*: `DeclarativeAggregate.unregistered_event_handling` defaults to `"error"` ([ADR-0104](../adrs/accepted/adr-0104-declarative-aggregate-root-and-domain-event-strictness.md)).
  - *Nullary Initial State*: Pure state initialization independent of instance identity (`initial_state()` nullary, [ADR-0103](../adrs/accepted/adr-0103-pure-functional-decider-pattern-and-immutable-state-folding.md)).
  - *Boundary-Crossing Snapshots*: `SnapshotPolicy` (e.g. `EveryNEvents` stride-crossing) and `SnapshotScheduler` composition ([ADR-0106](../adrs/accepted/adr-0106-snapshot-policies-scheduling-and-boundary-crossing-rehydration.md)); snapshots are regenerable caches, events are ground truth.
- **Exemplar User Journey**:
  - Alex defines an `Order` aggregate with `DeciderAggregate[OrderState, OrderCommand]`. Implements `initial_state() -> OrderState` with a clean draft state.
  - Authors a pure business decider handling `CreateOrder`, `AddItem`, and `SubmitOrder`, emitting `OrderCreated`, `ItemAdded`, and `OrderSubmitted` events.
  - Persists through `AggregateRepository(event_store, aggregate_factory=Order)` with optimistic concurrency control.
  - Verifies state rehydration determinism across thousands of historical events, confident that `extra="forbid"` and `AggregateIdMismatchError` catch schema and routing defects before persistence.

---

## 2. Jordan — The Streaming & Distributed Systems Platform Engineer

- **Archetype & Title**: Distributed Systems Platform Engineer / Infrastructure Lead
- **Background & Technical Stack**: Apache Kafka, RabbitMQ (aio-pika), Redis Streams, PostgreSQL, Docker/Kubernetes, OpenTelemetry distributed tracing, AsyncIO event loops.
- **Core Responsibilities & Jobs-to-be-Done (JTBD)**:
  - Wire high-throughput, low-latency event streaming pipelines between event stores and message brokers.
  - Ensure uniform at-least-once delivery semantics across heterogeneous broker technologies (`KafkaEventBus`, `RabbitMQEventBus`, `RedisEventBus`, `InMemoryEventBus`).
  - Manage subscription lifecycle with catchup replays and live streaming runners (`CatchupRunner`, `LiveRunner`, `SubscriptionManager`).
  - Guarantee partition ordering per subscription while processing events sequentially without unbounded buffering.
  - Protect streaming consumers from poison-pill messages via Dead Letter Queues (DLQ), retry policies, and lag tracking.
- **Major Pain Points & Hazards**:
  - Dual-delivery race conditions and lost checkpoints when live streaming runners desynchronize from event store logs.
  - Poison-pill events blocking partition consumers indefinitely or causing silent cascade failure across message handlers.
  - Unbounded memory consumption from background publish tasks outrunning slow brokers or network backpressure.
  - Broker-specific delivery quirks causing message reordering, lost acknowledgments, or event-loop deadlocks.
- **Critical Invariants in eventsource-py**:
  - *Uniform At-Least-Once Delivery*: Consistent `EventBus` contract with `HandlerDispatchError` aggregation; broker acks withheld on dispatch failure ([ADR-0107](../adrs/accepted/adr-0107-uniform-message-bus-contract-and-at-least-once-delivery-semantics.md)).
  - *Feed-Driven Live Checkpointing*: `LiveRunner` checkpointing is strictly feed-driven against `GlobalEventFeed`, treating the message bus solely as a wake-up signal to eliminate gap/re-read races ([ADR-0108](../adrs/accepted/adr-0108-subscription-engine-feed-driven-checkpointing-and-ordered-delivery.md)).
  - *Strict Per-Subscription Ordering*: Delivery is strictly ordered per subscription; the next event is never dispatched until the current event is handled and checkpointed ([ADR-0108](../adrs/accepted/adr-0108-subscription-engine-feed-driven-checkpointing-and-ordered-delivery.md)).
  - *Bounded Live Batch Delivery*: `handle_batch()` dispatches pages already returned by the feed, never holding accumulator windows ([ADR-0108](../adrs/accepted/adr-0108-subscription-engine-feed-driven-checkpointing-and-ordered-delivery.md)).
  - *Bounded Background Publishing*: `publish(background=True)` enforces a bounded task ceiling with automatic inline degradation rather than task dropping ([ADR-0107](../adrs/accepted/adr-0107-uniform-message-bus-contract-and-at-least-once-delivery-semantics.md)).
  - *Pluggable Coordination & DLQ*: Segregated `CheckpointRepository` and DLQ persistence ports with explicit retry policies and leader election coordination ([ADR-0107](../adrs/accepted/adr-0107-uniform-message-bus-contract-and-at-least-once-delivery-semantics.md), [ADR-0108](../adrs/accepted/adr-0108-subscription-engine-feed-driven-checkpointing-and-ordered-delivery.md)).
- **Exemplar User Journey**:
  - Jordan deploys multi-instance consumer workers running `SubscriptionManager` with `LiveRunner` listening to Kafka topic notifications.
  - As notifications arrive, `LiveRunner` drains pages from `PostgreSQLEventStore` via `GlobalEventFeed`, dispatches sequentially to handlers, and commits checkpoints to `CheckpointRepository`.
  - When a downstream service experiences intermittent timeouts, the exponential backoff `ProjectionRetryPolicy` retries before routing persistent failures to the dead-letter queue without stalling the entire feed.

---

## 3. Taylor — The Multi-Tenant SaaS Architect

- **Archetype & Title**: Multi-Tenant Cloud Architect / SaaS Security Lead
- **Background & Technical Stack**: Multi-Tenant SaaS Architectures, AsyncIO ContextVars, PostgreSQL Row-Level Security, Tenant Sharding, Compliance & Data Privacy (GDPR, SOC 2, HIPAA).
- **Core Responsibilities & Jobs-to-be-Done (JTBD)**:
  - Enforce ironclad tenant data isolation across shared infrastructure without forcing developers to pass `tenant_id` as an explicit parameter through every internal layer.
  - Guarantee that uncommitted events generated under one tenant context cannot be saved to another tenant's stream.
  - Prevent cross-tenant data leaks during global event queries, projection updates, and subscription replays.
  - Maintain transparent tenant context propagation across asynchronous task trees and context switches.
- **Major Pain Points & Hazards**:
  - Accidental tenant context bleeding between concurrent async requests sharing worker threads or event loops.
  - Query leaks where a projection or feed read retrieves events belonging to a foreign tenant.
  - Silent persistence of foreign tenant events due to missing validation or permissive defaults.
  - Context resurrection bugs where a reset or cleared tenant context revives an old tenant ID.
- **Critical Invariants in eventsource-py**:
  - *Ambient Tenant Isolation*: Thread-safe, coroutine-safe ambient tenant context propagation via `tenant_scope` and `ContextVar` (`domain/tenant_context.py`, [ADR-0110](../adrs/accepted/adr-0110-ambient-multi-tenant-saas-isolation-model.md)).
  - *Hard-Clear Reset Semantics*: `clear_tenant_context()` strictly invalidates context; attempting to reset with a stale token raises `TenantContextResetError` rather than resurrecting tenant identity ([ADR-0110](../adrs/accepted/adr-0110-ambient-multi-tenant-saas-isolation-model.md)).
  - *Strict Tenant Event Types*: `TenantDomainEvent` enforces non-null `tenant_id: UUID` at model definition, with unified provenance stamping falling back to ambient context ([ADR-0104](../adrs/accepted/adr-0104-declarative-aggregate-root-and-domain-event-strictness.md), [ADR-0110](../adrs/accepted/adr-0110-ambient-multi-tenant-saas-isolation-model.md)).
  - *Save-Time Isolation Guard*: `TenantAwareRepository` validates every uncommitted event on save, raising `TenantMismatchError` if any event disagrees with the active tenant scope ([ADR-0110](../adrs/accepted/adr-0110-ambient-multi-tenant-saas-isolation-model.md)).
  - *Enforced Tenant Preconditions on Load*: `TenantAwareRepository.load()` enforces active tenant context when `require_tenant_context=True`, failing fast if context is absent ([ADR-0110](../adrs/accepted/adr-0110-ambient-multi-tenant-saas-isolation-model.md)).
  - *Storage-Layer Tenant Filtering*: Feed reads and category queries push `tenant_id` into adapter queries (`FeedReadOptions(tenant_id=...)`), preventing foreign tenant rows from entering memory ([ADR-0109](../adrs/accepted/adr-0109-projection-engine-deterministic-replay-and-additive-read-model-reconciliation.md), [ADR-0110](../adrs/accepted/adr-0110-ambient-multi-tenant-saas-isolation-model.md)).
- **Exemplar User Journey**:
  - Taylor configures API gateway middleware to wrap incoming tenant HTTP requests in `async with tenant_scope(request.tenant_id):`.
  - Application handlers invoke `TenantAwareRepository(base_repo).load(aggregate_id)` and execute business deciders.
  - If a bug in custom application code attempts to attach an event bearing another tenant's ID or if no tenant context is set, `TenantAwareRepository` halts execution before any store write occurs.
  - Dedicated tenant projections process filtered streams using `replay(feed, projections, tenant_id=...)`, ensuring read databases remain strictly partitioned.

---

## 4. Chris — The SRE / Resilience & Cutover Operator

- **Archetype & Title**: Principal Site Reliability Engineer / Database Operations Specialist
- **Background & Technical Stack**: High-Availability Systems, PostgreSQL DBA, Zero-Downtime Data Migrations, Chaos Engineering, OpenTelemetry, Distributed Locking, Circuit Breakers.
- **Core Responsibilities & Jobs-to-be-Done (JTBD)**:
  - Migrate mission-critical event stores (e.g. legacy schemas or database engines) with zero downtime, zero event loss, and zero split-brain writes.
  - Coordinate multi-phase migration states: dual-writing live events, executing historical bulk copies, and executing atomic cutover under tight timeout bounds.
  - Monitor cutover lag, circuit breakers, and database health metrics during live traffic cutovers.
  - Ensure graceful process lifecycle, resource cleanup, and connection pool management during service restarts and deployments.
- **Major Pain Points & Hazards**:
  - Split-brain dual writes corrupting source or target event stores during migration failover.
  - Extended write pauses causing client request timeouts or cascading upstream service outages.
  - Unbounded catchup lag preventing cutover execution or dropping uncopied historical events.
  - Zombie database connections or unreleased advisory locks blocking application cutover or exhausting connection pools.
- **Critical Invariants in eventsource-py**:
  - *Deterministic 5-State Migration Phase Machine*: Strict progression through `INITIAL` -> `DUAL_WRITE` -> `BULK_COPY` -> `CATCH_UP` -> `CUTOVER` ([ADR-0111](../adrs/accepted/adr-0111-zero-downtime-5-state-live-store-migration-and-distributed-locking.md)).
  - *Source-First Dual Writing & Best-Effort Mirroring*: Dual-write always secures the authoritative source before mirroring, isolating source transactions from target store latency ([ADR-0111](../adrs/accepted/adr-0111-zero-downtime-5-state-live-store-migration-and-distributed-locking.md)).
  - *Bounded Cutover Timeout & Automatic Rollback*: Cutover write pause is strictly bounded by `cutover_timeout_ms` (default 500ms); timeout or lock failure triggers instant automatic rollback to `DUAL_WRITE` without data corruption ([ADR-0111](../adrs/accepted/adr-0111-zero-downtime-5-state-live-store-migration-and-distributed-locking.md)).
  - *Zero-Lag Strict Cutover & In-Phase Resync*: `cutover_max_lag_events` defaults to `0`; operators can run on-demand `run_resync_pass(migration_id)` to recover clamped lag anchors without restarting the migration ([ADR-0111](../adrs/accepted/adr-0111-zero-downtime-5-state-live-store-migration-and-distributed-locking.md)).
  - *Advisory Lock Coordination*: Distributed migration locking powered by PostgreSQL session-level advisory locks via `PostgreSQLLockManager` (`ports/locks.py`, [ADR-0111](../adrs/accepted/adr-0111-zero-downtime-5-state-live-store-migration-and-distributed-locking.md)).
  - *Transparent Position Mapping*: `PositionMapper` translates subscription checkpoints from source position to target position, preserving consumer state across store switchovers ([ADR-0111](../adrs/accepted/adr-0111-zero-downtime-5-state-live-store-migration-and-distributed-locking.md)).
  - *Lifecycle Port & Engine Ownership*: `SupportsClose` port and explicit `owns_engine` parameter prevent shared database connection pools from being silently disposed ([ADR-0105](../adrs/accepted/adr-0105-clean-storage-ports-composed-protocols-and-connection-lifecycle.md)).
  - *Decomposed Migration Error Hierarchy*: Clear DAG error taxonomy with dedicated circuit breaking and honest connection exception reporting (`EventStoreConnectionError`, [ADR-0105](../adrs/accepted/adr-0105-clean-storage-ports-composed-protocols-and-connection-lifecycle.md), [ADR-0111](../adrs/accepted/adr-0111-zero-downtime-5-state-live-store-migration-and-distributed-locking.md)).
- **Exemplar User Journey**:
  - Chris initiates live migration between PostgreSQL event store clusters using `LiveMigrationCoordinator.start_migration()`.
  - Observes `DUAL_WRITE` and background `BULK_COPY` progress via OpenTelemetry metrics; when the target mirror drops a batch due to transient network latency, Chris executes `coordinator.run_resync_pass()` to reconcile the lag anchor.
  - Triggers cutover: coordinator acquires PostgreSQL advisory lock, pauses source writes for 120ms, verifies lag is 0, flips routing to target store, and updates subscription positions via `PositionMapper`.
  - Confirms zero failed customer requests and clean connection disposal across application pods.

---

## 5. Morgan — The Autonomous Coding Agent & Pair Programmer

- **Archetype & Title**: Autonomous AI Coding Agent & Pair Programmer
- **Background & Technical Stack**: SpecOps PMaC Engine, Python 3.13+ typing (PEP 695 type parameter syntax, PEP 696 defaults, PEP 692), AST linters (`import-linter`, `ruff`), Hypothesis, Mutmut, Playwright / pytest-bdd.
- **Core Responsibilities & Jobs-to-be-Done (JTBD)**:
  - Implement thin vertical slices and backlog tasks autonomously without human intervention or architectural regressions.
  - Exercise code exclusively through public frontdoor APIs and blackbox verification, refusing private mock backdoors or internal monkeypatching.
  - Ensure strict adherence to hexagonal ring layering (`adapters` > `application` > `ports` > `domain`).
  - Maintain comprehensive property-based tests (Hypothesis `@given(...)`) and achieve >=80% mutation kill scores (`mutmut`).
  - Maintain complete traceability from Personas -> PRDs -> Stories -> Tasks -> ADRs -> Commits.
- **Major Pain Points & Hazards**:
  - Ambiguous task requirements lacking explicit Acceptance Criteria, Definition of Ready (DoR), or Definition of Done (DoD).
  - Monolithic files exceeding 500 lines overwhelming context windows and causing hallucinated or conflicting edits.
  - Mock backdoors that test implementation trivia instead of public observable contracts, breaking under internal refactoring.
  - Untracked dependency drifts or illegal cross-ring imports breaking clean architectural boundaries.
- **Critical Invariants in eventsource-py**:
  - *Blackbox Frontdoor Verification*: Tests exercise public CLI, domain contracts, and port interfaces without reaching into private variables or test doubles that bypass invariants ([ADR-0003](../adrs/accepted/adr-0003-blackbox-frontdoor-verification.md), SpecOps Hard Invariant 2).
  - *Modular File Length Limit (<500 Lines)*: Strict anti-rot file length ceiling enforced by `uv run spec-ops health`, warning proactively at >=400 lines ([ADR-0002](../adrs/accepted/adr-0002-modular-file-length-limits-anti-rot.md), SpecOps Hard Invariant 1).
  - *Clean Ring Layering*: Strict layered architecture (`adapters > application > ports > domain`), where domain and ports never import observability or testing ([ADR-0102](../adrs/accepted/adr-0102-hexagonal-ring-layering-and-dependency-isolation.md)).
  - *Native Modern Python Typing*: PEP 695 type parameters (`class AggregateRoot[TState: BaseModel]`), PEP 696 defaults, and PEP 692 `Unpack` provide pristine static type contracts for static reasoning ([ADR-0103](../adrs/accepted/adr-0103-pure-functional-decider-pattern-and-immutable-state-folding.md), [ADR-0104](../adrs/accepted/adr-0104-declarative-aggregate-root-and-domain-event-strictness.md)).
  - *Strict Backlog Isolation & Preflight*: Git worktree isolation in `.worktrees/<task-id>`, immutable lockfiles (`uv.lock`), zero-warning preflight checks ([ADR-0004](../adrs/accepted/adr-0004-continuous-preflight-and-self-healing-ci.md), [ADR-0005](../adrs/accepted/adr-0005-worktree-concurrency-and-backlog-isolation.md), [ADR-0009](../adrs/accepted/adr-0009-immutable-supply-chain-lockfile-enforcement.md)).
  - *Universal Base Exception*: All library errors derive from `EventSourceError`, making catch contracts predictable ([ADR-0105](../adrs/accepted/adr-0105-clean-storage-ports-composed-protocols-and-connection-lifecycle.md)).
- **Exemplar User Journey**:
  - Morgan picks up refined task `TASK-XXXX` from `PRIORITY.md` in an isolated git worktree branch `task/TASK-XXXX`.
  - Reviews governing ADRs and Gherkin scenarios in `docs/project/user_stories/accepted/`.
  - Authors BDD frontdoor test using `pytest-bdd` and property tests using Hypothesis `@given(...)`.
  - Implements the feature within modular `<500 line` files following clean ring layering.
  - Validates preflight gates: `uv run spec-ops health` (0 violations), `uv run pytest`, `uv run mutmut run` (>=80% kill score), and commits with `SpecOps-Task: TASK-XXXX`.

---

## 6. Riley — The Open-Source Core Maintainer

- **Archetype & Title**: Core Library Maintainer / Release & Governance Steward
- **Background & Technical Stack**: PyPI Package Publishing, Modern Python Packaging (UV workspace, hatchling, pyproject.toml), Semantic Versioning, Diataxis Documentation Framework, GitHub Actions CI/CD, Supply Chain Security (SLSA, Sigstore).
- **Core Responsibilities & Jobs-to-be-Done (JTBD)**:
  - Maintain pristine public API surface, backward compatibility guarantees, and clear semantic versioning transitions.
  - Keep core installation lightweight and secure, isolating optional database and broker drivers behind dedicated dependency extras.
  - Enforce the project-wide "Pre-1.0 NO-SHIMS" policy: clear, intentional breaking changes over fragile backward-compatibility deprecation wrappers.
  - Ensure all documentation adheres to the Diataxis framework with continuously verified code snippets and bidirectional ADR traceability.
  - Guarantee supply-chain integrity, cryptographic provenance manifests, and zero-leak credential hygiene.
- **Major Pain Points & Hazards**:
  - Dependency bloat where heavyweight dependencies (SQLAlchemy, asyncpg, aiokafka, pika) are loaded unconditionally by downstream users.
  - Import-time overhead and runtime side-effects on bare `import eventsource`.
  - Stale documentation tutorials, broken cross-references, or unverified code snippets leading to user frustration.
  - Unintentional public API surface breaks or leaky internal module exports across minor releases.
- **Critical Invariants in eventsource-py**:
  - *Lazy Front Door*: PEP 562 `__getattr__` and `__dir__` dynamic loading in `eventsource/__init__.py` ensures bare `import eventsource` remains Tier-0 pure without loading SQLAlchemy or database drivers ([ADR-0112](../adrs/accepted/adr-0112-tier-0-packaging-pep-562-lazy-front-door-and-zero-overhead-observability.md)).
  - *Tier-0 Core / Extras Split*: Plain `pip install eventsource` requires only `pydantic` and `sqlalchemy`; all database, broker, and telemetry drivers reside behind named extras (`postgres`, `sqlite`, `kafka`, `rabbitmq`, `redis`, `telemetry`) ([ADR-0112](../adrs/accepted/adr-0112-tier-0-packaging-pep-562-lazy-front-door-and-zero-overhead-observability.md)).
  - *Pre-1.0 NO-SHIMS Policy*: Breaking changes are made cleanly and documented without leaving deprecated import shims or compatibility aliases ([ADR-0102](../adrs/accepted/adr-0102-hexagonal-ring-layering-and-dependency-isolation.md), [ADR-0112](../adrs/accepted/adr-0112-tier-0-packaging-pep-562-lazy-front-door-and-zero-overhead-observability.md)).
  - *Strict Single Export Surface*: Top-level `__all__` is verified and byte-identical across lazy and type-checking modes, with infrastructure exceptions quarantined to `ports/exceptions.py` ([ADR-0112](../adrs/accepted/adr-0112-tier-0-packaging-pep-562-lazy-front-door-and-zero-overhead-observability.md)).
  - *Living Diataxis Documentation & ADR Index Verification*: ADR index and MkDocs navigation are verified against the filesystem by automated tooling (`scripts/check_adr_index.py`), guaranteeing zero documentation drift ([ADR-0001](../adrs/accepted/adr-0001-specification-as-code-architecture.md), ADR-0101-ADR-0112).
  - *Immutable Lockfiles & Tamper-Evident Provenance*: Enforced by CI gates and `uv lock --check`, ensuring supply-chain integrity ([ADR-0009](../adrs/accepted/adr-0009-immutable-supply-chain-lockfile-enforcement.md), [ADR-0010](../adrs/accepted/adr-0010-secret-scanning-and-credential-leak-defense.md)).
- **Exemplar User Journey**:
  - Riley reviews an incoming pull request, verifying CI passes all quality gates: `import-linter` layering contracts, PEP 562 lazy frontdoor integrity, and Diataxis docs build.
  - Verifies that new capabilities are properly isolated behind extras in `pyproject.toml` and documented in `docs/reference/` and `docs/how-to/`.
  - Runs release workflow: validates `uv lock --check`, generates release notes with commit provenance trailers, and publishes signed wheels to PyPI with verified SLSA provenance.
