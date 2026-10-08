# User Personas

Archetypes representing the core users, operators, and developers interacting with eventsource-py.

---

## 1. Alex — The Event Sourced Systems Architect
- **Role**: Software engineer and distributed systems architect designing event-driven applications.
- **Pain Points**:
  - Concurrency bugs and version conflicts when multiple processes write to aggregate event streams.
  - Performance degradation replaying large event streams without snapshotting or projection state caching.
  - Complex boilerplate wiring together aggregate roots, deciders, and repositories.
- **Goals with eventsource-py**:
  - Expressive domain modeling supporting Decider, Declarative, and classic AggregateRoot patterns.
  - Pluggable, high-performance event stores (PostgreSQL, SQLite, In-Memory) with optimistic concurrency control.
  - Composable projections with deterministic replay, checkpoint persistence, and dead-letter queue (DLQ) support.

---

## 2. Jordan — The Backend Platform Engineer
- **Role**: Platform engineer operating streaming message brokers, databases, and multi-tenant SaaS backends.
- **Pain Points**:
  - Operational downtime during event store schema migrations and cutover.
  - High risk of data leaks across customer tenants in shared multi-tenant databases.
  - Broker partition failovers causing out-of-order message processing or lost events.
- **Goals with eventsource-py**:
  - First-class message bus adapters for Kafka, RabbitMQ, Redis, and In-Memory with uniform delivery semantics.
  - Zero-downtime live store migration with dual-write, historical bulk copy, and automated cutover.
  - Ambient tenant isolation via `tenant_scope` and `TenantAwareRepository` preventing cross-tenant leakage.

---

## 3. Morgan — The Autonomous Coding Agent
- **Role**: LLM-powered coding worker contributing features, bug fixes, and refactoring to the repository.
- **Pain Points**:
  - Ambiguous task contracts lacking explicit Definition of Ready (DoR) and Definition of Done (DoD).
  - Accidentally violating ring layering invariants (adapters -> application -> ports -> domain).
  - Large monolithic modules exceeding manageable reasoning context (<500 lines).
- **Goals with eventsource-py**:
  - Unbroken traceability from Personas -> PRDs -> Stories -> Tasks -> ADRs -> Commits.
  - Blackbox frontdoor test verification with zero private mock backdoor tampering.
  - Strict git worktree isolation preventing merge collisions across concurrent execution streams.

---

## 4. Riley — The Open-Source Library Maintainer
- **Role**: Core library maintainer responsible for package releases, PyPI publishing, and architectural integrity.
- **Pain Points**:
  - Dependency floor drifts and runtime platform incompatibilities on Python 3.13.
  - Stale Diataxis documentation, broken API links, and untracked architectural decisions.
  - Unintentional public surface breaks across release cycles.
- **Goals with eventsource-py**:
  - Automated quality gates verifying 100% test pass rates, linting, and type checking (`uv run --all-extras --locked ...`).
  - Living Diataxis documentation and interactive 2D graph visualizer deployed continuously to GitHub Pages.
  - Strict Merkle supply-chain audit manifests and tamper-evident provenance tracking.
