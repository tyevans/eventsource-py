---
id: '0001'
title: Production-Ready Event Sourcing and Live Migration Framework
status: Accepted
created: 2026-10-07
target_persona: Alex
component: core
---

# PRD-0001 — Production-Ready Event Sourcing and Live Migration Framework

## Who this is for

- **Alex (The Event Sourced Systems Architect)**: Engineers designing event-driven microservices, state machines, and CQRS architectures.
- **Jordan (The Backend Platform Engineer)**: Platform operators managing Kafka, RabbitMQ, Redis, PostgreSQL, and multi-tenant persistence.
- **Morgan (The Autonomous Coding Agent)**: Coding agents requiring clean domain contracts and blackbox test suites.
- **Riley (The Open-Source Library Maintainer)**: Maintainers ensuring API stability, ring layering integrity, and verified documentation.

## What the person cannot do today

- **Monolithic State Corruption**: Traditional CRUD databases overwrite state in place, losing historical audit trails and making point-in-time state reconstruction impossible.
- **Downtime Migration Penalty**: Migrating event storage schemas or backends historically required taking applications offline to prevent split-brain writes.
- **Multi-Tenant Leakage**: Ad-hoc tenant filtering in application queries easily leaks private data across customer boundaries in multi-tenant SaaS deployments.
- **Broker Concurrency Pitfalls**: Hand-rolled message bus integrations frequently suffer from message loss, out-of-order delivery, or blocking retries.

## What good looks like

1. **Expressive Domain Modeling**:
   - Support for pure functional Decider pattern (`DeciderAggregate[TState, TCommand]`), declarative handler registration (`DeclarativeAggregate` with `@handles`), and classic imperative `AggregateRoot`.
   - Automatic aggregate versioning, optimistic concurrency conflict detection (`ExpectedVersionError`), and immutable domain events (`DomainEvent`).

2. **Pluggable Storage and Bus Ports**:
   - Strict ring layering (`adapters` -> `application` -> `ports` -> `domain`).
   - Production-ready `EventStore` implementations: PostgreSQL (with JSONB, advisory locks), SQLite, and In-Memory.
   - Uniform `EventBus` implementations: Kafka, RabbitMQ, Redis, and In-Memory.

3. **Resilient Projections and Subscriptions**:
   - `DeclarativeProjection` with automatic event handler routing and synchronous/asynchronous execution models.
   - Persistent checkpointing, dead-letter queue (DLQ) error isolation, and lag monitoring telemetry.
   - Dual-runner subscription engine supporting catchup replay and live event streaming.

4. **Ambient Multi-Tenancy and Live Migration**:
   - `tenant_scope` ambient context propagation with `TenantDomainEvent` and `TenantAwareRepository` write enforcement.
   - Zero-downtime live event store migration orchestrating dual-writing, background bulk copying, phase reconciliation, and atomic cutover.

## What this does not do

- **UI / Frontend State Management**: `eventsource-py` is a backend Python library; frontend event dispatching and rendering are caller responsibilities.
- **Cross-Tenant Data Blending**: No operations or query helpers permit cross-tenant data mingling without explicit unscoped bypass.
- **Distributed Coordinator Consensus Engine**: Distributed lease coordination delegates to proven infrastructure (PostgreSQL advisory locks) rather than embedding a Raft/Paxos consensus daemon.

## Checkable Outcomes

1. Executing commands on a `DeciderAggregate` or `DeclarativeAggregate` appends versioned events to an `EventStore` and detects concurrent write conflicts.
2. Publishing events to `EventBus` adapters (Kafka, RabbitMQ, Redis, InMemory) delivers messages to subscribed handlers matching filter predicates.
3. Catchup and live subscription runners replay historic events through `DeclarativeProjection` instances and persist stream checkpoints.
4. Setting an ambient `tenant_scope` prevents uncommitted events with foreign tenant IDs from persisting to `TenantAwareRepository`.
5. Running `LiveMigrationCoordinator` transfers historic events between source and target stores while dual-writing live stream appends without message loss.

## Linked User Stories

- [`US-0001`](../../user_stories/accepted/us-0001-define-aggregates-and-record-events.md): Define Aggregates and Record Committed Events
- [`US-0002`](../../user_stories/accepted/us-0002-append-and-replay-events-across-storage-adapters.md): Append and Replay Events Across Storage Adapters
- [`US-0003`](../../user_stories/accepted/us-0003-publish-and-subscribe-to-events-via-message-buses.md): Publish and Subscribe to Events via Message Buses
- [`US-0004`](../../user_stories/accepted/us-0004-project-events-into-read-models-with-checkpoints.md): Project Events into Read Models with Checkpoints
- [`US-0005`](../../user_stories/accepted/us-0005-scope-events-and-repositories-to-tenants.md): Scope Events and Repositories to Tenants
- [`US-0006`](../../user_stories/accepted/us-0006-migrate-event-stores-zero-downtime.md): Migrate Event Stores Zero-Downtime
