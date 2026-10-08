# Architecture Decision Records

This directory contains the foundational Architecture Decision Records (ADRs) governing `eventsource-py`.

## Canonical Architectural Foundations

- [1. Async-First Concurrency Model](0001-async-first-concurrency-model.md) — Pure asyncio core throughout stores, buses, runners, and repositories.
- [2. Hexagonal Ring Layering and Dependency Isolation](0002-hexagonal-ring-layering-and-dependency-isolation.md) — Strict layered architecture: adapters > application > ports > domain.
- [3. Pure Functional Decider Pattern and Immutable State Folding](0003-pure-functional-decider-pattern-and-immutable-state-folding.md) — Typed DeciderAggregate, immutable Pydantic states, and pure state folding.
- [4. Declarative Aggregate Root and Domain Event Strictness](0004-declarative-aggregate-root-and-domain-event-strictness.md) — Declarative @handles decorators, single-source wire names, and strict event validation.
- [5. Clean Storage Ports, Composed Protocols, and Connection Lifecycle](0005-clean-storage-ports-composed-protocols-and-connection-lifecycle.md) — Segregated store ports, explicit connection ownership, and clean shutdown.
- [6. Snapshot Policies, Scheduling, and Boundary-Crossing Rehydration](0006-snapshot-policies-scheduling-and-boundary-crossing-rehydration.md) — Composed snapshot policies decoupled from stores with boundary-crossing rehydration.
- [7. Uniform Message Bus Contract and At-Least-Once Delivery Semantics](0007-uniform-message-bus-contract-and-at-least-once-delivery-semantics.md) — Uniform EventBus protocol across brokers with at-least-once delivery and DLQ isolation.
- [8. Subscription Engine, Feed-Driven Checkpointing, and Ordered Sequential Delivery](0008-subscription-engine-feed-driven-checkpointing-and-ordered-delivery.md) — Feed-driven checkpointing, sequential subscriber delivery, and page-based batching.
- [9. Projection Engine, Deterministic Replay, and Additive Read-Model Reconciliation](0009-projection-engine-deterministic-replay-and-additive-read-model-reconciliation.md) — StoreProjection base, replay drivers, version conflict protection, and additive schemas.
- [10. Ambient Multi-Tenant SaaS Isolation Model](0010-ambient-multi-tenant-saas-isolation-model.md) — ContextVar tenant propagation, hard reset safety, and storage query pushdown.
- [11. Zero-Downtime 5-State Live Store Migration and Distributed Locking](0011-zero-downtime-5-state-live-store-migration-and-distributed-locking.md) — 5-state live migration coordinator, bounded pause rollback, and advisory locks.
- [12. Tier-0 Packaging, PEP 562 Lazy Front Door, and Zero-Overhead Observability](0012-tier-0-packaging-pep-562-lazy-front-door-and-zero-overhead-observability.md) — Lightweight base install, named extras, lazy frontdoor loading, and no-op tracing.
