# ADR Registry

This registry tracks all Architectural Decision Records (ADRs) governing `eventsource`, divided into SpecOps System / SDLC Guardrails (ADR-0001 through ADR-0010) and Core Domain / Architecture Decisions (ADR-0101 through ADR-0166).

---

## SpecOps System & SDLC Guardrails

| ID | Title | Status | Scope |
|---|---|---|---|
| [ADR-0001](accepted/adr-0001-specification-as-code-architecture.md) | Specification as Code and Opinionated SDLC Guardrails | Accepted | `core` |
| [ADR-0002](accepted/adr-0002-modular-file-length-limits-anti-rot.md) | Modular Source File Length Limit (<500 Lines Anti-Rot Rule) | Accepted | `core` |
| [ADR-0003](accepted/adr-0003-blackbox-frontdoor-verification.md) | Blackbox Frontdoor Verification and Zero Backdoor Testing | Accepted | `core` |
| [ADR-0004](accepted/adr-0004-continuous-preflight-and-self-healing-ci.md) | Continuous Pre-flight Verification and Self-Healing CI Loops | Accepted | `core` |
| [ADR-0005](accepted/adr-0005-worktree-concurrency-and-backlog-isolation.md) | Git Worktree Concurrency and Strict Backlog Isolation | Accepted | `core` |
| [ADR-0006](accepted/adr-0006-bdd-gherkin-user-stories-and-playwright-e2e.md) | Behavior-Driven Development (BDD) with Gherkin User Stories and Playwright | Accepted | `core` |
| [ADR-0007](accepted/adr-0007-domain-driven-design-and-bounded-contexts.md) | Domain-Driven Design (DDD) Layering and Explicit Bounded Contexts | Accepted | `core` |
| [ADR-0008](accepted/adr-0008-zero-trust-worker-process-sandboxing.md) | Zero-Trust Autonomous Worker Process Sandboxing | Accepted | `core` |
| [ADR-0009](accepted/adr-0009-immutable-supply-chain-lockfile-enforcement.md) | Immutable Supply-Chain Lockfile Enforcement | Accepted | `core` |
| [ADR-0010](accepted/adr-0010-secret-scanning-and-credential-leak-defense.md) | Real-Time Secret Scanning and Credential Leak Defense | Accepted | `core` |

---

## Eventsource Core Domain & Engine Architecture Decisions

The architectural decisions governing `eventsource` domain models, store ports, bus adapters, projection runners, and zero-downtime migrations (mapped from legacy ADRs 0001 through 0066).

| ID | Legacy | Title | Status | Bounded Context |
|---|---|---|---|---|
| [ADR-0101](accepted/adr-0101-async-first-design.md) | 0001 | Async-First Design | Accepted | `ports` |
| [ADR-0107](accepted/adr-0107-event-bus-delivery-semantics.md) | 0007 | Event Bus Delivery Semantics and Tracing Contract | Accepted | `bus` |
| [ADR-0108](accepted/adr-0108-mutation-testing-tool-selection.md) | 0008 | Mutation Testing Tool Selection: mutmut Plus cosmic-ray, Not One Tool | Accepted | `testing` |
| [ADR-0109](accepted/adr-0109-multi-instance-subscription-coordination.md) | 0009 | Multi-Instance Subscription Coordination | Accepted | `subscriptions` |
| [ADR-0110](accepted/adr-0110-uniform-event-bus-contract.md) | 0010 | Uniform Event Bus Contract: background Semantics and BaseEventBus | Accepted | `bus` |
| [ADR-0111](accepted/adr-0111-handler-error-isolation-with-no-ack.md) | 0011 | Uniform Handler-Error Isolation with HandlerDispatchError and No-Ack-on-Failure | Accepted | `bus` |
| [ADR-0112](accepted/adr-0112-event-type-auto-derivation.md) | 0012 | Event Type Auto-Derivation from Class Name | Accepted | `domain` |
| [ADR-0113](accepted/adr-0113-handler-registry-composition.md) | 0013 | Handler Registry and Adapter as Collaborators | Accepted | `projections` |
| [ADR-0114](accepted/adr-0114-live-migration-cutover-semantics.md) | 0014 | Live Migration Cutover Semantics | Accepted | `migration` |
| [ADR-0115](accepted/adr-0115-optional-dependency-extras.md) | 0015 | Optional Dependency Extras and the Core/Backend Split | Accepted | `core` |
| [ADR-0116](accepted/adr-0116-optional-tracing-no-op-by-default.md) | 0016 | Optional Tracing, No-Op by Default | Accepted | `observability` |
| [ADR-0117](superseded/adr-0117-snapshot-strategy-pattern.md) | 0017 | Snapshot Strategy Pattern | Superseded (by ADR-0121) | `snapshots` |
| [ADR-0118](accepted/adr-0118-tenant-isolation-model.md) | 0018 | Tenant Isolation Model | Accepted | `multitenancy` |
| [ADR-0119](accepted/adr-0119-clean-architecture-store-ports.md) | 0019 | Clean-Architecture Store Ports and Opaque Positions | Accepted | `ports` |
| [ADR-0120](accepted/adr-0120-broker-backend-collaborator-decomposition.md) | 0020 | Broker Backend Collaborator Decomposition | Accepted | `bus` |
| [ADR-0121](accepted/adr-0121-snapshot-policy-scheduler-composition.md) | 0021 | Snapshot Policy/Scheduler Composition | Accepted | `snapshots` |
| [ADR-0122](accepted/adr-0122-command-objects-and-decider-style.md) | 0022 | Command Objects and the Decider Aggregate Style | Accepted | `domain` |
| [ADR-0123](accepted/adr-0123-postgresql-advisory-locks.md) | 0023 | PostgreSQL Advisory Locks for Distributed Coordination | Accepted | `locks` |
| [ADR-0124](accepted/adr-0124-projection-persistence-ports.md) | 0024 | Projection Persistence Ports | Accepted | `ports` |
| [ADR-0125](accepted/adr-0125-legacy-store-retirement.md) | 0025 | Legacy Store Surface Retirement | Accepted | `stores` |
| [ADR-0126](accepted/adr-0126-outbox-ring-migration.md) | 0026 | Outbox Ring Migration | Accepted | `adapters` |
| [ADR-0127](accepted/adr-0127-schema-correctness-fixes.md) | 0027 | Schema Correctness Fixes | Accepted | `adapters` |
| [ADR-0128](accepted/adr-0128-strict-cutover-and-in-phase-resync.md) | 0028 | Strict Cutover and In-Phase Resync | Accepted | `migration` |
| [ADR-0129](accepted/adr-0129-locks-readmodels-and-engine-rings.md) | 0029 | Locks, Read Models, and the Engine Factory: Completing the Ring Migration | Accepted | `adapters` |
| [ADR-0130](accepted/adr-0130-top-level-module-ring-consolidation.md) | 0030 | Top-Level Module Ring Consolidation | Accepted | `core` |
| [ADR-0131](accepted/adr-0131-bus-ring-split.md) | 0031 | Bus Ring Split | Accepted | `bus` |
| [ADR-0132](accepted/adr-0132-subscriptions-ring-migration.md) | 0032 | Subscriptions Ring Migration | Accepted | `subscriptions` |
| [ADR-0133](accepted/adr-0133-events-handlers-internal-ring-migration.md) | 0033 | Events, Handlers, and Internal Ring Migration | Accepted | `domain` |
| [ADR-0134](accepted/adr-0134-migration-ring-and-layers-contract.md) | 0034 | Migration Ring Migration and Full Layers Contract | Accepted | `migration` |
| [ADR-0135](accepted/adr-0135-lazy-front-door.md) | 0035 | PEP 562 Lazy Front Door | Accepted | `core` |
| [ADR-0136](accepted/adr-0136-snapshot-port-composed-protocols.md) | 0036 | Snapshot Port as Composed Protocols | Accepted | `ports` |
| [ADR-0137](accepted/adr-0137-store-lifecycle-port.md) | 0037 | Store Lifecycle Port and Explicit Engine Ownership | Accepted | `ports` |
| [ADR-0138](accepted/adr-0138-multitenancy-dissolution.md) | 0038 | Multi-Tenancy Ring Dissolution | Accepted | `multitenancy` |
| [ADR-0139](accepted/adr-0139-schema-ddl-to-adapters.md) | 0039 | Schema DDL Package Relocated to Adapters | Accepted | `adapters` |
| [ADR-0140](accepted/adr-0140-out-of-ring-settlement.md) | 0040 | Out-of-Ring Settlement: observability/ and testing/ | Accepted | `core` |
| [ADR-0141](accepted/adr-0141-infrastructure-exceptions-to-ports.md) | 0041 | Infrastructure Exceptions Move to ports/exceptions.py | Accepted | `ports` |
| [ADR-0142](accepted/adr-0142-domain-event-strictness.md) | 0042 | Domain Event and Handler Strictness | Accepted | `domain` |
| [ADR-0143](accepted/adr-0143-domain-model-guards-and-vocabulary.md) | 0043 | Domain Model Guards, Vocabulary, and the Decider-First Teaching Layer | Accepted | `domain` |
| [ADR-0144](accepted/adr-0144-migration-error-module-decomposition.md) | 0044 | Migration Error Module Decomposition | Accepted | `migration` |
| [ADR-0145](accepted/adr-0145-pep695-type-parameter-syntax.md) | 0045 | PEP 695 Type-Parameter Syntax | Accepted | `core` |
| [ADR-0146](accepted/adr-0146-aggregate-type-single-source.md) | 0046 | aggregate_type Has One Source: the Aggregate Class | Accepted | `domain` |
| [ADR-0147](accepted/adr-0147-live-runner-feed-driven-checkpointing.md) | 0047 | Live Runner Checkpointing Is Feed-Driven, Not Bus-Driven | Accepted | `subscriptions` |
| [ADR-0148](accepted/adr-0148-failure-paths-report-and-retain.md) | 0048 | Failure Paths Report Honestly and Retain What They Cannot Handle | Accepted | `adapters` |
| [ADR-0149](accepted/adr-0149-snapshot-boundary-crossing.md) | 0049 | Snapshots Fire on Crossing a Boundary, Not Landing on One | Accepted | `snapshots` |
| [ADR-0150](accepted/adr-0150-read-model-version-conflict-error-name.md) | 0050 | The Read-Model Conflict Error Gets Its Own Name | Accepted | `readmodels` |
| [ADR-0151](accepted/adr-0151-adapters-common-shared-port-semantics.md) | 0051 | adapters/_common/ Holds Port Semantics No Adapter Should Re-Derive | Accepted | `adapters` |
| [ADR-0152](accepted/adr-0152-feed-read-aggregate-type-filter.md) | 0052 | The Global Feed Filters By Aggregate Type | Accepted | `stores` |
| [ADR-0153](accepted/adr-0153-sqlite-snapshot-store-owns-its-connection.md) | 0053 | SQLiteSnapshotStore Owns One Connection And Closes It | Accepted | `snapshots` |
| [ADR-0154](accepted/adr-0154-projection-replay-driver.md) | 0054 | Rebuilding a Projection Is a Foreground Driver, Not a Subscription | Accepted | `projections` |
| [ADR-0155](accepted/adr-0155-generic-store-projection-base.md) | 0055 | StoreProjection Forwards the Projection Constructor by Name, Once | Accepted | `projections` |
| [ADR-0156](accepted/adr-0156-decider-initial-state-is-nullary.md) | 0056 | initial_state() Is Nullary; the Command Carries the Aggregate Id | Accepted | `domain` |
| [ADR-0157](accepted/adr-0157-tenant-load-enforcement.md) | 0057 | Tenant Load Enforcement Is a Precondition, and Says So | Accepted | `multitenancy` |
| [ADR-0158](accepted/adr-0158-eventsource-error-as-universal-base.md) | 0058 | EventSourceError is the universal base for library exceptions | Accepted | `core` |
| [ADR-0159](accepted/adr-0159-ordered-subscription-delivery.md) | 0059 | Subscription Delivery Is Ordered Per Subscription | Accepted | `subscriptions` |
| [ADR-0160](accepted/adr-0160-bounded-background-publishing.md) | 0060 | Background Publishing Is Bounded, and Degrades to Inline | Accepted | `bus` |
| [ADR-0161](accepted/adr-0161-leader-lease-protocol-deleted.md) | 0061 | The Lease Half of Leader Election Is Not Ours to Declare | Accepted | `subscriptions` |
| [ADR-0162](accepted/adr-0162-single-declaration-sites-for-shutdown-timeout-and-retry-policy.md) | 0062 | Single declaration sites for shutdown timeout and retry policy | Accepted | `application` |
| [ADR-0163](accepted/adr-0163-live-batch-delivery-is-a-page-not-a-window.md) | 0063 | Live Batch Delivery Is A Page, Not A Window | Accepted | `subscriptions` |
| [ADR-0164](accepted/adr-0164-telemetry-attribute-catalogue-is-not-a-wishlist.md) | 0064 | The Telemetry Attribute Catalogue Is Not a Wishlist | Accepted | `observability` |
| [ADR-0165](accepted/adr-0165-an-event-cannot-name-another-aggregate.md) | 0065 | An Event Cannot Name an Aggregate Other Than the One Emitting It | Accepted | `domain` |
| [ADR-0166](accepted/adr-0166-read-model-schema-reconciliation-is-additive-and-opt-in.md) | 0066 | Read-Model Schema Reconciliation Is Additive and Opt-In | Accepted | `readmodels` |
