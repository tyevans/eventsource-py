---
id: '0001'
title: Define Aggregates and Record Committed Events
status: Accepted
created: 2026-10-07
persona: Alex (The Event-Sourced Domain Architect)
target_bc: domain
feature: FEAT-CORE-AGGREGATE
governing_prd: PRD-0001
scenarios:
- Command execution on DeciderAggregate emits domain events
- Concurrent command on stale aggregate version raises ExpectedVersionError
- Nullary initial_state evolves state from command carrying aggregate identity
- DeclarativeAggregate routes events via handles decorators and rejects unregistered
  events
- Aggregate rejects event naming a different aggregate with AggregateIdMismatchError
- Aggregate type is strictly defined on aggregate class ClassVar preventing repository
  miscategorization
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0101
- ADR-0103
- ADR-0104
---

# US-0001 — Define Aggregates and Record Committed Events

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** event sourced domain architect (Alex),
**I want** to model business domains using pure `DeciderAggregate` or `DeclarativeAggregate` state machines,
**So that** domain invariants are enforced synchronously, stream crosstalk is precluded, and events are committed with optimistic concurrency control.

## Acceptance Criteria

```gherkin
Scenario: Command execution on DeciderAggregate emits domain events
  Given an uncommitted Order aggregate in draft state
  When the caller issues a "ShipOrder" command with valid tracking info
  Then an "OrderShipped" domain event is generated
  And the uncommitted version is incremented by 1.
```

```gherkin
Scenario: Concurrent command on stale aggregate version raises ExpectedVersionError
  Given an aggregate committed at version 5
  When another worker attempts to commit a change expecting version 4
  Then an "ExpectedVersionError" is raised
  And no uncommitted events are appended to the event store.
```

```gherkin
Scenario: Nullary initial_state evolves state from command carrying aggregate identity
  Given a DeciderAggregate class whose initial_state takes no arguments
  When a "CreateOrder" command carrying order ID "ord-123" is executed
  Then the decider extracts identity from the command rather than the state
  And emits an "OrderCreated" event bound to stream "ord-123".
```

```gherkin
Scenario: DeclarativeAggregate routes events via handles decorators and rejects unregistered events
  Given a DeclarativeAggregate subclass with unregistered_event_handling defaulted to "error"
  When an event with no matching "@handles" decorator is replayed against the aggregate
  Then an "UnhandledEventError" is raised
  And aggregate state evolution halts immediately.
```

```gherkin
Scenario: Aggregate rejects event naming a different aggregate with AggregateIdMismatchError
  Given an Order aggregate instance with identity "ord-123"
  When a command handler attempts to emit an event naming foreign aggregate "ord-999"
  Then an "AggregateIdMismatchError" is raised
  And the foreign event is not accepted into uncommitted events.
```

```gherkin
Scenario: Aggregate type is strictly defined on aggregate class ClassVar preventing repository miscategorization
  Given an AggregateRoot subclass defining ClassVar "aggregate_type = 'Order'"
  When an AggregateRepository is constructed for that aggregate class
  Then the repository infers aggregate_type from the class attribute
  And stream keys and event categories are guaranteed to match without manual override drift.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0007, ADR-0112, ADR-0122, ADR-0142, ADR-0143, ADR-0145, ADR-0146, ADR-0148, ADR-0156, ADR-0165
- **Verified Test Suites**:
  - `tests/unit/domain/test_aggregate.py`: Verifies aggregate lifecycle, version tracking, and optimistic concurrency checks.
  - `tests/unit/domain/test_decider.py`: Verifies pure functional `DeciderAggregate` state evolution with nullary `initial_state()`.
  - `tests/unit/domain/test_declarative.py`: Verifies declarative event handler routing with `@handles` and strict rejection of unregistered events.
  - `tests/unit/domain/test_events.py`: Verifies frozen `DomainEvent` immutability, `extra="forbid"`, and automatic type derivation.
  - `tests/unit/application/aggregates/`: Verifies aggregate command execution, error retention, and stream boundary isolation.
  - `tests/unit/repositories/test_aggregate_repository.py`: Verifies repository interaction, optimistic version conflict detection (`ExpectedVersionError`), and single-source `aggregate_type` derivation.
- **Architectural Invariants Verified**:
  - *Frontdoor Contract*: Aggregates tested exclusively through public commands and event replay without private state tampering.
  - *Stream Boundary Guard*: `AggregateIdMismatchError` raised when an aggregate attempts to emit events for foreign streams.
  - *Type Consistency*: `aggregate_type` strictly derived from `ClassVar[str]` preventing repository misconfiguration.
