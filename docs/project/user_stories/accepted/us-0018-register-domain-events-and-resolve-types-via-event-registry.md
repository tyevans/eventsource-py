---
id: '0018'
title: Register Domain Events and Resolve Types via Thread-Safe Event Registry
status: Accepted
created: 2026-10-09
persona: Alex (The Event-Sourced Domain Architect)
target_bc: domain
feature: FEAT-EVENT-REGISTRY
governing_prd: PRD-0001
scenarios:
- Register domain event class with auto-derived event_type name
- Register domain event class with explicit wire name override
- Rejection of conflicting event class registration with DuplicateEventTypeError
- Safe resolution and lookup of registered event class by wire name
- Rejection of unmapped event type lookup with EventTypeNotFoundError
- Isolated independent registry instances for multi-bounded-context testing
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0007
- ADR-0104
---

# US-0018: Register Domain Events and Resolve Types via Thread-Safe Event Registry

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** event-sourced domain architect (Alex),
**I want** to register domain event classes into a thread-safe `EventRegistry` using class decorators or explicit registration and resolve concrete event types dynamically,
**So that** polymorphic wire-format JSON event payloads deserialize into strongly-typed domain events, duplicate event type collisions are prevented at import time, and isolated bounded contexts can maintain independent event registries.

## Acceptance Criteria

```gherkin
Scenario: Register domain event class with auto-derived event_type name
  Given a DomainEvent subclass "OrderPlaced" without an explicit event_type
  When the class is decorated with "@register_event"
  Then the default registry records "OrderPlaced" pointing to that class
  And looking up "OrderPlaced" returns the concrete OrderPlaced class.
```

```gherkin
Scenario: Register domain event class with explicit wire name override
  Given a DomainEvent subclass "OrderPlacedV2"
  When the class is decorated with "@register_event(event_type='order.placed.v2')"
  Then the registry maps wire name "order.placed.v2" to "OrderPlacedV2"
  And resolving "order.placed.v2" returns "OrderPlacedV2".
```

```gherkin
Scenario: Rejection of conflicting event class registration with DuplicateEventTypeError
  Given an event type name "OrderPlaced" already registered to class "OrderPlaced"
  When another class "DifferentOrderPlaced" attempts to register under "OrderPlaced"
  Then a "DuplicateEventTypeError" is raised
  And the existing registration remains unmodified.
```

```gherkin
Scenario: Safe resolution and lookup of registered event class by wire name
  Given registered event types in the registry
  When storage or serialization adapters invoke "get_event_class(name)" or "get_event_class_or_none(name)"
  Then the corresponding DomainEvent class is returned safely without side effects.
```

```gherkin
Scenario: Rejection of unmapped event type lookup with EventTypeNotFoundError
  Given an unregistered event type name "UnknownEvent"
  When a consumer invokes "get_event_class('UnknownEvent')"
  Then an "EventTypeNotFoundError" is raised listing available registered event types.
```

```gherkin
Scenario: Isolated independent registry instances for multi-bounded-context testing
  Given custom EventRegistry instances created for isolated test suites
  When events are registered on an isolated registry instance
  Then the global default_registry remains unaffected and cross-test pollution is prevented.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified (100% test pass rate, 0 backdoor mocks)
- **Implementation Modules**:
  - `src/eventsource/domain/events.py`: `EventRegistry`, `default_registry`, `@register_event` decorator.
  - `src/eventsource/domain/exceptions.py`: `DuplicateEventTypeError`, `EventTypeNotFoundError`.
- **Verified Test Suites**:
  - `tests/unit/domain/test_event_registry.py`: Full coverage of registration, duplicate rejection, and registry isolation.
  - `tests/unit/adapters/serialization/test_json.py`: Verifies polymorphic wire deserialization via the registry.
- **Architectural Invariants Verified**:
  - *Thread Safety*: Registry mutation guarded by internal locks.
  - *Strict Typing*: Event classes must inherit from `DomainEvent`.
  - *Immutability*: Registered types cannot be silently overwritten.
