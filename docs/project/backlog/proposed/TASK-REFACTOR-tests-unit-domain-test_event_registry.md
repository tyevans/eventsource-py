---
id: REFACTOR-tests-unit-domain-test_event_registry
title: Refactor and Decompose Legacy File test_event_registry.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-domain-test_event_registry: Refactor Legacy File test_event_registry.py

## Summary
The grandfathered debt file `tests/unit/domain/test_event_registry.py` contains 755 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_event_registry_order.py, test_event_registry_default.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/domain/test_event_registry/` with submodules:
- `test_event_registry_order.py`: OrderCreated, OrderShipped, PaymentReceived, TestEventRegistryBasicOperations, TestEventRegistryLookup, TestEventTypeResolution, TestDuplicateRegistration, TestRegistryClearAndUnregister, TestDecoratorRegistration, TestRegistryIsolation, TestThreadSafety, TestRegistryIteration, TestErrorMessages, TestEventDeserialization, TestVersionedEventSchemas
- `test_event_registry_default.py`: EventWithoutTypeDefault, TestDefaultRegistry

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/domain/test_event_registry.py (755 lines):
  Submodule 'test_event_registry_order.py' (~617 lines):
    - [class] OrderCreated (lines 37-43)
    - [class] OrderShipped (lines 46-51)
    - [class] PaymentReceived (lines 54-59)
    - [class] TestEventRegistryBasicOperations (lines 68-115)
    - [class] TestEventRegistryLookup (lines 118-205)
    - [class] TestEventTypeResolution (lines 208-234)
    - [class] TestDuplicateRegistration (lines 237-267)
    - [class] TestRegistryClearAndUnregister (lines 270-302)
    - [class] TestDecoratorRegistration (lines 305-354)
    - [class] TestRegistryIsolation (lines 425-452)
    - [class] TestThreadSafety (lines 455-571)
    - [class] TestRegistryIteration (lines 574-609)
    - [class] TestErrorMessages (lines 612-658)
    - [class] TestEventDeserialization (lines 661-709)
    - [class] TestVersionedEventSchemas (lines 712-755)
  Submodule 'test_event_registry_default.py' (~70 lines):
    - [class] EventWithoutTypeDefault (lines 62-65)
    - [class] TestDefaultRegistry (lines 357-422)
  Suggested barrel exports:
    from .test_event_registry_order import OrderCreated, OrderShipped, PaymentReceived, TestEventRegistryBasicOperations, TestEventRegistryLookup, TestEventTypeResolution, TestDuplicateRegistration, TestRegistryClearAndUnregister, TestDecoratorRegistration, TestRegistryIsolation, TestThreadSafety, TestRegistryIteration, TestErrorMessages, TestEventDeserialization, TestVersionedEventSchemas
    from .test_event_registry_default import EventWithoutTypeDefault, TestDefaultRegistry

    __all__ = ["OrderCreated", "OrderShipped", "PaymentReceived", "TestEventRegistryBasicOperations", "TestEventRegistryLookup", "TestEventTypeResolution", "TestDuplicateRegistration", "TestRegistryClearAndUnregister", "TestDecoratorRegistration", "TestRegistryIsolation", "TestThreadSafety", "TestRegistryIteration", "TestErrorMessages", "TestEventDeserialization", "TestVersionedEventSchemas", "EventWithoutTypeDefault", "TestDefaultRegistry"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
