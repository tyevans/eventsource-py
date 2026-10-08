---
id: REFACTOR-tests-unit-domain-test_domain_event
title: Refactor and Decompose Legacy File test_domain_event.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-domain-test_domain_event: Refactor Legacy File test_domain_event.py

## Summary
The grandfathered debt file `tests/unit/domain/test_domain_event.py` contains 908 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_domain_event_order.py, test_domain_event_aggregate.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/domain/test_domain_event/` with submodules:
- `test_domain_event_order.py`: OrderCreated, OrderShipped, TestDomainEventCreation, TestDomainEventImmutability, TestDomainEventSerialization, TestDomainEventCausation, TestDomainEventMetadata, TestDomainEventStringRepresentation, TestDomainEventValidation, TestDomainEventSubclassing, TestDomainEventMultiTenancy, TestExtraForbid, TestDomainEventEquality, TestTypesVocabulary
- `test_domain_event_aggregate.py`: TestDomainEventAggregateVersion, TestAggregateTypePattern

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/domain/test_domain_event.py (908 lines):
  Submodule 'test_domain_event_order.py' (~801 lines):
    - [class] OrderCreated (lines 25-33)
    - [class] OrderShipped (lines 36-42)
    - [class] TestDomainEventCreation (lines 45-173)
    - [class] TestDomainEventImmutability (lines 176-215)
    - [class] TestDomainEventSerialization (lines 218-329)
    - [class] TestDomainEventCausation (lines 332-473)
    - [class] TestDomainEventMetadata (lines 476-534)
    - [class] TestDomainEventStringRepresentation (lines 568-612)
    - [class] TestDomainEventValidation (lines 615-670)
    - [class] TestDomainEventSubclassing (lines 673-737)
    - [class] TestDomainEventMultiTenancy (lines 740-783)
    - [class] TestExtraForbid (lines 786-797)
    - [class] TestDomainEventEquality (lines 800-866)
    - [class] TestTypesVocabulary (lines 895-908)
  Submodule 'test_domain_event_aggregate.py' (~53 lines):
    - [class] TestDomainEventAggregateVersion (lines 537-565)
    - [class] TestAggregateTypePattern (lines 869-892)
  Suggested barrel exports:
    from .test_domain_event_order import OrderCreated, OrderShipped, TestDomainEventCreation, TestDomainEventImmutability, TestDomainEventSerialization, TestDomainEventCausation, TestDomainEventMetadata, TestDomainEventStringRepresentation, TestDomainEventValidation, TestDomainEventSubclassing, TestDomainEventMultiTenancy, TestExtraForbid, TestDomainEventEquality, TestTypesVocabulary
    from .test_domain_event_aggregate import TestDomainEventAggregateVersion, TestAggregateTypePattern

    __all__ = ["OrderCreated", "OrderShipped", "TestDomainEventCreation", "TestDomainEventImmutability", "TestDomainEventSerialization", "TestDomainEventCausation", "TestDomainEventMetadata", "TestDomainEventStringRepresentation", "TestDomainEventValidation", "TestDomainEventSubclassing", "TestDomainEventMultiTenancy", "TestExtraForbid", "TestDomainEventEquality", "TestTypesVocabulary", "TestDomainEventAggregateVersion", "TestAggregateTypePattern"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
