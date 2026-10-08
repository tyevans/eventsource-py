---
id: REFACTOR-tests-unit-domain-test_aggregate_snapshot_methods
title: Refactor and Decompose Legacy File test_aggregate_snapshot_methods.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-domain-test_aggregate_snapshot_methods: Refactor Legacy File test_aggregate_snapshot_methods.py

## Summary
The grandfathered debt file `tests/unit/domain/test_aggregate_snapshot_methods.py` contains 854 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_aggregate_snapshot_methods_order.py, test_aggregate_snapshot_methods_state.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/domain/test_aggregate_snapshot_methods/` with submodules:
- `test_aggregate_snapshot_methods_order.py`: OrderItem, OrderState, OrderCreated, OrderAggregate, DeclarativeOrderAggregate, ItemAdded, StatusChanged, ValueSet, SimpleAggregate, VersionedAggregate, TestSchemaVersion, TestRestoreFromSnapshot, TestRoundTrip, TestRestoreThenReplay, TestEdgeCases
- `test_aggregate_snapshot_methods_state.py`: SimpleState, TestSerializeState, TestGetStateType

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/domain/test_aggregate_snapshot_methods.py (854 lines):
  Submodule 'test_aggregate_snapshot_methods_order.py' (~553 lines):
    - [class] OrderItem (lines 30-35)
    - [class] OrderState (lines 38-47)
    - [class] OrderCreated (lines 62-66)
    - [class] OrderAggregate (lines 97-131)
    - [class] DeclarativeOrderAggregate (lines 167-182)
    - [class] ItemAdded (lines 69-75)
    - [class] StatusChanged (lines 78-82)
    - [class] ValueSet (lines 85-89)
    - [class] SimpleAggregate (lines 134-151)
    - [class] VersionedAggregate (lines 154-164)
    - [class] TestSchemaVersion (lines 190-231)
    - [class] TestRestoreFromSnapshot (lines 372-471)
    - [class] TestRoundTrip (lines 548-687)
    - [class] TestRestoreThenReplay (lines 695-766)
    - [class] TestEdgeCases (lines 774-854)
  Submodule 'test_aggregate_snapshot_methods_state.py' (~193 lines):
    - [class] SimpleState (lines 50-54)
    - [class] TestSerializeState (lines 239-364)
    - [class] TestGetStateType (lines 479-540)
  Suggested barrel exports:
    from .test_aggregate_snapshot_methods_order import OrderItem, OrderState, OrderCreated, OrderAggregate, DeclarativeOrderAggregate, ItemAdded, StatusChanged, ValueSet, SimpleAggregate, VersionedAggregate, TestSchemaVersion, TestRestoreFromSnapshot, TestRoundTrip, TestRestoreThenReplay, TestEdgeCases
    from .test_aggregate_snapshot_methods_state import SimpleState, TestSerializeState, TestGetStateType

    __all__ = ["OrderItem", "OrderState", "OrderCreated", "OrderAggregate", "DeclarativeOrderAggregate", "ItemAdded", "StatusChanged", "ValueSet", "SimpleAggregate", "VersionedAggregate", "TestSchemaVersion", "TestRestoreFromSnapshot", "TestRoundTrip", "TestRestoreThenReplay", "TestEdgeCases", "SimpleState", "TestSerializeState", "TestGetStateType"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
