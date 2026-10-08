---
id: REFACTOR-tests-unit-application-projections-test_tenant_filter
title: Refactor and Decompose Legacy File test_tenant_filter.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-projections-test_tenant_filter: Refactor Legacy File test_tenant_filter.py

## Summary
The grandfathered debt file `tests/unit/application/projections/test_tenant_filter.py` contains 603 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_tenant_filter_event.py, test_tenant_filter_order.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/projections/test_tenant_filter/` with submodules:
- `test_tenant_filter_event.py`: TenantEvent, NonTenantEvent, tenant_event_1, tenant_event_2, non_tenant_event, TestShouldProcessEvent, SampleProjection, tenant_id_1, tenant_id_2, TestStaticTenantFilter, TestCallableTenantFilter, TestNoFilter, TestGetTenantFilterValue, TestTenantFilterLogging, TestEdgeCases, TestTenantFilterTypeAlias, TestBackwardCompatibility
- `test_tenant_filter_order.py`: OrderCreated, OrderShipped, order_created_tenant_1, order_created_tenant_2, order_created_no_tenant

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/projections/test_tenant_filter.py (603 lines):
  Submodule 'test_tenant_filter_event.py' (~476 lines):
    - [class] TenantEvent (lines 26-30)
    - [class] NonTenantEvent (lines 33-36)
    - [function] tenant_event_1 (lines 96-98)
    - [function] tenant_event_2 (lines 102-104)
    - [function] non_tenant_event (lines 108-110)
    - [class] TestShouldProcessEvent (lines 341-391)
    - [class] SampleProjection (lines 56-77)
    - [function] tenant_id_1 (lines 84-86)
    - [function] tenant_id_2 (lines 90-92)
    - [class] TestStaticTenantFilter (lines 134-192)
    - [class] TestCallableTenantFilter (lines 198-294)
    - [class] TestNoFilter (lines 300-335)
    - [class] TestGetTenantFilterValue (lines 397-441)
    - [class] TestTenantFilterLogging (lines 447-466)
    - [class] TestEdgeCases (lines 472-522)
    - [class] TestTenantFilterTypeAlias (lines 528-561)
    - [class] TestBackwardCompatibility (lines 567-603)
  Submodule 'test_tenant_filter_order.py' (~19 lines):
    - [class] OrderCreated (lines 39-43)
    - [class] OrderShipped (lines 46-50)
    - [function] order_created_tenant_1 (lines 114-116)
    - [function] order_created_tenant_2 (lines 120-122)
    - [function] order_created_no_tenant (lines 126-128)
  Suggested barrel exports:
    from .test_tenant_filter_event import TenantEvent, NonTenantEvent, tenant_event_1, tenant_event_2, non_tenant_event, TestShouldProcessEvent, SampleProjection, tenant_id_1, tenant_id_2, TestStaticTenantFilter, TestCallableTenantFilter, TestNoFilter, TestGetTenantFilterValue, TestTenantFilterLogging, TestEdgeCases, TestTenantFilterTypeAlias, TestBackwardCompatibility
    from .test_tenant_filter_order import OrderCreated, OrderShipped, order_created_tenant_1, order_created_tenant_2, order_created_no_tenant

    __all__ = ["TenantEvent", "NonTenantEvent", "tenant_event_1", "tenant_event_2", "non_tenant_event", "TestShouldProcessEvent", "SampleProjection", "tenant_id_1", "tenant_id_2", "TestStaticTenantFilter", "TestCallableTenantFilter", "TestNoFilter", "TestGetTenantFilterValue", "TestTenantFilterLogging", "TestEdgeCases", "TestTenantFilterTypeAlias", "TestBackwardCompatibility", "OrderCreated", "OrderShipped", "order_created_tenant_1", "order_created_tenant_2", "order_created_no_tenant"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
