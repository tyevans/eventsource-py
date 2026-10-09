---
id: REFACTOR-examples-subscriptions-multi_subscriber
title: Refactor and Decompose Legacy File multi_subscriber.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-examples-subscriptions-multi_subscriber: Refactor Legacy File multi_subscriber.py

## Summary
The grandfathered debt file `examples/subscriptions/multi_subscriber.py` contains 760 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (multi_subscriber_product.py, multi_subscriber_inventory.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `examples/subscriptions/multi_subscriber/` with submodules:
- `multi_subscriber_product.py`: ProductCreated, ProductPriceChanged, ProductState, ProductAggregate, ProductCatalogProjection, SalesAnalyticsProjection, main
- `multi_subscriber_inventory.py`: InventoryAdded, InventoryReserved, InventorySold, InventoryDashboardProjection

## AST Decomposition Blueprint
Decomposition Blueprint for examples/subscriptions/multi_subscriber.py (760 lines):
  Submodule 'multi_subscriber_product.py' (~545 lines):
    - [class] ProductCreated (lines 58-65)
    - [class] ProductPriceChanged (lines 69-75)
    - [class] ProductState (lines 113-120)
    - [class] ProductAggregate (lines 123-218)
    - [class] ProductCatalogProjection (lines 226-280)
    - [class] SalesAnalyticsProjection (lines 379-466)
    - [function] main (lines 474-756)
  Submodule 'multi_subscriber_inventory.py' (~105 lines):
    - [class] InventoryAdded (lines 79-85)
    - [class] InventoryReserved (lines 89-95)
    - [class] InventorySold (lines 99-105)
    - [class] InventoryDashboardProjection (lines 288-371)
  Suggested barrel exports:
    from .multi_subscriber_product import ProductCreated, ProductPriceChanged, ProductState, ProductAggregate, ProductCatalogProjection, SalesAnalyticsProjection, main
    from .multi_subscriber_inventory import InventoryAdded, InventoryReserved, InventorySold, InventoryDashboardProjection

    __all__ = ["ProductCreated", "ProductPriceChanged", "ProductState", "ProductAggregate", "ProductCatalogProjection", "SalesAnalyticsProjection", "main", "InventoryAdded", "InventoryReserved", "InventorySold", "InventoryDashboardProjection"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
