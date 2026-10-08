---
id: REFACTOR-examples-projection_example
title: Refactor and Decompose Legacy File projection_example.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-examples-projection_example: Refactor Legacy File projection_example.py

## Summary
The grandfathered debt file `examples/projection_example.py` contains 595 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (projection_example_order.py, projection_example_stats.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `examples/projection_example/` with submodules:
- `projection_example_order.py`: OrderPlaced, OrderShipped, OrderDelivered, OrderCancelled, OrderState, OrderAggregate, OrderListProjection, DailyRevenueProjection, main
- `projection_example_stats.py`: CustomerStatsProjection

## AST Decomposition Blueprint
Decomposition Blueprint for examples/projection_example.py (595 lines):
  Submodule 'projection_example_order.py' (~441 lines):
    - [class] OrderPlaced (lines 51-59)
    - [class] OrderShipped (lines 63-69)
    - [class] OrderDelivered (lines 73-78)
    - [class] OrderCancelled (lines 82-87)
    - [class] OrderState (lines 95-105)
    - [class] OrderAggregate (lines 108-185)
    - [class] OrderListProjection (lines 196-277)
    - [class] DailyRevenueProjection (lines 343-381)
    - [function] main (lines 389-591)
  Submodule 'projection_example_stats.py' (~61 lines):
    - [class] CustomerStatsProjection (lines 280-340)
  Suggested barrel exports:
    from .projection_example_order import OrderPlaced, OrderShipped, OrderDelivered, OrderCancelled, OrderState, OrderAggregate, OrderListProjection, DailyRevenueProjection, main
    from .projection_example_stats import CustomerStatsProjection

    __all__ = ["OrderPlaced", "OrderShipped", "OrderDelivered", "OrderCancelled", "OrderState", "OrderAggregate", "OrderListProjection", "DailyRevenueProjection", "main", "CustomerStatsProjection"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
