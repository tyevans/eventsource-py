---
id: REFACTOR-eventsource-application-migration-router
title: Refactor and Decompose Legacy File router.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-router: Refactor Legacy File router.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/router.py` contains 734 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (router_not.py, router_tenant.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/router/` with submodules:
- `router_not.py`: StoreNotFoundError
- `router_tenant.py`: TenantStoreRouter

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/application/migration/router.py (734 lines):
  Submodule 'router_not.py' (~14 lines):
    - [class] StoreNotFoundError (lines 81-94)
  Submodule 'router_tenant.py' (~629 lines):
    - [class] TenantStoreRouter (lines 97-725)
  Suggested barrel exports:
    from .router_not import StoreNotFoundError
    from .router_tenant import TenantStoreRouter

    __all__ = ["StoreNotFoundError", "TenantStoreRouter"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
