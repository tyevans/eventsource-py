---
id: REFACTOR-eventsource-application-migration-exceptions
title: Refactor and Decompose Legacy File exceptions.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-exceptions: Refactor Legacy File exceptions.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/exceptions.py` contains 671 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (exceptions_migration.py, exceptions_cutover.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/exceptions/` with submodules:
- `exceptions_migration.py`: MigrationError, MigrationNotFoundError, MigrationAlreadyExistsError, MigrationStateError, InvalidPhaseTransitionError, ConsistencyError, BulkCopyError, DualWriteError, PositionMappingError, CircuitBreakerOpenError
- `exceptions_cutover.py`: CutoverError, CutoverTimeoutError, CutoverLagError

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/application/migration/exceptions.py (671 lines):
  Submodule 'exceptions_migration.py' (~479 lines):
    - [class] MigrationError (lines 34-166)
    - [class] MigrationNotFoundError (lines 169-195)
    - [class] MigrationAlreadyExistsError (lines 198-232)
    - [class] MigrationStateError (lines 235-271)
    - [class] InvalidPhaseTransitionError (lines 274-308)
    - [class] ConsistencyError (lines 447-492)
    - [class] BulkCopyError (lines 495-535)
    - [class] DualWriteError (lines 538-574)
    - [class] PositionMappingError (lines 577-617)
    - [class] CircuitBreakerOpenError (lines 625-671)
  Submodule 'exceptions_cutover.py' (~130 lines):
    - [class] CutoverError (lines 311-347)
    - [class] CutoverTimeoutError (lines 350-387)
    - [class] CutoverLagError (lines 390-444)
  Suggested barrel exports:
    from .exceptions_migration import MigrationError, MigrationNotFoundError, MigrationAlreadyExistsError, MigrationStateError, InvalidPhaseTransitionError, ConsistencyError, BulkCopyError, DualWriteError, PositionMappingError, CircuitBreakerOpenError
    from .exceptions_cutover import CutoverError, CutoverTimeoutError, CutoverLagError

    __all__ = ["MigrationError", "MigrationNotFoundError", "MigrationAlreadyExistsError", "MigrationStateError", "InvalidPhaseTransitionError", "ConsistencyError", "BulkCopyError", "DualWriteError", "PositionMappingError", "CircuitBreakerOpenError", "CutoverError", "CutoverTimeoutError", "CutoverLagError"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
