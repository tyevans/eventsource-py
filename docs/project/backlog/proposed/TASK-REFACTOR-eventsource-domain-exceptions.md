---
id: REFACTOR-eventsource-domain-exceptions
title: Refactor and Decompose Legacy File exceptions.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-domain-exceptions: Refactor Legacy File exceptions.py

## Summary
The grandfathered debt file `src/eventsource/domain/exceptions.py` contains 834 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (exceptions_event.py, exceptions_not.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/domain/exceptions/` with submodules:
- `exceptions_event.py`: EventSourceError, EventNotFoundError, EventStoreError, EventBusError, EventVersionError, UnhandledEventError, DuplicateEventError, EventTypeNotFoundError, DuplicateEventTypeError, OptimisticLockError, ProjectionError, CommandRejectedError, SerializationError, AggregateTypeMismatchError, AggregateIdMismatchError, HandlerDispatchError, DuplicateHandlerError, SnapshotError, SnapshotDeserializationError, SnapshotSchemaVersionError, HandlerSignatureError, TenantContextResetError, TenantMismatchError
- `exceptions_not.py`: AggregateNotFoundError, AggregateNotCreatedError, AggregateTypeNotSetError, SnapshotNotFoundError, TenantContextNotSetError

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/domain/exceptions.py (834 lines):
  Submodule 'exceptions_event.py' (~632 lines):
    - [class] EventSourceError (lines 12-15)
    - [class] EventNotFoundError (lines 49-54)
    - [class] EventStoreError (lines 94-97)
    - [class] EventBusError (lines 100-103)
    - [class] EventVersionError (lines 114-148)
    - [class] UnhandledEventError (lines 151-189)
    - [class] DuplicateEventError (lines 373-374)
    - [class] EventTypeNotFoundError (lines 607-622)
    - [class] DuplicateEventTypeError (lines 625-642)
    - [class] OptimisticLockError (lines 18-46)
    - [class] ProjectionError (lines 57-63)
    - [class] CommandRejectedError (lines 76-91)
    - [class] SerializationError (lines 106-111)
    - [class] AggregateTypeMismatchError (lines 220-254)
    - [class] AggregateIdMismatchError (lines 257-301)
    - [class] HandlerDispatchError (lines 323-344)
    - [class] DuplicateHandlerError (lines 347-370)
    - [class] SnapshotError (lines 377-392)
    - [class] SnapshotDeserializationError (lines 395-475)
    - [class] SnapshotSchemaVersionError (lines 478-547)
    - [class] HandlerSignatureError (lines 645-707)
    - [class] TenantContextResetError (lines 743-786)
    - [class] TenantMismatchError (lines 789-834)
  Submodule 'exceptions_not.py' (~124 lines):
    - [class] AggregateNotFoundError (lines 66-73)
    - [class] AggregateNotCreatedError (lines 192-217)
    - [class] AggregateTypeNotSetError (lines 304-320)
    - [class] SnapshotNotFoundError (lines 550-596)
    - [class] TenantContextNotSetError (lines 715-740)
  Suggested barrel exports:
    from .exceptions_event import EventSourceError, EventNotFoundError, EventStoreError, EventBusError, EventVersionError, UnhandledEventError, DuplicateEventError, EventTypeNotFoundError, DuplicateEventTypeError, OptimisticLockError, ProjectionError, CommandRejectedError, SerializationError, AggregateTypeMismatchError, AggregateIdMismatchError, HandlerDispatchError, DuplicateHandlerError, SnapshotError, SnapshotDeserializationError, SnapshotSchemaVersionError, HandlerSignatureError, TenantContextResetError, TenantMismatchError
    from .exceptions_not import AggregateNotFoundError, AggregateNotCreatedError, AggregateTypeNotSetError, SnapshotNotFoundError, TenantContextNotSetError

    __all__ = ["EventSourceError", "EventNotFoundError", "EventStoreError", "EventBusError", "EventVersionError", "UnhandledEventError", "DuplicateEventError", "EventTypeNotFoundError", "DuplicateEventTypeError", "OptimisticLockError", "ProjectionError", "CommandRejectedError", "SerializationError", "AggregateTypeMismatchError", "AggregateIdMismatchError", "HandlerDispatchError", "DuplicateHandlerError", "SnapshotError", "SnapshotDeserializationError", "SnapshotSchemaVersionError", "HandlerSignatureError", "TenantContextResetError", "TenantMismatchError", "AggregateNotFoundError", "AggregateNotCreatedError", "AggregateTypeNotSetError", "SnapshotNotFoundError", "TenantContextNotSetError"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
